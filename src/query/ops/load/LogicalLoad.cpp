/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2012 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation version 3 of the License.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the GNU General Public License for the complete license terms.
*
* You should have received a copy of the GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/>.
*
* END_COPYRIGHT
*/

/*
 * @file LogicalLoad.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * Load operator for loading data from external files into array
 */
#include <log4cxx/logger.h>

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "system/SystemCatalog.h"
#include "system/Cluster.h"
#include "system/Resources.h"
#include "system/Warnings.h"

using namespace std;
using namespace boost;

static log4cxx::LoggerPtr oplogger(log4cxx::Logger::getLogger("scidb.ops.load"));

namespace scidb
{

/**
 * Must be called as LOAD('existing_array_name', '/path/to/file/on/instance')
 */
class LogicalLoad: public LogicalOperator
{
public:
    LogicalLoad(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_IN_ARRAY_NAME();       //0
                ADD_PARAM_CONSTANT("string");//1
                ADD_PARAM_VARIES();          //2
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        switch (_parameters.size()) {
          case 0:
          case 1:
            assert(false);
            break;
          case 2:
            res.push_back(PARAM_CONSTANT("int64"));
            break;
        }
        return res;
    }


    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, boost::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 0);

        const string &path = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[1])->getExpression(),
                                      query, TID_STRING).getString();

        int64_t instanceID = -2;
        if (_parameters.size() == 3)
        {
            instanceID = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[2])->getExpression(),
                                     query, TID_INT64).getInt64();
            if (instanceID < -2 || instanceID >= (int64_t) query->getInstancesCount())
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_INVALID_INSTANCE_ID,
                    _parameters[2]->getParsingContext()) << instanceID;
        }

        if (instanceID == -1)
        {
            //Distributed loading let's check file existence on all instances
            map<InstanceID, bool> instancesMap;
            Resources::getInstance()->fileExists(path, instancesMap, query);

            bool fileDetected = false;
            vector<InstanceID> instancesWithoutFile;
            for (map<InstanceID, bool>::const_iterator it = instancesMap.begin(); it != instancesMap.end(); ++it)
            {
                if (it->second)
                {
                    if (!fileDetected)
                        fileDetected = true;
                }
                else
                {
                    //Remembering file name on each missing file
                    LOG4CXX_WARN(oplogger, "File '" << path << "' not found on instance #" << it->first);
                    instancesWithoutFile.push_back(it->first);
                }
            }

            //Such file not found on any instance. Failing with exception
            if (!fileDetected)
            {
                throw USER_QUERY_EXCEPTION(
                    SCIDB_SE_QPROC, SCIDB_LE_FILE_NOT_FOUND,
                    _parameters[1]->getParsingContext()) << path;
            }

            //If some instances missing this file posting appropriate warning
            if (instancesWithoutFile.size())
            {
                stringstream instancesList;
                for (size_t i = 0, count = instancesWithoutFile.size();  i < count; ++i)
                {
                    instancesList << instancesWithoutFile[i] << (i == count - 1 ? "" : ", ");
                }
                LOG4CXX_WARN(oplogger, "File " << path << " not found on instances " << instancesList.str());
                query->postWarning(SCIDB_WARNING(SCIDB_W_FILE_NOT_FOUND_ON_INSTANCES) << path << instancesList.str());
            }
        }
        else if (instanceID == -2)
        {
            //This is loading from local instance. Throw error if file not found.
            if (!Resources::getInstance()->fileExists(path, query->getInstanceID(), query))
            {
                throw USER_QUERY_EXCEPTION(
                    SCIDB_SE_QPROC, SCIDB_LE_FILE_NOT_FOUND,
                    _parameters[1]->getParsingContext()) << path;
            }
        }
        else
        {
            //This is loading from single instance. Throw error if file not found.
            if (!Resources::getInstance()->fileExists(path, instanceID, query))
            {
                throw USER_QUERY_EXCEPTION(
                    SCIDB_SE_QPROC, SCIDB_LE_FILE_NOT_FOUND,
                    _parameters[1]->getParsingContext()) << path;
            }
        }

        const string& arrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        ArrayDesc arrayDesc;

        SystemCatalog::getInstance()->getArrayDesc(arrayName, arrayDesc);

        return arrayDesc;
    }

    void inferArrayAccess(boost::shared_ptr<Query>& query)
    {
        LogicalOperator::inferArrayAccess(query);
        assert(_parameters.size() > 0);
        assert(_parameters[0]->getParamType() == PARAM_ARRAY_REF);
        const string& arrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        assert(arrayName.find('@') == std::string::npos);
        boost::shared_ptr<SystemCatalog::LockDesc>  lock(new SystemCatalog::LockDesc(arrayName,
                                                                                     query->getQueryID(),
                                                                                     Cluster::getInstance()->getLocalInstanceId(),
                                                                                     SystemCatalog::LockDesc::COORD,
                                                                                     SystemCatalog::LockDesc::WR));
        boost::shared_ptr<SystemCatalog::LockDesc> resLock = query->requestLock(lock);
        assert(resLock);
        assert(resLock->getLockMode() >= SystemCatalog::LockDesc::WR);
    }
};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalLoad, "load")


} //namespace
