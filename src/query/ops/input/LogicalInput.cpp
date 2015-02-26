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
 * @file LogicalInput.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * Input operator for inputing data from external files into array
 */
#include <log4cxx/logger.h>

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "system/SystemCatalog.h"
#include "system/Cluster.h"
#include "system/Resources.h"

using namespace std;
using namespace boost;

static log4cxx::LoggerPtr oplogger(log4cxx::Logger::getLogger("scidb.ops.input"));

namespace scidb
{

/**
 * Must be called as INPUT('existing_array_name', '/path/to/file/on/node')
 */
class LogicalInput: public LogicalOperator
{
public:
    LogicalInput(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_IN_ARRAY_NAME();   //0
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

        int64_t nodeID = -2;
        if (_parameters.size() == 3)
        {
            nodeID = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[2])->getExpression(),
                                      query, TID_INT64).getInt64();
            if (nodeID < -2 ||   nodeID >= (int64_t) query->getNodesCount())
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_INVALID_NODE_ID,
                                           _parameters[2]->getParsingContext()) << nodeID;
        }

        const string &path = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[1])->getExpression(),
                                      query, TID_STRING).getString();

        if (nodeID == -1)
        {
            //Distributed loading let's check file existence on all nodes
            map<NodeID, bool> nodesMap;
            Resources::getInstance()->fileExists(path, nodesMap, query);

            bool fileDetected = false;
            vector<NodeID> nodesWithoutFile;
            for (map<NodeID, bool>::const_iterator it = nodesMap.begin(); it != nodesMap.end(); ++it)
            {
                if (it->second)
                {
                    if (!fileDetected)
                        fileDetected = true;
                }
                else
                {
                    //Remembering file name on each missing file
                    LOG4CXX_WARN(oplogger, "File '" << path << "' not found on node #" << it->first);
                    nodesWithoutFile.push_back(it->first);
                }
            }

            //Such file not found on any node. Failing with exception
            if (!fileDetected)
            {
                throw USER_QUERY_EXCEPTION(
                    SCIDB_SE_INFER_SCHEMA, SCIDB_LE_FILE_NOT_FOUND,
                    _parameters[1]->getParsingContext()) << path;
            }

            //If some nodes missing this file posting appropriate warning
            if (nodesWithoutFile.size())
            {
                stringstream nodesList;
                for (size_t i = 0, count = nodesWithoutFile.size();  i < count; ++i)
                {
                    nodesList << nodesWithoutFile[i] << (i == count - 1 ? "" : ", ");
                }
                LOG4CXX_WARN(oplogger, "File " << path << " not found on nodes " << nodesList.str());
                query->postWarning(SCIDB_WARNING(SCIDB_W_FILE_NOT_FOUND_ON_NODES) << path << nodesList.str());
            }
        }
        else if (nodeID == -2)
        {
            //This is loading from local node. Throw error if file not found.
            if (!Resources::getInstance()->fileExists(path, query->getNodeID(), query))
            {
                throw USER_QUERY_EXCEPTION(
                    SCIDB_SE_INFER_SCHEMA, SCIDB_LE_FILE_NOT_FOUND,
                    _parameters[1]->getParsingContext()) << path;
            }
        }
        else
        {
            //This is loading from single node. Throw error if file not found.
            if (!Resources::getInstance()->fileExists(path, nodeID, query))
            {
                throw USER_QUERY_EXCEPTION(
                    SCIDB_SE_INFER_SCHEMA, SCIDB_LE_FILE_NOT_FOUND,
                    _parameters[1]->getParsingContext()) << path;
            }
        }

        const string& arrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        ArrayDesc arrayDesc;

        SystemCatalog::getInstance()->getArrayDesc(arrayName, arrayDesc);

        return arrayDesc;
    }

};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalInput, "input")


} //namespace
