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
#include "query/ops/input/LogicalInput.h"

using namespace std;
using namespace boost;

static log4cxx::LoggerPtr oplogger(log4cxx::Logger::getLogger("scidb.ops.load"));

namespace scidb
{

/**
 * @brief The operator: load().
 *
 * @par Synopsis:
 *   load( outputArray, filename, instance=-2, format="", maxErrors=0, shadowArray="" )
 *
 * @par Summary:
 *   Loads data to an existing outputArray from a given file, and optionally stores to shadowArray.
 *
 * @par Input:
 *   - outputArray: the output array to store data into.
 *   - filename: where to load data from.
 *   - instance: which instance; default is -2. ??
 *   - format: ??
 *   - maxErrors: ??
 *   - shadowArray: if provided, the result array will be written to it.
 *
 * @par Output array:
 *   n/a
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - Must be called as INPUT('existing_array_name', '/path/to/file/on/instance').
 *   - This really needs to be checked by the author.
 */
class LogicalLoad: public LogicalInput
{
  public:
    LogicalLoad(const std::string& logicalName, const std::string& alias)
    : LogicalInput(logicalName, alias)
    {
    }

    void inferArrayAccess(boost::shared_ptr<Query>& query)
    {
        LogicalInput::inferArrayAccess(query);
        assert(_parameters.size() > 0);
        assert(_parameters[0]->getParamType() == PARAM_SCHEMA);

        const string& arrayName = ((boost::shared_ptr<OperatorParamSchema>&)_parameters[0])
            ->getSchema().getName();
        if (!SystemCatalog::getInstance()->containsArray(arrayName))
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_ARRAY_DOESNT_EXIST,
                _parameters[0]->getParsingContext()) << arrayName;
        }

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
