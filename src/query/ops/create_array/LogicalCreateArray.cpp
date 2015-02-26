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

/**
 * @file
 *
 * @brief Logical DDL operator which create new array
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#include "query/Operator.h"

namespace scidb
{

/**
 * @brief The operator: create_array().
 *
 * @par Synopsis:
 *   create_array( arrayName, arraySchema )
 *
 * @par Summary:
 *   Creates an array with a given name and schema.
 *
 * @par Input:
 *   - arrayName: the array name
 *   - arraySchema: the array schema of attrs and dims
 *
 * @par Output array:
 *        <
 *   <br>   attrs
 *   <br> >
 *   <br> [
 *   <br>   dims
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalCreateArray: public LogicalOperator
{
public:
    LogicalCreateArray(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        _properties.ddl = true;
        ADD_PARAM_OUT_ARRAY_NAME()
        ADD_PARAM_SCHEMA()
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> inputSchemas, boost::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 0);
        assert(_parameters.size() == 2);
        assert(_parameters[0]->getParamType() == PARAM_ARRAY_REF);
        assert(_parameters[1]->getParamType() == PARAM_SCHEMA);

        const string &name = ((boost::shared_ptr<OperatorParamArrayReference>&)_parameters[0])->getObjectName();

        if (SystemCatalog::getInstance()->containsArray(name))
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAY_ALREADY_EXIST,
                _parameters[0]->getParsingContext()) << name;
        }

        return ArrayDesc();
    }

    void inferArrayAccess(boost::shared_ptr<Query>& query)
    {
        LogicalOperator::inferArrayAccess(query);
        assert(_parameters.size() > 0);
        assert(_parameters[0]->getParamType() == PARAM_ARRAY_REF);

        const string& arrayName = ((boost::shared_ptr<OperatorParamArrayReference>&)_parameters[0])->getObjectName();
        assert(!arrayName.empty());
        assert(arrayName.find('@') == std::string::npos);
        boost::shared_ptr<SystemCatalog::LockDesc> lock(new SystemCatalog::LockDesc(arrayName,
                                                                                    query->getQueryID(),
                                                                                    Cluster::getInstance()->getLocalInstanceId(),
                                                                                    SystemCatalog::LockDesc::COORD,
                                                                                    SystemCatalog::LockDesc::CRT));
        boost::shared_ptr<SystemCatalog::LockDesc> resLock = query->requestLock(lock);
        assert(resLock);
        assert(resLock->getLockMode() >= SystemCatalog::LockDesc::CRT);
    }

};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalCreateArray, "create_array")

} //namespace
