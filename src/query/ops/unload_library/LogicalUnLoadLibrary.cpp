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
 * @file LogicalUnLoadLibrary.cpp
 *
 * @brief Logical DDL operator which unloads user defined library
 *
 * @author roman.simakov@gmail.com
 */

#include "query/Operator.h"
#include "system/Exceptions.h"


namespace scidb
{

class LogicalUnLoadLibrary: public LogicalOperator
{
public:
    LogicalUnLoadLibrary(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_CONSTANT("string")
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, boost::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 0);
        //FIXME: Need parameters to infer the schema correctly
        ArrayDesc schema;
        return schema;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalUnLoadLibrary, "unload_library")

} //namespace
