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
 * LogicalReverse.cpp
 *
 *  Created on: Aug 6, 2010
 *      Author: knizhnik@garret.ru
 */

#include "query/Operator.h"
#include "system/Exceptions.h"


namespace scidb {

class LogicalReverse: public  LogicalOperator
{
public:
    LogicalReverse(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
	{
        ADD_PARAM_INPUT()
	}

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
	{
        assert(schemas.size() == 1);
        ArrayDesc const& schema = schemas[0];
        Dimensions const& dims = schema.getDimensions();
        for (size_t i = 0; i < dims.size(); i++)
        {
            if (dims[i].getLength() == INFINITE_LENGTH) {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REVERSE_ERROR1);
            }
        }
        // TODO: Why do we point name of array? It can be wrong if such already used in query. No?
        return ArrayDesc("reverse", schema.getAttributes(), dims);
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalReverse, "reverse")


}  // namespace scidb
