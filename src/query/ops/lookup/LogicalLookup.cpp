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
 * LogicalLookup.cpp
 *
 *  Created on: Jul 26, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "system/Exceptions.h"

namespace scidb {

using namespace std;

class LogicalLookup: public  LogicalOperator
{
public:
	LogicalLookup(const std::string& logicalName, const std::string& alias):
	    LogicalOperator(logicalName, alias)
	{
	    // Lookup operator has two input arrays
        ADD_PARAM_INPUT()
        ADD_PARAM_INPUT()
	}

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 2);
        return ArrayDesc("lookup",  schemas[1].getAttributes(), schemas[0].getDimensions());
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalLookup, "lookup")


}  // namespace scidb
