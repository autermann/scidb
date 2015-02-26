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
 * \file LogicalCancel.cpp
 *
 * \author roman.simakov@gmail.com
 * \brief Cancel operator cancels query with given ID
 */

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "system/SystemCatalog.h"


using namespace std;

namespace scidb {

class LogicalCancel: public LogicalOperator
{
public:
    LogicalCancel(const string& logicalName, const std::string& alias):
	    LogicalOperator(logicalName, alias)
	{
        ADD_PARAM_CONSTANT("int64")
        _properties.ddl = true;
	}

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
	{
        // This operator does not produce any array
        return ArrayDesc();
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalCancel, "cancel")


}  // namespace scidb
