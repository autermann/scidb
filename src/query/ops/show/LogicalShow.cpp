/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2011 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation version 3 of the License, or
* (at your option, const std::string& alias) any later version.
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
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief Shows object. E.g. schema of array.
 */

#include "query/Operator.h"
#include "query/OperatorLibrary.h"

using namespace std;
using namespace boost;

namespace scidb
{

/**
 * @brief The operator: show().
 *
 * @par Synopsis:
 *   show( schemaArray | schema )
 *
 * @par Summary:
 *   Shows the schema of an array.
 *
 * @par Input:
 *   - schemaArray | schema: an array where the schema is used, or the schema itself.
 *
 * @par Output array:
 *        <
 *   <br>   schema: string
 *   <br> >
 *   <br> [
 *   <br>   i: start=end=0, chunk interval=1
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
class LogicalShow: public LogicalOperator
{
public:
	LogicalShow(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_SCHEMA();
    	_usage = "show(<array name | anonymous schema>)";
    }

    ArrayDesc inferSchema(vector<ArrayDesc> inputSchemas, shared_ptr<Query> query)
    {
        assert(inputSchemas.size() == 0);
        assert(_parameters.size() == 1);

		Attributes atts(1);
		atts[0] = AttributeDesc((AttributeID)0, "schema",  TID_STRING, 0, 0 );

		Dimensions dims(1);
		dims[0] = DimensionDesc("i", 0, 0, 0, 0, 1, 0);

		return ArrayDesc("", atts, dims);
    }

};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalShow, "show")


} //namespace
