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
 * @file LogicalDimensions.cpp
 *
 * @author knizhnik@garret.ru
 *
 * Dimensions operator for dimensioning data from external files into array
 */

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "array/Metadata.h"
#include "system/SystemCatalog.h"

namespace scidb
{

using namespace std;
using namespace boost;

/**
 * @brief The operator: dimensions().
 *
 * @par Synopsis:
 *   dimensions( srcArray )
 *
 * @par Summary:
 *   List the dimensions of the source array.
 *
 * @par Input:
 *   - srcArray: a source array.
 *
 * @par Output array:
 *        <
 *   <br>   name: string
 *   <br>   start: int64,
 *   <br>   length: uint64
 *   <br>   chunk_interval: int32
 *   <br>   chunk_overlap: int32
 *   <br>   low: int64
 *   <br>   high: int64
 *   <br>   type: string
 *   <br> >
 *   <br> [
 *   <br>   No: start=0, end=#dimensions less 1, chunk interval=#dimensions.
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
class LogicalDimensions: public LogicalOperator
{
public:
    LogicalDimensions(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_IN_ARRAY_NAME()
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, boost::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 0);
        assert(_parameters.size() == 1);

        const string &arrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        ArrayDesc arrayDesc;
        SystemCatalog::getInstance()->getArrayDesc(arrayName, LAST_VERSION, arrayDesc);

        size_t nAttrs = arrayDesc.getDimensions().size();
        vector<AttributeDesc> attributes(8);
        attributes[0] = AttributeDesc((AttributeID)0, "name",  TID_STRING, 0, 0);
        attributes[1] = AttributeDesc((AttributeID)1, "start",  TID_INT64, 0, 0);
        attributes[2] = AttributeDesc((AttributeID)2, "length",  TID_UINT64, 0, 0);
        attributes[3] = AttributeDesc((AttributeID)3, "chunk_interval",  TID_INT32, 0, 0);
        attributes[4] = AttributeDesc((AttributeID)4, "chunk_overlap",  TID_INT32, 0, 0);
        attributes[5] = AttributeDesc((AttributeID)5, "low",  TID_INT64, 0, 0);
        attributes[6] = AttributeDesc((AttributeID)6, "high",  TID_INT64, 0, 0);
        attributes[7] = AttributeDesc((AttributeID)7, "type",  TID_STRING, 0, 0);
        vector<DimensionDesc> dimensions(1);
        dimensions[0] = DimensionDesc("No", 0, 0, nAttrs-1, nAttrs-1, nAttrs, 0);
        return ArrayDesc("Dimensions", attributes, dimensions);
    }

};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalDimensions, "dimensions")


} //namespace
