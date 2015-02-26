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
 * LogicalSort.cpp
 *
 *  Created on: May 6, 2010
 *      Author: Knizhnik
 *      Author: polioocugh@gmail.com
 */

#include <query/Operator.h>
#include <system/Exceptions.h>

namespace scidb {

/**
 * @brief The operator: sort().
 *
 * @par Synopsis:
 *   sort( srcArray, attr = first attribute, defaultChunkSize = 1000000 )
 *
 * @par Summary:
 *   Produces a 1D array by sorting the non-empty cells of a source array.
 *
 * @par Input:
 *   - srcArray: the source array with srcAttrs and srcDim.
 *   - attr: the attribute to sort by. If not provided, the first attribute will be used.
 *   - defaultChunkSize: the default size of a chunk in the result array.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs: all the attributes are retained.
 *   <br> >
 *   <br> [
 *   <br>   n: start=0, end=MAX_COORDINATE, chunk interval = min{defaultChunkSize, #logical cells in srcArray)
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
class LogicalSort: public LogicalOperator
{
public:
	LogicalSort(const std::string& logicalName, const std::string& alias):
	    LogicalOperator(logicalName, alias)
	{
		ADD_PARAM_INPUT()
		ADD_PARAM_VARIES()

        _globalOperatorName = std::pair<std::string, std::string>("sort2", "physicalSort2");
	}

	std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
	{
		std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
		res.push_back(PARAM_IN_ATTRIBUTE_NAME("void"));
		res.push_back(PARAM_CONSTANT("int64"));
		res.push_back(END_OF_VARIES_PARAMS());
		return res;
	}

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
	{
        //Let's always call the output dimension "n". Because the input dimension no longer has any meaning!
        //It could've been "time", or "latitude", or "price" but after the sort it has no value.

        //Right now we always return an unbounded arary. You can use subarray to bound it if you need to
        //(but you should not need to very often!!). TODO: if we return bounded arrays, some logic inside
        //MergeSortArray gives bad results. We should fix this some day.

        //As far as chunk sizes, they can be a pain! So we allow the user to specify an optional chunk size
        //as part of the sort op.

        //If the user does not specify a chunk size, we'll use MIN( max_logical_size, 1 million).
        assert(schemas.size() >= 1);
        ArrayDesc const& schema = schemas[0];
        size_t chunkSize = 0;
        for(size_t i =0; i<_parameters.size(); i++)
        {
            if(_parameters[i]->getParamType()==PARAM_LOGICAL_EXPRESSION)
            {
                chunkSize = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[i])->getExpression(),
                                             query, TID_INT64).getInt64();
                if(chunkSize <= 0)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_CHUNK_SIZE_MUST_BE_POSITIVE);
                }
                break;
            }
        }

        size_t inputSchemaSize = schema.getSize();
        if (chunkSize == 0)
        {   //not set by user

            //1M is a good/recommended chunk size for most one-dimensional arrays -- unless you are using
            //large strings or UDTs
            chunkSize = 1000000;

            //If there's no way that the input has one million elements - reduce the chunk size further.
            //This is ONLY done for aesthetic purposes - don't want to see one million "()" on the screen.
            //In fact, sometimes it can become a liability...
            if(inputSchemaSize<chunkSize)
            {
                //make chunk size at least 1 to avoid errors
                chunkSize = std::max<size_t>(inputSchemaSize,1);
            }
        }

        Dimensions newDims(1);
        newDims[0] = DimensionDesc("n", 0, 0, MAX_COORDINATE, MAX_COORDINATE, chunkSize, 0);

        return ArrayDesc(schema.getName(), addEmptyTagAttribute(schema.getAttributes()), newDims);
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSort, "sort")


}  // namespace scidb
