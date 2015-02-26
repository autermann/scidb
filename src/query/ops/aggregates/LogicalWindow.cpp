
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
 * LogicalWindow.cpp
 *
 *  Created on: Apr 11, 2010
 *      Author: Knizhnik, poliocough@gmail.com
 */

#include <boost/shared_ptr.hpp>

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "query/LogicalExpression.h"
#include "WindowArray.h"

namespace scidb
{

using namespace std;

/**
 * @brief The operator: window().
 *
 * @par Synopsis:
 *   window( srcArray {, leftEdge, rightEdge}+ {, AGGREGATE_CALL}+ )
 *   <br> AGGREGATE_CALL := AGGREGATE_FUNC(inputAttr) [as resultName]
 *   <br> AGGREGATE_FUNC := approxdc | avg | count | max | min | sum | stdev | var | some_use_defined_aggregate_function
 *
 * @par Summary:
 *   Produces a result array with the same dimensions as the source array, where each cell stores some aggregates calculated over
 *   a window covering the current cell. A pair of (leftEdge, rightEdge) must exist for every dimension.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - leftEdge: how many cells to the left of the current cell (in one dimension) are included in the window.
 *   - rightEdge: how many cells to the right of the current cell (in one dimension) are included in the window.
 *   - 1 or more aggregate calls.
 *     Each aggregate call has an AGGREGATE_FUNC, an inputAttr and a resultName.
 *     The default resultName is inputAttr followed by '_' and then AGGREGATE_FUNC.
 *     For instance, the default resultName for sum(sales) is 'sales_sum'.
 *     The count aggregate may take * as the input attribute, meaning to count all the items in the group including null items.
 *     The default resultName for count(*) is 'count'.
 *
 * @par Output array:
 *        <
 *   <br>   the aggregate calls' resultNames
 *   <br> >
 *   <br> [
 *   <br>   srcDims
 *   <br> ]
 *
 * @par Examples:
 *   - Given array A <quantity: uint64, sales:double> [year, item] =
 *     <br> year, item, quantity, sales
 *     <br> 2011,  2,      7,     31.64
 *     <br> 2011,  3,      6,     19.98
 *     <br> 2012,  1,      5,     41.65
 *     <br> 2012,  2,      9,     40.68
 *     <br> 2012,  3,      8,     26.64
 *   - window(A, 0, 0, 1, 0, sum(quantity)) <quantity_sum: uint64> [year, item] =
 *     <br> year, item, quantity_sum
 *     <br> 2011,  2,      7
 *     <br> 2011,  3,      13
 *     <br> 2012,  1,      5
 *     <br> 2012,  2,      14
 *     <br> 2012,  3,      17
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalWindow: public LogicalOperator
{
public:
    LogicalWindow(const std::string& logicalName, const std::string& alias):
            LogicalOperator(logicalName, alias)
        {
                ADD_PARAM_INPUT()
                ADD_PARAM_VARIES()
        }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector<ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        if (_parameters.size() < schemas[0].getDimensions().size() * 2)
        {
            res.push_back(PARAM_CONSTANT("int64"));
        }
        else if (_parameters.size() == schemas[0].getDimensions().size() * 2)
        {
            res.push_back(PARAM_AGGREGATE_CALL());
        }
        else
        {
            res.push_back(END_OF_VARIES_PARAMS());
            res.push_back(PARAM_AGGREGATE_CALL());
        }
        return res;
    }

    inline ArrayDesc createWindowDesc(ArrayDesc const& desc)
    {
        Dimensions const& dims = desc.getDimensions();
        Dimensions aggDims(dims.size());
        for (size_t i = 0, n = dims.size(); i < n; i++)
        {
            DimensionDesc const& srcDim = dims[i];
            aggDims[i] = DimensionDesc(srcDim.getBaseName(),
                                       srcDim.getNamesAndAliases(),
                                       srcDim.getStartMin(),
                                       srcDim.getCurrStart(),
                                       srcDim.getCurrEnd(),
                                       srcDim.getEndMax(),
                                       srcDim.getChunkInterval(), 
                                       0,
                                       srcDim.getType(),
                                       srcDim.getFlags(),
                                       srcDim.getMappingArrayName(),
                                       srcDim.getComment(),
                                       srcDim.getFuncMapOffset(),
                                       srcDim.getFuncMapScale());
        }

        ArrayDesc output (desc.getName(), Attributes(), aggDims);

        for (size_t i = dims.size() * 2, size = _parameters.size(); i < size; i++)
        {
            addAggregatedAttribute( (shared_ptr <OperatorParamAggregateCall> &) _parameters[i], desc, output);
        }

        if ( desc.getEmptyBitmapAttribute())
        {
            AttributeDesc const* eAtt = desc.getEmptyBitmapAttribute();
            output.addAttribute(AttributeDesc(output.getAttributes().size(), eAtt->getName(),
                eAtt->getType(), eAtt->getFlags(), eAtt->getDefaultCompressionMethod()));
        }

        return output;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
    {
        assert(schemas.size() == 1);

        ArrayDesc const& desc = schemas[0];
        size_t nDims = desc.getDimensions().size();
        vector<WindowBoundaries> window(nDims);
        size_t windowSize = 1;
        for (size_t i = 0, size = nDims * 2, boundaryNo = 0; i < size; i+=2, ++boundaryNo)
        {
            window[boundaryNo] = WindowBoundaries(
                evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[i])
                    ->getExpression(), query, TID_INT64).getInt64(),
                evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[i+1])
                    ->getExpression(), query, TID_INT64).getInt64()
                );
            windowSize *= window[boundaryNo]._boundaries.second + window[boundaryNo]._boundaries.first + 1;
            if (window[boundaryNo]._boundaries.first < 0)
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_WINDOW_ERROR3,
                    _parameters[i]->getParsingContext());
            if (window[boundaryNo]._boundaries.second < 0)
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_WINDOW_ERROR3,
                    _parameters[i + 1]->getParsingContext());
        }
        if (windowSize <= 1)
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_WINDOW_ERROR4,
                _parameters[0]->getParsingContext());
        return createWindowDesc(desc);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalWindow, "window")


}  // namespace ops
