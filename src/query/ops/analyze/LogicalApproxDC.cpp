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
 * LogicalApproxDC.cpp
 * 
 *  Created on: May 10, 2010
 *      Author: egor.pugin@gmail.com
 */

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "query/Aggregate.h"

using namespace std;

namespace scidb
{

/**
 * @brief The operator: approxdc().
 *
 * @par Synopsis:
 *   approxdc( srcArray [, attr] )
 *
 * @par Summary:
 *   Produces a result array containing approximate counts of the number of distinct values of an attribute.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - 0 or 1 attributes from srcAttrs. If no attribute is provided, the first attribute is used.
 *
 * @par Output array:
 *        <
 *   <br>   attr_adc: the source attribute name, followed by '_adc'.
 *   <br> >
 *   <br> [
 *   <br>   i: start=0, end=0, chunk interval=1.
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
 *   - approxdc(A, sales) <sales_adc: uint64> [i] =
 *     <br> i, sales_adc
 *     <br> 0,    5
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - When used inside aggregate(), groupby approxdc is possible; but when used standalone, no dimension may be provided.
 *   - The naming xxx_adc is inconsistent with the naming in aggregate(...approxdc()). The latter uses xxx_approxdc. Should change.
 *
 */
class LogicalApproxDC : public LogicalOperator
{
public:
    LogicalApproxDC(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
        ADD_PARAM_VARIES()
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        if (_parameters.size() == 0)
            res.push_back(PARAM_IN_ATTRIBUTE_NAME("void"));
        else
            res.push_back(PARAM_IN_DIMENSION_NAME());
        return res;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        ArrayDesc const& desc = schemas[0];
        Dimensions const& dims = desc.getDimensions();
        Attributes const& attrs = desc.getAttributes();
        AttributeID aid = 0;
        if (_parameters.size() >= 1)
        {
            aid = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectNo();
        }
        AggregatePtr a_dc = AggregateLibrary::getInstance()->createAggregate("ApproxDC", TypeLibrary::getType(attrs[aid].getType()));
        Attributes aggAttrs(1);
        aggAttrs[0] = AttributeDesc((AttributeID)0,
                                    attrs[aid].getName() + "_adc",
                                    a_dc->getResultType().typeId(),
                                    AttributeDesc::IS_NULLABLE,
                                    0);

        if (_parameters.size() <= 1) { 
            Dimensions aggDims(1);
            aggDims[0] = DimensionDesc("i", 0, 0, 0, 0, 1, 0);
            return ArrayDesc(desc.getName(), aggAttrs, aggDims);
        } else { 
            vector<int> groupBy(_parameters.size()-1);
            size_t i;
            for (i = 0; i < groupBy.size(); ++i)
            {
                groupBy[i] = ((boost::shared_ptr<OperatorParamReference>&)_parameters[i + 1])->getObjectNo();
            }
            Dimensions aggDims(groupBy.size());
            for (size_t i = 0, n = aggDims.size(); i < n; i++) { 
                DimensionDesc const& srcDim = dims[groupBy[i]]; 
                aggDims[i] = DimensionDesc(  srcDim.getBaseName(),
                                             srcDim.getStartMin(),
                                             srcDim.getCurrStart(),
                                             srcDim.getCurrEnd(),
                                             srcDim.getEndMax(),
                                             i == 0 && groupBy[i] == 0 ? srcDim.getChunkInterval() : srcDim.getCurrLength(),
                                             0,
                                             srcDim.getType(),
                                             srcDim.getFlags(),
                                             srcDim.getMappingArrayName(),
                                             srcDim.getComment(),
                                             srcDim.getFuncMapOffset(),
                                             srcDim.getFuncMapScale());
            }
            return ArrayDesc(desc.getName(), aggAttrs, aggDims);
        }
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalApproxDC, "ApproxDC")

} //namespace scidb

