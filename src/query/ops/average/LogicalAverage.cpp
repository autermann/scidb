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
 * LogicalAverage.cpp
 * Example query : average (scan ('array_to_scan'), 'attribute_to_aggregate' )
 *  Created on: Mar 18, 2010
 *      Author: hkimura
 *      TODO : so far, this class always returns double type.
 *      TODO : so far, this class doesn't accept group-by.
 *      TODO : count/avg/average/min/max could be refactored to share their codes. almost same, after all.
 */

#include "query/Operator.h"
#include "system/Exceptions.h"

using namespace std;

namespace scidb
{

/**
 * @brief The operator: avg().
 *
 * @par Synopsis:
 *   avg( srcArray, [attr {, groupbyDim}*] )
 *
 * @par Summary:
 *   Computes the average value of an attribute. If no attribute is given, the first attribute is used. If a list of groupbyDims are given, compute one
 *   average value per distinct group.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *
 * @par Output array:
 *        <
 *   <br>   attr_avg: the source attribute name, followed by "_avg"
 *   <br> >
 *   <br> [
 *   <br>   groupbyDims (if provided); or
 *   <br>   'i' (if no groupbyDim is given): start=end=0, chunk interval=1.
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
 *   - avg(A, sales, year) <sales_avg:double> [year] =
 *     <br> year, sales_avg
 *     <br> 2011,  25.81
 *     <br> 2012,  36.3233
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - same as aggregate(srcArray, avg(attr), groupbyDims)
 *
 */
class LogicalAverage : public  LogicalOperator
{
public:
	LogicalAverage(const std::string& logicalName, const std::string& alias):
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
        AggregatePtr avgAggregate = AggregateLibrary::getInstance()->createAggregate("avg", TypeLibrary::getType(attrs[aid].getType()));
		Attributes aggAttrs(1);
		aggAttrs[0] = AttributeDesc((AttributeID)0,
		                            attrs[aid].getName() + "_avg",
		                            avgAggregate->getResultType().typeId(),
		                            AttributeDesc::IS_NULLABLE,
		                            0);

        if (_parameters.size() <= 1) { 
            Dimensions aggDims(1);
            aggDims[0] = DimensionDesc("i", 0, 0, 0, 0, 1, 0);
            return ArrayDesc(desc.getName(), aggAttrs, aggDims);
        } else { 
            vector<int> groupBy(_parameters.size() - 1);
            for (size_t i = 0; i < groupBy.size(); ++i)
            {
                groupBy[i] = ((boost::shared_ptr<OperatorParamReference>&)_parameters[i + 1])->getInputNo();
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

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalAverage, "avg")

} //namespace scidb

