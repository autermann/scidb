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
 * LogicalVar.cpp
 * Example query : Var (scan ('array_to_scan'), 'attribute_to_aggregate' )
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

class LogicalVar : public  LogicalOperator
{
public:
	LogicalVar(const std::string& logicalName, const std::string& alias):
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
        if (_parameters.size() >= 1) {
            aid = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectNo();
        }
        AggregatePtr varAggregate = AggregateLibrary::getInstance()->createAggregate("var", TypeLibrary::getType(attrs[aid].getType()));
        Attributes aggAttrs(1);
        aggAttrs[0] = AttributeDesc((AttributeID)0,
                                    attrs[aid].getName() + "_var",
                                    varAggregate->getResultType().typeId(),
                                    AttributeDesc::IS_NULLABLE,
                                    0);
        if (_parameters.size() <= 1) { 
            Dimensions aggDims(1);
            aggDims[0] = DimensionDesc("i", 0, 0, 0, 0, 1, 0);
            return ArrayDesc(desc.getName(), aggAttrs, aggDims);
        } else { 
            vector<int> groupBy(_parameters.size()-1);
            for (size_t i = 0; i < groupBy.size(); i++)
            {
                groupBy[i] =  ((boost::shared_ptr<OperatorParamReference>&)_parameters[i + 1])->getObjectNo();
            }
            Dimensions aggDims(groupBy.size());
            for (size_t i = 0, n = aggDims.size(); i < n; i++) { 
                DimensionDesc const& srcDim = dims[groupBy[i]]; 
                aggDims[i] = DimensionDesc(  srcDim.getBaseName(),
                                             srcDim.getStartMin(),
                                             srcDim.getEndMax(),
                                             i == 0 && groupBy[i] == 0 ? srcDim.getChunkInterval() : srcDim.getCurrLength(),
                                             0,
                                             srcDim.getType(),
                                             srcDim.getSourceArrayName());
            }
            return ArrayDesc(desc.getName(), aggAttrs, aggDims);
        }
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalVar, "var")

} //namespace scidb

