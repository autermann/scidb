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
 * LogicalCount.cpp
 *
 *  Created on: Mar 9, 2010
 *      Author: pavel
 */

#include "query/Operator.h"
#include "system/Exceptions.h"

using namespace std;
using namespace boost;

namespace scidb
{

class Count : public  LogicalOperator
{
public:
	Count(const std::string& logicalName, const std::string& alias):
			LogicalOperator(logicalName, alias)
	{
		ADD_PARAM_INPUT()
		ADD_PARAM_VARIES()
	}

	vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const vector<ArrayDesc> &schemas)
    {
        assert(schemas.size() == 1);

        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(PARAM_IN_DIMENSION_NAME());
        res.push_back(END_OF_VARIES_PARAMS());
        return res;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
		Attributes atts(1);

		AggregatePtr countAggregate = AggregateLibrary::getInstance()->createAggregate("count", TypeLibrary::getType(TID_VOID));
		atts[0] = AttributeDesc((AttributeID)0,
		                        "count",
		                        countAggregate->getResultType().typeId(),
		                        AttributeDesc::IS_NULLABLE,
		                        0 );

        if (_parameters.size() == 0) { 
            Dimensions dims(1);
            dims[0] = DimensionDesc("i", 0, 0, 0, 0, 1, 0);
            return ArrayDesc(schemas[0].getName(), atts, dims);
        } else { 
            std::vector<int> groupBy(_parameters.size());
            ArrayDesc const& desc = schemas[0];
            Dimensions const& dims = desc.getDimensions();
            size_t i, j;
            for (i = 0; i < groupBy.size(); i++) { 
                const string& dimName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[i])->getObjectName();
                const string& aliasName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[i])->getArrayName();
                for (j = 0; j < dims.size(); j++) { 
                    if (dims[j].hasNameOrAlias(dimName, aliasName)) { 
                        groupBy[i] = j;
                        break;
                    }
                }
                
                if (!(j < dims.size()))
                {
                    const string fullName = aliasName == "" ? dimName : aliasName + "." + dimName;
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_NOT_EXIST,
                                               _parameters[i]->getParsingContext()) << fullName;
                }
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
            return ArrayDesc(desc.getName(), atts, aggDims);
        }
	}

};

DECLARE_LOGICAL_OPERATOR_FACTORY(Count, "count")

} //namespace scidb
