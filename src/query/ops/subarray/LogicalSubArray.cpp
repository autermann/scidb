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
 * LogicalSubArray.cpp
 *
 *  Created on: May 20, 2010
 *      Author: knizhnik@garret.ru
 */

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "SubArray.h"

namespace scidb {

/***
 * Helper function to set the dimension start and length properties in the array descriptor.
 * Constructs a new array descriptor with the appropriate dimensions.
 ***/
    ArrayDesc setDimensions(ArrayDesc desc, Coordinates& lowPos, Coordinates& highPos, boost::shared_ptr<Query> const& query)
    {
        Dimensions dims = desc.getDimensions();
        Dimensions newDims(dims.size());
        
        for (size_t i = 0, n = dims.size(); i < n; i++) { 
            DimensionDesc const& srcDim = dims[i];
            string mappingArrayName = srcDim.getMappingArrayName();
            if (highPos[i] >= lowPos[i] && !mappingArrayName.empty() && srcDim.getType() != TID_INT64) { 
                string tmpMappingArrayName;
                size_t tmpArrayNo = 0;
                do { 
                    std::stringstream ss;
                    ss << mappingArrayName << '$' << ++tmpArrayNo;
                    tmpMappingArrayName = ss.str();
                } while (query->getTemporaryArray(tmpMappingArrayName));
                
                subarrayMappingArray(srcDim.getBaseName(), mappingArrayName, tmpMappingArrayName, lowPos[i], highPos[i], query);
                mappingArrayName = tmpMappingArrayName;
            }
            newDims[i] = DimensionDesc(srcDim.getBaseName(), srcDim.getNamesAndAliases(), 0, 0, highPos[i] - lowPos[i],
                                       highPos[i] - lowPos[i], srcDim.getChunkInterval(), srcDim.getChunkOverlap(),
                                       srcDim.getType(),
                                       srcDim.getFlags() | (srcDim.getFuncMapScale() != 1 ? DimensionDesc::COMPLEX_TRANSFORMATION : 0),
                                       mappingArrayName,
                                       srcDim.getComment(),
                                       srcDim.getFuncMapOffset() + lowPos[i] - srcDim.getStart(),
                                       srcDim.getFuncMapScale());
        }
        
        /***
         * FIXME: Don't really know what are the number of cells and the size of the array
         **/
        return ArrayDesc(desc.getName(), desc.getAttributes(), newDims);
    }


    class LogicalSubArray: public  LogicalOperator
    {
      public:
        LogicalSubArray(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
        {
            ADD_PARAM_INPUT()
            ADD_PARAM_VARIES()
        }

        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector<ArrayDesc> &schemas)
        {
            std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
            size_t i = _parameters.size();
            Dimensions const& dims = schemas[0].getDimensions();
            size_t nDims = dims.size();
            if (i < nDims*2) {
                res.push_back(PARAM_CONSTANT(dims[i < nDims ? i : i - nDims].getType()));
            }
            if (i == 0 || i >= nDims*2) { 
                res.push_back(END_OF_VARIES_PARAMS());
            }
            return res;
        }

        ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, boost::shared_ptr< Query> query)
        {
            assert(inputSchemas.size() == 1);
            assert(_parameters.size() == 0 || _parameters.size() == inputSchemas[0].getDimensions().size() * 2);
            
            for (Parameters::const_iterator it = _parameters.begin(); it != _parameters.end(); ++it)
            {
                assert(((boost::shared_ptr<OperatorParam>&)*it)->getParamType() == PARAM_LOGICAL_EXPRESSION);
                assert(((boost::shared_ptr<OperatorParamLogicalExpression>&)*it)->isConstant());
            }

            ArrayDesc& desc = inputSchemas[0];
            Dimensions const& dims = desc.getDimensions();
            size_t nDims = dims.size();

            // Fetch the low and high coordinates of the subarray window from the operator parameters
            Coordinates lowPos(nDims);
            Coordinates highPos(nDims);

            if (_parameters.size() == 0) { 
                for (size_t i = 0; i < nDims; i++)
                {
                    lowPos[i] = dims[i].getLowBoundary();
                    highPos[i] = dims[i].getHighBoundary();
                }
            } else { 
                for (size_t i = 0; i < nDims; i++)
                {
                    Value const& low = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[i])->getExpression(),
                                                query, dims[i].getType());
                    if (low.isNull()) {
                        lowPos[i] = dims[i].getLowBoundary();
                    } else {
                        lowPos[i] = desc.getOrdinalCoordinate(i, low, cmLowerBound, query);
                        if (dims[i].getStart() != MIN_COORDINATE && lowPos[i] < dims[i].getStart()) {
                            lowPos[i] = dims[i].getStart();
                        }
                    }
                    Value const& high = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[i+nDims])->getExpression(),
                                                 query, dims[i].getType());
                    if (high.isNull()) {
                        highPos[i] = dims[i].getHighBoundary();
                    } else {
                        highPos[i] = desc.getOrdinalCoordinate(i, high, cmUpperBound, query);
                        if (highPos[i] > dims[i].getEndMax())
                        {
                            highPos[i] = dims[i].getEndMax();
                        }
                    }
                    if (lowPos[i] > highPos[i])
                    {
                        highPos[i] = lowPos[i] - 1;
                        /*
                          throw USER_QUERY_EXCEPTION(SCIDB_E_WRONG_SYNTAX,
                          "Invalid coordinate range",
                          _parameters[i]->getParsingContext());
                        */
                    }
                }
            }

            /***
             * We first create a physical schema for the array and modify the dimension start and length
             * parameters.
             */
            return setDimensions(desc, lowPos, highPos, query);
        }
    };

    DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSubArray, "subarray")


}  // namespace scidb
