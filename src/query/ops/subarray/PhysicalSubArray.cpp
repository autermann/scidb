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
 * PhysicalSubArray.cpp
 *
 *  Created on: May 20, 2010
 *      Author: knizhnik@garret.ru
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "query/ops/subarray/SubArray.h"
#include "network/NetworkManager.h"


namespace scidb {

class PhysicalSubArray: public  PhysicalOperator
{
public:
    PhysicalSubArray(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
             PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

   inline Coordinates getWindowStart(ArrayDesc const& inputSchema, const boost::shared_ptr<Query>& query) const
    {
        Dimensions const& dims = inputSchema.getDimensions();
        size_t nDims = dims.size();
        Coordinates result (nDims);

        if (_parameters.size() == 0) { 
            for (size_t i = 0; i < nDims; i++)
            {
                result[i] = dims[i].getLowBoundary();
            }
        } else {         
            for (size_t i = 0; i < nDims; i++)
            {
                Value const& low = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i])->getExpression()->evaluate();
                if (low.isNull())
                {
                    result[i] = dims[i].getLowBoundary();
                }
                else
                {
                    result[i] = inputSchema.getOrdinalCoordinate(i, low, cmLowerBound, query);
                    if (dims[i].getStartMin() != MIN_COORDINATE && result[i] < dims[i].getStartMin())
                    {
                        result[i] = dims[i].getStartMin();
                    }
                }
            }
        }
        return result;
    }

   inline Coordinates getWindowEnd(ArrayDesc const& inputSchema, const boost::shared_ptr<Query>& query) const
    {
        Dimensions const& dims = inputSchema.getDimensions();
        size_t nDims = dims.size();
        Coordinates result (nDims);

        if (_parameters.size() == 0) { 
            for (size_t i = 0; i < nDims; i++)
            {
                result[i] = dims[i].getHighBoundary();
            }
        } else {         
            for (size_t i  = 0; i < nDims; i++)
            {
                Value const& high = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i + nDims])->getExpression()->evaluate();
                if (high.isNull())
                {
                    result[i] = dims[i].getHighBoundary();
                }
                else
                {
                    result[i] = inputSchema.getOrdinalCoordinate(i, high, cmUpperBound, query);
                    if (result[i] > dims[i].getEndMax())
                    {
                        result[i] = dims[i].getEndMax();
                    }
                }
            }
        }
        return result;
    }

    virtual bool changesDistribution(std::vector< ArrayDesc> const&) const
    {
        return true;
    }

    virtual bool outputFullChunks(std::vector< ArrayDesc> const& inputSchemas) const
    {
        boost::shared_ptr<Query> query(_query.lock());
        ArrayDesc const& input = inputSchemas[0];
        Coordinates windowStart = getWindowStart(input, query);
        Coordinates windowEnd = getWindowEnd(input, query);

        if ( input.coordsAreAtChunkStart(windowStart) &&
             input.coordsAreAtChunkEnd(windowEnd) )
        {
            return true;
        }

        return false;
    }

    virtual DimensionVector getOffsetVector(const std::vector< ArrayDesc> & inputSchemas) const
    {
        ArrayDesc const& desc = inputSchemas[0];
        Dimensions const& inputDimensions = desc.getDimensions();
        size_t numCoords = inputDimensions.size();
        DimensionVector result(numCoords);
        boost::shared_ptr<Query> query(_query.lock());
        Coordinates windowStart = getWindowStart(inputSchemas[0], query);

        for (size_t i = 0; i < numCoords; i++)
        {
            Coordinate arrayStartCoord = (inputDimensions[i]).getStart();
            result[i] = windowStart[i]-arrayStartCoord;
        }

        return result;
    }

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                 const std::vector< ArrayDesc> & inputSchemas) const
    {
        DimensionVector offset = getOffsetVector(inputSchemas);
        boost::shared_ptr<DistributionMapper> distMapper;
        ArrayDistribution inputDistro = inputDistributions[0];

        if (inputDistro.isUndefined())
        {
            return ArrayDistribution(psUndefined);
        }
        else
        {
            boost::shared_ptr<DistributionMapper> inputMapper = inputDistro.getMapper();
            if (!offset.isEmpty())
            {
                distMapper = DistributionMapper::createOffsetMapper(offset) ->combine(inputMapper);
            }
            else
            {
                distMapper = inputMapper;
            }
            return ArrayDistribution(inputDistro.getPartitioningSchema(), distMapper);
        }
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        size_t nDims = _schema.getDimensions().size();
        boost::shared_ptr<Query> query(_query.lock());
        PhysicalBoundaries window(getWindowStart(inputSchemas[0], query),
                                  getWindowEnd(inputSchemas[0], query));
        PhysicalBoundaries result = inputBoundaries[0].intersectWith(window);

        if (result.isEmpty())
        {
            return PhysicalBoundaries::createEmpty(nDims);
        }

        Coordinates newStart, newEnd;
        for (size_t i =0; i < nDims; i++)
        {
            newStart.push_back(0);
            newEnd.push_back( result.getEndCoords()[i] - result.getStartCoords()[i] );
        }

        return PhysicalBoundaries(newStart, newEnd, result.getDensity());
    }

        /***
         * SubArray is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
         * that overrides the chunkiterator method.
         */
    boost::shared_ptr< Array> execute(std::vector< boost::shared_ptr< Array> >& inputArrays,
                                      boost::shared_ptr< Query> query)
    {
        assert(inputArrays.size() == 1);
        if (inputArrays[0]->getSupportedAccess() != Array::RANDOM)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_UNSUPPORTED_INPUT_ARRAY) << getLogicalName();
        }

        boost::shared_ptr< Array> array = inputArrays[0];
        ArrayDesc const& desc = array->getArrayDesc();
        Dimensions const& srcDims = desc.getDimensions();
        Dimensions const& dstDims = _schema.getDimensions();
        size_t nDims = srcDims.size();
        
        /***
         * Fetch and calculate the subarray window
         */
        Coordinates lowPos = getWindowStart(desc, query);
        Coordinates highPos = getWindowEnd(desc, query);
        for(size_t i=0; i<nDims; i++)
        {
            if (lowPos[i] > highPos[i]) {
                return boost::shared_ptr<Array>(new MemArray(_schema));
            }
            if (query->getCoordinatorID() != COORDINATOR_INSTANCE) { 
                string const& oldMapping = srcDims[i].getMappingArrayName();
                string const& newMapping = dstDims[i].getMappingArrayName();
                if (!newMapping.empty() && oldMapping != newMapping) { 
                    subarrayMappingArray(srcDims[i].getBaseName(), oldMapping, newMapping, lowPos[i], highPos[i], query);
                }
            }
        }
        /***
         * Create an iterator-based array implementation for the operator
         */
        boost::shared_ptr< Array> arr = boost::shared_ptr< Array>( new SubArray(_schema, lowPos, highPos, inputArrays[0] ));
        return arr;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSubArray, "subarray", "physicalSubArray")

}  // namespace scidb
