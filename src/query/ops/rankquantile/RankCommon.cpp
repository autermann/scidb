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
#include "RankCommon.h"

#include <fstream>
#include <iostream>
#include <iomanip>

namespace scidb
{


shared_ptr<SharedBuffer> rMapToBuffer( CountsMap const& input, size_t nCoords)
{
    size_t totalSize = input.size() * (nCoords * sizeof(Coordinate) + sizeof(uint64_t));
    shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, totalSize));
    Coordinate *dst = (Coordinate*) buf->getData();
    BOOST_FOREACH (CountsMap::value_type bucket, input)
    {
        Coordinates const& coords = bucket.first;

        for (size_t i=0; i< nCoords; i++)
        {
            *dst = coords[i];
            ++dst;
        }

        uint64_t* dMaxRank = (uint64_t*) dst;
        *dMaxRank = bucket.second;
        ++dMaxRank;
        dst = (Coordinate*) dMaxRank;
    }
    return buf;
}

void updateRmap(CountsMap& input, shared_ptr<SharedBuffer> buf, size_t nCoords)
{
    if (buf.get() == 0)
    {
        return;
    }

    Coordinate *dst;
    dst = (Coordinate*) buf->getData();
    while ((char*) dst != (char*) buf->getData() + buf->getSize())
    {
        Coordinates coords(nCoords);
        for (size_t i =0; i<nCoords; i++)
        {
            coords[i] = *dst;
            ++dst;
        }

        uint64_t* dMaxRank = (uint64_t*) dst;
        if (input.count(coords) == 0)
        {
            input[coords] = *dMaxRank;
        }
        else if (input[coords] < *dMaxRank)
        {
            input[coords] = *dMaxRank;
        }

        ++dMaxRank;
        dst = (Coordinate *)dMaxRank;
    }
}

ArrayDesc getRankingSchema(ArrayDesc const& inputSchema, AttributeID rankedAttributeID, bool dualRank)
{
    AttributeDesc rankedAttribute = inputSchema.getAttributes()[rankedAttributeID];
    AttributeID attID = 0;

    Attributes outputAttrs;
    outputAttrs.push_back(AttributeDesc(attID++, rankedAttribute.getName(), rankedAttribute.getType(), rankedAttribute.getFlags(), rankedAttribute.getDefaultCompressionMethod()));
    outputAttrs.push_back(AttributeDesc(attID++, rankedAttribute.getName() + "_rank", TID_DOUBLE,  AttributeDesc::IS_NULLABLE, 0));

    if (dualRank)
    {
        outputAttrs.push_back(AttributeDesc(attID++, rankedAttribute.getName() + "_hrank", TID_DOUBLE,  AttributeDesc::IS_NULLABLE, 0));
    }

    AttributeDesc const* emptyTag = inputSchema.getEmptyBitmapAttribute();
    if (emptyTag)
    {
        outputAttrs.push_back(AttributeDesc(attID++, emptyTag->getName(), emptyTag->getType(), emptyTag->getFlags(), emptyTag->getDefaultCompressionMethod()));
    }

    //no overlap. otherwise quantile gets a count that's too large
    Dimensions const& dims = inputSchema.getDimensions();
    Dimensions outDims(dims.size());
    for (size_t i = 0, n = dims.size(); i < n; i++)
    {
        DimensionDesc const& srcDim = dims[i];
        outDims[i] = DimensionDesc(srcDim.getBaseName(),
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

    return ArrayDesc(inputSchema.getName(), outputAttrs, outDims);
}

//inputArray must be distributed round-robin
shared_ptr<Array> buildRankArray(shared_ptr<Array>& inputArray,
                                 AttributeID rankedAttributeID,
                                 Dimensions const& groupedDimensions,
                                 shared_ptr<Query> query,
                                 shared_ptr<RankingStats> rstats)
{
    const ArrayDesc& input = inputArray->getArrayDesc();
    TypeId inputType = input.getAttributes()[rankedAttributeID].getType();

    shared_ptr<PreSortMap> preSortMap;
    if (inputType == TID_INT64)
    {
       preSortMap.reset(new PrimitivePreSortMap<int64_t>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_DOUBLE)
    {
       preSortMap.reset(new PrimitivePreSortMap<double>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_FLOAT)
    {
       preSortMap.reset(new PrimitivePreSortMap<float>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_UINT64)
    {
       preSortMap.reset(new PrimitivePreSortMap<uint64_t>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_INT32)
    {
       preSortMap.reset(new PrimitivePreSortMap<int32_t>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_UINT32)
    {
       preSortMap.reset(new PrimitivePreSortMap<uint32_t>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_INT16)
    {
       preSortMap.reset(new PrimitivePreSortMap<int16_t>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_UINT16)
    {
       preSortMap.reset(new PrimitivePreSortMap<uint16_t>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_INT8)
    {
       preSortMap.reset(new PrimitivePreSortMap<int8_t>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_UINT8)
    {
       preSortMap.reset(new PrimitivePreSortMap<uint8_t>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_BOOL)
    {
       preSortMap.reset(new PrimitivePreSortMap<bool>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_CHAR)
    {
       preSortMap.reset(new PrimitivePreSortMap<char>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_DATETIME)
    {
        preSortMap.reset(new PrimitivePreSortMap<time_t>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else
    {
       preSortMap.reset(new ValuePreSortMap(inputArray, rankedAttributeID, groupedDimensions));
    }

    ArrayDesc outputSchema = getRankingSchema(input,rankedAttributeID);
    shared_ptr<Array> runningRank = shared_ptr<Array>(new RankArray(outputSchema, inputArray, preSortMap, rankedAttributeID, false, rstats));
    size_t nInstances = query->getInstancesCount();
    for (size_t i =1; i<nInstances; i++)
    {
        LOG4CXX_DEBUG(logger, "Performing rotation "<<i);
        runningRank = redistribute(runningRank, query, psRoundRobin, "", -1, boost::shared_ptr<DistributionMapper>(), i);
        runningRank = boost::shared_ptr<Array> (new RankArray(outputSchema, runningRank, preSortMap, 0, true, rstats));
    }

    return runningRank;
}

//inputArray must be distributed round-robin
shared_ptr<Array> buildDualRankArray(shared_ptr<Array>& inputArray,
                                     AttributeID rankedAttributeID,
                                     Dimensions const& groupedDimensions,
                                     shared_ptr<Query> query,
                                     shared_ptr<RankingStats> rstats)
{
    const ArrayDesc& input = inputArray->getArrayDesc();
    TypeId inputType = input.getAttributes()[rankedAttributeID].getType();

    shared_ptr<PreSortMap> preSortMap;
    if (inputType == TID_INT64)
    {
        preSortMap.reset(new PrimitivePreSortMap<int64_t>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_DOUBLE)
    {
        preSortMap.reset(new PrimitivePreSortMap<double>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_FLOAT)
    {
        preSortMap.reset(new PrimitivePreSortMap<float>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_UINT64)
    {
        preSortMap.reset(new PrimitivePreSortMap<uint64_t>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_INT32)
    {
        preSortMap.reset(new PrimitivePreSortMap<int32_t>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_UINT32)
    {
        preSortMap.reset(new PrimitivePreSortMap<uint32_t>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_INT16)
    {
        preSortMap.reset(new PrimitivePreSortMap<int16_t>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_UINT16)
    {
        preSortMap.reset(new PrimitivePreSortMap<uint16_t>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_INT8)
    {
        preSortMap.reset(new PrimitivePreSortMap<int8_t>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_UINT8)
    {
        preSortMap.reset(new PrimitivePreSortMap<uint8_t>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_BOOL)
    {
        preSortMap.reset(new PrimitivePreSortMap<bool>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_CHAR)
    {
        preSortMap.reset(new PrimitivePreSortMap<char>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else if (inputType == TID_DATETIME)
    {
        preSortMap.reset(new PrimitivePreSortMap<time_t>(inputArray, rankedAttributeID, groupedDimensions));
    }
    else
    {
        preSortMap.reset(new ValuePreSortMap(inputArray, rankedAttributeID, groupedDimensions));
    }

    ArrayDesc dualRankSchema = getRankingSchema(input,rankedAttributeID, true);
    shared_ptr<Array> runningRank = shared_ptr<Array>(new DualRankArray(dualRankSchema, inputArray, preSortMap, rankedAttributeID, false, rstats));

    size_t nInstances = query->getInstancesCount();

    for (size_t i =1; i<nInstances; i++)
    {
        LOG4CXX_DEBUG(logger, "Performing rotation "<<i);
        runningRank = redistribute(runningRank, query, psRoundRobin, "", -1, boost::shared_ptr<DistributionMapper>(), i);
        runningRank = boost::shared_ptr<Array> (new DualRankArray(dualRankSchema, runningRank, preSortMap, 0, true, rstats));
    }

    ArrayDesc outputSchema = getRankingSchema(input,rankedAttributeID);
    return boost::shared_ptr<Array> (new AvgRankArray(outputSchema, runningRank));
}

}
