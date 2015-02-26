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
 * @file Operator.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Implementation of basic operator methods.
 */

#include "boost/make_shared.hpp"
#include "boost/function.hpp"
#include "boost/bind.hpp"
#include <log4cxx/logger.h>

#include "query/Operator.h"
#include "query/OperatorLibrary.h"
#include "network/NetworkManager.h"
#include "network/BaseConnection.h"
#include "network/MessageUtils.h"
#include "system/SystemCatalog.h"
#include "array/DBArray.h"
#include "array/FileArray.h"
#include "query/QueryProcessor.h"
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"

using namespace std;
using namespace boost;

namespace scidb
{

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.operator"));

template <typename T>
inline static T min( T const& lhs, T const& rhs)
{
    return lhs < rhs ? lhs : rhs;
}

template <typename T>
inline static T max( T const& lhs, T const& rhs)
{
    return lhs > rhs ? lhs : rhs;
}


/*
 * PhysicalBoundaries methods
 */
PhysicalBoundaries::PhysicalBoundaries(Coordinates const& start, Coordinates const& end, double density):
    _startCoords(start), _endCoords(end), _density(density)
{
    if (_startCoords.size() != _endCoords.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES);

    if (density < 0.0 || density > 1.0)
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES);

    for (size_t i = 0; i< _startCoords.size(); i++)
    {
        if (_startCoords[i] < MIN_COORDINATE)
        {
            _startCoords[i] = MIN_COORDINATE;
        }
        else if (_startCoords[i] > MAX_COORDINATE)
        {
            _startCoords[i] = MAX_COORDINATE;
        }

        if (_endCoords[i] < MIN_COORDINATE)
        {
            _endCoords[i] = MIN_COORDINATE;
        }
        else if (_endCoords[i] > MAX_COORDINATE)
        {
            _endCoords[i] = MAX_COORDINATE;
        }
    }
}

PhysicalBoundaries PhysicalBoundaries::createFromFullSchema(ArrayDesc const& schema )
{
    Coordinates resultStart, resultEnd;

    for (size_t i =0; i<schema.getDimensions().size(); i++)
    {
        resultStart.push_back(schema.getDimensions()[i].getStartMin());
        resultEnd.push_back(schema.getDimensions()[i].getEndMax());
    }

    return PhysicalBoundaries(resultStart, resultEnd);
}

PhysicalBoundaries PhysicalBoundaries::createEmpty(size_t numDimensions)
{
    Coordinates resultStart, resultEnd;

    for (size_t i =0; i<numDimensions; i++)
    {
        resultStart.push_back(MAX_COORDINATE);
        resultEnd.push_back(MIN_COORDINATE);
    }

    return PhysicalBoundaries(resultStart, resultEnd);
}


bool PhysicalBoundaries::isEmpty() const
{
    if (_startCoords.size() == 0)
    {
        return true;
    }

    for (size_t i = 0; i<_startCoords.size(); i++)
    {
        if (_startCoords[i] > _endCoords[i])
        {
            return true;
        }
    }
    return false;
}

uint64_t PhysicalBoundaries::getCellNumber(Coordinates const & in, Dimensions const& dims)
{
    uint64_t cellNumber = 0;
    if (in.size() != dims.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_DIMENSIONS_DONT_MATCH_COORDINATES);

    for (size_t i =0; i<dims.size(); i++)
    {
        if (in[i] < dims[i].getStartMin() || in[i] > dims[i].getEndMax())
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_DIMENSIONS_DONT_MATCH_COORDINATES);

        cellNumber *= dims[i].getLength();
        cellNumber += (in[i] - dims[i].getStart());
    }
    return cellNumber;
}

Coordinates PhysicalBoundaries::reshapeCoordinates (Coordinates const& in,
                                                    Dimensions const& currentDims,
                                                    Dimensions const& newDims)
{
    uint64_t cellNumber = getCellNumber(in,currentDims);

    Coordinates result;
    for (size_t i =0; i<newDims.size(); i++)
    {
        uint64_t otherDimIncrements = 1;
        for (size_t j = i+1; j<newDims.size(); j++)
        {
            otherDimIncrements *= newDims[j].getLength();
        }

        result.push_back(newDims[i].getStart() + cellNumber / otherDimIncrements);
        cellNumber = cellNumber % otherDimIncrements;
    }

    return result;
}

uint64_t PhysicalBoundaries::getNumCells (Coordinates const& start, Coordinates const& end)
{
    if (PhysicalBoundaries(start,end).isEmpty())
    {
        return 0;
    }

    uint64_t result = 1;
    for ( size_t i = 0; i < end.size(); i++)
    {
        if (start[i] <= MIN_COORDINATE || end[i] >= MAX_COORDINATE)
        {
            return INFINITE_LENGTH;
        }
        else if(end[i] >= start[i])
        {
            result *= (end[i] - start[i] + 1);
        }
        else
        {
            result *= 0;
        }
    }

    if (result > INFINITE_LENGTH)
    {
        result = INFINITE_LENGTH;
    }
    return result;
}


uint64_t PhysicalBoundaries::getNumCells() const
{
    return getNumCells(_startCoords, _endCoords);
}

uint64_t PhysicalBoundaries::getRawCellNumber(Coordinates const & in) const
{
    uint64_t cellNumber = 0;
    for (size_t i =0; i<in.size(); i++)
    {
        cellNumber *= (_endCoords[i] - _startCoords[i] + 1);
        cellNumber += (in[i] - _startCoords[i]);
    }
    return cellNumber;
}

Coordinates PhysicalBoundaries::coordsFromRawCellNumber (uint64_t cellNumber) const
{
    Coordinates result;
    for (size_t i =0; i<_startCoords.size(); i++)
    {
        uint64_t otherDimIncrements = 1;
        for (size_t j = i+1; j<_startCoords.size(); j++)
        {
            otherDimIncrements *= (_endCoords[j] - _startCoords[j] + 1);
        }

        result.push_back(_startCoords[i] + cellNumber / otherDimIncrements);
        cellNumber = cellNumber % otherDimIncrements;
    }

    return result;
}

uint64_t PhysicalBoundaries::getCellsPerChunk (Dimensions const& dims)
{
    uint64_t cellsPerChunk = 1;
    for (size_t i = 0; i<dims.size(); i++)
    {
        cellsPerChunk *= dims[i].getChunkInterval();
    }
    return cellsPerChunk;
}

uint64_t PhysicalBoundaries::getNumChunks(Dimensions const& dims) const
{
    if (_startCoords.size() != dims.size())

    if (isEmpty())
    {
        return 0;
    }

    uint64_t result = 1;
    for (size_t i =0; i < _endCoords.size(); i++)
    {
        if (_startCoords[i]<=MIN_COORDINATE || _endCoords[i]>=MAX_COORDINATE)
        {
            return INFINITE_LENGTH;
        }

        DimensionDesc dim = dims[i];
        if (_startCoords[i] < dim.getStart() || _endCoords[i] > dim.getEndMax())
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES);
        Coordinate arrayStart = dim.getStart();
        uint32_t chunkInterval = dim.getChunkInterval();
        Coordinate physStart = _startCoords[i]; //TODO:OPTAPI (- overlap) ?
        Coordinate physEnd = _endCoords[i];

        int64_t numChunks = chunkInterval == 0 ? 0 :
                            ((physEnd - arrayStart + chunkInterval) / chunkInterval) - ((physStart - arrayStart) / chunkInterval);

        result *= numChunks;
    }
    return result;
}

std::ostream& operator<< (std::ostream& stream, const PhysicalBoundaries& bounds)
{
    stream<<"start "<<bounds._startCoords<<" end "<<bounds._endCoords<<" density "<<bounds._density;
    return stream;
}

bool PhysicalBoundaries::isInsideBox (Coordinate const& in, size_t const& dimensionNum) const
{
    if (dimensionNum >= _startCoords.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_WRONG_NUMBER_OF_DIMENSIONS);

    Coordinate start = _startCoords[dimensionNum];
    Coordinate end = _endCoords[dimensionNum];

    return in >= start && in <= end;
}

PhysicalBoundaries PhysicalBoundaries::intersectWith (PhysicalBoundaries const& other) const
{
    if (_startCoords.size() != other._startCoords.size()
            || _startCoords.size() != other._endCoords.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_BOUNDARIES);

    if (isEmpty() || other.isEmpty())
    {
        return createEmpty(_startCoords.size());
    }

    Coordinates start;
    for (size_t i =0; i<_startCoords.size(); i++)
    {
        Coordinate myStart = _startCoords[i];
        Coordinate otherStart = other._startCoords[i];
        start.push_back( myStart < otherStart ? otherStart : myStart );
    }

    Coordinates end;
    for (size_t i =0; i<_endCoords.size(); i++)
    {
        Coordinate myEnd = _endCoords[i];
        Coordinate otherEnd = other._endCoords[i];
        end.push_back( myEnd > otherEnd ? otherEnd : myEnd);
    }

    double myCells = getNumCells();
    double otherCells = other.getNumCells();
    double intersectionCells = getNumCells(start,end);

    double resultDensity = 1.0;
    if (intersectionCells > 0)
    {
        double maxMyDensity = min( _density * myCells / intersectionCells, 1.0 );
        double maxOtherDensity = min ( other._density * otherCells / intersectionCells, 1.0);
        resultDensity = min (maxMyDensity, maxOtherDensity);
    }

    return PhysicalBoundaries(start,end, resultDensity);
}

PhysicalBoundaries PhysicalBoundaries::unionWith (PhysicalBoundaries const& other) const
{
    if (_startCoords.size() != other._startCoords.size()
            || _startCoords.size() != other._endCoords.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_BOUNDARIES);

    if (isEmpty())
    {
        return other;
    }

    else if (other.isEmpty())
    {
        return *this;
    }

    Coordinates start;
    for (size_t i =0; i<_startCoords.size(); i++)
    {
        Coordinate myStart = _startCoords[i];
        Coordinate otherStart = other._startCoords[i];
        start.push_back( myStart > otherStart ? otherStart : myStart );
    }

    Coordinates end;
    for (size_t i =0; i<_endCoords.size(); i++)
    {
        Coordinate myEnd = _endCoords[i];
        Coordinate otherEnd = other._endCoords[i];
        end.push_back( myEnd < otherEnd ? otherEnd : myEnd);
    }

    double myCells = getNumCells();
    double otherCells = getNumCells();
    double resultCells = getNumCells(start, end);
    double maxDensity = min ( (myCells * _density + otherCells * other._density ) / resultCells, 1.0);

    return PhysicalBoundaries(start,end, maxDensity);
}

PhysicalBoundaries PhysicalBoundaries::crossWith (PhysicalBoundaries const& other) const
{
    if (isEmpty() || other.isEmpty())
    {
        return createEmpty(_startCoords.size()+other._startCoords.size());
    }

    Coordinates start, end;
    for (size_t i=0; i<_startCoords.size(); i++)
    {
        start.push_back(_startCoords[i]);
        end.push_back(_endCoords[i]);
    }
    for (size_t i=0; i<other.getStartCoords().size(); i++)
    {
        start.push_back(other.getStartCoords()[i]);
        end.push_back(other.getEndCoords()[i]);
    }

    return PhysicalBoundaries(start,end, _density * other._density);
}

PhysicalBoundaries PhysicalBoundaries::reshape(Dimensions const& oldDims, Dimensions const& newDims) const
{
    if (isEmpty())
    {
        return createEmpty(newDims.size());
    }

    Coordinates start = reshapeCoordinates(_startCoords, oldDims, newDims);
    Coordinates end = reshapeCoordinates(_endCoords, oldDims, newDims);

    if (newDims.size() > oldDims.size())
    {
        bool dimensionFull = false;

        for (size_t i = 0; i < start.size(); i++)
        {
            if (dimensionFull)
            {
                if (newDims[i].getStartMin() <= MIN_COORDINATE || newDims[i].getEndMax() >= MAX_COORDINATE)
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_CREATE_BOUNDARIES_FROM_INFINITE_ARRAY);

                start[i] = newDims[i].getStartMin();
                end[i] = newDims[i].getEndMax();
            }
            else if(end[i] > start[i])
            {
                dimensionFull = true;
            }
        }
    }

    double startingCells = getNumCells();
    double resultCells = getNumCells(start, end);

    return PhysicalBoundaries(start,end, _density * startingCells / resultCells);
}

shared_ptr<SharedBuffer> PhysicalBoundaries::serialize() const
{
    size_t totalSize = sizeof(size_t) + sizeof(double) + _startCoords.size()*sizeof(Coordinate) + _endCoords.size()*sizeof(Coordinate);
    MemoryBuffer* buf = new MemoryBuffer(NULL, totalSize);

    size_t* sizePtr = (size_t*)buf->getData();
    *sizePtr = _startCoords.size();
    sizePtr++;

    double* densityPtr = (double*) sizePtr;
    *densityPtr = _density;
    densityPtr++;

    Coordinate* coordPtr = (Coordinate*) densityPtr;

    for(size_t i = 0; i<_startCoords.size(); i++)
    {
        *coordPtr = _startCoords[i];
        coordPtr++;
    }

    for(size_t i = 0; i<_endCoords.size(); i++)
    {
        *coordPtr = _endCoords[i];
        coordPtr++;
    }

    assert((char*) coordPtr == (char*)buf->getData() + buf->getSize());
    return shared_ptr<SharedBuffer> (buf);
}

PhysicalBoundaries PhysicalBoundaries::deSerialize(shared_ptr<SharedBuffer> const& buf)
{
    size_t* numCoordsPtr = (size_t*) buf->getData();
    size_t numCoords = *numCoordsPtr;
    numCoordsPtr++;

    double* densityPtr = (double*) buf->getData();
    double density = *densityPtr;
    densityPtr++;

    Coordinates start, end;
    Coordinate* coordPtr = (Coordinate*) densityPtr;

    for(size_t i =0; i<numCoords; i++)
    {
        start.push_back(*coordPtr);
        coordPtr++;
    }

    for(size_t i =0; i<numCoords; i++)
    {
        end.push_back(*coordPtr);
        coordPtr++;
    }

    return PhysicalBoundaries(start,end,density);
}


uint32_t PhysicalBoundaries::getCellSizeBytes(const Attributes& attrs )
{
    uint32_t totalBitSize = 0;
    Config* cfg = Config::getInstance();

    for (size_t i = 0; i < attrs.size(); i++)
    {
        const AttributeDesc attr = attrs[i];
        Type cellType = TypeLibrary::getType(attr.getType());
        uint32_t bitSize = cellType.bitSize();

        if (bitSize == 0)
        {
            bitSize =  cfg->getOption<int>(CONFIG_STRING_SIZE_ESTIMATION) * 8;
        }
        if (attr.isNullable())
        {
            bitSize += 1;
        }
        totalBitSize += bitSize;
    }
    return (totalBitSize + 7)/8;
}

double PhysicalBoundaries::getSizeEstimateBytes(const ArrayDesc& schema) const
{
    uint64_t numCells = getNumCells();
    uint64_t numChunks = getNumChunks(schema.getDimensions());
    size_t numDimensions = schema.getDimensions().size();
    size_t numAttributes = schema.getAttributes().size();

    uint32_t cellSize = getCellSizeBytes(schema.getAttributes());

    //we assume that every cell is part of sparse chunk
    cellSize += numAttributes * (numDimensions  * sizeof(Coordinate) + sizeof(int));
    double size = numCells * 1.0 * cellSize ;

    //Assume all chunks are sparse and add header
    size += ( sizeof(SparseChunkIterator::SparseChunkHeader) * 1.0) *
            numChunks *
            numAttributes;

    return size * _density;
}


/*
 * ArrayDistribution methods
 */
bool operator== (ArrayDistribution const& lhs, ArrayDistribution const& rhs)
{
    return lhs._partitioningSchema == rhs._partitioningSchema &&
           (lhs._partitioningSchema != psLocalNode || lhs._nodeId == rhs._nodeId) &&
           ((!lhs.hasMapper() && !rhs.hasMapper()) || ( lhs.hasMapper() && rhs.hasMapper() && *lhs._distMapper.get() == *rhs._distMapper.get()));
}

std::ostream& operator<<(std::ostream& stream, const ArrayDistribution& dist)
{
    switch (dist._partitioningSchema)
    {
        case psReplication:     stream<<"repl";
                                break;
        case psRoundRobin:      stream<<"roro";
                                break;
        case psLocalNode:       stream<<"loca";
                                break;
        case psByRow:           stream<<"byro";
                                break;
        case psByCol:           stream<<"byco";
                                break;
        case psUndefined:       stream<<"undefined";
                                break;
    }

    if (dist._partitioningSchema == psLocalNode)
    {
        stream<<" node "<<dist._nodeId;
    }
    if (dist._distMapper.get() != NULL)
    {
        stream<<" "<<*dist._distMapper;
    }
    return stream;
}


/**
 * Implementation of SCATTER/GATHER method.
 */
void sync(NetworkManager* networkManager, boost::shared_ptr<Query> query, uint64_t nodeCount)
{
    boost::shared_ptr<MessageDesc> msg = boost::make_shared<MessageDesc>(mtSyncRequest);
    boost::shared_ptr<scidb_msg::DummyQuery> record = msg->getRecord<scidb_msg::DummyQuery>();
    msg->setQueryID(query->getQueryID());
    networkManager->sendOutMessage(msg);

    LOG4CXX_DEBUG(logger, "Sending sync to every one and waiting for " << nodeCount - 1 << " sync confirmations")
    Semaphore::ErrorChecker ec = bind(&Query::validate, query);
    query->syncSG.enter(nodeCount - 1, ec);
    LOG4CXX_DEBUG(logger, "All confirmations received - continuing")
}


void barrier(int barrierId, NetworkManager* networkManager, boost::shared_ptr<Query> query, uint64_t nodeCount)
{
    boost::shared_ptr<MessageDesc> barrierMsg = boost::make_shared<MessageDesc>(mtBarrier);
    boost::shared_ptr<scidb_msg::DummyQuery> barrierRecord = barrierMsg->getRecord<scidb_msg::DummyQuery>();
    barrierMsg->setQueryID(query->getQueryID());
    barrierRecord->set_barrier_id(barrierId);
    networkManager->sendOutMessage(barrierMsg);

    LOG4CXX_DEBUG(logger, "Sending barrier to every one and waiting for " << nodeCount - 1 << " barrier messages")
    Semaphore::ErrorChecker ec = bind(&Query::validate, query);
    query->semSG[barrierId].enter(nodeCount - 1, ec);
    LOG4CXX_DEBUG(logger, "All barrier messages received - continuing")
}

NodeID getNodeForChunk(boost::shared_ptr< Query> query,
                       Coordinates chunkPosition,
                        ArrayDesc const& desc,
                        PartitioningSchema ps,
                        boost::shared_ptr<DistributionMapper> distMapper,
                        NodeID defaultNodeId)
{
    const uint64_t nodeCount = query->getNodesCount();
    NodeID result = defaultNodeId;

    Dimensions const& dims = desc.getDimensions();
    uint64_t dim0Length = dims[0].getLength();
    uint64_t dim1Length = dims.size() > 1 ? dims[1].getLength() : 0;
    DimensionVector shape;

    if (distMapper.get() != NULL)
    {
        chunkPosition = distMapper->translate(chunkPosition);
        shape = distMapper->getShapeVector();

        dim0Length = shape[0];
        dim1Length = shape.numDimensions() > 1 ? shape[1] : 0;
    }

    switch (ps)
    {
        case psRoundRobin:
            result = desc.getChunkNumber(chunkPosition, shape) % nodeCount;
            break;
        case psByRow:
            result = (chunkPosition[0] - dims[0].getStart()) / dims[0].getChunkInterval()
                / (((dim0Length + dims[0].getChunkInterval() - 1) / dims[0].getChunkInterval() + nodeCount - 1) / nodeCount);
            break;
        case psByCol:
        {
                if (dims.size() > 1)
                {
                result = (chunkPosition[1] - dims[1].getStart()) / dims[1].getChunkInterval()
                          / (((dim1Length + dims[1].getChunkInterval() - 1) / dims[1].getChunkInterval() + nodeCount - 1) / nodeCount);
                }
                else
                {
                        result = 0;
                }
                break;
        }

        case psUndefined:
            throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_REDISTRIBUTE_ERROR);

        case psReplication:
        case psLocalNode:
            break;
    }

    return result;
}


static std::vector<AggregatePtr> copyAggList(std::vector<AggregatePtr> const& input)
{
    std::vector<AggregatePtr> result(input.size());
    for (size_t i=0; i<input.size(); i++)
    {
        if (input[i].get())
        {
            result[i]=input[i]->clone();
        }
    }
    return result;
}

boost::shared_ptr<MemArray> redistributeAggregate(boost::shared_ptr<MemArray> inputArray,
                                                  boost::shared_ptr<Query> query,
                                                  vector <AggregatePtr> const& aggs)
{
    LOG4CXX_DEBUG(logger, "SG_AGGREGATE started");

    uint64_t totalBytesSent = 0;

    NetworkManager* networkManager = NetworkManager::getInstance();
    const uint64_t nodeCount = query->getNodesCount();

    if (nodeCount == 1)
    {
        return inputArray;
    }

    ArrayDesc desc = inputArray->getArrayDesc();

    size_t nAttrs = desc.getAttributes().size();
    if (desc.getEmptyBitmapAttribute() != NULL && (desc.getEmptyBitmapAttribute()->getId() != nAttrs-1 || aggs[nAttrs-1] != NULL))
        throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_REDISTRIBUTE_AGGREGATE_ERROR1);

    boost::shared_ptr<MemArray> outputArray(new MemArray(desc));
    bool isEmptyable = desc.getEmptyBitmapAttribute() != NULL;
    query->setSGContext(shared_ptr<SGContext>(new SGContext(outputArray, copyAggList(aggs) )));
    barrier(0, networkManager, query, nodeCount);

    size_t msgCounter = 0;
    size_t syncInterval = 10;
    uint64_t arraySize = desc.getUsedSpace();
    if (arraySize != INFINITE_LENGTH && arraySize != 0)
    {
        size_t bufSize = Config::getInstance()->getOption<int>(CONFIG_NETWORK_BUFFER)*MB;
        size_t chunkSize = arraySize / desc.getNumberOfChunks();
        if (chunkSize != 0)
        {
            syncInterval = (bufSize + chunkSize - 1) / chunkSize;
        }
    }
    //Regular redistribute() assumes that if there is a chunk for attribute 0, there must also be a chunk at same position
    //for all other attributes. Not the case with aggregate state arrays! Aggregates that ignore nulls may produce a lot less
    //chunks than aggregates that count nulls.
    for (AttributeID attrId = nAttrs; attrId-- != 0;)
    {
        shared_ptr<ConstArrayIterator> inputIter = inputArray->getConstIterator(attrId);
        shared_ptr<ArrayIterator> outputIter = outputArray->getIterator(attrId);
        while (!inputIter->end())
        {
            Coordinates chunkPosition = inputIter->getPosition();
            NodeID nodeID =  getNodeForChunk(query, chunkPosition, desc, psRoundRobin, shared_ptr<DistributionMapper>(), 0);
            const ConstChunk& chunk = inputIter->getChunk();
            AttributeDesc const& attributeDesc = chunk.getAttributeDesc();
            const Coordinates& coordinates = chunk.getFirstPosition(false);
            MessageType mt = aggs[attrId].get() ? mtAggregateChunk : mtChunk;

            boost::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
            if (chunk.isRLE() && isEmptyable && !attributeDesc.isEmptyIndicator()) { 
                emptyBitmap = chunk.getEmptyBitmap();
            }
            
            if ( nodeID != query->getNodeID() )
            {
                boost::shared_ptr<CompressedBuffer> buffer = boost::make_shared<CompressedBuffer>();
                chunk.compress(*buffer, emptyBitmap);

                boost::shared_ptr<MessageDesc> chunkMsg = boost::make_shared<MessageDesc>(mt, buffer);
                boost::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
                chunkRecord->set_eof(false);
                chunkRecord->set_sparse(chunk.isSparse());
                chunkRecord->set_rle(chunk.isRLE());
                chunkRecord->set_compression_method(buffer->getCompressionMethod());
                chunkRecord->set_attribute_id(attrId);
                chunkRecord->set_decompressed_size(buffer->getDecompressedSize());
                chunkRecord->set_count(chunk.isCountKnown() ? chunk.count() : 0);
                chunkMsg->setQueryID(query->getQueryID());
                for (size_t i = 0; i < coordinates.size(); i++)
                {
                    chunkRecord->add_coordinates(coordinates[i]);
                }

                networkManager->send(nodeID, chunkMsg);
                LOG4CXX_TRACE(logger, "Sending chunk with att=" << attrId << " to node=" << nodeID);
                totalBytesSent += buffer->getDecompressedSize();
                if (++msgCounter >= syncInterval) {
                    sync(networkManager, query, nodeCount);
                    msgCounter = 0;
                }                
            }
            else
            {
                ScopedMutexLock cs(query->resultCS);
                if (outputIter->setPosition(coordinates))
                {
                    Chunk& dstChunk = outputIter->updateChunk();
                    if (aggs[attrId].get())
                    {
                        dstChunk.aggregateMerge(chunk, aggs[attrId], query);
                    }
                    else
                    {
                        dstChunk.merge(chunk, query);
                    }
                }
                else
                {
                    outputIter->copyChunk(chunk, emptyBitmap);                        
                }
                LOG4CXX_TRACE(logger, "Storing chunk with att=" << attrId << " locally")
            }
            ++(*inputIter);
         }
     }

     sync(networkManager, query, nodeCount);
     barrier(1, networkManager, query, nodeCount);

     query->setSGContext(shared_ptr<SGContext>());

     LOG4CXX_DEBUG(logger, "Finishing SG_AGGREGATE work; sent " << totalBytesSent << " bytes.")
     return outputArray;
}


AggregatePtr resolveAggregate(boost::shared_ptr <OperatorParamAggregateCall>const& aggregateCall,
                              Attributes const& inputAttributes,
                              AttributeID* inputAttributeID,
                              string* outputName)
{
    const shared_ptr<OperatorParam> &acParam = aggregateCall->getInputAttribute();

    try
    {
        if (PARAM_ASTERISK == acParam->getParamType())
        {
            AggregatePtr agg = AggregateLibrary::getInstance()->createAggregate( aggregateCall->getAggregateName(), TypeLibrary::getType(TID_VOID));

            if (inputAttributeID)
            {
                *inputAttributeID = (AttributeID) -1;
            }
            if (outputName)
            {
                *outputName = aggregateCall->getAlias().size() ? aggregateCall->getAlias() : agg->getName();
            }
            return agg;
        }
        else if (PARAM_ATTRIBUTE_REF == acParam->getParamType())
        {
            const shared_ptr<OperatorParamAttributeReference> &ref = (const shared_ptr<OperatorParamAttributeReference>&) acParam;

            AttributeDesc const& inputAttr = inputAttributes[ref->getObjectNo()];
            Type const& inputType = TypeLibrary::getType(inputAttr.getType());
            AggregatePtr agg = AggregateLibrary::getInstance()->createAggregate( aggregateCall->getAggregateName(), inputType);

            if (inputAttributeID)
            {
                *inputAttributeID = inputAttr.getId();
            }
            if (outputName)
            {
                *outputName = aggregateCall->getAlias().size() ? aggregateCall->getAlias() : inputAttr.getName() + "_" + agg->getName();
            }
            return agg;
        }
        else
        {
            // All other must be throwed out during translation
            assert(0);
        }
    }
    catch(const UserException &e)
    {
        if (SCIDB_LE_AGGREGATE_NOT_FOUND == e.getLongErrorCode())
        {
            throw CONV_TO_USER_QUERY_EXCEPTION(e, acParam->getParsingContext());
        }

        throw;
    }
}

void addAggregatedAttribute (boost::shared_ptr <OperatorParamAggregateCall>const& aggregateCall, ArrayDesc const& inputDesc, ArrayDesc& outputDesc)
{
    string outputName;

    AggregatePtr agg = resolveAggregate(aggregateCall, inputDesc.getAttributes(), 0, &outputName);

    outputDesc.addAttribute( AttributeDesc(outputDesc.getAttributes().size(),
                                           outputName,
                                           agg->getResultType().typeId(),
                                           AttributeDesc::IS_NULLABLE,
                                           0));
}


boost::shared_ptr<Array> redistribute(boost::shared_ptr<Array> inputArray,
                               boost::shared_ptr<Query> query,
                               PartitioningSchema ps,
                               const string& resultArrayName,
                               NodeID nodeID,
                               boost::shared_ptr<DistributionMapper> distMapper)
{
    LOG4CXX_DEBUG(logger, "SG started with partitioning schema = " << ps << ", nodeID = " << nodeID)

    uint64_t totalBytesSent = 0;

    /**
     * Creating result array with the same descriptor as the input one
     */
    NetworkManager* networkManager = NetworkManager::getInstance();
    const uint64_t nodeCount = query->getNodesCount();

    if (nodeCount == 1 || (ps == psLocalNode && (int64_t) nodeID == -1)) {
        return inputArray;
    }

    ArrayDesc desc = inputArray->getArrayDesc();

    size_t nAttrs = desc.getAttributes().size();
    AttributeDesc const* emptyAttr = desc.getEmptyBitmapAttribute();
    if (emptyAttr != NULL && emptyAttr->getId() != nAttrs-1)
        throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_REDISTRIBUTE_ERROR1);
    
    boost::shared_ptr<Array> outputArray;
    ArrayID resultArrayId = 0;
    SystemCatalog* catalog = NULL;

    if (resultArrayName.empty()) {
        outputArray = createTmpArray(desc);
        LOG4CXX_DEBUG(logger, "Temporary array was opened")
    }
    else {
        outputArray = boost::shared_ptr<Array>(new DBArray(resultArrayName, query));
        resultArrayId = outputArray->getHandle();
        catalog = SystemCatalog::getInstance();
        query->lowBoundary = catalog->getLowBoundary(resultArrayId);
        query->highBoundary = catalog->getHighBoundary(resultArrayId);
        LOG4CXX_DEBUG(logger, "Array " << resultArrayName << " was opened")
    }

    /**
     * Assigning result of this operation for current query and signal to concurrent handlers that they
     * can continue to work
     */
    query->setSGContext(shared_ptr<SGContext>(new SGContext(outputArray)));

    barrier(0, networkManager, query, nodeCount);

    size_t msgCounter = 0;

    /**
     *  Sending out our parts of the input array
     */
    vector<boost::shared_ptr<ConstArrayIterator> > inputIters;
    vector<boost::shared_ptr<ArrayIterator> > outputIters;

    for (AttributeID attrId = 0; attrId < nAttrs; attrId++)
    {
        inputIters.push_back(inputArray->getConstIterator(attrId));
        outputIters.push_back(outputArray->getIterator(attrId));
    }

    size_t syncInterval = 10;
    uint64_t arraySize = desc.getUsedSpace();
    if (arraySize != INFINITE_LENGTH && arraySize != 0) {
        size_t bufSize = Config::getInstance()->getOption<int>(CONFIG_NETWORK_BUFFER)*MB;
        size_t chunkSize = arraySize / desc.getNumberOfChunks();
        if (chunkSize != 0) {
            syncInterval = (bufSize + chunkSize - 1) / chunkSize;
        }
    }
    while (!inputIters[0]->end())
    {
        Coordinates chunkPosition = inputIters[0]->getPosition();
        nodeID = getNodeForChunk(query, chunkPosition,desc,ps,distMapper,nodeID);

        boost::shared_ptr<ConstRLEEmptyBitmap> sharedEmptyBitmap;

        for (AttributeID attrId = nAttrs; attrId-- != 0;)
        {
            // Sending current chunk to nodeID node or saving locally
            const ConstChunk& chunk = inputIters[attrId]->getChunk();
            AttributeDesc const& attributeDesc = chunk.getAttributeDesc();
            assert(attributeDesc.getId() == attrId);

            const Coordinates& coordinates = chunk.getFirstPosition(false);

            boost::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
            if (chunk.isRLE() && emptyAttr != NULL) { 
                if (!sharedEmptyBitmap) {                         
                    sharedEmptyBitmap = chunk.getEmptyBitmap();
                }
                if (!attributeDesc.isEmptyIndicator()) {                         
                    emptyBitmap = sharedEmptyBitmap;
                }
            }
            if ((NodeID)nodeID != query->getNodeID() || ps == psReplication)
            {
                boost::shared_ptr<CompressedBuffer> buffer = boost::make_shared<CompressedBuffer>();
                chunk.compress(*buffer, emptyBitmap);
                boost::shared_ptr<MessageDesc> chunkMsg = boost::make_shared<MessageDesc>(mtChunk, buffer);
                boost::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
                chunkRecord->set_eof(false);
                chunkRecord->set_sparse(chunk.isSparse());
                chunkRecord->set_rle(chunk.isRLE());
                chunkRecord->set_compression_method(buffer->getCompressionMethod());
                chunkRecord->set_attribute_id(attrId);
                chunkRecord->set_decompressed_size(buffer->getDecompressedSize());
                chunkRecord->set_count(chunk.isCountKnown() ? chunk.count() : 0);
                chunkMsg->setQueryID(query->getQueryID());
                for (size_t i = 0; i < coordinates.size(); i++) {
                    chunkRecord->add_coordinates(coordinates[i]);
                }

                if (ps != psReplication) {
                    networkManager->send(nodeID, chunkMsg);
                    LOG4CXX_TRACE(logger, "Sending chunk with att=" << attrId << " to node=" << nodeID)
                    msgCounter++;
                }
                else {
                    networkManager->sendOutMessage(chunkMsg);
                    LOG4CXX_TRACE(logger, "Sending out chunk with att=" << attrId << " to every node")
                    msgCounter += nodeCount - 1;
                }
                if (msgCounter >= syncInterval) {
                    sync(networkManager, query, nodeCount);
                    msgCounter = 0;
                }
                totalBytesSent += buffer->getDecompressedSize();
            }
            if ((NodeID)nodeID == query->getNodeID() || ps == psReplication)
            {
                ScopedMutexLock cs(query->resultCS);
                if (outputIters[attrId]->setPosition(coordinates)) {
                    Chunk& dstChunk = outputIters[attrId]->updateChunk();
                    if (dstChunk.isReadOnly())
                        throw USER_EXCEPTION(SCIDB_SE_REDISTRIBUTE, SCIDB_LE_CANT_MERGE_READONLY_CHUNK);
                    dstChunk.merge(chunk, query);
                } else {
                    outputIters[attrId]->copyChunk(chunk, emptyBitmap);
                }
                Coordinates const& first = chunk.getFirstPosition(false);
                Coordinates const& last = chunk.getLastPosition(false);
                for (size_t i = 0, n = query->lowBoundary.size(); i < n; i++) {
                    if (first[i] < query->lowBoundary[i]) {
                        query->lowBoundary[i] = first[i];
                    }
                    if (last[i] > query->highBoundary[i]) {
                        query->highBoundary[i] = last[i];
                    }
                }
                LOG4CXX_TRACE(logger, "Storing chunk with att=" << attrId << " locally")
            }
            ++(*inputIters[attrId]);
        }
    }

    sync(networkManager, query, nodeCount);
    barrier(1, networkManager, query, nodeCount);

    /**
     * Reset rSG Context to NULL
     */
    query->setSGContext(shared_ptr<SGContext>());

    if (resultArrayId != 0) {
        catalog->updateArrayBoundaries(resultArrayId, query->lowBoundary, query->highBoundary);
    }
    LOG4CXX_DEBUG(logger, "Finishing SCATTER/GATHER work; sent " << totalBytesSent << " bytes.")

    return outputArray;
}

PhysicalBoundaries findArrayBoundaries(shared_ptr<Array> srcArray,
                                       boost::shared_ptr<Query> query,
                                       bool global)
{
    ArrayDesc const& srcDesc = srcArray->getArrayDesc();
    ArrayID id = srcDesc.getId();

    if (SystemCatalog::getInstance()->containsArray(srcDesc.getId()))
    {
        Coordinates lo = SystemCatalog::getInstance()->getLowBoundary(id);
        Coordinates hi = SystemCatalog::getInstance()->getHighBoundary(id);
        return PhysicalBoundaries(lo,hi);
    }

    size_t nDims = srcDesc.getDimensions().size();
    Coordinates lo(nDims, MAX_COORDINATE), hi(nDims, MIN_COORDINATE);
    {
        shared_ptr<ConstArrayIterator> arrayIterator = srcArray->getConstIterator(srcDesc.getAttributes()[0].getId());
        while (!arrayIterator->end())
        {
            {
                ConstChunk const& chunk = arrayIterator->getChunk();
                Coordinates chunkLo = chunk.getFirstPosition(false);
                Coordinates chunkHi = chunk.getLastPosition(false);
                for (size_t i=0; i< lo.size(); i++)
                {
                    if (chunkLo[i] < lo[i])
                    {   lo[i]=chunkLo[i]; }
                    if (chunkHi[i] > hi[i])
                    {   hi[i]=chunkHi[i]; }
                }
            }
            ++(*arrayIterator);
        }
    }

    PhysicalBoundaries localBoundaries(lo,hi);

    if (global)
    {
        NetworkManager* networkManager = NetworkManager::getInstance();
        const size_t nNodes = (size_t)query->getNodesCount();
        const size_t myNodeId = (size_t)query->getNodeID();

        shared_ptr<SharedBuffer> buf;
        if (myNodeId != 0)
        {
            networkManager->send(0, localBoundaries.serialize(), query);
            localBoundaries = PhysicalBoundaries::deSerialize(networkManager->receive(0, query));
        }
        else
        {
            for (size_t node = 1; node < nNodes; node++)
            {
                PhysicalBoundaries receivedBoundaries = PhysicalBoundaries::deSerialize(networkManager->receive(node,query));
                localBoundaries = localBoundaries.unionWith(receivedBoundaries);
            }

            for (size_t node = 1; node < nNodes; node++)
            {
                networkManager->send(node, localBoundaries.serialize(), query);
            }
        }
    }

    return localBoundaries;
}


class BroadcastedBuffer
{
    shared_ptr<SharedBuffer> buf;
    boost::shared_ptr<Query> query;
  public:
    BroadcastedBuffer(shared_ptr<SharedBuffer> buf, boost::shared_ptr<Query> query)  {
        this->buf = buf;
        this->query = query;
    }

    ~BroadcastedBuffer() {
        NetworkManager* networkManager = NetworkManager::getInstance();
        const size_t nNodes = (size_t)query->getNodesCount();
        for (size_t node = 1; node < nNodes; node++)
        {
            networkManager->send(node, buf, query);
        }
    }
};

template <class SetType, class MapType>
boost::shared_ptr<MapType> __buildSortedXIndex( SetType& attrSet,
                                                boost::shared_ptr<Query> query,
                                                string const& indexArrayName,
                                                Coordinate indexArrayStart,
                                                uint64_t maxElements)
{
    shared_ptr<SharedBuffer> buf;
    NetworkManager* networkManager = NetworkManager::getInstance();
    const size_t nNodes = (size_t)query->getNodesCount();
    const size_t myNodeId = (size_t)query->getNodeID();

    if (myNodeId != 0)
    {
        networkManager->send(0, attrSet.sort(true), query);
        buf = networkManager->receive(0, query);
    }
    else
    {
        for (size_t node = 1; node < nNodes; node++)
        {
            attrSet.add(networkManager->receive(node, query));
        }

        if (maxElements)
        {
            if (attrSet.size() > maxElements)
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TOO_MANY_COORDINATES);
        }
        buf = attrSet.sort(false);
        BroadcastedBuffer broadcast(buf, query);

        if (indexArrayName.size())
        {
            //
            // Create array descriptor for storing index
            //
            Dimensions indexMapDim(1);
            Attributes indexMapAttr(1);
            size_t dimSize = attrSet.size();
            Coordinate start = indexArrayStart;
            indexMapDim[0] = DimensionDesc("no", start, start, start + dimSize - 1, start + dimSize - 1, dimSize, 0);
            indexMapAttr[0] = AttributeDesc(0, "value", attrSet.getType(), 0, 0);
            ArrayDesc indexMapDesc(indexArrayName, indexMapAttr, indexMapDim, ArrayDesc::LOCAL);
            ArrayID mappingArrayID = SystemCatalog::getInstance()->addArray(indexMapDesc, psReplication);
            assert(mappingArrayID > 0);
        }
    }

    size_t *nCoordsPtr = (size_t*)((char*)buf->getData() + buf->getSize() - sizeof(size_t));
    size_t nCoords = *nCoordsPtr;

    {
        //
        // Store array with coordinates index locally at each node
        //
        if (nCoords != 0 && indexArrayName.size())
        {
            DBArray indexMapArray(indexArrayName, query);
            shared_ptr<ArrayIterator> arrayIterator = indexMapArray.getIterator(0);
            Coordinates start(1);
            start[0] = indexArrayStart;
            Chunk& chunk = arrayIterator->newChunk(start, 0);
            chunk.allocate(buf->getSize() - sizeof(size_t));
            chunk.setRLE(false); 
            memcpy(chunk.getData(), buf->getData(), buf->getSize() - sizeof(size_t));
            chunk.write(query);
        }
    }
    if (nCoords != 0)
    {
        return boost::shared_ptr<MapType>(new MapType(attrSet.getType(), indexArrayStart, nCoords, buf->getData(), buf->getSize() - sizeof(size_t)));
    }
    else
    {
        return boost::shared_ptr<MapType>();
    }
}

template <class SetType, class MapType>
boost::shared_ptr<MapType> __buildSortedXIndex( shared_ptr<Array> srcArray,
                                                  AttributeID attrID,
                                                  boost::shared_ptr<Query> query,
                                                  string const& indexArrayName,
                                                  Coordinate indexArrayStart,
                                                  uint64_t maxElements)
{
    ArrayDesc const& srcDesc = srcArray->getArrayDesc();
    AttributeDesc sourceAttribute = srcDesc.getAttributes()[attrID];

    SetType attrSet(sourceAttribute.getType());
    {
        shared_ptr<ConstArrayIterator> arrayIterator = srcArray->getConstIterator(attrID);
        while (!arrayIterator->end())
        {
            {
                shared_ptr<ConstChunkIterator> chunkIterator = arrayIterator->getChunk().getConstIterator();
                while (!chunkIterator->end())
                {
                    Value const& value = chunkIterator->getItem();
                    if (value.isNull())
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CANT_REDIMENSION_NULL);
                    attrSet.add(value);
                    ++(*chunkIterator);
                }
            }
            ++(*arrayIterator);
        }
    }

    return __buildSortedXIndex<SetType, MapType> (attrSet, query, indexArrayName, indexArrayStart, maxElements);
}

boost::shared_ptr<AttributeMap> buildFunctionalMapping(DimensionDesc const& dim)
{
    FunctionLibrary* lib = FunctionLibrary::getInstance();
    std::vector<TypeId> inputArgTypes(3, TID_INT64);
    std::vector<FunctionPointer> converters;
    bool supportsVectorMode;
    FunctionDescription toDesc, fromDesc;

    if (lib->findFunction(dim.getType(), inputArgTypes, fromDesc, converters, supportsVectorMode, false) && fromDesc.getOutputArg() == dim.getType() && converters.size() == 0) {
        inputArgTypes[0] = dim.getType();
        if (lib->findFunction("ordinal", inputArgTypes, toDesc, converters, supportsVectorMode, false) && toDesc.getOutputArg() == TID_INT64 && converters.size() == 0) {
            return  boost::shared_ptr<AttributeMap>(new AttributeMap(dim, toDesc.getFuncPtr(), fromDesc.getFuncPtr()));
        }
    }
    return boost::shared_ptr<AttributeMap>();
}


boost::shared_ptr<AttributeMap> buildSortedIndex (AttributeSet& attrSet,
                                                  boost::shared_ptr<Query> query,
                                                  string const& indexArrayName,
                                                  Coordinate indexArrayStart,
                                                  uint64_t maxElements)
{
    return __buildSortedXIndex<AttributeSet, AttributeMap> (attrSet, query, indexArrayName, indexArrayStart, maxElements);
}

boost::shared_ptr<AttributeMap> buildSortedIndex (shared_ptr<Array> srcArray,
                                                  AttributeID attrID,
                                                  boost::shared_ptr<Query> query,
                                                  string const& indexArrayName,
                                                  Coordinate indexArrayStart,
                                                  uint64_t maxElements)
{
    return __buildSortedXIndex<AttributeSet, AttributeMap> (srcArray, attrID, query, indexArrayName, indexArrayStart, maxElements);
}

boost::shared_ptr<AttributeMultiMap> buildSortedMultiIndex (AttributeMultiSet& attrSet,
                                                            boost::shared_ptr<Query> query,
                                                            string const& indexArrayName,
                                                            Coordinate indexArrayStart,
                                                            uint64_t maxElements)
{
    return __buildSortedXIndex<AttributeMultiSet, AttributeMultiMap> (attrSet, query, indexArrayName, indexArrayStart, maxElements);
}

boost::shared_ptr<AttributeMultiMap> buildSortedMultiIndex (shared_ptr<Array> srcArray,
                                                            AttributeID attrID,
                                                            boost::shared_ptr<Query> query,
                                                            string const& indexArrayName,
                                                            Coordinate indexArrayStart,
                                                            uint64_t maxElements)
{
    return __buildSortedXIndex<AttributeMultiSet, AttributeMultiMap> (srcArray, attrID, query, indexArrayName, indexArrayStart, maxElements);
}


void BaseLogicalOperatorFactory::registerFactory()
{
    OperatorLibrary::getInstance()->addLogicalOperatorFactory(this);
}

void BasePhysicalOperatorFactory::registerFactory()
{
    OperatorLibrary::getInstance()->addPhysicalOperatorFactory(this);
}

VersionID OperatorParamArrayReference::getVersion() const
{
    return _version;
}

const string &OperatorParamArrayReference::getIndex() const
{
    return _index;
}

void LogicalOperator::inferArrayAccess(boost::shared_ptr<Query>& query)
{
    for (size_t i=0, end=_parameters.size(); i<end; ++i) {
        const shared_ptr<OperatorParam>& param = _parameters[i];
        string arrayName;
        if (param->getParamType() == PARAM_ARRAY_REF) {
            arrayName = ((boost::shared_ptr<OperatorParamReference>&)param)->getObjectName();
        } else if (param->getParamType() == PARAM_SCHEMA) {
            arrayName = ((boost::shared_ptr<OperatorParamSchema>&)param)->getSchema().getName();
        }
        if (arrayName.empty()) {
            continue;
        }

        string baseName = arrayName.substr(0, arrayName.find('@'));
        shared_ptr<SystemCatalog::LockDesc> lock(new SystemCatalog::LockDesc(baseName,
                                                                             query->getQueryID(),
                                                                             Cluster::getInstance()->getLocalNodeId(),
                                                                             SystemCatalog::LockDesc::COORD,
                                                                             SystemCatalog::LockDesc::RD));
        query->requestLock(lock);
    }
}

} //namespace
