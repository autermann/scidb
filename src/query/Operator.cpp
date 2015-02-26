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

#include <boost/make_shared.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <log4cxx/logger.h>

#include <query/Operator.h>
#include <query/OperatorLibrary.h>
#include <network/NetworkManager.h>
#include <network/BaseConnection.h>
#include <network/MessageUtils.h>
#include <system/SystemCatalog.h>
#include <array/DBArray.h>
#include <array/FileArray.h>
#include <query/QueryProcessor.h>
#include <system/BlockCyclic.h>
#include <system/Config.h>
#include <system/SciDBConfigOptions.h>
#include <smgr/io/Storage.h>
#include <boost/functional/hash.hpp>
#include <util/Hashing.h>
#include <util/Timing.h>

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

        DimensionDesc const& dim = dims[i];
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
           (lhs._partitioningSchema != psLocalInstance || lhs._instanceId == rhs._instanceId) &&
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
        case psLocalInstance:       stream<<"loca";
                                break;
        case psByRow:           stream<<"byro";
                                break;
        case psByCol:           stream<<"byco";
                                break;
        case psUndefined:       stream<<"undefined";
                                break;
        case psGroupby:         stream<<"groupby";
                                break;
        case psScaLAPACK:       stream<<"ScaLAPACK";
                                break;
    default:
            assert(0);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "operator<<(std::ostream& stream, const ArrayDistribution& dist)";
    }

    if (dist._partitioningSchema == psLocalInstance)
    {
        stream<<" instance "<<dist._instanceId;
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
void sync(NetworkManager* networkManager, boost::shared_ptr<Query> query, uint64_t instanceCount)
{
    boost::shared_ptr<MessageDesc> msg = boost::make_shared<MessageDesc>(mtSyncRequest);
    boost::shared_ptr<scidb_msg::DummyQuery> record = msg->getRecord<scidb_msg::DummyQuery>();
    msg->setQueryID(query->getQueryID());
    networkManager->sendOutMessage(msg);

    LOG4CXX_DEBUG(logger, "Sending sync to every one and waiting for " << instanceCount - 1 << " sync confirmations")
    Semaphore::ErrorChecker ec = bind(&Query::validate, query);
    query->syncSG.enter(instanceCount - 1, ec);
    LOG4CXX_DEBUG(logger, "All confirmations received - continuing")
}


void barrier(int barrierId, NetworkManager* networkManager, boost::shared_ptr<Query> query, uint64_t instanceCount)
{
    boost::shared_ptr<MessageDesc> barrierMsg = boost::make_shared<MessageDesc>(mtBarrier);
    boost::shared_ptr<scidb_msg::DummyQuery> barrierRecord = barrierMsg->getRecord<scidb_msg::DummyQuery>();
    barrierMsg->setQueryID(query->getQueryID());
    barrierRecord->set_barrier_id(barrierId);
    networkManager->sendOutMessage(barrierMsg);

    LOG4CXX_DEBUG(logger, "Sending barrier to every one and waiting for " << instanceCount - 1 << " barrier messages")
    Semaphore::ErrorChecker ec = bind(&Query::validate, query);
    query->semSG[barrierId].enter(instanceCount - 1, ec);
    LOG4CXX_DEBUG(logger, "All barrier messages received - continuing")
}

/**
 * Compute hash over the groupby dimensions.
 * @param   allDims   Coordinates containing all the dims.
 * @param   isGroupby   For every dimension, whether it is a groupby dimension.
 *
 * @note The result can be larger than #instances!!! The caller should mod it.
 *
 */
InstanceID hashForGroupby(const Coordinates& allDims, const vector<bool>& isGroupby ) {
    assert(allDims.size()==isGroupby.size());
    Coordinates groups;
    for (size_t i=0; i<allDims.size(); ++i) {
        if (isGroupby[i]) {
            groups.push_back(allDims[i]);
        }
    }
    return VectorHash<Coordinate>()(groups);
}

/**
 * Compute the instanceID for a group.
 * For psGroupby, if the logic is modified, also change PhysicalQuantile.cpp::GroupbyQuantileArrayIterator::getInstanceForChunk.
 */
InstanceID getInstanceForChunk(boost::shared_ptr< Query> query,
                       Coordinates chunkPosition,
                       ArrayDesc const& desc,
                       PartitioningSchema ps,
                       boost::shared_ptr<DistributionMapper> distMapper,
                       size_t shift,
                       InstanceID defaultInstanceId,
                       PartitioningSchemaData* psData)
{
    const uint64_t instanceCount = query->getInstancesCount();
    InstanceID result = defaultInstanceId;

    Dimensions const& dims = desc.getDimensions();
    uint64_t dim0Length = dims[0].getLength();
    uint64_t dim1Length = dims.size() > 1 ? dims[1].getLength() : 0;
    DimensionVector shape;

    if (distMapper.get() != NULL)
    {
        chunkPosition = distMapper->translate(chunkPosition);
    }

    switch (ps)
    {
        case psRoundRobin:  // WARNING: hashed, NOT bock-cyclic!
                            // as getChunkNumber() returns hashed patterns

            if(false) { // needed for debugging ScaLAPACK until the end of Cheshire
                std::cerr << "getInstanceForChunk to hashed, chunkPos= " << chunkPosition << std::endl;
                std::cerr << "conversion to RoundRobin, chunkPos=" << chunkPosition << std::endl;
            }
            result = desc.getChunkNumber(chunkPosition) % instanceCount;
            break;
        case psByRow:
            result = (chunkPosition[0] - dims[0].getStart()) / dims[0].getChunkInterval()
                / (((dim0Length + dims[0].getChunkInterval() - 1) / dims[0].getChunkInterval() + instanceCount - 1) / instanceCount);
            break;
        case psByCol:
        {
                if (dims.size() > 1)
                {
                result = (chunkPosition[1] - dims[1].getStart()) / dims[1].getChunkInterval()
                          / (((dim1Length + dims[1].getChunkInterval() - 1) / dims[1].getChunkInterval() + instanceCount - 1) / instanceCount);
                }
                else
                {
                        result = 0;  // TODO Tigor ; you wanted a comment because you wanted to look at this line
                }
                break;
        }

        case psScaLAPACK:
        {
            if (dims.size() <= 2) {      // not defined for > 2
                result = iidForScaLAPACK(chunkPosition, dims, *(query.get()));
                if(false) { // needed for debugging ScaLAPACK until the end of Cheshire
                    std::cerr << "getInstanceForChunk to ScaLAPACK, chunkPos= " << chunkPosition << std::endl;
                    std::cerr << " -> " <<result<< " shift:" <<shift<< " IC:" <<instanceCount<< std::endl ;
                }
            } else {
                throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_REDISTRIBUTE_ERROR);
            }

        } break;

        case psUndefined:

        case psReplication:
        case psLocalInstance:
            break;

        case psGroupby:
        {
            PartitioningSchemaDataGroupby* pIsGroupbyDim = dynamic_cast<PartitioningSchemaDataGroupby*>(psData);
            if (pIsGroupbyDim!=NULL) {
                result = hashForGroupby(chunkPosition, pIsGroupbyDim->_arrIsGroupbyDim);
            } else {
                assert(false);
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "getInstanceForChunk - psGroupby";
            }
            break;
        }

        default:
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "getInstanceForChunk";
    }

    return (result + shift) % instanceCount;
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

/**
 * Scan through the inputIters that are not ended, and set whether each inputIters[i] is atMinPos.
 * @param[in]    inputIters  a vector of array iterators
 * @param[inout] ended       a vector telling whether each input iterator has ended
 * @param[out]   atMinPos    a vector telling whether each input iterator (that is not ended) is at the min position
 * @param[out]   minPos      the min position
 * @return  whether there exists a valid min position, i.e. whether at least one ended[i] is false
 *
 * @note The input vectors (except minPos) must be pre-allocated. This applies to the output vector atMinPos as well.
 */
bool checkIfAtMin(vector<shared_ptr<ConstArrayIterator> >const& inputIters, vector<bool>& ended, vector<bool>& atMinPos, Coordinates& minPos)
{
    assert(inputIters.size() == ended.size());
    assert(ended.size() == atMinPos.size());

    minPos.clear();
    AttributeID nAttrs = inputIters.size();

    for (AttributeID attrId = nAttrs; attrId-- > 0; ) {
        if (ended[attrId]) {
            continue;
        }
        if (inputIters[attrId]->end()) {
            ended[attrId] = true;
            continue;
        }

        Coordinates const& pos = inputIters[attrId]->getPosition();
        if (minPos.size()==0) {
            minPos = pos;
            atMinPos[attrId] = true;
        } else {
            int64_t c = coordinatesCompare(pos, minPos);
            if (c==0) {
                atMinPos[attrId] = true;
            } else if (c<0) { // all the previous ones are NOT at min position!
                minPos = pos;
                for (AttributeID i=attrId+1; i<nAttrs; ++i) {
                    atMinPos[i] = false;
                }
                atMinPos[attrId] = true;
            } else { // this one is NOT at min position!
                atMinPos[attrId] = false;
            }
        }
    }

    return minPos.size()>0;
}

/**
 * Works ONLY with RLE.
 */
boost::shared_ptr<MemArray> redistributeAggregate(boost::shared_ptr<MemArray> inputArray,
                                                  boost::shared_ptr<Query> query,
                                                  vector <AggregatePtr> const& aggs,
                                                  shared_ptr<RedimInfo> redimInfo)
{
    LOG4CXX_DEBUG(logger, "SG_AGGREGATE started");

    uint64_t totalBytesSent = 0;
    uint64_t totalBytesSynced = 0;

    NetworkManager* networkManager = NetworkManager::getInstance();
    const uint64_t instanceCount = query->getInstancesCount();

    //At this point, we always assume that there will be many-to-one communication (i.e. everyone sends chunk {1,1}
    //to instance 0 at the same time). So cap the network buffer.
    //TODO: what we can do here is build a map of all local chunks, send all maps to coordinator, then coordinator
    //can determine in fact the worst-case divisor for this equation. We can do this because our input should always
    //be materialized here.
    size_t networkBufferLimit = Config::getInstance()->getOption<int>(CONFIG_NETWORK_BUFFER)*MB / instanceCount;

    if (instanceCount == 1)
    {
        return inputArray;
    }

    ArrayDesc const& desc = inputArray->getArrayDesc();
    size_t nAttrs = desc.getAttributes().size();
    bool isEmptyable = (desc.getEmptyBitmapAttribute() != NULL);
    if (isEmptyable && (desc.getEmptyBitmapAttribute()->getId() != nAttrs-1 || aggs[nAttrs-1] != NULL)) {
        throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_REDISTRIBUTE_AGGREGATE_ERROR1);
    }

    // In redistributeAggregate as long as the array is emptyable, we always cache the empty bitmap, i.e. we do NOT send the empty bitmap along with the real attribute chunks.
    // We need a separate variable, in case we want to change the logic in the future.
    bool shouldCacheEmptyBitmap = isEmptyable;

    boost::shared_ptr<MemArray> outputArray(new MemArray(desc));

    barrier(0, networkManager, query, instanceCount);
    // set SG context after the barrier
    query->setOperatorContext(make_shared<SGContext>(shouldCacheEmptyBitmap, instanceCount, outputArray, copyAggList(aggs), redimInfo ));

    // timing
    LOG4CXX_DEBUG(logger, "[redistributeAggregate] Begin, reporting a timing every 10 chunks.");
    size_t chunkID = 0;
    ElapsedMilliSeconds timing;
    uint64_t elapsed = 0;
    size_t reportATimingAfterHowManyChunks = 10; // report a timing after how many chunks?

    //Regular redistribute() assumes that if there is a chunk for attribute 0, there must also be a chunk at same position
    //for all other attributes. Not the case with aggregate state arrays! Aggregates that ignore nulls may produce a lot less
    //chunks than aggregates that count nulls.
    //
    // We maintain a vector of shared_ptr<ConstArrayIterator>.
    // We maintain a state vector, describing the state of each ConstArrayIterator.
    // The possible states are: ALREADY_END, AT_MIN_POS, LARGER_THAN_MIN_POS
    // In each iteration, we process the chunks of the ConstArrayIterators that are AT_MIN_POS, and we increment them.
    //
    vector<shared_ptr<ConstArrayIterator> > inputIters(nAttrs);
    vector<shared_ptr<ArrayIterator> > outputIters(nAttrs); // assigned later, under the mutex protection
    vector<bool> ended(nAttrs);
    vector<bool> atMinPos(nAttrs);
    Coordinates chunkPosition;

    for (AttributeID attrId = nAttrs; attrId-- > 0; ) {
        inputIters[attrId] = inputArray->getConstIterator(attrId);
        ended[attrId] = false;
    }

    // Each iteration of the while loop advances the inputIters that are at minPos
    while (true) {
        bool hasMoreData = checkIfAtMin(inputIters, ended, atMinPos, chunkPosition);
        if (!hasMoreData) {
            break;
        }

        // assert that the empty tag chunk exists
        if (isEmptyable) {
            if (ended[nAttrs-1] || !atMinPos[nAttrs-1]) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "redistributeAggregate";
            }
        }

        // sharedEmptyBitmap is the empty bitmap at a given chunkPos, to be shared by all the attributes.
        shared_ptr<ConstRLEEmptyBitmap> sharedEmptyBitmap;

        for (AttributeID attrId = nAttrs; attrId-- != 0;) {
            if (ended[attrId] || !atMinPos[attrId]) {
                continue;
            }

            InstanceID instanceID =  getInstanceForChunk(query, chunkPosition, desc, psRoundRobin, shared_ptr<DistributionMapper>(), 0, 0);
            const ConstChunk& chunk = inputIters[attrId]->getChunk();
            if (!chunk.isRLE()) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "redistributeAggregate; must be RLE";
            }

            bool isEmptyIndicator = (isEmptyable && attrId+1 == nAttrs);

            if (chunk.getSize() > 0)
            {
                if (isEmptyable) {
                    if (attrId+1 == nAttrs) {
                        assert(!sharedEmptyBitmap);
                        sharedEmptyBitmap = chunk.getEmptyBitmap();
                        assert(sharedEmptyBitmap);
                    } else {
                        assert(sharedEmptyBitmap);
                    }
                }

                const Coordinates& coordinates = chunk.getFirstPosition(false);
                MessageType mt = aggs[attrId].get() ? mtAggregateChunk : mtChunk;

                if ( instanceID != query->getInstanceID() )
                {
                    boost::shared_ptr<CompressedBuffer> buffer = boost::make_shared<CompressedBuffer>();
                    shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
                    if (isEmptyable && !shouldCacheEmptyBitmap && !isEmptyIndicator) {
                        emptyBitmap = sharedEmptyBitmap;
                        assert(emptyBitmap);
                    }
                    chunk.compress(*buffer, emptyBitmap);
                    assert(buffer && buffer->getData());

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

                    networkManager->send(instanceID, chunkMsg);
                    LOG4CXX_TRACE(logger, "Sending chunk with att=" << attrId << " to instance=" << instanceID);
                    totalBytesSent += buffer->getDecompressedSize();
                    if (totalBytesSent > totalBytesSynced + networkBufferLimit) {
                        sync(networkManager, query, instanceCount);
                        totalBytesSynced = totalBytesSent;
                    }
                }
                else
                {
                    ScopedMutexLock cs(query->resultCS);
                    if (!outputIters[attrId]) {
                        outputIters[attrId] = outputArray->getIterator(attrId);
                    }
                    if (outputIters[attrId]->setPosition(coordinates))
                    {
                        Chunk& dstChunk = outputIters[attrId]->updateChunk();
                        if (aggs[attrId].get())
                        {
                            if( desc.getEmptyBitmapAttribute() == NULL && dstChunk.isRLE() && chunk.isRLE())
                            {
                                dstChunk.nonEmptyableAggregateMerge(chunk, aggs[attrId], query);
                            }
                            else
                            {
                                dstChunk.aggregateMerge(chunk, aggs[attrId], query);
                            }
                        }
                        else
                        {
                            dstChunk.merge(chunk, query);
                        }
                    }
                    else
                    {
                        shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
                        if (isEmptyable && !isEmptyIndicator) {
                            emptyBitmap = sharedEmptyBitmap;
                        }
                        outputIters[attrId]->copyChunk(chunk, emptyBitmap);
                    }
                    LOG4CXX_TRACE(logger, "Storing chunk with att=" << attrId << " locally")
                }

                // timing
                ++ chunkID;
                if (chunkID%reportATimingAfterHowManyChunks ==0) {
                    elapsed = timing.elapsed();
                    LOG4CXX_DEBUG(logger, "[redistributeAggregate] reading " << chunkID << " chunks took " << elapsed << " ms, or " << ElapsedMilliSeconds::toString(elapsed));
                    if (chunkID==100) {
                        reportATimingAfterHowManyChunks  = 100;
                        LOG4CXX_DEBUG(logger, "[redistributeAggregate] Now reporting a number after 100 chunks.");
                    } else if (chunkID==1000) {
                        reportATimingAfterHowManyChunks = 1000;
                        LOG4CXX_DEBUG(logger, "[redistributeAggregate] Now reporting a number after 1000 chunks.");
                    }
                }
            }

            ++(*inputIters[attrId]);
        } // end for
    } // end while

    elapsed = timing.elapsed();
    LOG4CXX_DEBUG(logger, "[redistributeAggregate] reading " << chunkID << " chunks took " << elapsed << " ms, or " << ElapsedMilliSeconds::toString(elapsed));

    sync(networkManager, query, instanceCount);
    barrier(1, networkManager, query, instanceCount);

    query->unsetOperatorContext();

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

    assert(false);
    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "resolveAggregate";
    return AggregatePtr();
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
                                      InstanceID instanceID,
                                      boost::shared_ptr<DistributionMapper> distMapper,
                                      size_t shift,
                                      PartitioningSchemaData* psData)
{
    LOG4CXX_DEBUG(logger, "SG started with partitioning schema = " << ps << ", instanceID = " << instanceID)

    uint64_t totalBytesSent = 0;
    uint64_t totalBytesSynced = 0;
    size_t networkBufferLimit = Config::getInstance()->getOption<int>(CONFIG_NETWORK_BUFFER)*MB;

    /**
     * Creating result array with the same descriptor as the input one
     */
    NetworkManager* networkManager = NetworkManager::getInstance();
    const uint64_t instanceCount = query->getInstancesCount();

    assert(instanceID == COORDINATOR_INSTANCE_MASK || instanceID == ALL_INSTANCES_MASK || (size_t)instanceID < query->getInstancesCount());

    if (ps == psLocalInstance || ps == psReplication) {
        networkBufferLimit /= instanceCount;
    }

    if (instanceID == COORDINATOR_INSTANCE_MASK) { 
        instanceID = query->getCoordinatorInstanceID();
    }
    if (instanceCount == 1 || (ps == psLocalInstance && instanceID == ALL_INSTANCES_MASK)) {
        return inputArray;
    }

    ArrayDesc const& desc = inputArray->getArrayDesc();
    Dimensions const& srcDims = desc.getDimensions();
    size_t nDims = srcDims.size();

    size_t nAttrs = desc.getAttributes().size();
    bool isEmptyable = (desc.getEmptyBitmapAttribute() != NULL);
    if (isEmptyable && desc.getEmptyBitmapAttribute()->getId() != nAttrs-1) {
        throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_REDISTRIBUTE_ERROR1);
    }

    // In redistribute, we never cache empty bitmap, for now
    bool shouldCacheEmptyBitmap = false;
    
    boost::shared_ptr<Array> outputArray;
    ArrayID resultArrayId = 0;
    SystemCatalog* catalog = NULL;
    PhysicalBoundaries bounds = PhysicalBoundaries::createEmpty(nDims);

    if (resultArrayName.empty()) {
        outputArray = createTmpArray(desc);
        LOG4CXX_DEBUG(logger, "Temporary array was opened")
    } else {
        outputArray = boost::shared_ptr<Array>(new DBArray(resultArrayName, query));
        resultArrayId = outputArray->getHandle();
        catalog = SystemCatalog::getInstance();
        LOG4CXX_DEBUG(logger, "Array " << resultArrayName << " was opened")
    }

    ArrayDesc const& dstDesc = outputArray->getArrayDesc();
    Dimensions const& dims = dstDesc.getDimensions();

    barrier(0, networkManager, query, instanceCount);
    /**
     * Assigning result of this operation for current query and signal to concurrent handlers that they
     * can continue to work (after the barrier)
     */
    shared_ptr<SGContext> sgCtx = make_shared<SGContext>(shouldCacheEmptyBitmap, instanceCount, outputArray);
    query->setOperatorContext(sgCtx);

    /**
     *  Sending out our parts of the input array
     */
    vector<boost::shared_ptr<ConstArrayIterator> > inputIters;

    for (AttributeID attrId = 0; attrId < nAttrs; attrId++)
    {
        inputIters.push_back(inputArray->getConstIterator(attrId));
    }

    // we want to set outputIters lazily, after getting mutex.
    // Here we just allocate placeholders.
    vector<boost::shared_ptr<ArrayIterator> > outputIters(nAttrs);

    while (!inputIters[0]->end())
    {
        Coordinates chunkPosition = inputIters[0]->getPosition();
        instanceID = getInstanceForChunk(query, chunkPosition,desc,ps,distMapper,shift,instanceID, psData);

        // sharedEmptyBitmap is the empty bitmap at a given chunkPos, to be shared by all the attributes.
        shared_ptr<ConstRLEEmptyBitmap> sharedEmptyBitmap;

        for (AttributeID attrId = nAttrs; attrId-- != 0;)
        {
            // Sending current chunk to instanceID instance or saving locally
            const ConstChunk& chunk = inputIters[attrId]->getChunk();
            if (chunk.getSize() > 0)
            {
                if (resultArrayId != 0 && attrId == nAttrs - 1) {
                    bounds.updateFromChunk(&chunk, dstDesc.getEmptyBitmapAttribute() == NULL);
                }

                AttributeDesc const& attributeDesc = chunk.getAttributeDesc();
                assert(attributeDesc.getId() == attrId);

                const Coordinates& coordinates = chunk.getFirstPosition(false);

                boost::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
                if (chunk.isRLE() && isEmptyable) {
                    if (!sharedEmptyBitmap) {
                        sharedEmptyBitmap = chunk.getEmptyBitmap();
                    }
                    if (!attributeDesc.isEmptyIndicator()) {
                        emptyBitmap = sharedEmptyBitmap;
                    }
                }
                if ((InstanceID)instanceID != query->getInstanceID() || ps == psReplication)
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
                        networkManager->send(instanceID, chunkMsg);
                        LOG4CXX_TRACE(logger, "Sending chunk with att=" << attrId << " to instance=" << instanceID)
                    }
                    else {
                        networkManager->sendOutMessage(chunkMsg);
                        LOG4CXX_TRACE(logger, "Sending out chunk with att=" << attrId << " to every instance")
                    }
                    totalBytesSent += buffer->getDecompressedSize();
                    if (totalBytesSent > totalBytesSynced + networkBufferLimit) {
                        sync(networkManager, query, instanceCount);
                        totalBytesSynced = totalBytesSent;
                    }
                }
                if ((InstanceID)instanceID == query->getInstanceID() || ps == psReplication)
                {
                    ScopedMutexLock cs(query->resultCS);
                    if(sgCtx->_targetVersioned)
                    {
                        sgCtx->_newChunks.insert(coordinates);
                    }
                    assert(attrId < nAttrs);
                    if (!outputIters[attrId]) {
                        outputIters[attrId] = outputArray->getIterator(attrId);
                    }
                    if (outputIters[attrId]->setPosition(coordinates)) {
                        Chunk& dstChunk = outputIters[attrId]->updateChunk();
                        if (dstChunk.isReadOnly()) {
                            throw USER_EXCEPTION(SCIDB_SE_REDISTRIBUTE, SCIDB_LE_CANT_MERGE_READONLY_CHUNK);
                        }
                        dstChunk.merge(chunk, query);
                    } else {
                        outputIters[attrId]->copyChunk(chunk, emptyBitmap);
                    }
                }
            }
            ++(*inputIters[attrId]);
        }
    }

    sync(networkManager, query, instanceCount);
    barrier(1, networkManager, query, instanceCount);
    LOG4CXX_DEBUG(logger, "SG termination barrier reached.");

    /**
     * Reset SG Context to NULL
     */
    query->unsetOperatorContext();

    if (sgCtx->onSGCompletionCallback) { 
        LOG4CXX_DEBUG(logger, "Invoke SG callback.");
        sgCtx->onSGCompletionCallback(sgCtx->callbackArg);
    }

    if (resultArrayId != 0) {
        catalog->updateArrayBoundaries(dstDesc, bounds);
        if (sgCtx->_targetVersioned)
        {   //storing sg and array is mutable - insert tombstones:
            StorageManager::getInstance().removeDeadChunks(outputArray->getArrayDesc(), sgCtx->_newChunks, query);
        }

        StorageManager::getInstance().flush();
        query->replicationBarrier();

        Dimensions const& srcDims = inputArray->getArrayDesc().getDimensions();
        for (size_t i = 0; i < nDims; i++) {
            string const& dstMappingArrayName = dims[i].getMappingArrayName();
            string const& srcMappingArrayName = srcDims[i].getMappingArrayName();
            if (dims[i].getType() != TID_INT64 && srcMappingArrayName != dstMappingArrayName) { 
                boost::shared_ptr<Array> tmpMappingArray = query->getTemporaryArray(srcMappingArrayName);
                if (tmpMappingArray) { 
                    // Source array contains temporary NIDs, when we store this array in database, we also need to persist NIDs
                    DBArray dbMappingArray(dstMappingArrayName, query);
                    ArrayDesc const& tmpMappingArrayDesc = tmpMappingArray->getArrayDesc();
                    ArrayDesc const& dbMappingArrayDesc = dbMappingArray.getArrayDesc();
                    if (query->getCoordinatorID() == COORDINATOR_INSTANCE 
                        && tmpMappingArrayDesc.getDimensions()[0].getChunkInterval() != dbMappingArrayDesc.getDimensions()[0].getChunkInterval()) { 
                        LOG4CXX_DEBUG(logger, "Update chunk interval for NID array " << dbMappingArrayDesc.getName() << "(" << dbMappingArrayDesc.getId() << ") from " << dbMappingArrayDesc.getDimensions()[0].getChunkInterval() << " to " << tmpMappingArrayDesc.getDimensions()[0].getChunkInterval());
                        SystemCatalog::getInstance()->updateArray(ArrayDesc(dbMappingArrayDesc.getId(),
                                                                            dbMappingArrayDesc.getUAId(),
                                                                            dbMappingArrayDesc.getVersionId(),
                                                                            dbMappingArrayDesc.getName(),
                                                                            dbMappingArrayDesc.getAttributes(),
                                                                            tmpMappingArrayDesc.getDimensions(),
                                                                            dbMappingArrayDesc.getFlags()));
                    }
                    shared_ptr<ArrayIterator> srcArrayIterator = tmpMappingArray->getIterator(0);
                    shared_ptr<ArrayIterator> dstArrayIterator = dbMappingArray.getIterator(0);
                    Chunk& dstChunk = dstArrayIterator->newChunk(srcArrayIterator->getPosition(), 0);
                    ConstChunk const& srcChunk = srcArrayIterator->getChunk();
                    PinBuffer scope(srcChunk);
                    dstChunk.setRLE(false);
                    dstChunk.allocate(srcChunk.getSize());
                    memcpy(dstChunk.getData(), srcChunk.getData(), srcChunk.getSize());
                    dstChunk.write(query);
                }
            }
        }
    }
    LOG4CXX_DEBUG(logger, "Finishing SCATTER/GATHER work; sent " << totalBytesSent << " bytes.");
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
        const size_t nInstances = (size_t)query->getInstancesCount();
        const size_t myInstanceId = (size_t)query->getInstanceID();

        shared_ptr<SharedBuffer> buf;
        if (myInstanceId != 0)
        {
            networkManager->send(0, localBoundaries.serialize(), query);
            localBoundaries = PhysicalBoundaries::deSerialize(networkManager->receive(0, query));
        }
        else
        {
            for (size_t instance = 1; instance < nInstances; instance++)
            {
                PhysicalBoundaries receivedBoundaries = PhysicalBoundaries::deSerialize(networkManager->receive(instance,query));
                localBoundaries = localBoundaries.unionWith(receivedBoundaries);
            }

            for (size_t instance = 1; instance < nInstances; instance++)
            {
                networkManager->send(instance, localBoundaries.serialize(), query);
            }
        }
    }

    return localBoundaries;
}

void PhysicalBoundaries::updateFromChunk(ConstChunk const* chunk, bool chunkShapeOnly)
{
    size_t nDims = _startCoords.size();
    if (chunk == NULL)
    {   return; }

    //chunk iteration is expensive - only perform if needed
    Coordinates const& chunkFirstPos = chunk->getFirstPosition(false), chunkLastPos = chunk->getLastPosition(false);
    bool updateLowBound = false, updateHiBound = false;
    for (size_t i = 0; i < nDims; i++)
    {
       if (chunkFirstPos[i] < _startCoords[i])
       {
           if ( chunkShapeOnly )
           {    _startCoords[i] = chunkFirstPos[i]; }
           else
           {   updateLowBound = true; }
       }

       if (chunkLastPos[i] > _endCoords[i])
       {
           if ( chunkShapeOnly)
           {    _endCoords[i] = chunkLastPos[i]; }
           else
           {    updateHiBound = true; }
       }
    }

    //The chunk is inside the box and cannot expand the bounds. Early exit.
    if (!updateLowBound && !updateHiBound)
    {   return; }

    //TODO: there is a further optimization opportunity here. The given chunk *should* always be a bitmap
    //chunk. Once we verify that, we can iterate over bitmap segments and compute coordinates. Committing
    //this due to timing constraints - should revisit and optimize this when possible.
    boost::shared_ptr<ConstChunkIterator> citer = chunk->materialize()->getConstIterator();
    while (!citer->end() && (updateLowBound || updateHiBound))
    {
        Coordinates const& pos = citer->getPosition();
        bool updated = false;
        for (size_t j = 0; j < nDims; j++)
        {
            if (updateHiBound && pos[j] > _endCoords[j])
            {
                _endCoords[j] = pos[j];
                updated = true;
            }
            if (updateLowBound && pos[j] < _startCoords[j])
            {
                _startCoords[j] = pos[j];
                updated = true;
            }
        }
        if(updated) //it's likely that no further update can come from this chunk
        {
            if(updateHiBound)
            {
                size_t k=0;
                while (k<nDims && _endCoords[k]>=chunkLastPos[k])
                { k++; }
                if (k==nDims) //no more useful data for hi bound could come from this chunk!
                {   updateHiBound=false; }
            }
            if(updateLowBound)
            {
                size_t k=0;
                while (k<nDims && _startCoords[k]<=chunkFirstPos[k])
                { k++; }
                if (k==nDims) //no more useful data for lo bound could come from this chunk!
                {   updateLowBound=false; }
            }
        }
        ++(*citer);
    }
}

PhysicalBoundaries PhysicalBoundaries::trimToDims(Dimensions const& dims) const
{
    SCIDB_ASSERT(_startCoords.size() == dims.size());
    size_t nDims = dims.size();

    Coordinates resStart(nDims);
    Coordinates resEnd(nDims);

    for(size_t i=0; i<nDims; i++)
    {
        resStart[i] = std::max<Coordinate> (dims[i].getStartMin(), _startCoords[i]);
        resEnd[i] = std::min<Coordinate> (dims[i].getEndMax(), _endCoords[i]);
    }

    return PhysicalBoundaries(resStart, resEnd, _density);
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
        const size_t nInstances = (size_t)query->getInstancesCount();
        for (size_t instance = 1; instance < nInstances; instance++)
        {
            networkManager->send(instance, buf, query);
        }
    }
};

template <class SetType, class MapType>
boost::shared_ptr<MapType> __buildSortedXIndex( SetType& attrSet,
                                                boost::shared_ptr<Query> query,
                                                string const& indexArrayName,
                                                Coordinate indexArrayStart,
                                                uint64_t maxElements,
                                                bool distinct = true)
{
    shared_ptr<SharedBuffer> buf;
    NetworkManager* networkManager = NetworkManager::getInstance();
    const size_t nInstances = (size_t)query->getInstancesCount();
    const size_t myInstanceId = (size_t)query->getInstanceID();

    if (myInstanceId != 0)
    {
        networkManager->send(0, attrSet.sort(true), query);
        buf = networkManager->receive(0, query);
    }
    else
    {
        for (size_t instance = 1; instance < nInstances; instance++)
        {
            attrSet.add(networkManager->receive(instance, query), instance);
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
            SystemCatalog::getInstance()->addArray(indexMapDesc, psReplication);
            assert(indexMapDesc.getId()>0);
        }
    }

    size_t *nCoordsPtr = (size_t*)((char*)buf->getData() + buf->getSize() - sizeof(size_t));
    size_t nCoords = *nCoordsPtr;

    //
    // Store array with coordinates index locally at each instance
    //
    if (nCoords != 0 && indexArrayName.size())
    {
        DBArray indexMapArray(indexArrayName, query);
        shared_ptr<ArrayIterator> arrayIterator = indexMapArray.getIterator(0);
        Coordinates start(1);
        start[0] = indexArrayStart;
        Chunk& chunk = arrayIterator->newChunk(start, 0);
        size_t chunkSize = buf->getSize() - sizeof(size_t);
        if (!distinct) { 
            chunkSize -= nCoords*sizeof(uint16_t);
        }
        chunk.allocate(chunkSize);
        chunk.setRLE(false); 
        memcpy(chunk.getData(), buf->getData(), chunkSize);
        chunk.write(query);
    }
    if (nCoords != 0)
    {                                               
        if (distinct) { 
            return boost::shared_ptr<MapType>(new MapType(attrSet.getType(), indexArrayStart, nCoords, buf->getData(), buf->getSize() - sizeof(size_t)));
        } else { 
            return boost::shared_ptr<MapType>(new MapType(attrSet.getType(), indexArrayStart, nCoords, buf->getData(), buf->getSize() - sizeof(size_t), myInstanceId));
        }
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
                                                uint64_t maxElements,
                                                bool distinct = true)
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
                    if (!value.isNull())
                    {
                        attrSet.add(value);
                    }
                    ++(*chunkIterator);
                }
            }
            ++(*arrayIterator);
        }
    }

    return __buildSortedXIndex<SetType, MapType> (attrSet, query, indexArrayName, indexArrayStart, maxElements, distinct);
}

boost::shared_ptr<AttributeMultiMap> buildFunctionalMapping(DimensionDesc const& dim)
{
    FunctionLibrary* lib = FunctionLibrary::getInstance();
    std::vector<TypeId> inputArgTypes(3, TID_INT64);
    std::vector<FunctionPointer> converters;
    bool supportsVectorMode;
    FunctionDescription toDesc, fromDesc;

    if (lib->findFunction(dim.getType(), inputArgTypes, fromDesc, converters, supportsVectorMode, false) && fromDesc.getOutputArg() == dim.getType() && converters.size() == 0) {
        inputArgTypes[0] = dim.getType();
        if (lib->findFunction("ordinal", inputArgTypes, toDesc, converters, supportsVectorMode, false) && toDesc.getOutputArg() == TID_INT64 && converters.size() == 0) {
            return  boost::shared_ptr<AttributeMultiMap>(new AttributeMultiMap(dim, toDesc.getFuncPtr(), fromDesc.getFuncPtr()));
        }
    }
    return boost::shared_ptr<AttributeMultiMap>();
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

boost::shared_ptr<AttributeMultiMap> buildSortedMultiIndex (AttributeBag& attrSet,
                                                            boost::shared_ptr<Query> query,
                                                            string const& indexArrayName,
                                                            Coordinate indexArrayStart,
                                                            uint64_t maxElements)
{
    return __buildSortedXIndex<AttributeBag, AttributeMultiMap> (attrSet, query, indexArrayName, indexArrayStart, maxElements, false);
}

boost::shared_ptr<AttributeMultiMap> buildSortedMultiIndex (shared_ptr<Array> srcArray,
                                                            AttributeID attrID,
                                                            boost::shared_ptr<Query> query,
                                                            string const& indexArrayName,
                                                            Coordinate indexArrayStart,
                                                            uint64_t maxElements)
{
    return __buildSortedXIndex<AttributeBag, AttributeMultiMap> (srcArray, attrID, query, indexArrayName, indexArrayStart, maxElements, false);
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
                                                                             Cluster::getInstance()->getLocalInstanceId(),
                                                                             SystemCatalog::LockDesc::COORD,
                                                                             SystemCatalog::LockDesc::RD));
        query->requestLock(lock);
    }
}

InjectedErrorListener<OperatorInjectedError> PhysicalOperator::_injectedErrorListener;

boost::shared_ptr<ThreadPool> PhysicalOperator::_globalThreadPoolForOperators;
boost::shared_ptr<JobQueue> PhysicalOperator::_globalQueueForOperators;
Mutex PhysicalOperator::_mutexGlobalQueueForOperators;

boost::shared_ptr<JobQueue> PhysicalOperator::getGlobalQueueForOperators()
{
    ScopedMutexLock cs(_mutexGlobalQueueForOperators);
    if (!_globalThreadPoolForOperators) {
        _globalQueueForOperators = boost::shared_ptr<JobQueue>(new JobQueue());
        _globalThreadPoolForOperators = boost::shared_ptr<ThreadPool>(
                new ThreadPool(Config::getInstance()->getOption<int>(CONFIG_EXEC_THREADS), _globalQueueForOperators));
        _globalThreadPoolForOperators->start();
    }
    return _globalQueueForOperators;
}

} //namespace
