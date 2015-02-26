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

#include "array/RLEArray.h"
#include "array/Compressor.h"

#include <log4cxx/logger.h>
#include "smgr/io/Storage.h"

namespace scidb
{

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.rqarray"));

/************* ConstRQChunk *************/

void ConstRLEChunk::initialize(ArrayDesc const * desc, const Address &address, int compMethod)
{
    _hasOverlap = false;
    _compressionMethod = compMethod;
    _arrayDesc = desc;
    _firstPositionNoOlap = address.coords;
    _addr = address;
    Dimensions dim = desc->getDimensions();

    _firstPosition.clear();
    _lastPositionNoOlap.clear();
    _lastPosition.clear();
    _chunkIntervals.clear();

    for (uint32_t i = 0; i < dim.size(); ++i)
    {
        if (dim[i].getChunkOverlap())
        {
            _hasOverlap = true;
        }

        _firstPosition.push_back( std::max<Coordinate>(_firstPositionNoOlap[i] - dim[i].getChunkOverlap(), dim[i].getStart()));
        _lastPosition.push_back( std::min<Coordinate>(_firstPositionNoOlap[i] + dim[i].getChunkInterval() + 2 * dim[i].getChunkOverlap() - 1, dim[i].getEndMax()));
        _lastPositionNoOlap.push_back( std::min<Coordinate>(_firstPositionNoOlap[i] + dim[i].getChunkInterval() - 1, dim[i].getEndMax()));

        _chunkIntervals.push_back(_lastPosition[i] - _firstPosition[i] + 1);
    }

}

void ConstRLEChunk::initialize(ArrayDesc const * desc, const Address &address, int compMethod,
                               ConstRLEPayload const& payload, shared_ptr<RLEEmptyBitmap> bitMask)
{
    initialize(desc, address, compMethod);
    _payload = payload;
    _emptyMask = bitMask;
}

size_t ConstRLEChunk::count() const
{
    return _emptyMask->count();
}

bool ConstRLEChunk::isCountKnown() const
{
    return true;
}

int ConstRLEChunk::getCompressionMethod() const
{
    return _compressionMethod;
}

void ConstRLEChunk::compress(CompressedBuffer& buf) const
{
    //do we compress just the data or the bitmask too?
    throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NOT_IMPLEMENTED) << "ConstRLEChunk::compress";
}

void ConstRLEChunk::getCoordinatePayloads(vector<char*> &coordPayloads,
                                          vector<size_t>& coordSizes,
                                          boost::shared_ptr<Query>& query) const
{
    size_t nCoords = _firstPosition.size();
    vector<bool> nonIntCoords(nCoords, false);
    coordSizes = vector<size_t> (nCoords, 0);
    coordPayloads = vector<char*> (nCoords, 0);
    vector<char*> coordWriters(nCoords, 0);

    Dimensions const& dims = _arrayDesc->getDimensions();
    for (size_t i = 0; i < nCoords; i++)
    {
        if (dims[i].getType() != TID_INT64)
        {
            nonIntCoords[i] = true;
            coordSizes[i] = TypeLibrary::getType(dims[i].getType()).byteSize();
            if (coordSizes[i] == 0)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NOT_IMPLEMENTED)
                    << "filter on var-size dimensions";
            }
        }
        else
        {
            coordSizes[i] = sizeof(int64_t);
        }
        coordPayloads[i] = new char[coordSizes[i] * count()];
        coordWriters[i] = coordPayloads[i];
    }

    ConstRLEEmptyBitmap::iterator iter = _emptyMask->getIterator();
    while (!iter.end())
    {
        size_t pos = iter.getLPos();
        for (int i = (int) nCoords; --i >= 0;)
        {
            int64_t coord = _firstPosition[i] + (pos % _chunkIntervals[i]);
            pos /= _chunkIntervals[i];
            if (!nonIntCoords[i])
            {
                *((int64_t*) coordWriters[i]) = coord;
            }
            else
            {
               Value v = StorageManager::getInstance().reverseMapCoordinate(_arrayDesc->getCoordinateIndexArrayName(i),
                                                                            dims[i], coord, query);
                memcpy(coordWriters[i], v.data(), coordSizes[i]);
            }
            coordWriters[i] += coordSizes[i];
        }
        ++iter;
    }

}

inline void ConstRLEChunk::pos2coord(uint32_t const& pos, Coordinates& coord) const
{
    uint32_t currentPos = pos;
    for (int i = (int) coord.size(); --i >= 0;)
    {
        coord[i] = _firstPosition[i] + (currentPos % _chunkIntervals[i]);
        currentPos /= _chunkIntervals[i];
    }
}

inline uint32_t ConstRLEChunk::coord2pos(Coordinates const& coord) const
{
    uint32_t pos = 0;
    for (size_t i = 0, n = coord.size(); i < n; i++)
    {
        pos *= _chunkIntervals[i];
        pos += coord[i] - _firstPosition[i];
    }
    return pos;
}

boost::shared_ptr<ConstChunkIterator> ConstRLEChunk::getConstIterator(int mode) const
{
    if (getAttributeDesc().getType()==TID_INDICATOR)
    {
        return boost::shared_ptr<ConstChunkIterator>(new ConstRLEBitmaskIterator(*_arrayDesc, _addr.attId, this, mode));
    }
    return boost::shared_ptr<ConstChunkIterator>(new ConstRLEChunkIterator(*_arrayDesc, _addr.attId, this, mode));

}

const Coordinates& ConstRLEChunk::getFirstPosition(bool withOverlap) const
{
    if (withOverlap)
    {
        return _firstPosition;
    }
    return _firstPositionNoOlap;
}

const Coordinates& ConstRLEChunk::getLastPosition(bool withOverlap) const
{
    if (withOverlap)
    {
        return _lastPosition;
    }
    return _lastPositionNoOlap;
}

const ArrayDesc& ConstRLEChunk::getArrayDesc() const
{
    return *_arrayDesc;
}

const AttributeDesc& ConstRLEChunk::getAttributeDesc() const
{
    return _arrayDesc->getAttributes()[_addr.attId];
}


/*******************************************************************/

ConstRLEBitmaskIterator::ConstRLEBitmaskIterator(ArrayDesc const &array, const AttributeID attId, ConstRLEChunk const * const dataChunk, int mode):
    _source(dataChunk),
    _coords(dataChunk->_firstPosition.size()),
    _mode(mode),
    _emptyMask(dataChunk->getEmptyMask()),
    _bmIter(_source->getEmptyMask()->getIterator())
{
    if (_mode & ChunkIterator::IGNORE_OVERLAPS)
    {
        _emptyMask = getOverlapMask();
        _bmIter = _emptyMask->getIterator();
    }
}

ConstRLEEmptyBitmap const* ConstRLEBitmaskIterator::getOverlapMask()
{
    if (_overlapMask.get()) //we built it before
    {
        return (ConstRLEEmptyBitmap const*) _overlapMask.get();
    }

    if(_source->hasOverlap())
    {
        Coordinates const& startNoOverlap = _source->getFirstPosition(false);
        Coordinates const& endNoOverlap = _source->getLastPosition(false);
        Coordinates c(startNoOverlap.size(), 0);

        ConstRLEEmptyBitmap::iterator iter = _emptyMask->getIterator();

        _overlapMask.reset(new RLEEmptyBitmap());
        while (!iter.end())
        {
            _source->pos2coord( iter.getLPos(), c);
            bool include = true;
            for (size_t i=0; i< startNoOverlap.size(); i++)
            {
                if (c[i] < startNoOverlap[i] || c[i] > endNoOverlap[i])
                {
                    include = false;
                }
            }

            if(include)
            {
                _overlapMask->addPositionPair(iter.getLPos(), iter.getPPos());
            }

            ++iter;
        }
        return (ConstRLEEmptyBitmap const*) _overlapMask.get();
    }

    return _emptyMask;
}

Coordinates const& ConstRLEBitmaskIterator::getPosition()
{
    _source->pos2coord(_bmIter.getLPos(), _coords);
    return _coords;
}

bool ConstRLEBitmaskIterator::setPosition(Coordinates const& pos)
{
    position_t p = _source->coord2pos(pos);
    return _bmIter.setPosition(p);
}

void ConstRLEBitmaskIterator::reset()
{
    _bmIter.reset();
}

bool ConstRLEBitmaskIterator::isEmpty()
{
    return false;
}

bool ConstRLEBitmaskIterator::end()
{
    return _bmIter.end();
}

Value & ConstRLEBitmaskIterator::getItem()
{
    if (end())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
    }

    _value.setBool(true);
    return _value;
}

void ConstRLEBitmaskIterator::operator ++()
{
    ++_bmIter;
}

ConstChunk const& ConstRLEBitmaskIterator::getChunk()
{
    return *_source;
}


ConstRLEChunkIterator::ConstRLEChunkIterator(ArrayDesc const &array, const AttributeID attId, ConstRLEChunk const * const dataChunk, int mode) :
    ConstRLEBitmaskIterator(array, attId, dataChunk, mode), _pIter(dataChunk->getPayload().getIterator())
{}

Value& ConstRLEChunkIterator::getItem()
{
    if (end())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
    }

    position_t pos = _bmIter.getPPos();

    //uncommon case: _pIter is out of sync due to a setPosition call
    if (_pIter.end() || _pIter.getPPos() > pos)
    {
        if(!_pIter.setPosition(pos))
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
        }
    }

    //more common case: _pIter is a few elements behind _bmIter
    while(_pIter.getPPos() < pos)
    {
        ++_pIter;
    }

    _pIter.getItem(_value);
    return _value;
}

}

