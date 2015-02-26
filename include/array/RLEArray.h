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

#ifndef _RQ_ARRAY_
#define _RQ_ARRAY_

#include "array/Array.h"
#include "array/Bitmask.h"
#include "query/TypeSystem.h"
#include "array/MemArray.h"

#include <boost/scoped_ptr.hpp>
#include <log4cxx/logger.h>

// Array and ArrayIterator will not really change
// all of this focuses on the reimplementation of chunk and its iterator
namespace scidb
{

class ConstRLEChunk;
class RLEArray;

//basically behaves like ConstArrayIterator, just gives access to a RQChunk
class ConstRLEArrayIterator: public ConstArrayIterator
{
public:
    virtual ConstRLEChunk const& getRQChunk() = 0;
    virtual ConstChunk const& getChunk()
    {
        return (ConstChunk const&) getRQChunk();
    }
};

class RLEArray: public Array
{
    friend class RQArrayIterator;
    friend class ConstRLEArrayIterator;

public:
    RLEArray(ArrayDesc const& arr) :
        desc(arr)
    {}

    virtual bool isRLE() const
    {
        return true;
    }

    virtual ArrayDesc const& getArrayDesc() const
    {
        return desc;
    }

    virtual boost::shared_ptr<ChunkIterator> getIterator(int iterationMode = ChunkIterator::NO_EMPTY_CHECK) const
    {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ILLEGAL_OPERATION) << "ConstRLEArrayIterator::getIterator";
    }

    virtual boost::shared_ptr<ConstRLEArrayIterator> getConstRqArrayIterator(AttributeID attId) const
    {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ILLEGAL_OPERATION) << "ConstRLEArrayIterator::getConstRqArrayIterator";
    }

    virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attId) const
    {
        return boost::static_pointer_cast<ConstArrayIterator> (getConstRqArrayIterator(attId));
    }

    //TODO: [ap] this will guarantee that the client reads the array from a single thread.
    //When ready for multiple concurrent readers - remove this method and test
    virtual bool supportsRandomAccess() const
    {
        return false;
    }

protected:
    const ArrayDesc desc;
};

/* chunk-level metadata -- this needs to go in the chunk header and thus is NYI
 struct ChunkMetadata {
 uint32_t count; // payload count
 uint32_t nonEmpties; // 1s in the empty bitmask
 Value min;
 Value max;
 Value sum;
 uint32_t additionalData[256]; // reserved for later
 } */

class ConstRLEChunk: public ConstChunk
{
    friend class ConstRLEBitmaskIterator;
    friend class ConstRLEArrayIterator;
    friend class RLEArray;

public:
    ConstRLEChunk()
    {}

    virtual ~ConstRLEChunk()
    {}

    //Create empty chunk with NULL payload and NULL bitmask
    void initialize(ArrayDesc const* desc, const Address &address, int compressionMethod);

    void initialize(ArrayDesc const* desc, const Address &address, int compressionMethod,
                    ConstRLEPayload const& payload, shared_ptr<RLEEmptyBitmap> bitMask);

    // number of elements in payload
    size_t count() const;

    // number of materialized elements + missings
    size_t getNonEmpties() const;

    bool isCountKnown() const;
    int getCompressionMethod() const;

    void compress(CompressedBuffer& buf) const;

    void getCoordinatePayloads(vector<char*> & coordPayloads, vector<size_t>& coordSizes, boost::shared_ptr<Query>& query) const;

    void pos2coord(uint32_t const& pos, Coordinates& coord) const;
    uint32_t coord2pos(Coordinates const& coord) const;
    void setCompressionMethod(uint32_t compressMethod)
    {
        _compressionMethod = compressMethod;
    }

    const Coordinates& getFirstPosition(bool withOverlap) const;
    const Coordinates& getLastPosition(bool withOverlap) const;

    const ArrayDesc& getArrayDesc() const;
    const AttributeDesc& getAttributeDesc() const;

    boost::shared_ptr<ConstChunkIterator> getConstIterator(int) const;

    bool isInitialized() const;

    Address const& getAddress() const
    {
        return _addr;
    }

    ConstRLEEmptyBitmap const* getEmptyMask() const
    {
        return (ConstRLEEmptyBitmap*) _emptyMask.get();
    }

    ConstRLEPayload const& getPayload() const
    {
        return (ConstRLEPayload&) _payload;
    }

    void setMask (boost::shared_ptr<RLEEmptyBitmap> emptyMask)
    {
        _emptyMask = emptyMask;
    }

    void setPayload(ConstRLEPayload const& payload)
    {
        _payload = payload;
    }

    inline bool hasOverlap() const
    {
        return _hasOverlap;
    }

protected:
    Address _addr;
    const ArrayDesc *_arrayDesc;
    Coordinates _firstPosition, _lastPosition, _firstPositionNoOlap, _lastPositionNoOlap;
    std::vector<size_t> _chunkIntervals;
    uint32_t _compressionMethod;

    bool _hasOverlap;
    RLEPayload _payload;
    boost::shared_ptr<RLEEmptyBitmap> _emptyMask;
};

class ConstRLEBitmaskIterator: public ConstChunkIterator
{
protected:
    const ConstRLEChunk * _source;
    Coordinates _coords;
    uint32_t _mode;

    ConstRLEEmptyBitmap const* _emptyMask;
    ConstRLEEmptyBitmap::iterator _bmIter;

    //if we need to IGNORE_OVERLAPS, we create a temporary overlap mask and place it here.
    boost::scoped_ptr<RLEEmptyBitmap> _overlapMask;
    Value _value;

public:
    ConstRLEBitmaskIterator(ArrayDesc const &array, const AttributeID attId, ConstRLEChunk const * const dataChunk, int mode);
    ~ConstRLEBitmaskIterator() {}
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos);
    virtual void reset();
    virtual bool isEmpty();
    virtual bool end();
    virtual Value& getItem();
    virtual void operator ++();

    virtual ConstRLEEmptyBitmap const* getOverlapMask();

    virtual ConstChunk const& getChunk();
    virtual int getMode()
    {
        return _mode;
    }
};

class ConstRLEChunkIterator: public ConstRLEBitmaskIterator
{
protected:
    ConstRLEPayload::iterator _pIter;

public:
    ConstRLEChunkIterator(ArrayDesc const &array, const AttributeID attId, ConstRLEChunk const * const dataChunk, int mode);
    virtual ~ConstRLEChunkIterator() {}
    virtual Value& getItem();
};



}

#endif
