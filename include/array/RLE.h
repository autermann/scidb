/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) © 2008-2011 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation version 3 of the License, or
* (at your option) any later version.
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

#ifndef __RLE_H__
#define __RLE_H__

#include <stdlib.h>
#include <assert.h>
#include <inttypes.h>
#include <string.h>
#include <map>
#include <vector>
#include <boost/utility.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/serialization/access.hpp>
#include <boost/serialization/split_member.hpp>
#include "util/StackAlloc.h"

namespace scidb
{

class Value;
class ConstChunk;
class RLEPayload;
class Query;
class ArrayDesc;
    
typedef int64_t position_t;
typedef int64_t Coordinate;
typedef std::vector<Coordinate> Coordinates;


#define USE_STACK_ALLOCATOR_FOR_VALUE_MAP true

#ifdef USE_STACK_ALLOCATOR_FOR_VALUE_MAP
typedef std::map<position_t, Value, std::less<position_t>, StackAlloc<std::pair<position_t, Value> > > ValueMap;
#else
typedef std::map<position_t, Value> ValueMap;
#endif

extern bool checkChunkMagic(ConstChunk const& chunk);

class RLEEmptyBitmap;
class ConstRLEEmptyBitmap
{
    friend class RLEEmptyBitmap;
public:
    struct Segment { 
        position_t lPosition;   // start position of sequence of set bits
        position_t length;  // number of set bits
        position_t pPosition; // index of value in payload
    };

    struct Header { 
        uint64_t magic;
        size_t   nSegs;
        uint64_t nNonEmptyElements;
    };

  protected:
    size_t nSegs;
    Segment* seg;
    uint64_t nNonEmptyElements;
    ConstChunk const* chunk;
    bool chunkPinned;

    /**
     * Default constructor
     */
    ConstRLEEmptyBitmap() {
        nSegs = 0;
        nNonEmptyElements = 0;
        seg = NULL;
        chunk  = NULL;
    }

  public:
    size_t getValueIndex(position_t pos) const { 
        size_t l = 0, r = nSegs;
        while (l < r) { 
            size_t m = (l + r) >> 1;
            if (seg[m].lPosition + seg[m].length <= pos) { 
                l = m + 1;
            } else { 
                r = m;
            }
        }
        return (r < nSegs && seg[r].lPosition <= pos) ? seg[r].pPosition + pos - seg[r].lPosition : size_t(-1);
    }

    /**
     * Check if element at specified position is empty
     */
    bool isEmpty(position_t pos) const { 
        size_t l = 0, r = nSegs;
        while (l < r) { 
            size_t m = (l + r) >> 1;
            if (seg[m].lPosition + seg[m].length <= pos) { 
                l = m + 1;
            } else { 
                r = m;
            }
        }
        return r == nSegs || seg[r].lPosition > pos;
    }

    /**
     * Get number of RLE segments
     */
    size_t nSegments() const { 
        return nSegs;
    }

    /**
     * Get next i-th segment corresponding to non-empty elements
     */
    Segment const& getSegment(size_t i) const {
        assert(i < nSegs);
        return seg[i];
    }

    /**
     * Find segment of non-empty elements with position greater or equal than specified.
     */
    size_t findSegment(position_t pos) const { 
        size_t l = 0, r = nSegs;
        while (l < r) { 
            size_t m = (l + r) >> 1;
            if (seg[m].lPosition + seg[m].length <= pos) { 
                l = m + 1;
            } else { 
                r = m;
            }
        }
        return r;            
    }


    /**
     * Method to be called to save bitmap in chunk body
     */
    void pack(char* dst) const;
    
    /**
     * Get size needed to pack bitmap (used to dermine size of chunk)
     */
    size_t packedSize() const;

    /**
     * Constructor for initializing Bitmap with raw chunk data
     */
    ConstRLEEmptyBitmap(char const* src);

    ConstRLEEmptyBitmap(ConstChunk const& chunk);

    virtual ~ConstRLEEmptyBitmap();

    class iterator
    {
      private:
        ConstRLEEmptyBitmap const* _bm;
        size_t _currSeg;
        Segment const* _cs;
        position_t _currLPos;

      public:
        iterator(ConstRLEEmptyBitmap const* bm): _bm(bm)
        {
            reset();
        }
        iterator() {}

            
        void reset()
        {
            _currSeg = 0;
            if(!end())
            {
                _cs = &_bm->getSegment(_currSeg);
                _currLPos = _cs->lPosition;
            }
        }

        bool end()
        {
            return _currSeg >= _bm->nSegments();
        }

        position_t const& getLPos()
        {
            assert(!end());
            return _currLPos;
        }

        position_t getPPos()
        {
            assert(!end());
            return _cs->pPosition + _currLPos - _cs->lPosition;
        }

        bool setPosition(position_t lPos)
        {
            _currSeg = _bm->findSegment(lPos);
            if (end() || _bm->getSegment(_currSeg).lPosition > lPos)
            {
                _currSeg = _bm->nSegments();
                return false;
            }
            _cs = &_bm->getSegment(_currSeg);
            _currLPos = lPos;
            return true;
        }

        bool skip(size_t n); 

        void operator ++()
        {
            assert(!end());
            if (_currLPos + 1 < _cs->lPosition + _cs->length)
            {
                _currLPos ++;
            }
            else
            {
                _currSeg++;
                if (!end())
                {
                    _cs = &_bm->getSegment(_currSeg);
                    _currLPos = _cs->lPosition;
                }
            }
        }
    };

    iterator getIterator() const
    {
        return iterator(this);
    }

    uint64_t count() const
    {
        return nNonEmptyElements;
    }

    /**
     * Merge THIS with VM and return the result as a new RLEEmptyBitmap
     */
    boost::shared_ptr<RLEEmptyBitmap> merge(ValueMap& vm);

    /**
     * Merge THIS with other bitmap
     */
    boost::shared_ptr<RLEEmptyBitmap> merge(ConstRLEEmptyBitmap const& other);

    /**
     * Extract subregion from bitmap.
     *
     * @param lowerOrigin lower coordinates of original array.
     * @param upperOrigin upper coordinates of original array.
     * @param lowerResult lower coordinates of subarray.
     * @param lowreResult lower coordinates of subarray.
     *
     * @return bitmap with same shape and zeros in (Original MINUS Subarray) areas.
     */
    boost::shared_ptr<RLEEmptyBitmap> cut(
            Coordinates const& lowerOrigin,
            Coordinates const& upperOrigin,
            Coordinates const& lowerResult,
            Coordinates const& upperResult) const;

    /**
     * Merge THIS with MERGEBITS and return the result as a new RLEEmptyBitmap
     * MERGEBITS must have one BIT for each "1" in THIS.
     */
    boost::shared_ptr<RLEEmptyBitmap> merge(uint8_t const* mergeBits);

    /**
     * Join THIS with other bitmap
     */
    boost::shared_ptr<RLEEmptyBitmap> join(ConstRLEEmptyBitmap const& other);
};

std::ostream& operator<<(std::ostream& stream, ConstRLEEmptyBitmap const& map);

class RLEEmptyBitmap : public ConstRLEEmptyBitmap
{
  private:
    std::vector<Segment> container;

    position_t addRange(position_t lpos, position_t ppos, uint64_t sliceSize, size_t level, Coordinates const& chunkSize, Coordinates const& origin, Coordinates const& first, Coordinates const& last);

  public:
    void reserve(size_t size) {
        container.reserve(size);
    }

    void clear()
    {
        container.clear();
        seg = NULL;
        nSegs = 0;
        nNonEmptyElements = 0;
    }

    void addSegment(Segment const& segm)
    {
        if (nSegs > 0)
        {
            assert(segm.lPosition >= container[nSegs-1].lPosition + container[nSegs-1].length &&
                   segm.pPosition >= container[nSegs-1].pPosition + container[nSegs-1].length);
        }

        container.push_back(segm);
        seg = &container[0];
        nNonEmptyElements += segm.length;
        nSegs++;
    }

    void addPositionPair(position_t const& lPosition, position_t const& pPosition)
    {
        nNonEmptyElements += 1;
        if (nSegs > 0 &&
            container[nSegs-1].lPosition + container[nSegs-1].length == lPosition &&
            container[nSegs-1].pPosition + container[nSegs-1].length == pPosition)
        {
            container[nSegs-1].length++;
        }
        else
        {
            Segment ns;
            ns.lPosition=lPosition;
            ns.pPosition=pPosition;
            ns.length=1;
            addSegment(ns);
        }
    }

    RLEEmptyBitmap& operator=(ConstRLEEmptyBitmap const& other)
    {
        nSegs = other.nSegments();
        nNonEmptyElements = other.nNonEmptyElements;
        container.resize(nSegs);
        memcpy(&container[0], other.seg, nSegs*sizeof(Segment));
        seg = &container[0];
        return *this;
    }

    RLEEmptyBitmap(ConstRLEEmptyBitmap const& other):
        ConstRLEEmptyBitmap()
    {
        *this = other;
    }

    RLEEmptyBitmap& operator=(RLEEmptyBitmap const& other)
    {
        nSegs = other.nSegs;
        nNonEmptyElements = other.nNonEmptyElements;
        container = other.container;
        seg = &container[0];
        return *this;
    }

    RLEEmptyBitmap(RLEPayload& payload);

    RLEEmptyBitmap(RLEEmptyBitmap const& other):
            ConstRLEEmptyBitmap()
    {
        *this = other;
    }

    /**
     * Default constructor
     */
    RLEEmptyBitmap(): ConstRLEEmptyBitmap()
    {}

    /*
     * Create fully dense bitmask of nBits bits
     */
    RLEEmptyBitmap(position_t nBits): ConstRLEEmptyBitmap(), container(1)
    {
        container[0].lPosition = 0;
        container[0].length = nBits;
        container[0].pPosition = 0;
        nSegs=1;
        nNonEmptyElements = nBits;
        seg = &container[0];
    }

    /**
     * Constructor of bitmap from ValueMap (which is used to be filled by ChunkIterator)
     */
    RLEEmptyBitmap(ValueMap& vm, bool all = false);

    /**
     * Constructor of RLE bitmap from dense bit vector 
     */
    RLEEmptyBitmap(char* data, size_t numBits);

    /**
     * Constructor for initializing Bitmap from specified chunk
     */
    RLEEmptyBitmap(ConstChunk const& chunk);
};

class RLEPayload;

/**
  * class ConstRLEPayload
  * This class allows to store values as stride-major-ordered array with RLE-pack of data.
  * We have the "payload" array where we store values.
  * "payload" array splited to separated parts, which name "segment"
  * Every "segment" has description (struct Segment), all Segments stored to "container"
  * Every Segment has following fields:
  *  - pPosition - physical position (stride-major-order) of first value from segment
  *  - valueIndex - byte number inside "payload" array where located data for this segment, or "missingReason" if segment absent (nulls, empty, etc)
  *  - same - bit which describes, contain this segment equal values or different
  *  - null - bit which desribes, what exactly represent valueIndex.
  *
  * NOTE: Payload that is constructed from memory allocated elsewhere - not responsible for freeing memory.
  * NOTE: Cannot add values
  */
class ConstRLEPayload
{
friend class boost::serialization::access;
friend class RLEPayload;
public:
    struct Segment {
        position_t pPosition; // position in chunk of first element
        uint32_t   valueIndex : 30; // index of element in payload array or missing reason
        uint32_t   same:1; // sequence of same values
        uint32_t   null:1; // trigger if value is NULL (missingReason) or normal value (valueIndex)

        /**
         * NOTE: Danger method implementation!!!
         * If you copy structure to separate variable this method will not work
         * without any warnings.
         * First Idea to remove this method at all and force implement it directly in code.
         * This prevents user from wrong usage.
         */
        uint64_t length() const {
            return this[1].pPosition - pPosition;
        }

        template<class Archive>
        void save(Archive & ar, const unsigned int version) const
        {
            position_t pPosition__ = pPosition;
            uint32_t valueIndex__ = valueIndex;
            uint8_t same__ = same;
            uint8_t null__ = null;
            ar & pPosition__;
            ar & valueIndex__;
            ar & same__;
            ar & null__;
        }
        template<class Archive>
        void load(Archive & ar, const unsigned int version)
        {
            position_t pPosition__;
            uint32_t valueIndex__;
            uint8_t same__;
            uint8_t null__;
            ar & pPosition__;
            ar & valueIndex__;
            ar & same__;
            ar & null__;
            pPosition = pPosition__;
            valueIndex = valueIndex__;
            same = same__;
            null = null__;
        }
        BOOST_SERIALIZATION_SPLIT_MEMBER()
    } __attribute__ ((packed));

    struct Header { 
        uint64_t magic;
        size_t   nSegs;
        size_t   elemSize;
        size_t   dataSize;
        size_t   varOffs;
        bool     isBoolean;
    };

  protected:
    size_t nSegs;
    size_t elemSize;
    size_t dataSize;
    size_t varOffs;
    bool   isBoolean;

    Segment* seg;
    // case 1:
    // 1,1,1,2,2,3,0,0,0,0,0,5,5,5
    // seg = {0,0,true}, {3,1,true}, {5,2,true}, {6,3,true}, {11 ,4,true}, {14}
    // case 2:
    // 1,2,3,4,5,0,0,0,0
    // seg = {0,0,false}, {5,5,true}, {10}
    char* payload;

    ConstRLEPayload(): nSegs(0), elemSize(0), dataSize(0), varOffs(0), isBoolean(false), seg(NULL), payload(NULL)
    {}

  public:
    
    size_t count() const { 
        return nSegs == 0 ? 0 : seg[nSegs].pPosition;
    }

    bool isBool() const
    {
        return isBoolean;
    }

    /**
     * Get value data by the given index
     * @param placeholder for exracted value
     * @param index of value obtained through Segment::valueIndex
     */
    void getValueByIndex(Value& value, size_t index) const;

    /**
     * Get pointer to raw value data for the given poistion
     * @param placeholder for exracted value
     * @param pos element position
     * @return true if values exists i payload, false othrwise
     */
    bool getValueByPosition(Value& value, position_t pos) const;

    /**
     * Return pointer for raw data for non-nullable types
     */
    char* getRawValue(size_t index) const { 
        return payload + index*(elemSize == 0 ? 4 : elemSize);
    }

    /**
     * Return pointer for raw data of variable size types
     */
    char* getRawVarValue(size_t index, size_t& size) const;

    /**
     * Get number of RLE segments
     */
    size_t nSegments() const { 
        return nSegs;
    }

    /**
     * Get element size (0 for varying size types)
     */
    size_t elementSize() const { 
        return elemSize;
    }

    /**
     * Get payload size in bytes
     */
    size_t payloadSize() const { 
        return dataSize;
    }

    /**
     * Get number of items in payload
     */
    size_t payloadCount() const {
        return dataSize / (elemSize == 0 ? 4 : elemSize);
    }

    /**
     * Get next i-th segment
     */
    Segment const& getSegment(size_t i) const {
        assert(i < nSegs);
        return seg[i];
    }

    /**
     * Find segment containing elements with position greater or equal than specified
     */
    size_t findSegment(position_t pos) const {
        size_t l = 0, r = nSegs;
        while (l < r) {
            size_t m = (l + r) >> 1;
            if (seg[m+1].pPosition <= pos) {
                l = m + 1;
            } else {
                r = m;
            }
        }
        return r;
    }

    /**
     * Method to be called to save payload in chunk body
     */
    void pack(char* dst) const;

    /**
     * Get size needed to pack bitmap (used to determine size of chunk)
     */
    size_t packedSize() const;

    /**
     * Constructor for initializing payload with raw chunk data
     */
    ConstRLEPayload(char const* src);

    void getCoordinates(ArrayDesc const& array, size_t dim, Coordinates const& chunkPos, Coordinates const& tilePos, boost::shared_ptr<Query> const& query, Value& dst, bool withOverlap) const;

    bool checkBit(size_t bit) const { 
        return (payload[bit >> 3] & (1 << (bit & 7))) != 0;
    }
    
    char* getFixData() const { 
        return payload;
    }
    
    char* getVarData() const { 
        return payload + varOffs;
    }
    

    virtual ~ConstRLEPayload()
    {}

    class iterator
    {
    protected:
        ConstRLEPayload const* _payload;
        size_t _currSeg;
        Segment const* _cs;
        position_t _currPpos;

    public:
        //defined in .cpp because of value constructor
        iterator(ConstRLEPayload const* payload);
        iterator() {}

        void reset()
        {
            _currSeg = 0;
            if(!end())
            {
                _cs = &_payload->getSegment(_currSeg);
                _currPpos = _cs->pPosition;
            }
        }

        bool end() const
        {
            return _currSeg >= _payload->nSegments();
        }

        int getMissingReason() const
        {
            assert(!end());
            return _cs->valueIndex;
        }

        bool isNull() const
        {
            assert(!end());
            return _cs->null;
        }

        bool isSame() const
        {
            assert(!end());
            return _cs->same;
        }

        position_t const& getPPos() const
        {
            assert(!end());
            return _currPpos;
        }
        
        uint64_t getSegLength() const
        {
            assert(!end());
            return _cs->length();
        }

        uint64_t getRepeatCount() const
        {
            assert(!end());
            return _cs->same ? _cs->length() - _currPpos + _cs->pPosition : 1;
        }

        uint64_t available() const
        {
            assert(!end());
            return _cs->length() - _currPpos + _cs->pPosition;
        }

       bool checkBit() const
        {
            assert(_payload->isBoolean);
            return _payload->checkBit(_cs->valueIndex + (_cs->same ? 0 : _currPpos - _cs->pPosition));
        }

        void toNextSegment()
        {
            assert(!end());
            _currSeg ++;
            if (!end())
            {
                _cs = &_payload->getSegment(_currSeg);
                _currPpos = _cs->pPosition;
            }
        }

        char* getRawValue(size_t& valSize)
        {
            size_t index = _cs->same ? _cs->valueIndex : _cs->valueIndex + _currPpos - _cs->pPosition;
            return _payload->getRawVarValue(index, valSize);
        }

        char* getFixedValues()
        {
            size_t index = _cs->same ? _cs->valueIndex : _cs->valueIndex + _currPpos - _cs->pPosition;
            return _payload->payload + index*_payload->elemSize;
        }

        bool isDefaultValue(Value const& defaultValue);

        //defined in .cpp because of value methods
        void getItem(Value &item);

        void operator ++()
        {
            assert(!end());
            if (_currPpos + 1 < position_t(_cs->pPosition + _cs->length()))
            {
                _currPpos ++;
            }
            else
            {
                _currSeg ++;
                if(!end())
                {
                    _cs = &_payload->getSegment(_currSeg);
                    _currPpos = _cs->pPosition;
                }
            }
        }

        bool setPosition(position_t pPos)
        {
            _currSeg = _payload->findSegment(pPos);
            if (end())
            {
                return false;
            }

            assert (_payload->getSegment(_currSeg).pPosition <= pPos);

            _cs = &_payload->getSegment(_currSeg);
            _currPpos = pPos;
            return true;
        }

        /**
          * Should applied just for bool-typed RLE (bitmap tiles).
          * Skip @param count positions in payload and return number of "1" values.
          * Data Tile just store values without knownledge about physical positions inside tile, while a bitmap helps to understand where which value stay.
          * It is important for skip data from data tile:
          *   you call dataReader += bitmapReader.skip(physicalPositionsCount) and receive the consistent positions inside bitmapReader and dataReader.
          */
        uint64_t skip(uint64_t count)
        {  
            uint64_t setBits = 0;
            while (!end()) { 
                if (_currPpos + count >= _cs->pPosition + _cs->length()) { 
                    uint64_t tail = _cs->length() - _currPpos + _cs->pPosition;
                    count -= tail;
                    if (_cs->same)  {
                        setBits += _payload->checkBit(_cs->valueIndex) ? tail : 0;
                    }  else {
                        position_t beg = _cs->valueIndex + _currPpos - _cs->pPosition; 
                        position_t end = _cs->valueIndex + _cs->length();
                        while (beg < end) { 
                            setBits += _payload->checkBit(beg++);
                        }
                    } 
                    toNextSegment();
                } else { 
                    if (_cs->same)  { 
                        setBits += _payload->checkBit(_cs->valueIndex) ? count : 0;
                    } else {   
                        position_t beg = _cs->valueIndex + _currPpos - _cs->pPosition; 
                        position_t end = beg + count;
                        while (beg < end) { 
                            setBits += _payload->checkBit(beg++);
                        }
                    }
                    _currPpos += count;
                    break;
                }
            }
            return setBits;
        }
 
        void operator +=(uint64_t count)
        {
            assert(!end());
            _currPpos += count;
            if (_currPpos >= position_t(_cs->pPosition + _cs->length())) {
                if (++_currSeg < _payload->nSegments()) {
                    _cs = &_payload->getSegment(_currSeg);
                    if (_currPpos < position_t(_cs->pPosition + _cs->length())) {
                        return;
                    }
                }
                setPosition(_currPpos);
            }
        }
    };

    iterator getIterator() const
    {
        return iterator(this);
    }
};

std::ostream& operator<<(std::ostream& stream, ConstRLEPayload const& payload);


class RLEPayload : public ConstRLEPayload
{
  private:
    std::vector<Segment> container;
    std::vector<char> data;
    uint64_t _valuesCount;

  public:
    void appendValue(std::vector<char>& varPart, Value const& val, size_t valueIndex);

    void setVarPart(char const* data, size_t size);
    void setVarPart(std::vector<char>& varPart);

    friend class RLEPayloadAppender;

    void append(RLEPayload& payload);

    /**
     * Add raw fixed data for non-nullable types
     * @param n a number of new items
     * @return index of the first new item
     */
    size_t addRawValues(size_t n = 1) {
        assert(elemSize != 0);
        const size_t ret = dataSize / elemSize;
        data.resize(dataSize += elemSize * n);
        payload = &data[0];
        return ret;
    }

    /**
     * Add raw var data for non-nullable types
     * @param n a number of new items
     * @return index of the first new item
     */
    size_t addRawVarValues(size_t n = 1) {
        assert(elemSize == 0);
        const size_t fixedSize = 4; // TODO: Maybe sizeof(void*)?
        const size_t ret = dataSize / fixedSize;
        data.resize(dataSize += fixedSize * n);
        payload = &data[0];
        return ret;
    }

    /**
     * Add raw bool data for non-nullable types
     * @param n a number of new items
     * @return index of the first new item
     */
    size_t addBoolValues(size_t n = 1) {
        assert(elemSize == 1 && isBoolean);
        size_t ret = _valuesCount;
        _valuesCount += n;
        dataSize = (_valuesCount >> 3) + 1;
        data.resize(dataSize);
        payload = &data[0];
        return ret;
    }

    /**
     * @return number of elements
     */
    size_t getValuesCount() const  {
        if (isBoolean)
            return _valuesCount;
        const size_t fixedSize = elemSize == 0 ? 4 : elemSize;
        return dataSize / fixedSize;
    }

    /**
     * Add new segment
     */
    void addSegment(const Segment& segment) {
        assert(container.size() == 0 || container[container.size() - 1].pPosition < segment.pPosition);
        container.push_back(segment);
        seg = &container[0];
    }

    /**
     * Assign segments pointer from other payload.
     * Sometimes it's safe to just copy pointer but for conversion
     * constant inplace it's impossible for example.
     * That's why copy param is tru by default
     */
    void assignSegments(const ConstRLEPayload& payload, bool copy = true) 
    {

        if (copy) {
            nSegs = payload.nSegments();
            container.resize(nSegs + 1);
            memcpy(&container[0], payload.seg, (nSegs + 1) * sizeof(Segment));
            seg = &container[0];
        } else {
            seg = payload.seg;
            nSegs = payload.nSegs;
        }
    }

    /**
     * Assignment operator: deep copy from const payload into non-const
     */
    RLEPayload& operator=(ConstRLEPayload const& other) 
    {
        nSegs = other.nSegments();
        elemSize = other.elemSize;
        dataSize = other.dataSize;
        varOffs = other.varOffs;
        isBoolean = other.isBoolean;
        container.resize(nSegs+1);
        memcpy(&container[0], other.seg, (nSegs+1)*sizeof(Segment));
        seg = &container[0];
        data.resize(dataSize);
        memcpy(&data[0], other.payload, dataSize);
        payload = &data[0];
        return *this;
    }

    RLEPayload(ConstRLEPayload const& other):
        ConstRLEPayload() 
    {
        *this = other;
    }

    RLEPayload& operator=(RLEPayload const& other) 
    {
        nSegs = other.nSegs;
        elemSize = other.elemSize;
        dataSize = other.dataSize;
        varOffs = other.varOffs;
        isBoolean = other.isBoolean;
        container = other.container;
        seg = &container[0];
        data = other.data;
        payload = &data[0];
        _valuesCount = other._valuesCount;
        return *this;
    }

    /**
     * Copy constructor: deep copy
     */
    RLEPayload(RLEPayload const& other):
        ConstRLEPayload() 
    {
        *this = other;
    }

    RLEPayload();

    /**
     * Constructor of bitmap from ValueMap (which is used to be filled by ChunkIterator)
     * @param vm ValueMap of inserted {position,value} pairs
     * @param nElems number of elements present in the chunk
     * @param elemSize fixed size of element (in bytes), 0 for varying size types
     * @param defaultValue default value used to fill holes (elements not specified in ValueMap)
     * @param subsequent all elements in ValueMap are assumed to be subsequent
     */
    RLEPayload(ValueMap& vm, size_t nElems, size_t elemSize, Value const& defaultVal, bool isBoolean, bool subsequent);

    /**
     * Constructor which is used to fill a non-emptyable RLE chunk with default values.
     * @param[in]  defaultVal  the default value of the attribute
     * @param[in]  logicalSize the number of logical cells in the chunk
     * @param[in]  elemSize    fixed size of element (in bytes), 0 for varying size types
     * @param[in]  isBoolean   whether the element is of boolean type
     */
    RLEPayload(Value const& defaultVal, size_t logicalSize, size_t elemSize, bool isBoolean);

    /**
     * Constructor of RLE bitmap from dense non-nullable data
     */
    RLEPayload(char* rawData, size_t rawSize, size_t varOffs, size_t elemSize, size_t nElems, bool isBoolean);
    void unpackRawData(char* rawData, size_t rawSize, size_t varOffs, size_t elemSize, size_t nElems, bool isBoolean);

    RLEPayload(const class Type& type);

    RLEPayload(size_t bitSize);

    //
    // Yet another appender: correct handling of boolean and varying size types
    //
    class append_iterator : boost::noncopyable
    {
        RLEPayload* result;
        std::vector<char> varPart;
        RLEPayload::Segment segm;
        Value* prevVal;
        size_t valueIndex;
        size_t segLength;

        void init();

      public:        
        RLEPayload* getPayload() { 
            return result;
        }

        explicit append_iterator(RLEPayload* dstPayload);
        explicit append_iterator(size_t bitSize);
        void flush();
        void add(Value const& v, uint64_t count = 1);
        /**
         * add not more than @param limit values from @param inputIterator
         * Flag @param setupPrevVal just a workaround for bug with mixed
         * add(iterator&, limit) and add(Value const&) calls - after that payload
         * can be broken.
         * I did not fix bug directly according to potential performance regression
         *
         * @return count of added value from @param inputIterator
         * please note - this method add just single segment from @param inputIterator.
         * if your @param limit can be larger than segmentLength from @param inputIterator
         * you should compare @return and @param limit, and repeat call with
         * (@param limit = @param limit - @return)
         * (if @param inputIterator still has values, of course).
         */
        uint64_t add(iterator& inputIterator, uint64_t limit, bool setupPrevVal = false);
        ~append_iterator();
    };
     
    /**
     * Clear all data
     */
    void clear();

    /**
     * Use this method to copy payload data according to an empty bitmask and start and stop
     * positions.
     * @param [in] payload an input payload
     * @param [in] emptyMap an input empty bitmap mask according to which data should be extracted
     * @param [in] vStart a logical position of start from data should be copied
     * @param [in] vEnd a logical position of stop where data should be copied
     */
    void unPackTile(const ConstRLEPayload& payload, const ConstRLEEmptyBitmap& emptyMap, position_t vStart, position_t vEnd);

    /**
     * Use this method to copy empty bitmask to payload 
     * positions.
     * @param [in] emptyMap an input empty bitmap mask according to which data should be extracted
     * @param [in] vStart a logical position of start from data should be copied
     * @param [in] vEnd a logical position of stop where data should be copied
     */
    void unPackTile(const ConstRLEEmptyBitmap& emptyMap, position_t vStart, position_t vEnd);

     /**
     * Complete adding segments to the chunk
     */
    void flush(position_t chunkSize);

    void trim(position_t lastPos);

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & nSegs;
        ar & elemSize;
        ar & dataSize;
        ar & varOffs;
        ar & container;
        ar & data;
        ar & isBoolean;
        if (Archive::is_loading::value) {
            seg = &container[0];
            payload = &data[0];
        }
    }
};

class RLEPayloadAppender
{
private:
    RLEPayload _payload;

    ssize_t _nextSeg;
    ssize_t _nextPPos;
    ssize_t _nextValIndex;
    bool _finalized;

public:
    RLEPayloadAppender(size_t bitSize): _payload(bitSize), _nextSeg(0), _nextPPos(0), _nextValIndex(0), _finalized(false)
    {
        //no boolean yet!
        if (bitSize <= 1)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_NOT_IMPLEMENTED) << "payload appender for size <= 1";
    }

    ~RLEPayloadAppender()
    {}

    void append(Value const& v);

    void finalize()
    {
        _payload.container.resize(_nextSeg+1);
        _payload.container[_nextSeg].pPosition = _nextPPos;
        _payload._valuesCount = _nextValIndex;
        _payload.dataSize = _nextValIndex * _payload.elemSize;
        _payload.isBoolean = false;
        _payload.nSegs = _nextSeg;
        _payload.payload = &_payload.data[0];
        _payload.seg = &_payload.container[0];
        _finalized = true;
    }

    RLEPayload const* getPayload()
    {
        assert(_finalized);
        return &_payload;
    }
};

}

#endif
