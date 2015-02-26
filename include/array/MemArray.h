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

/**
 * @file MemArray.h
 *
 * @brief In-Memory (temporary) array implementation
 */

#ifndef MEM_ARRAY_H_
#define MEM_ARRAY_H_

#include <array/Array.h>
#include <vector>
#include <map>
#include <assert.h>
#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>
#include <query/Query.h>
#include <util/Lru.h>

using namespace std;
using namespace boost;

namespace scidb
{
    /**
     * An Address is used to specify the location of a chunk inside an array.
     */
    struct Address
    {
        /*
         * Attribute identifier
         */
        AttributeID attId;
        /**
         * Chunk coordinates
         */
        Coordinates coords;

        /**
         * Default constructor
         */
        Address()
        {}

        /**
         * Constructor
         * @param attId attribute identifier
         * @param coords element coordinates
         */
        Address(AttributeID attId, Coordinates const& coords) :
            attId(attId), coords(coords)
        {}

        /**
         * Copy constructor
         * @param addr the object to copy from
         */
        Address(const Address& addr)
        {
            this->attId = addr.attId;
            this->coords = addr.coords;
        }

        /**
         * Partial comparison function, used to implement std::map
         * @param other another aorgument of comparison
         * @return true if "this" preceeds "other" in partial order
         */
        bool operator <(const Address& other) const
        {
            if (attId != other.attId)
            {
                return attId < other.attId;
            }
            if (coords.size() != other.coords.size())
            {
                return coords.size() < other.coords.size();
            }
            for (size_t i = 0, n = coords.size(); i < n; i++)
            {
                if (coords[i] != other.coords[i])
                {
                    return coords[i] < other.coords[i];
                }
            }
            return false;
        }

        /**
         * Equality comparison
         * @param other another aorgument of comparison
         * @return true if "this" equals to "other"
         */
        bool operator ==(const Address& other) const
        {
            if (attId != other.attId)
            {
                return false;
            }
            assert(coords.size() == other.coords.size());
            for (size_t i = 0, n = coords.size(); i < n; i++)
            {
                if (coords[i] != other.coords[i])
                {
                    return false;
                }
            }
            return true;
        }

        /**
         * Inequality comparison
         * @param other another aorgument of comparison
         * @return true if "this" not equals to "other"
         */
        bool operator !=(const Address& other) const
        {
            return !(*this == other);
        }

        /**
         * Compute a hash of coordiantes
         * @return a 64-bit hash of the chunk coordinates.
         */
        uint64_t hash() const
        {
            uint64_t h = 0;
            for (int i = coords.size(); --i >= 0;)
            {
                h ^= coords[i];
            }
            return h;
        }
    };

    /**
     * Map coordinates to offset within chunk
     */
    class CoordinatesMapper
    {
      protected:
        size_t      _nDims;
        position_t  _logicalChunkSize;
        Coordinates _origin;
        Coordinates _chunkIntervals;

        // Internal init function that is shared by the constructors.
        void init(Coordinates const& firstPosition, Coordinates const& lastPosition);

      public:
        /**
         * Constructor.
         * @param  chunk  A chunk from which the first position and last position will be used.
         */
        CoordinatesMapper(ConstChunk const& chunk);

        /**
         * Constructor.
         * @param  firstPosition  The first position of the chunk.
         * @param  lastPosition   The last position of the chunk.
         */
        CoordinatesMapper(Coordinates const& firstPosition, Coordinates const& lastPosition);

        /**
         * Constructor
         * @param  chunkPos  The chunk position without overlap.
         * @param  dims      The dimensions.
         */
        CoordinatesMapper(Coordinates const& chunkPos, Dimensions const& dims);

        void pos2coord(position_t pos, Coordinates& coord) const 
        {
            assert(_origin.size()>0);

            if (_nDims == 1) {
                coord[0] = _origin[0] + pos;
                assert(pos < _chunkIntervals[0]);
            } else if (_nDims == 2) {
                coord[1] = _origin[1] + (pos % _chunkIntervals[1]);
                pos /= _chunkIntervals[1];
                coord[0] = _origin[0] + pos;
                assert(pos < _chunkIntervals[0]);
            } else { 
                for (int i = (int)_nDims; --i >= 0;) {
                    coord[i] = _origin[i] + (pos % _chunkIntervals[i]);
                    pos /= _chunkIntervals[i];
                }
                assert(pos == 0);
            }
        }

        position_t coord2pos(Coordinates const& coord) const
        {
            if (_nDims == 1) {
                return coord[0] - _origin[0];
            } else if (_nDims == 2) {
                return (coord[0] - _origin[0])*_chunkIntervals[1] + (coord[1] - _origin[1]);
            } else { 
                position_t pos = 0;
                for (size_t i = 0, n = _nDims; i < n; i++) {
                    pos *= _chunkIntervals[i];
                    pos += coord[i] - _origin[i];
                }
                return pos;
            }
        }
        
        /**
         * Retrieve the number of dimensions used by the mapper.
         * @return the number of dimensions
         */
        size_t getNumDims() const
        {
            return _nDims;
        }
    };
       
    /**
     * Map coordinates to offset within chunk and vice versa, while the chunk is given at run time.
     * The difference from CoordinatesMapper is that this object can be created once and reused for all chunks in the array.
     */
    class ArrayCoordinatesMapper
    {
      protected:
        Dimensions  _dims;

      public:
        /**
         * Constructor.
         * @param  dims   the dimensions
         */
        ArrayCoordinatesMapper(Dimensions const& dims)
        : _dims(dims)
        {
            assert(dims.size()>0);
        }


        /**
         * Convert chunkPos to lows and intervals.
         * Without overlap, lows[i] = chunkPos[i]; but with overlap, lows[i] may be smaller (but can't be smaller than dims[i].getStart().
         * Similar with highs[i] (may be larger than the last logical coord, but can't be larger than dims[i].getEndMax().
         * The interval is just highs[i]-lows[i]+1.
         * @param chunkPos   the chunk position
         * @param lows       [out] the low point
         * @param intervals  [out] the intervals of each dim
         */
        inline void chunkPos2LowsAndIntervals(Coordinates const& chunkPos, Coordinates& lows, Coordinates& intervals) const
        {
            assert(chunkPos.size()==_dims.size());
            assert(lows.size()==_dims.size());
            assert(intervals.size()==_dims.size());

            for (size_t i=0; i<_dims.size(); ++i) {
                lows[i] = chunkPos[i] - _dims[i].getChunkOverlap();
                if (lows[i] < _dims[i].getStart()) {
                    lows[i] = _dims[i].getStart();
                }
                Coordinate high = chunkPos[i] + _dims[i].getChunkInterval()+_dims[i].getChunkOverlap() - 1;
                if (high > _dims[i].getEndMax()) {
                    high = _dims[i].getEndMax();
                }
                intervals[i] = high - lows[i] + 1;

                assert(intervals[i]>0);
            }
        }

        /**
         * Given a position in a chunk, and the chunkPos, compute the coordinate.
         * @param chunkPos   the chunk position
         * @param pos        position in a chunk, as returned by coord2pos
         * @param coord      [out] the coordinate
         */
        void pos2coord(Coordinates const& chunkPos, position_t pos, Coordinates& coord) const
        {
            Coordinates lows(chunkPos.size());
            Coordinates intervals(chunkPos.size());
            chunkPos2LowsAndIntervals(chunkPos, lows, intervals);
            pos2coordWithLowsAndIntervals(lows, intervals, pos, coord);
        }

        /**
         * Upon repeated call to pos2coord with the same chunkPos, the performance can be improved by
         * converting chunkPos to lows and intervals, then call pos2coord with lows and intervals.
         * @param lows       the low point of the chunk
         * @param intervals  the intervals of the chunk
         * @param pos        position in a chunk, as returned by coord2pos
         * @param coord      [out] the coordinate
         */
        void pos2coordWithLowsAndIntervals(Coordinates const& lows, Coordinates const& intervals, position_t pos, Coordinates& coord) const
        {
            assert(lows.size() == _dims.size());
            assert(intervals.size() == _dims.size());
            assert(coord.size() == _dims.size());

            size_t i = _dims.size();
            while (i>0) {
                --i;
                coord[i] = lows[i] + (pos % intervals[i]);
                pos /= intervals[i];
            }
            assert(pos == 0);
        }

        /**
         * Given a coord in a chunk, and the chunkPos, compute the position in the chunk.
         * @param chunkPos the chunk position
         * @param coord    the coord in the chunk
         * @return the position
         */
        position_t coord2pos(Coordinates const& chunkPos, Coordinates const& coord) const
        {
            Coordinates lows(_dims.size());
            Coordinates intervals(_dims.size());
            chunkPos2LowsAndIntervals(chunkPos, lows, intervals);
            return coord2posWithLowsAndIntervals(lows, intervals, coord);
        }

        /**
         * Upon repeated call to coord2ps with the same chunkPos, the performance can be improved by
         * converting chunkPos to lows and intervals, then call coord2pos with lows and intervals.
         * @param lows       the low point of the chunk
         * @param intervals  the intervals of the chunk
         * @param coord      the coord in the chunk
         * @return the position
         */
        position_t coord2posWithLowsAndIntervals(Coordinates const& lows, Coordinates const& intervals, Coordinates const& coord) const
        {
            assert(lows.size() == _dims.size());
            assert(intervals.size() == _dims.size());
            assert(coord.size() == _dims.size());

            position_t pos = 0;
            for (size_t i = 0; i < _dims.size(); ++i) {
                pos *= intervals[i];
                pos += coord[i] - lows[i];
            }
            return pos;
        }
    };

    class MemArray;
    class MemArrayIterator;

    /**
     * Chunk of temporary (in-memory) array
     */
    class MemChunk : public Chunk
    {
        friend class MemArray;
        friend class MemArrayIterator;
        friend class MemChunkIterator;
        friend class SharedMemCache;
      protected:
        Address addr; // address of first chunk element
        boost::shared_array<char>   data; // uncompressed data (may be NULL if swapped out)
        size_t  size;
        size_t  nElems;
        int     compressionMethod;
        bool    sparse;
        bool    rle;
        Coordinates firstPos;
        Coordinates firstPosWithOverlaps;
        Coordinates lastPos;
        Coordinates lastPosWithOverlaps;
        ArrayDesc const* arrayDesc;
        Chunk* bitmapChunk;
        Array const* array;
        boost::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
      public:
        MemChunk();
        ~MemChunk();

        boost::shared_ptr<ConstRLEEmptyBitmap> getEmptyBitmap() const;
        void setEmptyBitmap(boost::shared_ptr<ConstRLEEmptyBitmap> const& bitmap);

        Address const& getAddress() const
        {
            return addr;
        }

        size_t count() const;
        bool   isCountKnown() const;
        void setCount(size_t count);

        virtual bool isTemporary() const;

        bool isMaterialized() const;
        bool isSparse() const;
        bool isRLE() const;
        void setSparse(bool sparse);
        void setRLE(bool rle);
        void fillRLEBitmap();

        void initialize(Array const* array, ArrayDesc const* desc, const Address& firstElem, int compressionMethod);
        void initialize(ConstChunk const& srcChunk);

        void setBitmapChunk(Chunk* bitmapChunk);

        bool isInitialized() const
        {
            return arrayDesc != NULL;
        }

        ConstChunk const* getBitmapChunk() const;

        Array const& getArray() const;
        const ArrayDesc& getArrayDesc() const;
        const AttributeDesc& getAttributeDesc() const;
        int getCompressionMethod() const;
        void* getData() const;
        size_t getSize() const;
        void allocate(size_t size);
        void reallocate(size_t size);
        void free();
        Coordinates const& getFirstPosition(bool withOverlap) const;
        Coordinates const& getLastPosition(bool withOverlap) const;
        boost::shared_ptr<ChunkIterator> getIterator(boost::shared_ptr<Query> const& query, int iterationMode);
        boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
        bool pin() const;
        void unPin() const;
        void compress(CompressedBuffer& buf, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const;
        void decompress(const CompressedBuffer& buf);
    };

    class LruMemChunk;
    typedef LRUSecondary<LruMemChunk*> MemChunkLru;
    typedef LRUSecondary<LruMemChunk*>::ListIterator MemChunkLruIterator;

    /**
     * Chunk of temporary array which body can be located either in memory either on disk
     */
    class LruMemChunk : public MemChunk
    {
        friend class MemArray;
        friend class MemArrayIterator;
        friend class SharedMemCache;

    private:
        MemChunkLruIterator _whereInLru;

        int64_t      swapFileOffset;
        size_t       accessCount;
        size_t       swapFileSize;

      public:
        bool pin() const;
        void unPin() const;

        bool isTemporary() const;

        bool isEmpty() const;

        /**
         * Take a note that this LruMemChunk has been removed from the Lru.
         */
        void prune();

        void removeFromLru();

        void pushToLru();

        virtual void write(boost::shared_ptr<Query>& query);

        LruMemChunk();
        ~LruMemChunk();
    };
    

    /**
     * Structure to share mem chunks.
     */
    class SharedMemCache
    {
    private:
        // The LRU of LruMemChunk objects.
        MemChunkLru _theLru;

        uint64_t _usedMemSize;
        uint64_t _usedMemThreshold;
        Mutex _mutex;
        size_t _swapNum;
        size_t _loadsNum;

        static SharedMemCache _sharedMemCache;

    public:
        SharedMemCache();
        void pinChunk(LruMemChunk& chunk);
        void unpinChunk(LruMemChunk& chunk);
        void swapOut();
        void deleteChunk(LruMemChunk& chunk);
        void cleanupArray(MemArray &array);
        static SharedMemCache& getInstance() {
            return _sharedMemCache;
        }

        /**
         * Get the LRU.
         * @return a reference to the LRU object.
         */
        static MemChunkLru& getLru() {
            return _sharedMemCache._theLru;
        }

        uint64_t getUsedMemSize() const {
            return _sharedMemCache._usedMemSize;
        }

        size_t getSwapNum() const {
            return _swapNum;
        }

        size_t getLoadsNum() const {
            return _loadsNum;
        }

        void setMemThreshold(uint64_t memThreshold) {
            _usedMemThreshold = memThreshold;
        }
    };

    /**
     * Temporary (in-memory) array implementation
     */
    class MemArray : public Array
    {
        friend class MemChunk;
        friend class LruMemChunk;
        friend class MemChunkIterator;
        friend class MemArrayIterator;
        friend class SharedMemCache;
      public:
        //
        // Sparse chunk iterator
        //
        virtual string const& getName() const;
        virtual ArrayID getHandle() const;

        virtual ArrayDesc const& getArrayDesc() const;

        virtual boost::shared_ptr<ArrayIterator> getIterator(AttributeID attId);
        virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attId) const;

        Chunk& operator[](Address const& addr);

        MemArray(ArrayDesc const& arr);
        MemArray(const MemArray& other);

        /**
         * Construct by first creating an empty MemArray with the shape of input,
         * then append all data from input to this.
         * @param input the input array used for the array descriptor and the data
         * @param vertical the method to use when copying data. True by default,
         * meaning copy each attribute separately - first all chunks of attribute 1,
         * then all chunks of attribute 2 and so on... False means copy all attributes
         * at the same time - first all attributes for the first chunk, then all
         * attributes for the second chunk. The value false must be used when input
         * array does not support the independent scanning of attributes
         * (i.e. MergeSortArray).
         */
        MemArray(boost::shared_ptr<Array> input, bool vertical = true);
        ~MemArray();

        /**
         * @see Array::isMaterialized()
         */
        virtual bool isMaterialized() const
        {
            return true;
        }

      private:
        void initLRU();
        void swapOut();
        void pinChunk(LruMemChunk& chunk);
        void unpinChunk(LruMemChunk& chunk);

        ArrayDesc desc;
        uint64_t _usedFileSize;
        int _swapFile;
        map<Address, LruMemChunk> _chunks;
        Mutex _mutex;
    };

    /**
     * Temporary (in-memory) array iterator
     */
    class MemArrayIterator : public ArrayIterator
    {
        map<Address, LruMemChunk>::iterator curr;
        map<Address, LruMemChunk>::iterator last;
        MemArray& array;
        Address addr;
        Chunk* currChunk;
        boost::shared_ptr<Array> parent;
        bool positioned;

        void position();

      public:
        void setParentArray(boost::shared_ptr<Array> arr) {
            parent = arr;
        }
        MemArrayIterator(MemArray& arr, AttributeID attId);
        ConstChunk const& getChunk();
        bool end();
        void operator ++();
        Coordinates const& getPosition();
        bool setPosition(Coordinates const& pos);
        void setCurrent();
        void reset();
        Chunk& newChunk(Coordinates const& pos);
        Chunk& newChunk(Coordinates const& pos, int compressionMethod);
        void deleteChunk(Chunk& chunk);
    };

    /**
     * Temporary (in-memory) array chunk iterator
     */
    class MemChunkIterator : public ChunkIterator
    {
      public:
        int getMode();
        Value& getItem();
        bool isEmpty();
        bool end();
        void operator ++();
        Coordinates const& getPosition();
        bool setPosition(Coordinates const& pos);
        void reset();
        void writeItem(const  Value& item);
        void flush();
        ConstChunk const& getChunk();
        bool supportsVectorMode() const;
        void setVectorMode(bool enabled);
        virtual boost::shared_ptr<Query> getQuery() { return _query.lock(); }

        /**
         * Temporary array chunk iterator constructor
         * @param desc descriptor of iterated array
         * @param attId identifier of iterated array attribute
         * @param dataChunk iterated chunk
         * @param bitmapChunk if iterated array has empty indicator attribute, then chunk of this
         * indicator attribute corresponding to the data chunk
         * @param newChunk whether iterator is used to fill new chunk with data
         * @param iterationMode bit mask of iteration mode flags
         */
        MemChunkIterator(ArrayDesc const& desc, AttributeID attId, Chunk* dataChunk, Chunk* bitmapChunk,
                         bool newChunk, int iterationMode, boost::shared_ptr<Query> const& query);
        virtual ~MemChunkIterator();

      protected:
        void seek(size_t offset);
        bool isEmptyCell();
        void findNextAvailable();

        ArrayDesc const& array;
        AttributeDesc const* attr;
        Chunk* dataChunk;
        Chunk* bitmapChunk;
        bool   dataChunkPinned;
        bool   bitmapChunkPinned;
        int    mode;
        Type   type;
        Value  value;
        Value  trueValue;
        Value  defaultValue;
        boost::shared_ptr<ConstChunkIterator> emptyBitmapIterator;
        char*  bufPos;
        char*  nullBitmap;
        char*  emptyBitmap;
        char*  buf;
        Coordinates currPos;
        Coordinates firstPos;
        Coordinates lastPos;
        Coordinates origin;
        size_t currElem;
        size_t elemSize;
        size_t nElems;
        size_t firstElem;
        size_t lastElem;
        size_t used;
        size_t varyingOffs;
        size_t nullBitmapSize;
        size_t nElemsPerStride;
        size_t maxTileSize;
        bool   isPlain;
        bool   moveToNextAvailable;
        bool   checkBounds;
        bool   hasCurrent;
    private:
        boost::weak_ptr<Query> _query;
    };

   /**
     * Sparse chunk iterator
     */
    class SparseChunkIterator : public ChunkIterator, CoordinatesMapper
    {
      public:
        int getMode();
        Value& getItem();
        bool isEmpty();
        bool end();
        void operator ++();
        Coordinates const& getPosition();
        bool setPosition(Coordinates const& pos);
        void reset();
        void writeItem(const  Value& item);
        void flush();
        ConstChunk const& getChunk();
        virtual boost::shared_ptr<scidb::Query> getQuery() { return _query.lock(); }

        /**
         * Temporary array chunk iterator constructor
         * @param desc descriptor of iterated array
         * @param attId identifier of iterated array attribute
         * @param dataChunk iterated chunk
         * @param bitmapChunk if iterated array has empty indicator attribute, then chunk of this
         * indicator attribute corresponding to the data chunk
         * @param newChunk whether iterator is used to fill new chunk with data
         * @param iterationMode bit mask of iteration mode flags
         */
        SparseChunkIterator(ArrayDesc const& desc, AttributeID attId, Chunk* dataChunk, Chunk* bitmapChunk,
                            bool newChunk, int iterationMode, boost::shared_ptr<Query> const& query);
        ~SparseChunkIterator();

        struct SparseChunkHeader {
            uint32_t nElems;
            uint32_t used;
        };

      protected:
        struct SparseMapValue {
            uint32_t isNull : 1;
            uint32_t offset : 31; // offset or missing reason

            SparseMapValue() {
                isNull = false;
                offset = 0;
            }
        };

        struct SparseElem : SparseMapValue {
            uint32_t position;
        };

        struct SparseElem64 : SparseMapValue {
            uint64_t position;
        };

        bool isEmptyCell();
        bool isOutOfBounds();
        void findNextAvailable();

        uint32_t binarySearch(uint64_t val);
        void     setCurrPosition();

        ArrayDesc const& array;
        AttributeDesc const& attrDesc;
        Chunk* dataChunk;
        Chunk* bitmapChunk;
        bool   dataChunkPinned;
        bool   bitmapChunkPinned;
        int    mode;
        AttributeID attrID;
        Type type;
        Value value;
        Value trueValue;
        Value defaultValue;
        boost::shared_ptr<ConstChunkIterator> emptyBitmapIterator;
        char*  emptyBitmap;
        char*  buf;
        Coordinates firstPos;
        Coordinates lastPos;
        Coordinates currPos;

        uint32_t nNonDefaultElems;
        uint64_t nextNonDefaultElem;
        uint64_t currElem;
        uint32_t currElemIndex;
        uint32_t currElemOffs;
        SparseElem* elemsList;
        SparseElem64* elemsList64;
        map<uint64_t, SparseMapValue> elemsMap;
        map<uint64_t, SparseMapValue>::iterator curr;

        size_t elemSize;
        size_t used;
        size_t allocated;
        bool   hasCurrent;
        bool   isEmptyIndicator;
        bool   isNullDefault;
        bool   isNullable;
        bool   skipDefaults;
        bool   checkBounds;
        bool   isNull;
        bool   moveToNextAvailable;
    private:
        boost::weak_ptr<Query> _query;

    };

    /**
     * Temporary (in-memory) array chunk iterator
     */
    class BaseChunkIterator: public ChunkIterator, protected CoordinatesMapper
    {
      protected:
        ArrayDesc const& array;
        AttributeID attrID;
        AttributeDesc const& attr;
        Chunk* dataChunk;
        bool   dataChunkPinned;
        bool   hasCurrent;
        bool   hasOverlap;
        bool   isEmptyable;
        int    mode;
        boost::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
        ConstRLEEmptyBitmap::iterator emptyBitmapIterator;

        Coordinates currPos;
        TypeId typeId;
        Type   type;
        Value const& defaultValue;
        position_t tilePos;
        position_t tileSize;
        bool isEmptyIndicator;

        BaseChunkIterator(ArrayDesc const& desc, AttributeID attr, Chunk* data, int iterationMode);
        ~BaseChunkIterator();

      public:
        int  getMode();
        bool isEmpty();
        bool end();
        bool setPosition(Coordinates const& pos);
        void operator ++();
        ConstChunk const& getChunk();
        bool supportsVectorMode() const;
        void setVectorMode(bool enabled);
        void reset();
        void writeItem(const Value& item);
        void flush();
        Coordinates const& getPosition();
        boost::shared_ptr<ConstRLEEmptyBitmap> getEmptyBitmap();
    };


    class RLEConstChunkIterator : public BaseChunkIterator
    {
      public:
        RLEConstChunkIterator(ArrayDesc const& desc, AttributeID attr, Chunk* data, Chunk* bitmap, int iterationMode);

        Value& getItem();
        bool setPosition(Coordinates const& pos);
        void operator ++();
        void reset();

      private:
        ConstRLEPayload payload;
        ConstRLEPayload::iterator payloadIterator;

        Value value;
    };

    class RLEBitmapChunkIterator : public BaseChunkIterator
    {
      public:
        Value& getItem();

        RLEBitmapChunkIterator(ArrayDesc const& desc, AttributeID attr, Chunk* data, Chunk* bitmap, int iterationMode);

      private:
        Value trueValue;
        Value value;
    };

    class RLEChunkIterator : public BaseChunkIterator
    {
      public:
        bool isEmpty();
        Value& getItem();
        void writeItem(const Value& item);
        void flush();
        bool setPosition(Coordinates const& pos);
        boost::shared_ptr<Query> getQuery();
        
        RLEChunkIterator(ArrayDesc const& desc, AttributeID attr, Chunk* data, Chunk* bitmap, int iterationMode, boost::shared_ptr<Query> const& q);
        virtual ~RLEChunkIterator();

      private:
        position_t getPos() { 
            return isEmptyable ? emptyBitmapIterator.getLPos() : emptyBitmapIterator.getPPos();
        }

        ValueMap values;
        Value    trueValue;
        Value    falseValue;
        Value    tmpValue;
        Value    tileValue;
        shared_ptr<ChunkIterator> emptyChunkIterator; 
        boost::weak_ptr<Query> query;
        RLEPayload payload;
        Chunk* bitmapChunk;
        RLEPayload::append_iterator appender;
        position_t prevPos;
    };

}

#endif

