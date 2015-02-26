/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2013 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
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
#include <boost/weak_ptr.hpp>
#include <boost/shared_array.hpp>
#include <query/Query.h>
#include <util/Lru.h>
#include <util/CoordinatesMapper.h>

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
        boost::shared_ptr<ConstChunkIterator>
        getConstIterator(boost::shared_ptr<Query> const& query, int iterationMode) const;
      public:
        MemChunk();
        ~MemChunk();

        boost::shared_ptr<ConstRLEEmptyBitmap> getEmptyBitmap() const;
        void setEmptyBitmap(boost::shared_ptr<ConstRLEEmptyBitmap> const& bitmap);

        Address const& getAddress() const
        {
            return addr;
        }

        /**
         * @see ConstChunk::isMemChunk
         */
        virtual bool isMemChunk() const
        {
            return true;
        }

        size_t count() const;
        bool   isCountKnown() const;
        void setCount(size_t count);

        virtual bool isTemporary() const;

        /**
         * @see ConstChunk::isMaterialized
         */
        bool isMaterialized() const;

        /**
         * @see ConstChunk::materialize
         */
        ConstChunk* materialize() const
        {
            assert(materializedChunk == NULL);
            return const_cast<MemChunk*> (this);
        }

        /**
         * @see Chunk::write
         */
        virtual void write(boost::shared_ptr<Query>& query);

        bool isSparse() const;
        bool isRLE() const;
        void setSparse(bool sparse);
        void setRLE(bool rle);
        void fillRLEBitmap();

        virtual void initialize(Array const* array, ArrayDesc const* desc, const Address& firstElem, int compressionMethod);
        virtual void initialize(ConstChunk const& srcChunk);

        void setBitmapChunk(Chunk* bitmapChunk);

        virtual bool isInitialized() const
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
     * Chunk of a temporary array whose body can be located either in memory either on disk.
     */
    class LruMemChunk : public MemChunk
    {
        friend class MemArray;
        friend class MemArrayIterator;
        friend class SharedMemCache;

    private:
        /**
         * Iterator indicating the position of the chunk in the LRU cache.
         */
        MemChunkLruIterator _whereInLru;

        /**
         * The offset into the array swap file where the chunk has been written to.
         */
        int64_t      _swapFileOffset;

        /**
         * The difference between the number of times pin was called, minus the number of times unPin was called.
         */
        size_t       _accessCount;

        /**
         * The size of the chunk in the swap file.
         */
        size_t       _swapFileSize;

        /**
         * The size of the chunk the last time we pinned or unPinned it. If you follow proper prodecure, the chunk size should
         * only change at unPin time; hence the name.
         */
        size_t       _sizeAtLastUnPin;

      public:
        /**
         * Create a new chunk, not in LRU with size 0.
         */
        LruMemChunk();

        ~LruMemChunk();

        /**
         * @see: MemChunk::pin
         */
        bool pin() const;

        /**
         * @see: MemChunk::unPin
         */
        void unPin() const;

        /**
         * @see: MemChunk::isTemporary
         */
        bool isTemporary() const;

        /**
         * Determine if the chunk is in the LRU.
         * @return true if the chunk is not in the LRU. False otherwise.
         */
        bool isEmpty() const;

        /**
         * Take a note that this LruMemChunk has been removed from the Lru.
         */
        void prune();

        /**
         * Remove the chunk from the LRU.
         */
        void removeFromLru();

        /**
         * Add the chunk to the LRU.
         */
        void pushToLru();

        /**
         * @see Chunk::write
         */
        virtual void write(boost::shared_ptr<Query>& query);

        /**
         * Initialize chunk
         * @param array to which this chunk belongs
         * @param desc the array descriptor
         * @param firsElem chunk coords
         * @param compressionMethod
         */
        void initialize(MemArray const* array, ArrayDesc const* desc,
        const Address& firstElem, int compressionMethod);
        virtual void initialize(Array const* array, ArrayDesc const* desc,
                                const Address& firstElem, int compressionMethod);
        virtual void initialize(ConstChunk const& srcChunk);
        boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
        boost::shared_ptr<ChunkIterator> getIterator(boost::shared_ptr<Query> const& query,
                                                     int iterationMode);
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

        /**
         * Retrieve the current memory threhsold.
         * @return the mem threshold.
         */
        uint64_t getMemThreshold() const
        {
            return _usedMemThreshold;
        }

        /**
         * Debugging aid: compute the size of the chunks on the LRU list.
         * @return the sum of the sizes of the chunks in the LRU. Note: chunks that are currently pinned are not accounted for here.
         */
        uint64_t computeSizeOfLRU();

        /**
         * Debugging aid: compare computeSizeOfLRU with getUsedMemSize
         * @return true if computeSizeOfLRU is <= getUsedMemSize. This is an invariant that must always be true. False otherwise.
         */
        bool sizeCoherent();

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

        MemArray(ArrayDesc const& arr, boost::shared_ptr<Query> const& query);

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
        MemArray(boost::shared_ptr<Array>& input, boost::shared_ptr<Query> const& query, bool vertical = true);
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
    private:
        MemArray(const MemArray&);
    };

    /**
     * Temporary (in-memory) array iterator
     */
    class MemArrayIterator : public ArrayIterator
    {
      private:
        map<Address, LruMemChunk>::iterator curr;
        map<Address, LruMemChunk>::iterator last;
        MemArray& _array;
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
        virtual boost::shared_ptr<Query> getQuery()
        {
            return Query::getValidQueryPtr(_array._query);
        }
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
        virtual boost::shared_ptr<Query> getQuery()
        {
            // XXX note: there is still code that does not set the query context correctly
            // e.g. Chunk::materialize() and other cases of using intermediate MemChunk objects
            // so we dont try to validate _query here. At the points where this context must absolutely
            // be present (i.e. DBArray, MemArray, InputArray, BuildArray, etc.), it will be validate 
            return _query.lock();
        }

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
        virtual boost::shared_ptr<scidb::Query> getQuery()
        {
            // See XXX note in MemChunkIterator
            return _query.lock();
        }

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
        uint64_t tilePos;
        uint64_t tileSize;
        bool isEmptyIndicator;
        boost::weak_ptr<Query> _query;

        BaseChunkIterator(ArrayDesc const& desc, AttributeID attr, Chunk* data, int iterationMode, boost::shared_ptr<Query> const& query);
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
        boost::shared_ptr<Query> getQuery()
        {
            // See XXX note in MemChunkIterator
            return _query.lock();
        }
    };

    class RLEConstChunkIterator : public BaseChunkIterator
    {
      public:
        RLEConstChunkIterator(ArrayDesc const& desc, AttributeID attr, Chunk* data, Chunk* bitmap, int iterationMode,
                              boost::shared_ptr<Query> const& query);

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

        RLEBitmapChunkIterator(ArrayDesc const& desc, AttributeID attr,
                               Chunk* data,
                               Chunk* bitmap,
                               int iterationMode,
                               boost::shared_ptr<Query> const& query);

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

        RLEChunkIterator(ArrayDesc const& desc, AttributeID attr,
                         Chunk* data,
                         Chunk* bitmap,
                         int iterationMode,
                         boost::shared_ptr<Query> const& query);
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
        RLEPayload payload;
        Chunk* bitmapChunk;
        RLEPayload::append_iterator appender;
        position_t prevPos;
    };
}

#endif

