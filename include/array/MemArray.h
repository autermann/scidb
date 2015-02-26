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
#include <query/Query.h>

using namespace std;
using namespace boost;

namespace scidb
{

    /**
     * Fully qualified address of array element
     */
    struct Address
    {
        /**
         * Array identifier
         */
        ArrayID arrId;
        /**
         * Attribute identifier
         */
        AttributeID attId;
        /**
         * Element coordinates
         */
        Coordinates coords;

        /**
         * Constructur
         * @param arrId array identifier
         * @param attId attribute identifier
         * @param coords element coordinates
         */
        Address(ArrayID arrId, AttributeID attId, Coordinates const& coords)
        {
            this->arrId = arrId;
            this->attId = attId;
            this->coords = coords;
        }

        /**
         * Default constructor
         */
        Address() {}

        /**
         * Copy constructor
         */
        Address(const Address& addr)
        {
            this->arrId = addr.arrId;
            this->attId = addr.attId;
            this->coords = addr.coords;
        }

        bool operator < (const Address& other) const
        {
            return less(other);
        }

        /**
         * Partial comparison function, used to implement std::map
         * @param other another aorgument of comparison
         * @return true if "this" preceeds "other" in partial order
         */
        bool less(const Address& other) const
        {
            if (arrId != other.arrId) {
                return arrId < other.arrId;
            }
            if (attId != other.attId) {
                return attId < other.attId;
            }
            if (coords.size() != other.coords.size()) {
                return coords.size() < other.coords.size();
            }
            for (size_t i = 0, n = coords.size(); i < n; i++) {
                if (coords[i] != other.coords[i]) {
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
        bool operator == (const Address& other) const
        {
            if (arrId != other.arrId) {
                return false;
            }
            if (attId != other.attId) {
                return false;
            }
            assert(coords.size() == other.coords.size());
            for (size_t i = 0, n = coords.size(); i < n; i++) {
                if (coords[i] != other.coords[i]) {
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
        bool operator != (const Address& other) const
        {
            return !(*this == other);
        }

        uint64_t hash() const
        {
            uint64_t h = 0;
            for (int i = coords.size(); --i >= 0;) {
                h ^= coords[i];
            }
            return h;
        }
    };

    /**
     * Address comparator to be used in std::map
     */
    struct AddressLess
    {
        bool operator()(const Address& c1, const Address& c2)
        {
            return c1.less(c2);
        }
    };

    /**
     * Map coordinates to offset within chunk
     */
    class CoordinatesMapper
    {
      protected:
        size_t      nDims;
        position_t  logicalChunkSize;
        Coordinates origin;
        Coordinates chunkIntervals;

      public:
        CoordinatesMapper(ConstChunk const& chunk); 

        void pos2coord(position_t pos, Coordinates& coord) const 
        {
            if (nDims == 1) { 
                coord[0] = origin[0] + pos;
                assert(pos < chunkIntervals[0]);
            } else if (nDims == 2) { 
                coord[1] = origin[1] + (pos % chunkIntervals[1]);
                pos /= chunkIntervals[1];
                coord[0] = origin[0] + pos;
                assert(pos < chunkIntervals[0]);
            } else { 
                for (int i = (int)nDims; --i >= 0;) {
                    coord[i] = origin[i] + (pos % chunkIntervals[i]);
                    pos /= chunkIntervals[i];
                }
                assert(pos == 0);
            }
        }

        position_t coord2pos(Coordinates const& coord) const
        {
            if (nDims == 1) { 
                return coord[0] - origin[0];
            } else if (nDims == 2) { 
                return (coord[0] - origin[0])*chunkIntervals[1] + (coord[1] - origin[1]);
            } else { 
                position_t pos = 0;
                for (size_t i = 0, n = nDims; i < n; i++) {
                    pos *= chunkIntervals[i];
                    pos += coord[i] - origin[i];
                }
                return pos;
            }
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
      protected:
        Address addr; // address of first chunk element
        void*   data; // uncompressed data (may be NULL if swapped out)
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

    /**
     * Chunk of temporary array which body can be located either in memory either on disk
     */
    class LruMemChunk : public MemChunk
    {
        friend class MemArray;
        friend class MemArrayIterator;

        LruMemChunk* next;
        LruMemChunk* prev;
        int64_t      swapFileOffset;
        size_t       accessCount;
        size_t       swapFileSize;

      public:
        bool pin() const;
        void unPin() const;

        bool isTemporary() const;

        bool isEmpty() {
            return next == this;
        }

        void prune() {
            next = prev = this;
        }

        void unlink() {
            next->prev = prev;
            prev->next = next;
            prune();
        }

        void link(LruMemChunk* elem) {
            elem->prev = this;
            elem->next = next;
            next = next->prev = elem;
        }

        virtual void write(boost::shared_ptr<Query>& query);

        LruMemChunk();
        ~LruMemChunk();
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
        MemArray(boost::shared_ptr<Array> input);
        ~MemArray();

      private:
        void initLRU();
        void swapOut();
        void pinChunk(LruMemChunk& chunk);
        void unpinChunk(LruMemChunk& chunk);

        ArrayDesc desc;
        map<Address, LruMemChunk, AddressLess> chunks;
        LruMemChunk lru;
        uint64_t usedMemSize;
        uint64_t usedMemThreshold;
        uint64_t usedFileSize;
        int swapFile;
        Mutex lruMutex;
    };

    /**
     * Temporary (in-memory) array iterator
     */
    class MemArrayIterator : public ArrayIterator
    {
        map<Address, LruMemChunk, AddressLess>::iterator curr;
        map<Address, LruMemChunk, AddressLess>::iterator last;
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
     * Coordinates comparator to be used in std::map
     */
    struct CoordinatesLess
    {
        bool operator()(const Coordinates& c1, const Coordinates& c2) const
        {
            assert(c1.size() == c2.size());
            for (size_t i = 0, n = c1.size(); i < n; i++) {
                if (c1[i] != c2[i]) {
                    return c1[i] < c2[i];
                }
            }
            return false;
        }
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
        bool   isPlain;
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

