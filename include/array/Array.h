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
 * @file Array.h
 *
 * @brief The Array interface of SciDB
 *
 * Arrays are accessed via chunk iterators, which in turn have item iterators.
 * We have constant and volatile iterators, for read-only or write-once access to the arrays.
 */

#ifndef ARRAY_H_
#define ARRAY_H_

#include <boost/shared_ptr.hpp>
#include <boost/noncopyable.hpp>
#include "array/Metadata.h"
#include "query/Aggregate.h"
#include "query/TypeSystem.h"
#include "query/Statistics.h"

namespace scidb
{
class Array;
class Query;
class Chunk;
class DBChunk;
class ConstArrayIterator;
class ConstRLEEmptyBitmap;

/** \brief SharedBuffer is an abstract class for binary data holding
 *
 * It's used in network manager for holding binary data.
 * Before using object you should pin it and unpin after using.
 */
class SharedBuffer
{
public:
   virtual ~SharedBuffer() { }
    /**
     * @return constant pointer to binary buffer. You can only read this data.
     * Note, data is available only during object is live.
     */
    virtual void* getData() const;

    /**
     * @return size of buffer in bytes
     */
    virtual size_t getSize() const;

    /**
     * Method allocates memory for buffer inside implementation.
     * Implementor will manage this buffer itself.
     * @param size is a necessary size of memory to be allocated.
     */
    virtual void allocate(size_t size);

    /**
     * Method reallocates memory for buffer inside implementation.
     * Old buffer content is copuied to the new location.
     * Implementor will manage this buffer itself.
     * @param size is a necessary size of memory to be allocated.
     */
    virtual void reallocate(size_t size);

    /**
     * Free memory. After execution of this method getData() should return NULL.
     */
    virtual void free();

    /**
     * Tell to increase reference counter to hold buffer in memory
     * @return true if buffer is pinned (need to be unpinned) false otherwise
     */
    virtual bool pin() const;

    /**
     * Tell to decrease reference counter to release buffer in memory
     * to know when it's not needed.
     */
    virtual void unPin() const;
};

class MemoryBuffer : public SharedBuffer
{
  private:
    char*  data;
    size_t size;
    bool   copied;

  public:
    void* getData() const
    {
        return data;
    }

    size_t getSize() const
    {
        return size;
    }

    void free()
    {
        if (copied) {
            delete[] data;
        }
        data = NULL;
    }

    bool pin() const
    {
        return false;
    }

    void unPin() const
    {
    }

    ~MemoryBuffer()
    {
        free();
    }

    MemoryBuffer(const void* ptr, size_t len, bool copy = true) {
        if (copy) {
            data = new char[len];
            if (ptr != NULL) {
                memcpy(data, ptr, len);
            }
            copied = true;
            currentStatistics->allocatedSize += len;
            currentStatistics->allocatedChunks++;
        } else {
            data = (char*)ptr;
            copied = false;
        }
        size = len;
    }
};

/**
 * Buffer with compressed data
 */
class CompressedBuffer : public SharedBuffer
{
  private:
    size_t compressedSize;
    size_t decompressedSize;
    void*  data;
    int    compressionMethod;
    int    accessCount;
  public:
    virtual void* getData() const;
    virtual size_t getSize() const;
    virtual void allocate(size_t size);
    virtual void reallocate(size_t size);
    virtual void free();
    virtual bool pin() const;
    virtual void unPin() const;

    int    getCompressionMethod() const;
    void   setCompressionMethod(int compressionMethod);

    size_t getDecompressedSize() const;
    void   setDecompressedSize(size_t size);

    CompressedBuffer(void* compressedData, int compressionMethod, size_t compressedSize, size_t decompressedSize);
    CompressedBuffer();
    ~CompressedBuffer();
};


/**
 * Macro to set coordinate in ChunkIterator::moveNext mask
 */
#define COORD(i) ((uint64_t)1 << (i))

class ConstChunk;
class MemChunk;

/**
 * Common const iterator interface
 */
class ConstIterator
{
public:
    /**
     * Check if end of chunk is reached
     * @return true if iterator reaches the end of the chunk
     */
    virtual bool end() = 0;

    /**
     * Position cursor to the next element (order of traversal depends on used iteration mode)
     */
    virtual void operator ++() = 0;

    /**
     * Get coordinates of the current element in the chunk
     */
    virtual Coordinates const& getPosition() = 0;

    /**
     * Set iterator's current positions
     * @return true if specified position is valid (belongs to the chunk and match current iteratation mode),
     * false otherwise
     */
    virtual bool setPosition(Coordinates const& pos) = 0;

    /**
     * Reset iteratot to the first element
     */
    virtual void reset() = 0;

    virtual ~ConstIterator();

};


/**
 * Iterator over items in the chunk. The chunk consists of a number of Value entries with
 * positions in the coordinate space, as well as flags:
 *      NULL - the value is unknown
 *      core - the value is a core value managed by the current instance
 *      overlap - the value is an overlap value, it can only be used for computation, but
 *              its managed by some other site
 */
class ConstChunkIterator : public ConstIterator
{
  public:
    /**
     * Constants used to specify iteration mode mask
     */
    enum IterationMode {
        /**
         * Ignore components having null value
         */
        IGNORE_NULL_VALUES  = 1,
        /**
         * Ignore empty array elements
         */
        IGNORE_EMPTY_CELLS = 2,
        /**
         * Ignore overlaps
         */
        IGNORE_OVERLAPS = 4,
        /**
         * Do not check for empty cells event if there is empty attribute in array
         */
        NO_EMPTY_CHECK = 8,
        /**
         * Flag used write iterator to initialize sparse chunk
         */
        SPARSE_CHUNK = 16,
        /**
         * Append to the existed chunk
         */
        APPEND_CHUNK = 32,
        /**
         * Ignore default value in sparse array
         */
        IGNORE_DEFAULT_VALUES = 64,
        /**
         * Vector mode
         */
        VECTOR_MODE = 128,
        /**
         * Tile mode
         */
        TILE_MODE = 256,
        /**
         * Data is written in stride-major order
         */
        SEQUENTIAL_WRITE = 512,
        /**
         * Intended tile mode
         */
        INTENDED_TILE_MODE = 1024
    };

    /**
     * Get current iteration mode
     */
    virtual int getMode() = 0;

    /**
     * Checks if iterator supports vector iteration mode
     */
    virtual bool supportsVectorMode() const;

    /**
     * Enable vector mode
     * @param enabled true to enable vector mode, false - for scalar mode
     */
    virtual void setVectorMode(bool enabled);

    /**
     * Get current element value
     */
    virtual Value& getItem() = 0;

    /**
     * Check if current array cell is empty (if iteration mode allows visiting of empty cells)
     */
    virtual bool isEmpty() = 0;

    /**
     * Move forward in the specified direction
     * @param direction bitmask of of coordinates in which direction movement is performed,
     * for example in case of two dimensional matrix [I=1:10, J=1:100]
     * moveNext(COORD(0)) increments I coordinate, moveNext(COORD(1)) increments J coordinate and
     * moveNext(COORD(0)|COORD(1)) increments both coordinates
     * @return false if movement in the specified direction is not possible
     */
    virtual bool forward(uint64_t direction = COORD(0));

    /**
     * Move backward in the specified direction
     * @param direction bitmask of of coordinates in which direction movement is performed,
     * for example in case of two dimensional matrix [I=1:10, J=1:100]
     * moveNext(COORD(0)) decrements I coordinate, moveNext(COORD(1)) decrements J coordinate and
     * moveNext(COORD(0)|COORD(1)) decrements both coordinates
     * @return false if movement in the specified direction is not possible
     */
    virtual bool backward(uint64_t direction = COORD(0));

    /**
     * Get iterated chunk
     */
    virtual ConstChunk const& getChunk() = 0;

    /**
     * Get first position in the iterated chunk according to the iteration mode
     */
    virtual Coordinates const& getFirstPosition();

    /**
     * Get last position in the iterated chunk according to the iteration mode
     */
    virtual Coordinates const& getLastPosition();
};


/**
 * The volatile iterator can also write items to the array
 */
class ChunkIterator : public ConstChunkIterator
{
protected:
    /**
     * Exception-safety control flag. This is checked by the 
     * RLEChunkIterator, MemChunkIterator and SparseChunkIterator during destruction.
     * It is used to make sure unPin() is called upon destruction, unless flush() already executed.
     * 
     * It would be ideal to place the whole machinery into the superclass. However it's not
     * possible because it's unsafe call subclass methods from the superclass destructor
     * 
     * Ergo, if you override the flush method, you need to pay careful attention to this flag.
     *
     * However the situation will improve because soon we WILL get rid of MemChunkIterator and
     * SparseChunkIterator. Then, RLEChunkIterator will be the only class that needs an auto-flush
     * mechanism and all will be contained in that class.
     * --AP 9.4.2012
     */
    bool _needsFlush;

public:
    ChunkIterator(): _needsFlush(true)
    {}

    /**
     * Update the current element value
     */
     virtual void writeItem(const  Value& item) = 0;

    /**
     * Save all changes done in the chunk
     */
    virtual void flush() = 0;

    virtual boost::shared_ptr<Query> getQuery() { return boost::shared_ptr<Query>(); }
};

/**
 * A read only chunk interface provides information on whether the chunk is:
 *   readonly - isReadOnly()
 *   positions:
 *      getFirstPosition(withOverlap) - provides the smallest position in stride-major order
 *      getLastPosition(withOverlap) - provides the largest position in stride-major order
 *      positions can be computed with or without taking overlap items into account
 *  Also the chunk can be:
 *  An iterator can be requested to access the items in the chunk:
 *      getConstIterator() - returns a read-only iterator to the items
 *      getIterator() - returns a volatile iterator to the items (the chunk cannot be read-only)
 */
class ConstChunk : public SharedBuffer
{
  public:
    /**
     * Check if this chunk may participate in a merge simply by bitwise or.
     * Possible if: not sparse, not RLE, attr not nullable, type not variable size, and default value is zero.
     */
    virtual bool isPossibleToMergeByBitwiseOr() const {
        if (isSparse() || isRLE()) {
            return false;
        }

        AttributeDesc const& attr = getAttributeDesc();
        if (attr.isNullable() || TypeLibrary::getType(attr.getType()).variableSize()) {
            return false;
        }

        return attr.getDefaultValue().isZero();
    }

    /**
     * Check if chunk contains plain data: non nullable, non-emptyable, non-sparse
     */
   virtual bool isPlain() const;

   virtual bool isReadOnly() const;

   /**
    * Check if chunk data is stored somewhere (in memory on on disk)
    */
   virtual bool isMaterialized() const;

   /**
    * Get disk chunk containing data of this chunk
    * @return DBChunk if data of this chunk is stored on the disk, NULL otherwise
    */
   virtual DBChunk const* getDiskChunk() const;

   size_t getBitmapSize() const;

   /**
    * Check if chunk contains sparse data
    */
   virtual bool isSparse() const;

   /**
    * Check if chunk is in RLE encoding
    */
   virtual bool isRLE() const;

   /**
    * Get array descriptor
    */
   virtual const ArrayDesc& getArrayDesc() const = 0;

   /**
    * Get chunk attribute descriptor
    */
   virtual const AttributeDesc& getAttributeDesc() const = 0;

   /**
    * Count number of present (non-empty) elements in the chunk
    */
   virtual size_t count() const;

   /**
    * Check if count of non-empty elements in the chunk is known
    */
   virtual bool isCountKnown() const;

   /**
    * Get numer of element in the chunk
    */
   size_t getNumberOfElements(bool withOverlap) const;

    /**
     * If chunk contains no gaps in its data: has no overlaps and fully belongs to non-emptyable array.
     */
   bool isSolid() const;

   virtual Coordinates const& getFirstPosition(bool withOverlap) const = 0;
   virtual Coordinates const& getLastPosition(bool withOverlap) const = 0;

   virtual Coordinates getHighBoundary(bool withOverlap) const;
   virtual Coordinates getLowBoundary(bool withOverlap) const;

   bool contains(Coordinates const& pos, bool withOverlap) const;

   virtual boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode = ConstChunkIterator::IGNORE_OVERLAPS|ConstChunkIterator::IGNORE_EMPTY_CELLS) const = 0;
   ConstChunkIterator* getConstIteratorPtr(int iterationMode = ConstChunkIterator::IGNORE_OVERLAPS|ConstChunkIterator::IGNORE_EMPTY_CELLS) {
      // TODO JHM ; temporary bridge to support concurrent development, to be removed by the end of RQ
#ifndef NO_SUPPPORT_FOR_SWIG_TARGETS_THAT_CANT_HANDLE_SHARED_PTRS
      return getConstIterator(iterationMode).operator->();
#else
      assert(false);
      return NULL;
#endif // NO_SUPPPORT_FOR_SWIG_TARGETS_THAT_CANT_HANDLE_SHARED_PTRS
   }

   virtual int getCompressionMethod() const = 0;

   /**
    * Compress chunk data info the specified buffer.
    * @param buf buffer where compressed data will be placed. It is intended to be initialized using default constructor and will be filled by this method.
    */
    virtual void compress(CompressedBuffer& buf, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const;

    virtual void* getData() const;
    virtual size_t getSize() const;
    virtual bool pin() const;
    virtual void unPin() const;

    virtual Array const& getArray() const = 0;
    
    void makeClosure(Chunk& closure, boost::shared_ptr<ConstRLEEmptyBitmap> const& emptyBitmap) const;

    virtual boost::shared_ptr<ConstRLEEmptyBitmap> getEmptyBitmap() const;
    virtual ConstChunk const* getBitmapChunk() const;

    ConstChunk* materialize() const;

    virtual void overrideTileMode(bool) {}
    
 protected:
    ConstChunk();
    virtual ~ConstChunk();
    
    MemChunk* materializedChunk;
    boost::shared_ptr<ConstArrayIterator> emptyIterator;
};

/**
 * New (intialized) chunk implementation
 */
class Chunk : public ConstChunk
{
   double expectedDensity;

 protected:
   Chunk() {
      expectedDensity = 0;
   }

 public:

   /**
    * Allocate and memcpy from a raw byte array.
    */
   virtual void allocateAndCopy(char const* input, size_t byteSize, bool isSparse, bool isRle, size_t count, boost::shared_ptr<Query>& query) {
       assert(getData()==NULL);
       assert(input!=NULL);

       allocate(byteSize);
       setSparse(isSparse);
       setRLE(isRle);
       setCount(count);
       memcpy(getData(), input, byteSize);

       write(query);
   }

   /**
    * Merge by bitwise-or with a byte array.
    */
   virtual void mergeByBitwiseOr(char const* input, size_t byteSize, boost::shared_ptr<Query>& query) {
       assert(isPossibleToMergeByBitwiseOr());

       if (byteSize != getSize()) {
           throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CANT_MERGE_CHUNKS_WITH_VARYING_SIZE);
       }
       char* dst = static_cast<char*>(getData());
       assert(dst!=NULL);
       assert(input!=NULL);

       bitwiseOpAndAssign<WrapperForOr<uint64_t>, WrapperForOr<char> >(dst, input, byteSize);

       write(query);
   }

   virtual bool isReadOnly() const {
      return false;
   }

   /**
    * Set expected sparse chunk density
    */
   void setExpectedDensity(double density) {
      expectedDensity = density;
   }

   /**
    * Get expected sparse chunk density
    */
   double getExpectedDensity() const {
      return expectedDensity;
   }

   /**
    * Trigger sparse data indicator
    */
   virtual void setSparse(bool sparse);

   virtual void setRLE(bool rle);

   /**
    * Decompress chunk from the specified buffer.
    * @param buf buffer containing compressed data.
    */
   virtual void decompress(const CompressedBuffer& buf);

   virtual boost::shared_ptr<ChunkIterator> getIterator(boost::shared_ptr<Query> const& query,
                                                        int iterationMode = ChunkIterator::NO_EMPTY_CHECK) = 0;

   virtual void merge(ConstChunk const& with,
                      boost::shared_ptr<Query>& query);

   /**
    * Perform a generic aggregate-merge of this with another chunk.
    * This is an older algorithm. Currently only used by aggregating redimension.
    * @param[in] with chunk to merge with. Must be filled out by an aggregating op.
    * @param[in] aggregate the aggregate to use
    * @param[in] query the query context
    */
   virtual void aggregateMerge(ConstChunk const& with,
                               AggregatePtr const& aggregate,
                               boost::shared_ptr<Query>& query);

   /** 
    * Perform an aggregate-merge of this with another chunk.
    * This function is optimized for current group-by aggregates, which 
    * are liable to produce sparse chunks with many nulls. This method does NOT work
    * if the intermediate aggregating array is emptyable (which is what redimension uses).
    * @param[in] with chunk to merge with. Must be filled out by an aggregating op.
    * @param[in] aggregate the aggregate to use
    * @param[in] query the query context
    */ 
   virtual void nonEmptyableAggregateMerge(ConstChunk const& with,
                                   AggregatePtr const& aggregate,
                                   boost::shared_ptr<Query>& query);

   virtual void write(boost::shared_ptr<Query>& query);
   virtual void truncate(Coordinate lastCoord);
   virtual void setCount(size_t count);
};

/**
 * An array const iterator iterates over the chunks of the array available at the local instance.
 * Order of iteration is not specified.
 */
class ConstArrayIterator : public ConstIterator
{
public:
    /**
     * Select chunk which contains element with specified position in main (not overlapped) area
     * @param pos element position
     * @return true if chunk with containing specified position is present at the local instance, false otherwise
     */
    virtual bool setPosition(Coordinates const& pos);

    /**
     * Restart iterations from the beginning
     */
    virtual void reset();

    /**
     * Get current chunk
     */
    virtual ConstChunk const& getChunk() = 0;
};

/**
 * The volatile iterator can also write chunks to the array
 */
class ArrayIterator : public ConstArrayIterator
{
public:
    virtual Chunk& updateChunk();

    /**
     * Create new chunk at the local instance using default compression method for this attribute.
     * Only one chunk can be created and filled by iterator at each moment of time.
     * @param position of the first element in the created chunk (not including overlaps)
     */
    virtual Chunk& newChunk(Coordinates const& pos) = 0;

    /**
     * Create new chunk at the local instance.
     * Only one chunk can be created and filled by iterator at each moment of time.
     * @param position of the first element in the created chunk (not including overlaps)
     */
    virtual Chunk& newChunk(Coordinates const& pos, int compressionMethod) = 0;

    /**
     * Copy chunk
     * @param srcChunk source chunk
     */
    virtual Chunk& copyChunk(ConstChunk const& srcChunk, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap);

    virtual Chunk& copyChunk(ConstChunk const& srcChunk) {
        boost::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
        return copyChunk(srcChunk, emptyBitmap);
    }

    virtual void deleteChunk(Chunk& chunk);

    virtual boost::shared_ptr<Query> getQuery() { return boost::shared_ptr<Query>(); }
};

class Array;

/**
 * Iterator through all array elements. This iterator combines array and chunk iterators.
 * Please notice that using that using random positioning in array can cause very significant degradation of performance
 */
class ConstItemIterator : public ConstChunkIterator
{
  public:
    virtual int getMode();
    virtual  Value& getItem();
    virtual bool isEmpty();
    virtual ConstChunk const& getChunk() ;
    virtual bool end();
    virtual void operator ++();
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos);
    virtual void reset();

    ConstItemIterator(Array const& array, AttributeID attrID, int iterationMode);

  private:
    boost::shared_ptr<ConstArrayIterator> arrayIterator;
    boost::shared_ptr<ConstChunkIterator> chunkIterator;
    int iterationMode;
};

/**
 * The array interface provides metadata about the array, including its handle, type and
 * array descriptors.
 * To access the data in the array, a constant (read-only) iterator can be requested, or a
 * volatile iterator can be used.
 */
class Array:
// TODO JHM ; temporary bridge to support concurrent development, to be removed by the end of RQ
#ifndef NO_SUPPPORT_FOR_SWIG_TARGETS_THAT_CANT_HANDLE_PROTECTED_BASE_CLASSES
    public SelfStatistics
#else
    protected SelfStatistics
#endif // NO_SUPPPORT_FOR_SWIG_TARGETS_THAT_CANT_ACCEPT_PROTECTED_BASE_CLASSES
{
public:

    /**
     * An enum that defines three levels of Array read access policy - ranging from most restrictive to least restrictive.
     */
    enum Access
    {
        /**
         * Most restrictive access policy wherein the array can only be iterated over one time.
         * If you need to read multiple attributes, you need to read all of the attributes horizontally, at the same time.
         * Imagine that reading the array is like scanning from a pipe - after a single scan, the data is no longer available.
         * This is the only supported access mode for InputArray and MergeSortArray
         */
        SINGLE_PASS = 0,

        /**
         * A policy wherein the array can be iterated over several times, and various attributes can be scanned independently,
         * but the ArrayIterator::setPosition() function is not supported.
         * This is less restrictive than SINGLE_PASS.
         * This is the least restrictive access mode fsupported by ConcatArray
         */
        MULTI_PASS  = 1,

        /**
         * A policy wherein the client of the array can use the full functionality of the API.
         * This is the least restrictive access policy and it's supported by the vast majority of Array subtypes.
         */
        RANDOM      = 2
    };

    virtual ~Array() {}

    /**
     * Get array name
     */
    virtual std::string const& getName() const;

    /**
     * Get array identifier
     */
    virtual ArrayID getHandle() const;

    virtual bool isRLE() const
    {
        return false;
    }

    /**
     * Determine if this array has an easily accessible list of chunk positions. In fact, a set of chunk positions can
     * be generated from ANY array simply by iterating over all of the chunks once. However, this function will return true
     * if retrieving the chunk positions is a separate routine that is more efficient than iterating over all chunks.
     * All materialized arrays can and should implement this function.
     * @return true if this array supports calling getChunkPositions(). false otherwise.
     */
    virtual bool hasChunkPositions() const
    {
        return false;
    }

    /**
     * Build and return a list of the chunk positions. Only callable if hasChunkPositions() returns true, throws otherwise.
     * @return the sorted set of coordinates, containing the first coordinate of every chunk present in the array
     */
    virtual boost::shared_ptr<CoordinateSet> getChunkPositions() const;

    /**
     * Determine if the array is materialized; which means all chunks are populated either memory or on disk, and available on request.
     * This returns false by default as that is the case with all arrays. It returns true for MemArray, etc.
     * @return true if this is materialized; false otherwise
     */
    virtual bool isMaterialized() const
    {
        return false;
    }

    /**
     * Get the least restrictive access mode that the array supports. The default for the abstract superclass is RANDOM
     * as a matter of convenience, since the vast majority of our arrays support it. Subclasses that have access
     * restrictions are responsible for overriding this appropriately.
     * @return the least restrictive access mode
     */
    virtual Access getSupportedAccess() const
    {
        return RANDOM;
    }

    /**
     * Extract subarray between specified coordinates in the buffer.
     * @param attrID extracted attribute of the array (should be fixed size)
     * @param buf buffer preallocated by caller which should be preallocated by called and be large enough
     * to fit all data.
     * @param first minimal coordinates of extract box
     * @param last maximal coordinates of extract box
     * @return number of extracted chunks
     */
    virtual size_t extractData(AttributeID attrID, void* buf, Coordinates const& first, Coordinates const& last) const;

    /**
     * Append data from the array
     * @param[in] input source array
     * @param[in] vertical the traversal order of appending: if true - append all chunks for attribute 0, then attribute 1...
     *            If false - append the first chunk for all attributes, then the second chunk...
     * @param[out] newChunkCoordinates if set - the method shall insert the coordinates of all appended chunks into the set pointed to.
     */
    virtual void append(boost::shared_ptr<Array>& input, bool const vertical = true,  std::set<Coordinates, CoordinatesLess>* newChunkCoordinates = NULL);

    /**
     * Get array descriptor
     */
    virtual ArrayDesc const& getArrayDesc() const = 0;

    /**
     * Get read-write iterator
     * @param attr attribute ID
     * @return iterator through chunks of spcified attribute
     */
    virtual boost::shared_ptr<ArrayIterator> getIterator(AttributeID attr);

    /**
     * Get read-only iterator
     * @param attr attribute ID
     * @return read-only iterator through chunks of spcified attribute
     */
    virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attr) const = 0;

    ConstArrayIterator* getConstIteratorPtr(AttributeID attr) const {
// TODO JHM ; temporary bridge to support concurrent development, to be removed by the end of RQ
#ifndef NO_SUPPPORT_FOR_SWIG_TARGETS_THAT_CANT_HANDLE_SHARED_PTRS
        return getConstIterator(attr).operator->();
#else
        assert(false);
        return NULL;
#endif // NO_SUPPPORT_FOR_SWIG_TARGETS_THAT_CANT_HANDLE_SHARED_PTRS
    }

    /**
     * Get read-only iterator thtough all array elements
     * @param attr attribute ID
     * @param iterationMode chunk iteration mode
     */
    virtual boost::shared_ptr<ConstItemIterator> getItemIterator(AttributeID attr, int iterationMode = ConstChunkIterator::IGNORE_OVERLAPS|ConstChunkIterator::IGNORE_EMPTY_CELLS) const;

    /**
     * Convert intgeger coordiantes to original coordinates
     * @param origCoords [OUT] original coordiantes
     * @param intCoords [IN] integer coordiantes
     */
    virtual void getOriginalPosition(std::vector<Value>& origCoords, Coordinates const& intCoords, const boost::shared_ptr<Query>& query = boost::shared_ptr<Query>()) const;
};

class PinBuffer {
    SharedBuffer const& buffer;
    bool pinned;
  public:
    PinBuffer(SharedBuffer const& buf) : buffer(buf) {
        pinned = buffer.pin();
    }
    
    bool isPinned() const { 
        return pinned;
    }

    ~PinBuffer() {
        if (pinned) {
            buffer.unPin();
        }
    }
};


/**
 * Constructed around a chunk pointer to automatically unpin the chunk on destruction.
 * May be initially constructed with NULL pointer, in which case the poiter may (or may not) be reset
 * to a valid chunk pointer.
 */
class UnPinner : public boost::noncopyable
{
private:
    Chunk* _buffer;

public:
    /**
     * Create an unpinner.
     * @param buffer the chunk pointer; can be NULL
     */
    UnPinner(Chunk* buffer) : _buffer(buffer)
    {}

    ~UnPinner()
    {
        if (_buffer)
        {
            _buffer->unPin();
        }
    }

    /**
     * Set or reset the unpinner pointer.
     * @param buf the chunk pointer; can be NULL
     */
    void set(Chunk* buf)
    {
        _buffer = buf;
    }
};


}

#endif /* ARRAY_H_ */
