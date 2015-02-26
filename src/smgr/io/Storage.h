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
 * Storage.h
 *
 *  Created on: 06.01.2010
 *      Author: knizhnik@garret.ru
 *      Description: Storage manager interface
 */

#ifndef STORAGE_H_
#define STORAGE_H_

#include <stdlib.h>
#include <string>
#include <exception>
#include <limits>

#include <system/Cluster.h>
#include "array/MemArray.h"
#include "array/Compressor.h"
#include <query/Query.h>

namespace scidb
{
    using namespace std;
    using namespace boost;

    /**
     * An extension of Address that specifies the chunk of a persistent array.
     *
     * Note: we do not use virtual methods here so there is no polymorphism.
     * We do not need virtual methods on these structures, so we opt for better performance.
     * Inheritance is only used to factor out similarity between two structures.
     *
     * Storage addresses have an interesting ordering scheme. They are ordered by
     * AttributeID, Coordinates, ArrayID (reverse). 
     *
     * Internally the storage manager keeps all chunks for a given array name in the same subtree.
     * For a given array, you will see this kind of ordering:
     *
     * AttributeID = 0
     *   Coordinates = {0,0}
     *     ArrayID = 1 --> CHUNK (this chunk exists in all versions >= 1)
     *     ArrayID = 0 --> CHUNK (this chunk exists only in version 0)
     *   Coordinates = {0,10}
     *     ArrayID = 2 --> NULL (tombstone)
     *     ArrayID = 0 --> CHUNK (this chunk exists only in versions 0 and 1; there's a tombstone at 2)
     * AttributeID = 1
     *   ...
     *
     * The key methods that implement iteration over an array are findChunk and findNextChunk.
     * For those methods, the address with zero-sized-list coordinates is considered to be the start of
     * the array. In practice, to find the first chunk in the array - create an address with coordinates
     * {} and then find the first chunk greater than it. This is why our comparison function cares about
     * the size of the coordinate list.
     */
    struct StorageAddress: public Address
    {
        /**
         * Array Identifier for the Versioned Array ID wherein this chunk first appeared.
         */
        ArrayID arrId;

        /**
         * Default constructor
         */
        StorageAddress():
            arrId(0)
        {}

        /**
         * Constructor
         * @param arrId array identifier
         * @param attId attribute identifier
         * @param coords element coordinates
         */
        StorageAddress(ArrayID arrId, AttributeID attId, Coordinates const& coords):
            Address(attId, coords), arrId(arrId)
        {}

        /**
         * Copy constructor
         * @param addr the object to copy from
         */
        StorageAddress(StorageAddress const& addr):
            Address(addr.attId, addr.coords), arrId(addr.arrId)
        {}

        /**
         * Partial comparison function, used to implement std::map
         * @param other another aorgument of comparison
         * @return true if "this" preceeds "other" in partial order
         */
        inline bool operator < (StorageAddress const& other) const
        {
            if(attId != other.attId)
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
            if (arrId != other.arrId)
            {
                //note: reverse ordering to keep most-recent versions at the front of the map
                return arrId > other.arrId;
            }
            return false;
        }

        /**
         * Equality comparison
         * @param other another aorgument of comparison
         * @return true if "this" equals to "other"
         */
        inline bool operator == (StorageAddress const& other) const
        {
            if (arrId != other.arrId)
            {
                return false;
            }

            return Address::operator ==( static_cast<Address const&>(other));
        }

        /**
         * Inequality comparison
         * @param other another argument of comparison
         * @return true if "this" not equals to "other"
         */
        inline bool operator != (StorageAddress const& other) const
        {
            return !(*this == other);
        }
    };

    class ListChunkDescriptorsArrayBuilder;
    class ListChunkMapArrayBuilder;

    /**
     * Descriptor of database segment.
     * It is used to describe partition or file used to store data
     */
    struct Segment
    {
        /**
         * Path to the file or raw partition
         */
        string path;
        /**
         * Size of raw partition or maximal size of the file
         */
        uint64_t size;

        /**
         * Segment constructor
         */
        Segment(string path, uint64_t size)
        {
            this->path = path;
            this->size = size;
        }

        /**
         * Default constructor
         */
        Segment() {}
    };

    /**
     * Storage manager interface
     */
    class Storage
    {
      public:
        virtual ~Storage() {}
        /**
         * Open storage manager at specified URL.
         * Format and sematic of storage URL depends in particular implementation of storage manager.
         * For local storage URL specifies path to the description file.
         * Description file has the following format:
         * ----------------------
         * <storage-header-path>
         * <segment1-size-Mb> <segment1-path>
         * <segment2-size-Mb> <segment2-path>
         * ...
         * ----------------------
         * @param url implementation dependent database url
         * @param cacheSize chunk cache size: amount of memory in bytes which can be used to cache most frequently used
         */
        virtual void open(const string& url, size_t cacheSize) = 0;

        /**
         * Get write iterator through array chunks available in the storage.
         * This iterator is used to create some new version K (=arrDesc.getId()).
         * It will only return new chunks that have been created in version K.
         * For chunks from versions (0...K-1) - use getConstArrayIterator. Furthermore, if
         * this is the first call to getArrayIterator for K, the caller must ensure that
         * a version of K exists in the system catalog first before calling this.
         * @param arrDesc array descriptor
         * @param attId attribute identifier
         * @param query in the context of which the iterator is requeted
         * @return shared pointer to the array iterator
         */
        virtual boost::shared_ptr<ArrayIterator> getArrayIterator(const ArrayDesc& arrDesc,
                                                                  AttributeID attId,
                                                                  boost::shared_ptr<Query>& query) = 0;

        /**
         * Get const array iterator through array chunks available in the storage.
         * @param arrDesc array descriptor
         * @param attId attribute identifier
         * @param query in the context of which the iterator is requeted
         * @return shared pointer to the array iterator
         */
        virtual boost::shared_ptr<ConstArrayIterator> getConstArrayIterator(const ArrayDesc& arrDesc,
                                                                            AttributeID attId,
                                                                            boost::shared_ptr<Query>& query) = 0;

        /**
         * Flush all changes to the physical device(s).
         * If power fault or system failure happens when there is some unflushed data, then these changes
         * can be lost
         */
        virtual void flush() = 0;

        /**
         * Close storage manager
         */
        virtual void close() = 0;

        /**
         * Set this instance identifier
         */
        virtual void setInstanceId(InstanceID id) = 0;

        /**
         * Get this instance identifier
         */
        virtual InstanceID getInstanceId() const = 0;

        /**
         * Remove a persistent array from the system. Does nothing if the
         * specified array is not present.
         * @param uaId the Unversioned Array ID
         * @param arrId the Versioned Array ID
         * @param timestamp optional - if set, remove only chunks older than timestamp
         */
        virtual void remove(ArrayUAID uaId, ArrayID arrId, uint64_t timestamp = 0) = 0;

        /**
         * Map value of this coordinate to the integer value
         * @param indexName name of coordinate index
         * @param value original coordinate value
         * @param query performing the mapping
         * @return ordinal number to which this value is mapped
         */
        virtual Coordinate mapCoordinate(string const& indexName, DimensionDesc const& dim,
                                         Value const& value, CoordinateMappingMode mode,
                                         const boost::shared_ptr<Query>& query) = 0;

        /**
         * Perform reverse mapping of integer dimension to the original dimension domain
         * @param indexName name of coordinate index
         * @param pos integer coordinate
         * @param query performing the mapping
         * @return original value for the coordinate
         */
        virtual Value reverseMapCoordinate(string const& indexName,
                                           DimensionDesc const& dim, Coordinate pos,
                                           const boost::shared_ptr<Query>& query) = 0;

        /**
         * Remove coordinate mapping 
         * @param indexName name of coordinate index        
         */
        virtual void removeCoordinateMap(string const& indexName) = 0;

        /**
         * Rollback uncompleted updates
         * @param map of updated array which has to be rollbacked
         */
        virtual void rollback(std::map<ArrayID,VersionID> const& undoUpdates) = 0;

        /**
         * Recover instance
         * @param recoveredInstance ID of recovered instance
         */
        virtual void recover(InstanceID recoveredInstance, boost::shared_ptr<Query>& query) = 0;

        struct DiskInfo 
        {             
            uint64_t used;
            uint64_t available;
            uint64_t clusterSize;
            uint64_t nFreeClusters;
            uint64_t nSegments;
        };

        virtual uint64_t getCurrentTimestamp() const = 0;
        
        virtual void getDiskInfo(DiskInfo& info) = 0;

        /**
         * Method for creating a list of chunk descriptors. Implemented by LocalStorage.
         * @param builder a class that creates a list array
         */
        virtual void listChunkDescriptors(ListChunkDescriptorsArrayBuilder& builder)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "chunk header retrieval is not supported by this storage type.";
        }

        /**
         * Method for creating a list of chunk map elements. Implemented by LocalStorage.
         * @param builder a class that creates a list array
         */
        virtual void listChunkMap(ListChunkMapArrayBuilder& builder)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "chunk map retrieval is not supported by this storage type.";
        }

        /**
         * Decompress chunk from the specified buffer
         * @param chunk destination chunk to receive decompressed data
         * @param buf buffer containing compressed data.
         */
        virtual void decompressChunk(Chunk* chunk, CompressedBuffer const& buf) = 0;

        /**
         * Compress chunk to the specified buffer
         * @param chunk chunk to be compressed
         * @param buf buffer where compressed data will be placed. It is inteded to be initialized using default constructore and will be filled by this method.
         */
        virtual void compressChunk(Chunk const* chunk, CompressedBuffer& buf) = 0;

        /**
         * Pin chunk in memory: prevent cache replacement algorithm to throw away this chunk from memory
         * @param chunk chunk to be pinned in memory
         */
        virtual void pinChunk(Chunk const* chunk) = 0;

        /**
         * Unpin chunk in memory: decrement access couter for this chunk
         * @param chunk chunk to be unpinned
         */
        virtual void unpinChunk(Chunk const* chunk) = 0;

        /**
         * Write new chunk in the storage.
         * @param chunk new chunk created by newChunk Method
         */
        virtual void writeChunk(Chunk* chunk, boost::shared_ptr<Query>& query) = 0;

        /**
         * Find and fetch a chunk from a particular array. Throws exception if chunk does not exist.
         * @param desc the array descriptor
         * @param addr the address of the chunk
         * @return the pointer to the chunk.
         */
        virtual Chunk* readChunk(ArrayDesc const& desc, StorageAddress const& addr) = 0;

        /**
         * Load chunk body from the storage.
         * @oaran desc the array descriptor
         * @param chunk loaded chunk
         */
        virtual void loadChunk(ArrayDesc const& desc, Chunk* chunk) = 0;

        /**
         * Get latch for the specified chunk
         * @param chunk chunk to be locked
         */
        virtual RWLock& getChunkLatch(DBChunk* chunk) = 0;

        /**
         * Create new chunk in the storage. There should be no chunk with such address in the storage.
         * @param desc the array descriptor
         * @param addr chunk address
         * @param compressionMethod compression method for this chunk
         * @throw SystemException if the chunk with such address already exists
         */
        virtual Chunk* createChunk(ArrayDesc const& desc, StorageAddress const& addr, int compressionMethod) = 0;

	    /**
         * Clone a persistent chunk; create a chunk in the target array which acts as a "pointer" to some chunk in the source array.
         * Currently, this function has only one caller - the message handler, which receives a message sent by a private method of CachedStorage.
         * This is only used when both arrays are immutable and (obviously) the chunk contains the exact same data.
         * @param pos the coordinates of the chunk
         * @param targetDesc the descriptor of the target array
         * @param targetAttrID the attribute id of the chunk in the target array 
         * @param sourceDesc the descriptor of the source array
         * @param sourceAttrID the attribute id of the chunk in the source array
         */
        virtual void cloneChunk(Coordinates const& pos, ArrayDesc const& targetDesc, AttributeID targetAttrID, ArrayDesc const& sourceDesc, AttributeID sourceAttrID) = 0;

        /**
         * Delete chunk
         * @param chunk chunk to be deleted
         */
        virtual void deleteChunk(Chunk& chunk) = 0;

        virtual size_t getNumberOfInstances() const = 0;

        /**
         * Given an array descriptor and an address of a chunk - compute the InstanceID of the primary instance
         * that shall be responsible for this chunk. Currently this uses our primitive round-robin hashing
         * for all cases except replication. To be improved.
         * Note the same replication scheme is used for both - regular chunks and tombstones.
         * @param desc the array descriptor
         * @param address the address of the chunk (or tombstone entry)
         * @return the instance id responsible for this datum
         */
        virtual InstanceID getPrimaryInstanceId(ArrayDesc const& desc, StorageAddress const& address) const =0;

        virtual Array const& getDBArray(ArrayID) = 0;
  
        /**
         * Get a list of the chunk positions for a particular persistent array. If the array is not found, no fields
         * shall be added to the chunks argument.
         * @param[in] desc the array descriptor. Must be for a persistent stored array with proper identifiers.
         * @param[in] query the query context. 
         * @param[out] chunks the set of coordinates to which the chunk positions of the array shall be appended.
         */       
        virtual void getChunkPositions(ArrayDesc const& desc, boost::shared_ptr<Query> const& query, CoordinateSet& chunks) = 0;

         /**
          * Given an array descriptor and a storage address for a chunk - find the storage address in the next chunk along the same attribute
          * in stride major order. The Array UAID and ID is taken from desc. The current coordinates and Attribute ID are taken from address.
          * The address whose coordinates are a zero-sized are considered to be the end of the array. If address.coords has size zero, then
          * the method shall attempt to find the first chunk for this array. Similarly if there is no next chunk, the method shall set
          * address.coords to a zero-sized list. Otherwise, the method shall set address.arrId and address.coords to the correct values
          * by which the next chunk can be retrieved.
          * @param desc the array descriptor for the desired array.
          * @param query the query context
          * @param address the address of the previous chunk
          * @return true if the chunk was found, false otherwise
          */
         virtual bool findNextChunk(ArrayDesc const& desc, boost::shared_ptr<Query> const& query, StorageAddress& address) =0;

         /**
          * Given and array descriptor and a desired storage address for the chunk, determine if there is a chunk at
          * address.attId, address.coords. If this version of this array (as described by desc) contains this chunk -
          * then set address.arrId to the proper value. Otherwise, set address.coords to a zero-sized list.
          * @param desc the array descriptor for the desired array
          * @param query the query context
          * @param address the address of the desired chunk
          * @return true if the chunk was found, false otherwise
          */
         virtual bool findChunk(ArrayDesc const& desc, boost::shared_ptr<Query> const& query, StorageAddress& address) =0;

         /**
          * Remove a previously existing chunk from existence in the given version on this instance.
          * This function alters the local storage only. This instance may or may not be responsible for this chunk and the caller bears
          * the responsibility of determining that. Note this method removes all attributes at once.
          * @param arrayDesc the array descriptor. The id field is used for version purposes.
          * @param coords the coordinates of the removed chunk
          */
         virtual void removeLocalChunkVersion(ArrayDesc const& arrayDesc, Coordinates const& coords) =0;

         /**
          * Remove a previously existing chunk from existence in the given version in the system.
          * Effectively this function sends the proper replica messages and then calls removeLocalChunkVersion on this instance.
          * Note this method removes all attributes at once.
          * @param arrayDesc the array descriptor 
          * @param coords the coordinates of the tombstone
          * @param query the query context
          */
         virtual void removeChunkVersion(ArrayDesc const& arrayDesc, Coordinates const& coords, boost::shared_ptr<Query>& query) =0;

         /**
          * Given an array descriptor desc and the coordinate set liveChunks - remove the chunk version for every
          * chunk that is in the array and NOT in liveChunks. This is used by overriding-storing ops to ensure that
          * new versions of arrays do not contain chunks from older versions unless explicitly added.
          * @param arrayDesc the array descriptor
          * @param liveChunks the set of chunks that should NOT be tombstoned
          * @param query the query context
          */
         virtual void removeDeadChunks(ArrayDesc const& arrayDesc, set<Coordinates, CoordinatesLess> const& liveChunks, boost::shared_ptr<Query>& query) = 0;
    };

    /**
     * Storage factory.
     * By default it points to local storage manager implementation.
     * But it is possible to register any storae manager implementation using setInstance method
     */
    class StorageManager
    {
      public:
        /**
         * Set custom miplementaiton of storage manager
         */
        static void setInstance(Storage& storage) {
            instance = &storage;
        }
        /**
         * Get instance of the storage (it is assumed that there can be only one storage in the application)
         */
        static Storage& getInstance() {
            return *instance;
        }
      private:
        static Storage* instance;
    };
}

#endif
