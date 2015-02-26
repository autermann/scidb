/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2014 SciDB, Inc.
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

/*
 * InternalStorage.h
 *
 *  Created on: 06.01.2010
 *      Author: knizhnik@garret.ru
 *              sfridella@paradigm4.com
 *      Description: Internal storage manager interface
 */

#ifndef INTERNAL_STORAGE_H_
#define INTERNAL_STORAGE_H_

#include <dirent.h>

#include "Storage.h"
#include "ReplicationManager.h"
#include "DataStore.h"
#include <map>
#include <vector>
#include <string>
#include <boost/unordered_map.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <array/MemArray.h>
#include <array/DBArray.h>
#include <query/DimensionIndex.h>
#include <util/Event.h>
#include <util/RWLock.h>
#include <util/ThreadPool.h>
#include <query/Query.h>
#include <util/InjectedError.h>
#include <system/Constants.h>

namespace scidb
{


    const size_t HEADER_SIZE = 4*1024; // align header on page boundary to allow aligned IO operations
    const size_t N_LATCHES = 101;      // XXX TODO: figure out if latching is still necessary after removing clone logic

    /**
     * Position of chunk in the storage
     */
    struct DiskPos
    {
        /**
         * Data store guid
         */
        DataStore::Guid dsGuid;
        
        /**
         * Position of chunk header in meta-data file
         */
        uint64_t hdrPos;

        /**
         * Offset of chunk within DataStore
         */
        uint64_t offs;

        bool operator < (DiskPos const& other) const {
            return (dsGuid != other.dsGuid)
                ? dsGuid < other.dsGuid
                : offs < other.offs;
        }
    };

    /**
     * Chunk header as it is stored on the disk
     */
    struct ChunkHeader
    {
        /**
         * The version of the storage manager that produced this chunk. Currently, this is always equal to SCIDB_STORAGE_VERSION.
         * Placeholder for the future.
         */
        uint32_t storageVersion;

        /**
         * The position of the chunk on disk
         */
        DiskPos  pos;

        /**
         * Versioned Array ID that contains this chunk.
         */
        ArrayID  arrId;

        /**
         * The Attribute ID the chunk belongs to.
         */
        AttributeID attId;

        /**
         * Size of the data after it has been compressed.
         */
        uint64_t compressedSize;

        /**
         * Size of the data prior to any compression.
         */
        uint64_t size;

        /**
         * The compression method used on this chunk.
         */
        int8_t   compressionMethod;

        /**
         * The special properties of this chunk.
         * @see enum Flags below
         */
        uint8_t  flags;

        /**
         * Number of coordinates the chunk has.
         * XXX: Somebody explain why this is stored per chunk? Seems wasteful.
         */
        uint16_t nCoordinates;

        /**
         * Actual size on disk: compressedSize + reserve.
         */
        uint64_t allocatedSize;

        /**
         * Number of non-empty cells in the chunk.
         */
        uint32_t nElems;

        /**
         * The instance ID this chunk must occupy; not equal to current instance id if this is a replica.
         */
        uint32_t instanceId;

        enum Flags {
            SPARSE_CHUNK = 1,
            DELTA_CHUNK = 2,
            RLE_CHUNK = 4,
            TOMBSTONE = 8
        };

        /**
         * Check if a given flag is set.
         * Usage:
         * ChunkHeader hdr; bool isTombstone = hdr.is<TOMBSTONE>();
         * @return true if the template argument flag is set, false otherwise.
         */
        template<Flags FLAG>
        inline bool is() const
        {
            return flags & FLAG;
        }

        /**
         * Set one of the flags in the chunk header.
         * Usage:
         * ChunkHeader tombHdr; tombHdr.set<TOMBSTONE>(true);
         * @param[in] the value to set the flag to
         */
        template<Flags FLAG>
        inline void set(bool value)
        {
            if(value)
            {
                flags |= FLAG;
            }
            else
            {
                flags &= ~(FLAG);
            }
        }
    };

    /**
     * Chunk header + coordinates
     */
    struct ChunkDescriptor
    {
        ChunkHeader hdr;
        Coordinate  coords[MAX_NUM_DIMS_SUPPORTED];

        void getAddress(StorageAddress& addr) const;
        std::string toString() const
        {
            std::stringstream ss;           
            ss << "ChunkDesc:"
               << " position=" << hdr.pos.hdrPos
               << ", arrId=" << hdr.arrId
               << ", attId=" << hdr.attId
               << ", instanceId=" << hdr.instanceId
               << ", coords=[ ";
            for (uint16_t i=0; (i < hdr.nCoordinates) && (i < MAX_NUM_DIMS_SUPPORTED); ++i) {
                ss << coords[i] << " ";
            }
            ss << "]";
            return ss.str();
        }
    };

    /**
     * Transaction log record
     */
    struct TransLogRecordHeader {
        ArrayUAID      arrayUAID;
        ArrayID        arrayId;
        VersionID      version;
        uint32_t       oldSize;
        uint64_t       newHdrPos;
        ChunkHeader hdr;
    };

    struct TransLogRecord : TransLogRecordHeader {
        uint32_t    hdrCRC;
        uint32_t    bodyCRC;
    };

    class CachedStorage;

    /**
     * Abstract class declaring methods for manipulation with deltas.
     */
    class VersionControl
    {
      public:
        /**
         * Extract content of specified version from the src chunk and place it in dst buffer
         * @param dst destination buffer. Implementation of this method should use
         * SharedBuffer.allocate(size_t size) method to allocate space in dst buffer,
         * and then SharedBuffer.getData() for getting address of allocated buffer.
         * Format of output data is one used in MemChunk
         * @param src source chunk with deltas. Implementation should use SharedBuffer.getData(),
         * SharedBuffer.getSize() methods to get content of the chunk. Format of chunk content
         * is implementation specific and is opaque for SciDB.
         * @param version identifier of version which should be extracted. Version is assumed
         * to be present in the src chunk
         */
        virtual void getVersion(Chunk& dst, ConstChunk const& src, VersionID version) = 0;

        /**
         * Create new version and add its delta to the destination chunk
         * @param dst destination chunk. This chunks already contains data: previous version
         * and optionally delta. Implementation should append new delta to this chunk.
         * Format of the dst chunk may be different depending on value of "append" flag.
         * If "append" is true, then dst already contains deltas - its format is determined by
         * implementation of version control. If "append" is false, then format of the content
         * of the chunk is one defined in MemChunk.
         * @param src source chunk. Chunk containing data of new version. Its format is specified
         * in MemChunk.
         * @param version identifier of created version
         * @param append determines format of destination chunk: if "append" is true, then it assumed
         * to already contain deltas - format is implementation specific, if "append" is false,
         * then MemChunk format is used
         * @return true if new deltas was successfully added to the destination chunk
         * or false if implementation for some reasons rejects to add new delta.
         * In the last case content of destination chunk is assumed to be unchanged
         */
         virtual bool newVersion(Chunk& dst, ConstChunk const& src, VersionID version, bool append) = 0;

         VersionControl() {
            instance = this;
        }

        virtual ~VersionControl() {}

        static VersionControl* instance;
    };

    /**
     * PersistentChunk is a container for a SciDB array chunk stored on disk.
     * PersistentChunk is an internal interface and should not be usable/visible via the Array/Chunk/Iterator APIs.
     * Technically speaking it does not need to inherit from scidb::Chunk, but it is currently.
     * Most scidb::Chunk interfaces are not directly supported by PersistentChunk.
     */
    class PersistentChunk : public Chunk, public boost::enable_shared_from_this<PersistentChunk>
    {
        friend class CachedStorage;
        friend class ListChunkMapArrayBuilder;
      private:
        PersistentChunk* _next; // L2-list to implement LRU
        PersistentChunk* _prev;
        StorageAddress _addr; // StorageAddress of first chunk element
        void*   _data; // uncompressed data (may be NULL if swapped out)
        ChunkHeader _hdr; // chunk header
        int     _accessCount; // number of active chunk accessors
        bool    _raw; // true if chunk is currently initialized or loaded from the disk
        bool    _waiting; // true if some thread is waiting completetion of chunk load from the disk
        uint64_t _timestamp;
        Coordinates _firstPosWithOverlaps;
        Coordinates _lastPos;
        Coordinates _lastPosWithOverlaps;
        Storage* _storage;

        void init();
        void calculateBoundaries(const ArrayDesc& ad);

        // -----------------------------------------
        // L2-List methods
        //
        bool isEmpty();
        void prune();
        void link(PersistentChunk* elem);
        void unlink();
        // -----------------------------------------
        void beginAccess();

      public:

        int getAccessCount() const { return _accessCount; } 
        bool isTemporary() const;
        void setAddress(const ArrayDesc& ad, const ChunkDescriptor& desc);
        void setAddress(const ArrayDesc& ad, const StorageAddress& firstElem, int compressionMethod);

        boost::shared_ptr<ConstRLEEmptyBitmap> getEmptyBitmap() const;

        RWLock& getLatch();

        virtual ConstChunk const* getPersistentChunk() const;

        bool isDelta() const;

        virtual bool isSparse() const;
        virtual bool isRLE() const;

        /**
         * @see ConstChunk::isMaterialized
         */
        virtual bool isMaterialized() const;

        /**
         * @see ConstChunk::materialize
         */
        ConstChunk* materialize() const;
        virtual void setSparse(bool sparse);
        virtual void setRLE(bool rle);

        virtual size_t count() const;
        virtual bool   isCountKnown() const;
        virtual void   setCount(size_t count);

        virtual const ArrayDesc& getArrayDesc() const ;
        virtual const AttributeDesc& getAttributeDesc() const;
        virtual int getCompressionMethod() const;
        void setCompressionMethod(int method);
        virtual void* getData() const;
        virtual void* getDataForLoad();
        void* getData(const scidb::ArrayDesc&);
        virtual size_t getSize() const;
        virtual void allocate(size_t size);
        virtual void reallocate(size_t size);
        virtual void free();
        virtual void compress(CompressedBuffer& buf, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const;
        virtual void decompress(const CompressedBuffer& buf);
        virtual Coordinates const& getFirstPosition(bool withOverlap) const;
        virtual Coordinates const& getLastPosition(bool withOverlap) const;
        virtual boost::shared_ptr<ChunkIterator> getIterator(boost::shared_ptr<Query> const& query, int iterationMode);
        virtual boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
        virtual bool pin() const;
        virtual void unPin() const;

        virtual void write(boost::shared_ptr<Query>& query);
        virtual void truncate(Coordinate lastCoord);

        /**
         * The purpose of this method is to satisfy scidb::Chunk interface
         * It should never be invoked. It will cause a crash if invoked.
         * @see Storage::getDBArray
         */
        Array const& getArray() const;

        const StorageAddress& getAddress() const
        {
            return _addr;
        }

        const ChunkHeader& getHeader() const
        {
            return _hdr;
        }

        uint64_t getTimestamp() const
        {
            return _timestamp;
        }

        size_t getCompressedSize() const
        {
            return _hdr.compressedSize;
        }

        void setCompressedSize(size_t size)
        {
            _hdr.compressedSize = size;
        }

        bool isRaw() const
        {
            return _raw;
        }

        void setRaw(bool status)
        {
            _raw = status;
        }

        PersistentChunk();
        ~PersistentChunk();
    };

    /**
     * Storage with LRU in-memory cache of chunks
     */
    class CachedStorage : public Storage, InjectedErrorListener<WriteChunkInjectedError>
    {
      //Inner Structures
      private:
        struct ChunkInitializer 
        { 
            CachedStorage& storage;
            PersistentChunk& chunk;

            ChunkInitializer(CachedStorage* sto, PersistentChunk& chn) : storage(*sto), chunk(chn) {}
            ~ChunkInitializer();
        };

        /**
         * The beginning section of the storage header file.
         */
        struct StorageHeader
        {
            /**
             * A constant special value the header file must begin with.
             * If it's not equal to SCIDB_STORAGE_HEADER_MAGIC, then we know for sure the file is corrupted.
             */
            uint32_t magic;

            /**
             * The smallest version number among all the chunks that are currently stored.
             * Currently it's always equal to versionUpperBound; this is a placeholder for the future.
             */
            uint32_t versionLowerBound;

            /**
             * The largest version number among all the chunks that are currently stored.
             * Currently it's always equal to versionLowerBound; this is a placeholder for the future.
             */
            uint32_t versionUpperBound;

            /**
             * Current position in storage header (offset to where new chunk header will be written).
             */
            uint64_t currPos;

            /**
             * Number of chunks in local storage.
             */
            uint64_t nChunks;

            /**
             * This instance ID.
             */
            InstanceID   instanceId;
        };

        class DBArrayIterator;

        class DBArrayIteratorChunk
        {
          public:
            PersistentChunk* toPersistentChunk(const ConstChunk* cChunk) const
            {
                assert(cChunk);
                ConstChunk const* constChunk = cChunk->getPersistentChunk();
                assert(constChunk);
                assert(dynamic_cast<PersistentChunk const*>(constChunk));
                return const_cast<PersistentChunk*>(static_cast<PersistentChunk const*>(constChunk));
            }
        };

        class DeltaChunk : public ConstChunk, public DBArrayIteratorChunk
        {
          public:
            const Array& getArray() const;
            const ArrayDesc& getArrayDesc() const;
            const AttributeDesc& getAttributeDesc() const;

            int getCompressionMethod() const;
            Coordinates const& getFirstPosition(bool withOverlap) const;
            Coordinates const& getLastPosition(bool withOverlap) const;
            boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
            boost::shared_ptr<ConstRLEEmptyBitmap> getEmptyBitmap() const;

            ConstChunk const* getPersistentChunk() const;
            bool isMaterialized() const;
            bool isSparse() const;
            bool isRLE() const;
            void* getData() const;
            size_t getSize() const;
            bool pin() const;
            void unPin() const;
            void compress(CompressedBuffer& buf, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const;

            void setInputChunk(Chunk* chunk, VersionID ver);
            DeltaChunk(DBArrayIterator& arrayIterator);

          private:
            void extract() const;

            DBArrayIterator& _arrayIterator;
            Chunk*    _inputChunk;
            VersionID _version;
            MemChunk  _versionChunk;
            bool      _extracted;
            size_t    _accessCount;
        };

        /**
         * This is the base class for the PersistentChunk wrapper that can be used to decouple the implementation of PersistentChunk from
         * the consumers of Array/Chunk/Iterator APIs.
         */
        class DBArrayChunkBase : public Chunk, public DBArrayIteratorChunk
        {
          public:
            DBArrayChunkBase(PersistentChunk* chunk);

            virtual const Array& getArray() const;
            virtual const ArrayDesc& getArrayDesc() const;
            virtual const AttributeDesc& getAttributeDesc() const;
            virtual int getCompressionMethod() const;
            virtual boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
            virtual boost::shared_ptr<ConstRLEEmptyBitmap> getEmptyBitmap() const;
            virtual boost::shared_ptr<ChunkIterator> getIterator(boost::shared_ptr<Query> const& query, int iterationMode);

            virtual bool isSparse() const;
            virtual bool isRLE() const;
            virtual bool isMaterialized() const
            {
                assert(!materializedChunk);
                assert(_inputChunk);
                return true;
            }
            virtual ConstChunk* materialize() const
            {
                assert(!materializedChunk);
                assert(_inputChunk);
                return static_cast<ConstChunk*>(const_cast<DBArrayChunkBase*>(this));
            }
            virtual void setSparse(bool sparse);
            virtual void setRLE(bool rle);
            virtual size_t count() const;
            virtual bool isCountKnown() const;
            virtual void setCount(size_t count);
            virtual ConstChunk const* getPersistentChunk() const;

            virtual void* getData() const;
            virtual void* getDataForLoad();
            virtual size_t getSize() const;
            virtual void allocate(size_t size);
            virtual void reallocate(size_t size);
            virtual void free();
            virtual void compress(CompressedBuffer& buf, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const;
            virtual void decompress(const CompressedBuffer& buf);
            virtual Coordinates const& getFirstPosition(bool withOverlap) const;
            virtual Coordinates const& getLastPosition(bool withOverlap) const;

            virtual bool pin() const;
            virtual void unPin() const;

            virtual void write(boost::shared_ptr<Query>& query);
            virtual void truncate(Coordinate lastCoord);

            AttributeID getAttributeId() const
            {
                return _inputChunk->getAddress().attId;
            }
            Coordinates const& getCoordinates() const
            {
                return _inputChunk->getAddress().coords;
            }

            virtual ~DBArrayChunkBase()
            {
                //XXX tigor TODO: add logic to make sure this chunk is unpinned
            }

          private:

            DBArrayChunkBase();
            DBArrayChunkBase(const DBArrayChunkBase&);
            DBArrayChunkBase operator=(const DBArrayChunkBase&);

            PersistentChunk* _inputChunk;
        };

        /**
         * This is a public wrapper for PersistentChunk that has access to the ArrayDesc information
         * and other Query specific information.
         */
        class DBArrayChunk : public DBArrayChunkBase
        {
          public:
            DBArrayChunk(DBArrayIterator& arrayIterator, PersistentChunk* chunk);

            virtual const Array& getArray() const;
            virtual const ArrayDesc& getArrayDesc() const;
            virtual const AttributeDesc& getAttributeDesc() const;
            virtual void write(boost::shared_ptr<Query>& query);
            virtual boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
            virtual boost::shared_ptr<ConstRLEEmptyBitmap> getEmptyBitmap() const;
            virtual boost::shared_ptr<ChunkIterator> getIterator(boost::shared_ptr<Query> const& query, int iterationMode);
            virtual void compress(CompressedBuffer& buf, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const;
            virtual void decompress(const CompressedBuffer& buf);

        private:

            DBArrayChunk();
            DBArrayChunk(const DBArrayChunk&);
            DBArrayChunk operator=(const DBArrayChunk&);

            DBArrayIterator& _arrayIter;
            int _nWriters;
        };

        /**
         * This is an internal wrapper for PersistentChunk that has access to the ArrayDesc information
         * but does not have direct access to DBArrayIterator and/or Query.
         */
        class DBArrayChunkInternal : public DBArrayChunkBase
        {
        public:
            DBArrayChunkInternal(const ArrayDesc& desc, PersistentChunk* chunk)
            : DBArrayChunkBase(chunk), _arrayDesc(desc)
            {}
            virtual const ArrayDesc& getArrayDesc() const
            {
                return _arrayDesc;
            }
            virtual const AttributeDesc& getAttributeDesc() const
            {
                assert(getArrayDesc().getAttributes().size() > 0);
                assert(DBArrayChunkBase::getAttributeId() < getArrayDesc().getAttributes().size());
                return getArrayDesc().getAttributes()[DBArrayChunkBase::getAttributeId()];
            }

        private:

            DBArrayChunkInternal();
            DBArrayChunkInternal(const DBArrayChunkInternal&);
            DBArrayChunkInternal operator=(const DBArrayChunkInternal&);
            void* operator new(size_t size);

            const ArrayDesc& _arrayDesc;
        };


        class DBArrayIterator : public ArrayIterator
        {
            friend class DeltaChunk;
            friend class DBArrayChunk;
            
        private:
            ArrayDesc const& getArrayDesc() const { return _array->getArrayDesc(); }
            AttributeDesc const& getAttributeDesc() const { return _attrDesc; }
            Array const& getArray() const { return *_array; }

            // This is the current map from the chunks returned to the user of DBArrayIterator
            // to the StorageManager PersistentChunks
            typedef boost::unordered_map<boost::shared_ptr<PersistentChunk>, boost::shared_ptr<DBArrayChunk> > DBArrayMap;
            DBArrayMap _dbChunks;
            DBArrayChunk* getDBArrayChunk(boost::shared_ptr<PersistentChunk>& dbChunk);

        private:
            Chunk* _currChunk;
            CachedStorage* _storage;

            DeltaChunk _deltaChunk;
            DeltaChunk _deltaBitmapChunk;
            AttributeDesc const& _attrDesc;
            StorageAddress _address;
            boost::weak_ptr<Query> _query;
            bool const _writeMode;
            boost::shared_ptr<const Array> _array;

        public:
            DBArrayIterator(CachedStorage* storage,
                            boost::shared_ptr<const Array>& array,
                            AttributeID attId,
                            boost::shared_ptr<Query>& query,
                            bool writeMode);

            ~DBArrayIterator();

            virtual ConstChunk const& getChunk();
            virtual bool end();
            virtual void operator ++();
            virtual Coordinates const& getPosition();
            virtual bool setPosition(Coordinates const& pos);
            virtual void reset();
            virtual Chunk& copyChunk(ConstChunk const& srcChunk, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap);
            virtual void   deleteChunk(Chunk& chunk);
            virtual Chunk& newChunk(Coordinates const& pos);
            virtual Chunk& newChunk(Coordinates const& pos, int compressionMethod);
            virtual boost::shared_ptr<Query> getQuery() { return Query::getValidQueryPtr(_query); }
        };

    private:

        // Data members

        union
        {
            StorageHeader _hdr;
            char          _filler[HEADER_SIZE];
        };

        std::vector<Compressor*> _compressors;

        typedef map <StorageAddress, boost::shared_ptr<PersistentChunk> > InnerChunkMap;
        typedef boost::unordered_map<ArrayUAID, shared_ptr< InnerChunkMap > > ChunkMap;

        ChunkMap _chunkMap;   // The root of the chunk map

        size_t _cacheSize;    // maximal size of memory used by cached chunks
        size_t _cacheUsed;    // current size of memory used by cached chunks (it can be larger than cacheSize if all chunks are pinned)
        Mutex _mutex;         // mutex used to synchronize access to the storage
        Event _loadEvent;     // event to notify threads waiting for completion of chunk load
        Event _initEvent;     // event to notify threads waiting for completion of chunk load
        PersistentChunk _lru; // header of LRU L2-list
        uint64_t _timestamp;

        bool _strictCacheLimit;
        bool _cacheOverflowFlag;
        Event _cacheOverflowEvent;

        int32_t _writeLogThreshold;

        File::FilePtr _hd; // storage header file descriptor
        File::FilePtr _log[2]; // _transaction logs
        uint64_t _logSizeLimit; // transaciton log size limit
        uint64_t _logSize;
        int _currLog;
        int _redundancy;
        int _nInstances;
        bool _syncReplication;
        bool _enableDeltaEncoding;

        RWLock _latches[N_LATCHES];  //XXX TODO: figure out if latches are necessary after removal of clone logic
        set<uint64_t> _freeHeaders;

        /// Cached RM pointer
        ReplicationManager* _replicationManager;

        // Methods

        /**
         * Perform metadata/lock recovery and storage rollback as part of the intialization.
         * It may block waiting for the remote coordinator recovery to occur.
         */
        void doTxnRecoveryOnStartup();

        /**
         * Wait for the replica items (i.e. chunks) to be sent to NetworkManager
         * @param replicas a list of replica items to wait on
         */
        void waitForReplicas(std::vector<boost::shared_ptr<ReplicationManager::Item> >& replicas);

        /**
         * Abort any outstanding replica items (in case of errors)
         * @param replicas a list of replica items to abort
         */
        void abortReplicas(vector<boost::shared_ptr<ReplicationManager::Item> >* replicasVec);

        /**
         * Unpin and free chunk (in case of errors)
         * @param chunk to clean up
         * @note it does not put the chunk on the LRU list
         */
        void cleanChunk(PersistentChunk* chunk);

        void notifyChunkReady(PersistentChunk& chunk);

        int chooseCompressionMethod(ArrayDesc const& desc, PersistentChunk& chunk, void* buf);

        /**
         * Determine if a particular chunk exists in the storage and return a pointer to it.
         * @param desc the array descriptor of the array
         * @param addr the address of the chunk in the array
         * @return pointer to the unloaded chunk object. Null if no such chunk is present.
         */
        boost::shared_ptr<PersistentChunk> lookupChunk(ArrayDesc const& desc, StorageAddress const& addr);

        void internalFreeChunk(PersistentChunk& chunk);

        void addChunkToCache(PersistentChunk& chunk);

        uint64_t getCurrentTimestamp() const
        {
            return _timestamp;
        }

        /**
         * Write bytes to DataStore indicated by pos
         * @param pos DataStore and offset to which to write
         * @param data Bytes to write
         * @param len Number of bytes to write
         * @param allocated Size of allocated region
         * @pre position in DataStore must be previously allocated
         * @throws userException if an error occurs
         */
        void writeBytesToDataStore(DiskPos const& pos,
                                   void const* data,
                                   size_t len,
                                   size_t allocated);

        /**
         * Force writing of chunk data to data store
         * Exception is thrown if write fails
         */
        void writeChunkToDataStore(DataStore& ds, PersistentChunk& chunk, void const* data);

        /**
         * Read chunk data from the disk
         * Exception is thrown if read fails
         */
        void readChunkFromDataStore(DataStore& ds, PersistentChunk const& chunk, void* data);

        /**
         * Fetch chunk from the disk
         */
        void fetchChunk(ArrayDesc const& desc, PersistentChunk& chunk);

        /**
         * Replicate chunk
         */
        void replicate(ArrayDesc const& desc, StorageAddress const& addr,
                       PersistentChunk* chunk, void const* data,
                       size_t compressedSize, size_t decompressedSize,
                       boost::shared_ptr<Query>& query,
                       std::vector<boost::shared_ptr<ReplicationManager::Item> >& replicas);

        /**
         * Assign replication instances for the particular chunk
         */
        void getReplicasInstanceId(InstanceID* replicas, ArrayDesc const& desc, StorageAddress const& address) const;

        /**
         * Check if chunk should be considered by DBArraIterator
         */
        bool isResponsibleFor(ArrayDesc const& desc, PersistentChunk const& chunk, boost::shared_ptr<Query> const& query);

        /**
         * Determine if a given chunk is a primary replica on this instance
         * @param chunk to examine
         * @return true if the chunk is a primary replica
         */
        bool isPrimaryReplica(PersistentChunk const* chunk)
        {
            assert(chunk);
            bool res = (chunk->getHeader().instanceId == _hdr.instanceId);
            assert(res || (_redundancy > 0));
            return res;
        }

        /**
         * Return summary disk usage information
         */
        void getDiskInfo(DiskInfo& info);

        /**
         * Delete all descriptors that are associated with a given array ID from the header file.
         * @param uaId the unversioned array ID to be removed
         * @param arrId the versioned array ID to be removed.
         */
        void deleteDescriptorsFor(ArrayUAID uaId, ArrayID arrId);

        /**
         * Helper: remove an immutable or unversioned array from the storage
         * @param arrID the id of the array
         */
        void removeUnversionedArray(ArrayID arrID);

        /**
         * Helper: remove a mutable array from the storage
         * @param uaId the unversioned ID
         * @param arrId the versioned ID
         */
        void removeMutableArray(ArrayUAID uaId, ArrayID arrId);

      public:
        /**
         * Constructor
         */
        CachedStorage();

        /**
         * @see Storage::getChunkPositions
         */       
        void getChunkPositions(ArrayDesc const& desc, boost::shared_ptr<Query> const& query, CoordinateSet& chunkPositions);

        /**
         * Cleanup and close smgr
         * @see Storage::close
         */
        void close();

        /**
         * @see Storage::loadChunk
         */
        void loadChunk(ArrayDesc const& desc, PersistentChunk* chunk);

        /**
         * @see Storage::getChunkLatch
         */
        RWLock& getChunkLatch(PersistentChunk* chunk);

        /**
         * @see Storage::pinChunk
         */
        void pinChunk(PersistentChunk const* chunk);

        /**
         * @see Storage::unpinChunk
         */
        void unpinChunk(PersistentChunk const* chunk);

        /**
         * @see Storage::decompressChunk
         */
        void decompressChunk(ArrayDesc const& desc, PersistentChunk* chunk, CompressedBuffer const& buf);

        /**
         * @see Storage::compressChunk
         */
        void compressChunk(ArrayDesc const& desc, PersistentChunk const* chunk, CompressedBuffer& buf);

        /**
         * @see Storage::createChunk
         */
        boost::shared_ptr<PersistentChunk> createChunk(ArrayDesc const& desc,
                                                       StorageAddress const& addr,
                                                       int compressionMethod,
                                                       const boost::shared_ptr<Query>& query);

        /**
         * @see Storage::deleteChunk
         */
        void deleteChunk(ArrayDesc const& desc, PersistentChunk& chunk);

        /**
         * @see Storage::remove
         */
        void remove(ArrayUAID uaId, ArrayID arrId);

        /**
         * @see Storage::cloneLocalChunk
         */
        void cloneLocalChunk(Coordinates const& pos,
                             ArrayDesc const& targetDesc, AttributeID targetAttrID,
                             ArrayDesc const& sourceDesc, AttributeID sourceAttrID,
                             boost::shared_ptr<Query>& query);

        /**
         * @see Storage::rollback
         */
        void rollback(std::map<ArrayID,VersionID> const& undoUpdates);

        /**
         * Read the storage description file to find path for chunk map file.
         * Iterate the chunk map file and build the chunk map in memory.  TODO:
         * We would like to be able to initialize the chunk map without iterating
         * the file.  In general the entire chunk map should not be required to
         * fit entirely in memory.  Chage this in the future.
         * @see Storage::open
         */
        void open(const string& storageDescriptorFilePath, size_t cacheSize);

        /**
         * Flush all changes to the physical device(s) for the indicated array. 
         * (optionally flush data for all arrays, if uaId == INVALID_ARRAY_ID). 
         * @see Storage::flush
         */
        void flush(ArrayUAID uaId = INVALID_ARRAY_ID);

        /**
         * @see Storage::getArrayIterator
         */
        boost::shared_ptr<ArrayIterator> getArrayIterator(boost::shared_ptr<const Array>& arr,
                                                          AttributeID attId,
                                                          boost::shared_ptr<Query>& query);

        /**
         * @see Storage::getConstArrayIterator
         */
        boost::shared_ptr<ConstArrayIterator> getConstArrayIterator(boost::shared_ptr<const Array>& arr,
                                                                    AttributeID attId,
                                                                    boost::shared_ptr<Query>& query);

        /**
         * @see Storage::writeChunk
         */
        void writeChunk(ArrayDesc const& desc, PersistentChunk* chunk, boost::shared_ptr<Query>& query);

        /**
         * @see Storage::readChunk
         */
        boost::shared_ptr<PersistentChunk> readChunk(ArrayDesc const& desc,
                                                     StorageAddress const& addr,
                                                     const boost::shared_ptr<Query>& query);

        /**
         * @see Storage::setInstanceId
         */
        void setInstanceId(InstanceID id);

        /**
         * @see Storage::getInstanceId
         */
        InstanceID getInstanceId() const;

        /**
         * @see Storage::getNumberOfInstances
         */
        size_t getNumberOfInstances() const;

        /**
         * @see Storage::getPrimaryInstanceId
         */
        InstanceID getPrimaryInstanceId(ArrayDesc const& desc, StorageAddress const& address) const;

        /**
         * @see Storage::listChunkDescriptors
         */
        void listChunkDescriptors(ListChunkDescriptorsArrayBuilder& builder);

        /**
         * @see Storage::listChunkMap
         */
        void listChunkMap(ListChunkMapArrayBuilder& builder);

        /**
         * @see Storage::findNextChunk
         */
        bool findNextChunk(ArrayDesc const& desc, boost::shared_ptr<Query> const& query, StorageAddress& address);

        /**
         * @see Storage::findChunk
         */
        bool findChunk(ArrayDesc const& desc, boost::shared_ptr<Query> const& query, StorageAddress& address);

        /**
         * @see Storage::removeLocalChunkVersion
         */
        void removeLocalChunkVersion(ArrayDesc const& arrayDesc, Coordinates const& coords, boost::shared_ptr<Query>& query);

        /**
         * @see Storage::removeChunkVersion
         */
        void removeChunkVersion(ArrayDesc const& arrayDesc, Coordinates const& coords, boost::shared_ptr<Query>& query);

        /**
         * @see Storage::removeDeadChunks
         */
        void removeDeadChunks(ArrayDesc const& arrayDesc, set<Coordinates, CoordinatesLess> const& liveChunks, boost::shared_ptr<Query>& query);

        /**
         * @see Storage::removeDeadChunks
         */
        void freeChunk(PersistentChunk* chunk);

        static CachedStorage instance;
    };

    inline static uint32_t calculateCRC32(void const* content, size_t content_length, uint32_t crc = ~0)
    {
        static const uint32_t table [] = {
            0x00000000, 0x77073096, 0xEE0E612C, 0x990951BA,
            0x076DC419, 0x706AF48F, 0xE963A535, 0x9E6495A3,
            0x0EDB8832, 0x79DCB8A4, 0xE0D5E91E, 0x97D2D988,
            0x09B64C2B, 0x7EB17CBD, 0xE7B82D07, 0x90BF1D91,

            0x1DB71064, 0x6AB020F2, 0xF3B97148, 0x84BE41DE,
            0x1ADAD47D, 0x6DDDE4EB, 0xF4D4B551, 0x83D385C7,
            0x136C9856, 0x646BA8C0, 0xFD62F97A, 0x8A65C9EC,
            0x14015C4F, 0x63066CD9, 0xFA0F3D63, 0x8D080DF5,

            0x3B6E20C8, 0x4C69105E, 0xD56041E4, 0xA2677172,
            0x3C03E4D1, 0x4B04D447, 0xD20D85FD, 0xA50AB56B,
            0x35B5A8FA, 0x42B2986C, 0xDBBBC9D6, 0xACBCF940,
            0x32D86CE3, 0x45DF5C75, 0xDCD60DCF, 0xABD13D59,

            0x26D930AC, 0x51DE003A, 0xC8D75180, 0xBFD06116,
            0x21B4F4B5, 0x56B3C423, 0xCFBA9599, 0xB8BDA50F,
            0x2802B89E, 0x5F058808, 0xC60CD9B2, 0xB10BE924,
            0x2F6F7C87, 0x58684C11, 0xC1611DAB, 0xB6662D3D,

            0x76DC4190, 0x01DB7106, 0x98D220BC, 0xEFD5102A,
            0x71B18589, 0x06B6B51F, 0x9FBFE4A5, 0xE8B8D433,
            0x7807C9A2, 0x0F00F934, 0x9609A88E, 0xE10E9818,
            0x7F6A0DBB, 0x086D3D2D, 0x91646C97, 0xE6635C01,

            0x6B6B51F4, 0x1C6C6162, 0x856530D8, 0xF262004E,
            0x6C0695ED, 0x1B01A57B, 0x8208F4C1, 0xF50FC457,
            0x65B0D9C6, 0x12B7E950, 0x8BBEB8EA, 0xFCB9887C,
            0x62DD1DDF, 0x15DA2D49, 0x8CD37CF3, 0xFBD44C65,

            0x4DB26158, 0x3AB551CE, 0xA3BC0074, 0xD4BB30E2,
            0x4ADFA541, 0x3DD895D7, 0xA4D1C46D, 0xD3D6F4FB,
            0x4369E96A, 0x346ED9FC, 0xAD678846, 0xDA60B8D0,
            0x44042D73, 0x33031DE5, 0xAA0A4C5F, 0xDD0D7CC9,

            0x5005713C, 0x270241AA, 0xBE0B1010, 0xC90C2086,
            0x5768B525, 0x206F85B3, 0xB966D409, 0xCE61E49F,
            0x5EDEF90E, 0x29D9C998, 0xB0D09822, 0xC7D7A8B4,
            0x59B33D17, 0x2EB40D81, 0xB7BD5C3B, 0xC0BA6CAD,

            0xEDB88320, 0x9ABFB3B6, 0x03B6E20C, 0x74B1D29A,
            0xEAD54739, 0x9DD277AF, 0x04DB2615, 0x73DC1683,
            0xE3630B12, 0x94643B84, 0x0D6D6A3E, 0x7A6A5AA8,
            0xE40ECF0B, 0x9309FF9D, 0x0A00AE27, 0x7D079EB1,

            0xF00F9344, 0x8708A3D2, 0x1E01F268, 0x6906C2FE,
            0xF762575D, 0x806567CB, 0x196C3671, 0x6E6B06E7,
            0xFED41B76, 0x89D32BE0, 0x10DA7A5A, 0x67DD4ACC,
            0xF9B9DF6F, 0x8EBEEFF9, 0x17B7BE43, 0x60B08ED5,

            0xD6D6A3E8, 0xA1D1937E, 0x38D8C2C4, 0x4FDFF252,
            0xD1BB67F1, 0xA6BC5767, 0x3FB506DD, 0x48B2364B,
            0xD80D2BDA, 0xAF0A1B4C, 0x36034AF6, 0x41047A60,
            0xDF60EFC3, 0xA867DF55, 0x316E8EEF, 0x4669BE79,

            0xCB61B38C, 0xBC66831A, 0x256FD2A0, 0x5268E236,
            0xCC0C7795, 0xBB0B4703, 0x220216B9, 0x5505262F,
            0xC5BA3BBE, 0xB2BD0B28, 0x2BB45A92, 0x5CB36A04,
            0xC2D7FFA7, 0xB5D0CF31, 0x2CD99E8B, 0x5BDEAE1D,

            0x9B64C2B0, 0xEC63F226, 0x756AA39C, 0x026D930A,
            0x9C0906A9, 0xEB0E363F, 0x72076785, 0x05005713,
            0x95BF4A82, 0xE2B87A14, 0x7BB12BAE, 0x0CB61B38,
            0x92D28E9B, 0xE5D5BE0D, 0x7CDCEFB7, 0x0BDBDF21,

            0x86D3D2D4, 0xF1D4E242, 0x68DDB3F8, 0x1FDA836E,
            0x81BE16CD, 0xF6B9265B, 0x6FB077E1, 0x18B74777,
            0x88085AE6, 0xFF0F6A70, 0x66063BCA, 0x11010B5C,
            0x8F659EFF, 0xF862AE69, 0x616BFFD3, 0x166CCF45,

            0xA00AE278, 0xD70DD2EE, 0x4E048354, 0x3903B3C2,
            0xA7672661, 0xD06016F7, 0x4969474D, 0x3E6E77DB,
            0xAED16A4A, 0xD9D65ADC, 0x40DF0B66, 0x37D83BF0,
            0xA9BCAE53, 0xDEBB9EC5, 0x47B2CF7F, 0x30B5FFE9,

            0xBDBDF21C, 0xCABAC28A, 0x53B39330, 0x24B4A3A6,
            0xBAD03605, 0xCDD70693, 0x54DE5729, 0x23D967BF,
            0xB3667A2E, 0xC4614AB8, 0x5D681B02, 0x2A6F2B94,
            0xB40BBE37, 0xC30C8EA1, 0x5A05DF1B, 0x2D02EF8D
        };

        unsigned char* buffer = (unsigned char*) content;
        
        while (content_length-- != 0)
        {
            crc = (crc >> 8) ^ table[(crc & 0xFF) ^ *buffer++];
        }
        return crc;
    }
    
}

#endif
