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

/*
 * InternalStorage.h
 *
 *  Created on: 06.01.2010
 *      Author: knizhnik@garret.ru
 *      Description: Internal storage manager interface
 */

#ifndef INTERNAL_STORAGE_H_
#define INTERNAL_STORAGE_H_

#include <dirent.h>

#include <vector>
#include <map>
#include <boost/unordered_map.hpp>
#include <boost/enable_shared_from_this.hpp>
#include "array/MemArray.h"
#include "Storage.h"
#include "array/DBArray.h"
#include "query/DimensionIndex.h"
#include "util/Event.h"
#include "util/RWLock.h"
#include "util/ThreadPool.h"
#include <query/Query.h>
#include <util/InjectedError.h>
#include "ReplicationManager.h"
#include <system/Constants.h>

namespace scidb
{
    const size_t MAX_SEGMENTS = 100;
    const size_t HEADER_SIZE = 4*1024; // align header on page boundary to allow aligned IO operations
    const size_t N_LATCHES = 101;

    typedef uint64_t ClusterID;

    /**
     * Position in the storage
     */
    struct DiskPos
    {
        /**
         * Segment number
         */
        uint64_t segmentNo : 8;
        
        /**
         * Position of chunk header
         */
        uint64_t hdrPos : 56;

        /**
         * Offset within segment
         */
        uint64_t offs;

        bool operator < (DiskPos const& other) const {
            return (segmentNo != other.segmentNo)
                ? segmentNo < other.segmentNo
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
        uint32_t compressedSize;

        /**
         * Size of the data prior to any compression.
         */
        uint32_t size;

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
        uint32_t allocatedSize;

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
        ArrayUAID     arrayUAID;
        ArrayID     arrayId;
        VersionID   version;
        uint32_t    oldSize;
        uint64_t    newHdrPos;
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
        PersistentChunk* _cloneOf;
        vector<PersistentChunk*> _clones;
        pthread_t _loader;

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
        struct CoordinateMap
        {
            shared_ptr<AttributeMultiMap> attrMap;
            shared_ptr<MemoryBuffer> coordMap;
            ArrayDesc indexArrayDesc;
            bool functionalMapping;
            bool initialized;
            bool raw;
            bool waiting;

            CoordinateMap() : functionalMapping(false), initialized(false), raw(false), waiting(false) {}
        };
        struct CoordinateMapInitializer 
        { 
            CachedStorage& storage;
            CoordinateMap& cm;
            bool initialized;

            CoordinateMapInitializer(CachedStorage* sto, CoordinateMap* map) : storage(*sto), cm(*map), initialized(false) {}
            ~CoordinateMapInitializer();
        };

        struct ChunkInitializer 
        { 
            CachedStorage& storage;
            PersistentChunk& chunk;

            ChunkInitializer(CachedStorage* sto, PersistentChunk& chn) : storage(*sto), chunk(chn) {}
            ~ChunkInitializer();
        };

        struct SegmentHeader {
            uint64_t used;
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
             * Used part of segments.
             */
            SegmentHeader segment[MAX_SEGMENTS];

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

            /**
             * From the setting chunk-segment-size.
             */
            uint64_t clusterSize;
        };

        struct Cluster {
            DiskPos  pos;
            uint64_t used;

            Cluster() {
                used = 0;
            }
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
      //Fields
        union
        {
            StorageHeader _hdr;
            char          _filler[HEADER_SIZE];
        };

        map< string, boost::shared_ptr<CoordinateMap> > _coordinateMap;

        vector<Compressor*> _compressors;

        typedef map <StorageAddress, boost::shared_ptr<PersistentChunk> > InnerChunkMap;
        typedef boost::unordered_map<ArrayUAID, shared_ptr< InnerChunkMap > > ChunkMap;

        /**
         * The root of the chunk map.
         */
        ChunkMap _chunkMap;

        size_t _cacheSize; // maximal size of memory used by cached chunks
        size_t _cacheUsed; // current size of memory used by cached chunks (it can be larger than cacheSize if all chunks are pinned)
        Mutex _mutex; // mutex used to synchronize access to the storage
        Event _loadEvent; // event to notify threads waiting for completion of chunk load
        Event _initEvent; // event to notify threads waiting for completion of chunk load
        PersistentChunk _lru; // header of LRU L2-list
        uint64_t _timestamp;

        bool _strictCacheLimit;
        bool _cacheOverflowFlag;
        Event _cacheOverflowEvent;

        vector<Segment> _segments;
        long _totalAvailable; // total available space in the storage (Gb)

        int _sd[MAX_SEGMENTS]; // file descriptors of segments
        int32_t _writeLogThreshold;

        int _hd; // storage header file descriptor
        int _log[2]; // _ransaction logs
        uint64_t _logSizeLimit; // transaciton log size limit
        uint64_t _logSize;
        int _currLog;
        int _redundancy;
        int _nInstances;
        bool _syncReplication;
        bool _enableDeltaEncoding;

        RWLock _latches[N_LATCHES];
        set<uint64_t> _freeHeaders;
        map<ArrayUAID, Cluster> _clusters;
        map<ClusterID, size_t> _liveChunksInCluster;
        vector< set<uint64_t> > _freeClusters;

        /// Cached RM pointer
        ReplicationManager* _replicationManager;

        //Methods:
        boost::shared_ptr<CoordinateMap> getCoordinateMap(string const& indexName, DimensionDesc const& dim,
                                                          const boost::shared_ptr<Query>& query);

        boost::shared_ptr<PersistentChunk> _cloneChunk(ArrayDesc const& dstDesc, StorageAddress const& addr,
                                                       PersistentChunk const& srcChunk, boost::shared_ptr<Query>& query);
        boost::shared_ptr<PersistentChunk> _cloneLocalChunk(ArrayDesc const& dstDesc,
                                                            StorageAddress const& addr,
                                                            PersistentChunk const& srcChunk,
                                                            ChunkDescriptor& cloneDesc,
                                                            boost::shared_ptr<Query>& query);
        boost::shared_ptr<PersistentChunk> _cloneLocalChunk(ArrayDesc const& dstDesc,
                                                            StorageAddress const& addr,
                                                            PersistentChunk const& srcChunk,
                                                            boost::shared_ptr<Query>& query);
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

        ClusterID getClusterID(uint32_t segmentNo, uint64_t offs) const
        {
            return offs / _hdr.clusterSize * MAX_SEGMENTS + segmentNo;
        }

        ClusterID getClusterID(DiskPos const& pos) const
        {
            return getClusterID(pos.segmentNo, pos.offs);
        }

        void getClusterPos(DiskPos& clusterPos, ClusterID id) const
        {
            clusterPos.offs = id / MAX_SEGMENTS * _hdr.clusterSize;
            clusterPos.segmentNo = uint32_t(id % MAX_SEGMENTS);
        }

        void freeCluster(ClusterID cid);

        /**
         * Get random segment for new chunk. Probability of choosing segment "i" is proportional to savilable
         * free space in this segment. So this method tries to balance usage of disk space especially in case
         * of adding new free disk to the list of database segments
         */
        int getRandomSegment();

        /**
         * Force wrinting of specified amount of data to the disk.
         * Exception is thrown if write failed
         */
        void writeAll(const DiskPos& pos, const void* data, size_t size);

        /**
         * Force wrinting of specified amount of data to the disk.
         * Exception is thrown if write failed
         */
        void writeAll(int fd, uint64_t offs, const void* data, size_t size);

        /**
         * Read requested amount of bytes from the disk.
         * Exception is thrown if read failed
         */
        void readAll(const DiskPos& pos, void* data, size_t size);

        /**
         * Read requested amount of bytes from the disk.
         * Exception is thrown if read failed
         */
        void readAll(int fd, uint64_t offs, void* data, size_t size);

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

        void getDiskInfo(DiskInfo& info);

        /**
         * Delete helper: unlink all the clones that point to this chunk.
         * @param chunk the chunk to be deleted
         */
        void unlinkChunkClones(PersistentChunk& chunk);

        /**
         * Delete all descriptors that are associated with a given array ID from the header file.
         * @param uaId the unversioned array ID to be removed
         * @param arrId the versioned array ID to be removed.
         */
        void deleteDescriptorsFor(ArrayUAID uaId, ArrayID arrId);

        /**
         * Helper: remove an immutable or unversioned array from the storage
         * @param arrID the id of the array
         * @param timestamp, values above 0 optional. If above 0, remove only chunks whose timestamp is >= timestamp
         */
        void removeUnversionedArray(ArrayID arrID, uint64_t timestamp);

        /**
         * Helper: remove a mutable array from the storage
         * @param uaId the unversioned ID
         * @param arrId the versioned ID
         */
        void removeMutableArray(ArrayUAID uaId, ArrayID arrId);

      public:
        /**
         * Default and mostly no-op. Initializes the file descriptors.
         */
        CachedStorage();

        /**
         * @see Storage::getChunkPositions
         */       
        void getChunkPositions(ArrayDesc const& desc, boost::shared_ptr<Query> const& query, CoordinateSet& chunkPositions);

        /**
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
        void remove(ArrayUAID uaId, ArrayID arrId, uint64_t timestamp);

        /**
         * @see Storage::mapCoordinate
         */
        Coordinate mapCoordinate(string const& indexName, DimensionDesc const& dim, Value const& value,
                                 CoordinateMappingMode mode, const boost::shared_ptr<Query>& query);

        /**
         * @see Storage::reverseMapCoordinate
         */
        Value reverseMapCoordinate(string const& indexName, DimensionDesc const& dim, Coordinate pos, const boost::shared_ptr<Query>& query);

        /**
         * @see Storage::removeCoordinateMap
         */
        void removeCoordinateMap(string const& indexName);

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
         * @see Storage::open
         */
        void open(const string& storageDescriptorFilePath, size_t cacheSize);

        /**
         * @see Storage::flush
         */
        void flush();

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
}

#endif
