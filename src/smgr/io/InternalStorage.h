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
#include <queue>
#include <map>
#include <set>
#include "array/MemArray.h"
#include "Storage.h"
#include "array/DBArray.h"
#include "DimensionIndex.h"
#include "util/Event.h"
#include "util/RWLock.h"
#include "util/ThreadPool.h"
#include <query/Query.h>

namespace scidb
{
    const size_t MAX_SEGMENTS = 100;
    const size_t MAX_COORDINATES = 100;
    const size_t HEADER_SIZE = 4*1024; // align header on page boundary to allow aligned IO operations
    const size_t N_LATCHES = 101;

    typedef uint64_t ClusterID;

    /**
     * Internal API to the storage. Used to provide abstract interface to storage manager (including DFS support)
     */
    class InternalStorage : public Storage
    {
      public:
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
         * Check if there is chunk with sepcified address
         * @param addr chunk address
         */
        virtual bool containsChunk(const Address& addr) = 0;

        /**
         * Write new chunk in the storage.
         * @param chunk new chunk created by newChunk Method
         */
        virtual void writeChunk(Chunk* chunk, boost::shared_ptr<Query>& query) = 0;

        /**
         * Read chunk from the storage. This method throws an exception when node with requested address doesn exists.
         * @param addr chunk address
         */
        virtual Chunk* readChunk(const Address& addr) = 0;

        /**
         * Load chunk body from the storage.
         * @param chunk loaded chunk
         */
        virtual void loadChunk(Chunk* chunk) = 0;

        /**
         * Get latch for the specified chunk
         * @param chunk chunk to be locked
         */
        virtual RWLock& getChunkLatch(DBChunk* chunk) = 0;

        /**
         * Create new chunk int the storage. There should be no chunk with such address in the storage.
         * @param addr chunk address
         * @param compressionMethod compression method for this chunk
         */
        virtual Chunk* newChunk(const Address& addr, int compressionMethod) = 0;

        /**
         * Clone chunk
         * @param addr chunk address
         * @param srcChunk chunk to be cloned
         */
        virtual Chunk* cloneChunk(const Address& addr, DBChunk const& srcChunk) = 0;

        /**
         * Delete chunk
         * @param chunk chunk to be deleted
         */
        virtual void deleteChunk(Chunk& chunk) = 0;

        virtual size_t getNumberOfNodes() const = 0;
    };

    /**
     * Position in the storage
     */
    struct DiskPos
    {
        /**
         * Segment number
         */
        uint32_t segmentNo;

        /**
         * Position of chunk header
         */
        uint32_t hdrPos;

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
        DiskPos  pos;
        ArrayID  arrId;
        AttributeID attId;
        uint32_t compressedSize;
        uint32_t size;
        int8_t   compressionMethod;
        uint8_t  flags;
        uint16_t nCoordinates;
        uint32_t allocatedSize;
        uint32_t nElems;
        uint32_t nodeId;

        enum Flags {
            SPARSE_CHUNK = 1,
            DELTA_CHUNK = 2,
            RLE_CHUNK = 4
        };
    };

    /**
     * Chunk header + coordinates
     */
    struct ChunkDescriptor
    {
        ChunkHeader hdr;
        Coordinate  coords[MAX_COORDINATES];

        void getAddress(Address& addr) const;
    };

    /**
     * Transaction log record
     */
    struct TransLogRecordHeader {
        ArrayID     array;
        ArrayID     versionArrayID;
        VersionID   version;
        uint32_t    oldSize;
        uint32_t    newHdrPos;
        ChunkHeader hdr;
    };

    struct TransLogRecord : TransLogRecordHeader {
        uint32_t    hdrCRC;
        uint32_t    bodyCRC;
    };

    class CachedStorage;
    class LocalStorage;

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
     * Chunk implementation
     */
    class DBChunk : public Chunk
    {
        friend class CachedStorage;
        friend class LocalStorage;
      private:
        DBChunk* next; // L2-list to implement LRU
        DBChunk* prev;
        Address addr; // address of first chunk element
        void*   data; // uncompressed data (may be NULL if swapped out)
        ChunkHeader hdr; // chunk header
        int     accessCount; // number of active chunk accessors
        int     nWriters;
        bool    raw; // true if chunk is currently initialized or loaded from the disk
        bool    waiting; // true if some thread is waiting completetion of chunk load from the disk
        uint64_t timestamp;
        Coordinates firstPosWithOverlaps;
        Coordinates lastPos;
        Coordinates lastPosWithOverlaps;
        boost::weak_ptr<const ArrayDesc> _arrayDescLink; // array descriptor
        Array* array;
        InternalStorage* storage;
        DBChunk* cloneOf;
        vector<DBChunk*> clones;
        pthread_t loader;
        
        void updateArrayDescriptor() const ;
        void init();
        void calculateBoundaries();

        // -----------------------------------------
        // L2-List methods
        //
        bool isEmpty();
        void prune();
        void link(DBChunk* elem);
        void unlink();
        // -----------------------------------------
        void beginAccess();

      public:
        bool isTemporary() const;
        
        void setAddress(const ChunkDescriptor& desc);
        void setAddress(const Address& firstElem, int compressionMethod);

        boost::shared_ptr<ConstRLEEmptyBitmap> getEmptyBitmap() const;

        RWLock& getLatch();

        virtual DBChunk const* getDiskChunk() const;

        bool isDelta() const;

        virtual bool isSparse() const;
        virtual bool isRLE() const;
        virtual bool isMaterialized() const;
        virtual void setSparse(bool sparse);
        virtual void setRLE(bool rle);

        virtual size_t count() const;
        virtual bool   isCountKnown() const;
        virtual void   setCount(size_t count);

        virtual const ArrayDesc& getArrayDesc() const;
        virtual const AttributeDesc& getAttributeDesc() const;
        virtual int getCompressionMethod() const;
        virtual void* getData() const;
        virtual size_t getSize() const;
        virtual void allocate(size_t size);
        virtual void reallocate(size_t size);
        virtual void free();
        virtual void compress(CompressedBuffer& buf, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const;
        virtual void decompress(const CompressedBuffer& buf);
        virtual Coordinates const& getFirstPosition(bool withOverlap) const;
        virtual Coordinates const& getLastPosition(bool withOverlap) const;
        virtual boost::shared_ptr<ChunkIterator> getIterator(boost::shared_ptr<Query>& query, int iterationMode);
        virtual boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
        virtual bool pin() const;
        virtual void unPin() const;

        virtual void write(boost::shared_ptr<Query>& query);
        virtual void truncate(Coordinate lastCoord);

        Array const& getArray() const;
        
        const Address& getAddress() const
        {
            return addr;
        }

        const ChunkHeader& getHeader() const
        {
            return hdr;
        }

        uint64_t getTimestamp() const
        {
            return timestamp;
        }

        size_t getCompressedSize() const
        {
            return hdr.compressedSize;
        }

        void setCompressedSize(size_t size)
        {
            hdr.compressedSize = size;
        }

        bool isRaw() const
        {
            return raw;
        }

        void setRaw(bool status)
        {
            raw = status;
        }
        
        DBChunk();
        ~DBChunk();
    };

    /**
     * Storage with LRU in-memory cache of chunks
     */
    class CachedStorage : public InternalStorage
    {
        struct CoordinateMap
        {
            shared_ptr<AttributeMap> attrMap;
            shared_ptr<MemoryBuffer> coordMap;
            ArrayDesc indexArrayDesc;
            bool functionalMapping;
        };
      protected:
        vector<Compressor*> compressors;
        map<Address, DBChunk, AddressLess> chunkMap; // map to get chunk by address
        size_t cacheSize; // maximal size of memory used by cached chunks
        size_t cacheUsed; // current size of memory used by cached chunks (it can be larger than cacheSize if all chunks are pinned)
        Mutex mutex; // mutex used to synchronize access to the storage
        Event event; // event to notify threads waiting for completion of chunk load
        DBChunk lru; // header of LRU L2-list
        Scatter* scatter; // scatter of chunks for virtual partitioning of the data
        uint64_t timestamp;


        void notifyChunkReady(DBChunk& chunk);
        void open(size_t cacheSize, const vector<Compressor*>& compressors);
        virtual int  chooseCompressionMethod(DBChunk& chunk, void* buf);
        virtual DBChunk* lookupChunk(const Address& addr, bool create);
        virtual void freeChunk(DBChunk& chunk);
        virtual void addChunkToCache(DBChunk& chunk);
    private:
        map< string, CoordinateMap> _coordinateMap;
        CoordinateMap& getCoordinateMap(string const& indexName, DimensionDesc const& dim,
                                        const boost::shared_ptr<Query>& query);

      public:
        virtual void   close();
        virtual void   loadChunk(Chunk* chunk);
        virtual RWLock&getChunkLatch(DBChunk* chunk);
        virtual void   pinChunk(Chunk const* chunk);
        virtual void   unpinChunk(Chunk const* chunk);
        virtual bool   containsChunk(const Address& addr);
        virtual void   decompressChunk(Chunk* chunk, CompressedBuffer const& buf);
        virtual void   compressChunk(Chunk const* chunk, CompressedBuffer& buf);
        virtual Chunk* newChunk(const Address& addr, int compressionMethod);
        virtual void   deleteChunk(Chunk& chunk);
        virtual Chunk* cloneChunk(const Address& addr, DBChunk const& srcChunk);
        virtual void   setScatter(Scatter* scatter);
        virtual void   remove(ArrayID arrId, bool allVersions);
        virtual Coordinate mapCoordinate(string const& indexName, DimensionDesc const& dim, Value const& value,
                                         CoordinateMappingMode mode, const boost::shared_ptr<Query>& query);
        virtual Value  reverseMapCoordinate(string const& indexName, DimensionDesc const& dim, Coordinate pos,
                                            const boost::shared_ptr<Query>& query);
        virtual void   cleanupCache();
    };

    //
    // Local storage implementation. This storage manage data located in one or more raw partitions or files at the local node file system.
    //
    class LocalStorage : public CachedStorage
    {
      protected:
        struct SegmentHeader {
            uint64_t used;
        };

        /**
         * Storage header as it was stored in the disk
         */
        struct StorageHeader
        {
            uint32_t magic;
            uint32_t version;
            SegmentHeader segment[MAX_SEGMENTS]; // used part of segments
            uint32_t currPos; // current position in storage header (offset to where new chunk header will be written)
            uint32_t nChunks; // number of chunks in local storage
            NodeID   nodeId; // this node ID
            uint64_t clusterSize;
        };

        struct Cluster {
            DiskPos  pos;
            uint64_t used;

            Cluster() {
                used = 0;
            }
        };

        ClusterID getClusterID(uint32_t segmentNo, uint64_t offs) const
        {
            return offs / hdr.clusterSize * MAX_SEGMENTS + segmentNo;
        }

        ClusterID getClusterID(DiskPos const& pos) const
        {
            return getClusterID(pos.segmentNo, pos.offs);
        }

        void getClusterPos(DiskPos& clusterPos, ClusterID id) const
        {
            clusterPos.offs = id / MAX_SEGMENTS * hdr.clusterSize;
            clusterPos.segmentNo = uint32_t(id % MAX_SEGMENTS);
        }

        class DBArrayIterator;

        class DeltaChunk : public ConstChunk
        {
          public:
            const Array& getArray() const;
            const ArrayDesc& getArrayDesc() const;
            const AttributeDesc& getAttributeDesc() const;
            int getCompressionMethod() const;
            Coordinates const& getFirstPosition(bool withOverlap) const;
            Coordinates const& getLastPosition(bool withOverlap) const;
            boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
            DBChunk const* getDiskChunk() const;
            bool isMaterialized() const;
            bool isSparse() const;
            bool isRLE() const;
            void* getData() const;
            size_t getSize() const;
            bool pin() const;
            void unPin() const;
            void compress(CompressedBuffer& buf, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const;
            
            boost::shared_ptr<ConstRLEEmptyBitmap> getEmptyBitmap() const;

            void setInputChunk(DBChunk* chunk, VersionID ver);
            DeltaChunk(DBArrayIterator& arrayIterator);

          private:
            void extract() const;

            DBArrayIterator& arrayIterator;
            DBChunk*  inputChunk;
            VersionID version;
            MemChunk  versionChunk;
            bool      extracted;
            size_t    accessCount;
        };

        class DBArrayIterator : public ArrayIterator
        {
            friend class DeltaChunk;

            Address addr;
            int defaultCompressionMethod;
            Chunk* currChunk;
            LocalStorage* storage;
            const ArrayDesc& array;
            uint64_t timestamp;
            VersionID version;
            DeltaChunk deltaChunk;
            DeltaChunk deltaBitmapChunk;
            AttributeDesc const& attrDesc;
            bool positioned;
            boost::weak_ptr<Query> _query;

            void setCurrent();

            void position() {
                if (!positioned) {
                    reset();
                }
            }

          public:
            DBArrayIterator(LocalStorage* storage, const ArrayDesc& arrDesc,
                            AttributeID attId, boost::shared_ptr<Query>& query);

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
            virtual boost::shared_ptr<Query> getQuery() { return _query.lock(); }
        };
        union {
            StorageHeader hdr;
            char          filler[HEADER_SIZE];
        };
        vector<Segment> segments;
        long totalAvailable; // total available space in the storage (Gb)

        int sd[MAX_SEGMENTS]; // file descriptors of segments

        int32_t _writeLogThreshold;

        int hd; // storage header file descriptor
        int log[2]; // _ransaction logs
        uint64_t logSizeLimit; // transaciton log size limit
        uint64_t logSize;
        int currLog;
        int redundancy;
        int nNodes;
        bool syncReplication;

        RWLock latches[N_LATCHES];
        queue<uint32_t> freeHeaders[MAX_COORDINATES];
        map<ArrayID, Cluster> clusters;
        map<ClusterID, size_t> liveChunksInCluster;
        vector< set<uint64_t> > freeClusters;
        size_t readAheadSize;

        void freeCluster(ClusterID cid);

        /**
         * Create clone of the chunk but do not write it to the disk
         */
        Chunk* cloneChunk(const Address& addr, DBChunk const& srcChunk, ChunkDescriptor& cloneDesc);

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
        void fetchChunk(DBChunk& chunk);

        /**
         * Replicate chunk
         */
        void replicate(Address const& addr, DBChunk& chunk, void const* data,
                       size_t compressedSize, size_t decompressedSize,
                       boost::shared_ptr<Query>& query);

        /**
         * Assign replication nodes for the particular chunk
         */
        void getReplicasNodeId(NodeID* replicas, DBChunk const& chunk) const;

        /**
         * Check if chunk should be considered by DBArraIterator
         */
        bool isResponsibleFor(DBChunk const& chunk, boost::shared_ptr<Query>& query);

        /**
         * Recover node
         * @param recoveredNode ID of recovered node
         */
        virtual void recover(NodeID recoveredNode, boost::shared_ptr<Query>& query);

        /**
         * Clone chunk
         * @param addr chunk address
         * @param srcChunk chunk to be cloned
         */
        virtual Chunk* cloneChunk(const Address& addr, DBChunk const& srcChunk);

        /**
         * Create new version of the chunknk
         * @param addr chunk address
         * @param srcChunk chunk to be patched
         */
        Chunk* patchChunk(const Address& addr, ConstChunk const& srcChunk, boost::shared_ptr<Query>& query);

        /**
         * Get chunk with specified array and attributes IDs having address greater than specified
         * @param addr address of chunk, if success "coords" component of addr is updated with coordinates of next chunk
         * @param timestamp query timestamp
         * @return next chunk or NULL if not found
         */
        DBChunk* nextChunk(Address& addr, uint64_t timestamp, boost::shared_ptr<Query>& query);

        void getDiskInfo(DiskInfo& info);

      private:

        /**
         * Perform metadata/lock recovery and storage rollback as part of the intialization.
         * It may block waiting for the remote coordinator recovery to occur.
         */
        void doTxnRecoveryOnStartup();

      public:
        LocalStorage();
        /**
         * Open storage manager
         * @param headerPath path to the storage header file: file containing information about locally available chunks
         * @param transactionLogPath path to the transaciotns log (there are two logs forming cyclic buffer with extenions *_1 and *_2)
         * @param transLogLimit maximal size of transaction log file
         * @param segments database segments: raw partitions or files containing data
         * @param cacheSize chunk cache size: amount of memory in bytes which can be used to cache most frequently used
         * chunks
         * @param compressors vector of available compressors
         */
        void open(const string& headerPath, const string& transactionLogPath, uint64_t transLogLimit, vector<Segment> const& segments, size_t cacheSize, const vector<Compressor*>& compressors);

        /**
         * Rollback uncompleted updates
         * @param map of updated array which has to be rollbacked
         */
        virtual void rollback(std::map<ArrayID,VersionID> const& undoUpdates);

        /**
         * Open storage using privided descriptor file
         * Description file has the following format:
         * ----------------------
         * <storage-header-path>
         * <segment1-size-Mb> <segment1-path>
         * <segment2-size-Mb> <segment2-path>
         * ...
         * ----------------------
         */
        virtual void open(const string& storageDescriptorFilePath, size_t cacheSize);
        virtual void close();
        virtual void flush();
        virtual boost::shared_ptr<ArrayIterator> getArrayIterator(const ArrayDesc& arrDesc, AttributeID attId,
                                                                  boost::shared_ptr<Query>& query);
        virtual void writeChunk(Chunk* chunk, boost::shared_ptr<Query>& query);
        virtual void loadChunk(Chunk* chunk);
        virtual RWLock&getChunkLatch(DBChunk* chunk);
        virtual Chunk* readChunk(const Address& addr);
        virtual void compressChunk(Chunk const* chunk, CompressedBuffer& buf);
        virtual bool isLocal();
        virtual void setNodeId(NodeID id);
        virtual NodeID getNodeId() const;
        virtual size_t getNumberOfNodes() const;

        virtual void remove(ArrayID id, bool allVersions);

        static LocalStorage instance;
    };

    //
    // Global storage implementation. This storage manages chunks located in DFS directory.
    //
    class GlobalStorage : public CachedStorage
    {
      protected:
        class DBArrayIterator : public ArrayIterator
        {
            DIR* dir;
            Address addr;
            int defaultCompressionMethod;
            Chunk* currChunk;
            GlobalStorage* storage;
            ArrayDesc const& array;
            uint64_t timestamp;
            boost::weak_ptr<Query> _query;

            void setCurrent();

          public:
            DBArrayIterator(GlobalStorage* storage, const ArrayDesc& arrDesc,
                            AttributeID attId, boost::shared_ptr<Query>& query);
            ~DBArrayIterator();

            virtual ConstChunk const& getChunk();
            virtual bool end();
            virtual void operator ++();
            virtual Coordinates const& getPosition();
            virtual bool setPosition(Coordinates const& pos);
            virtual void reset();
            virtual void   deleteChunk(Chunk& chunk);
            virtual Chunk& newChunk(Coordinates const& pos);
            virtual Chunk& newChunk(Coordinates const& pos, int compressionMethod);
            virtual boost::shared_ptr<Query> getQuery() { return _query.lock(); }
        };

        string dfsDir;
        string headerFilePath;
        NodeID nodeId;
        size_t nNodes;

        /**
         * Force wrinting of specified amount of data to the disk.
         * Exception is thrown if write failed
         */
        void writeAll(int fd, const void* data, size_t size);

        /**
         * Read requested amount of bytes from the disk.
         * Exception is thrown if read failed
         */
        void readAll(int fd, void* data, size_t size);

        virtual string getChunkPath(const Address& addr, bool withCoordinates = false);
        virtual void freeChunk(DBChunk& chunk);

      public:
        /**
         * Open global storage
         * @param headerPath path to the database local header file
         * @param rootDir root directory in the distributes file system where database chunks are located
         * @param cacheSize chunk cache size: amount of memory in bytes which can be used to cache most frequently used
         * chunks
         * @param compressors vector of available compressors
         */
        void open(const string& headerPath, const string& rootDir, size_t cacheSize, const vector<Compressor*>& compressors);

        /**
         * Rollback uncompleted updates
         * @param map of updated array which has to be rollbacked
         */
        virtual void rollback(std::map<ArrayID,VersionID> const& undoUpdates);

        virtual void recover(NodeID recoveredNode, boost::shared_ptr<Query>& query);

        virtual void getDiskInfo(DiskInfo& info);

        /**
         * Open global storage using provided descriptor file
         * Description file has the following format:
         * ----------------------
         * <storage-header-path>
         * <distributed-file-system-directory>
         * ----------------------
         */
        virtual void   open(const string& storageDescriptorFilePath, size_t cacheSize);
        virtual void   flush();
        virtual boost::shared_ptr<ArrayIterator> getArrayIterator(const ArrayDesc& arrDesc, AttributeID attId,
                                                                  boost::shared_ptr<Query>& query);
        virtual void   writeChunk(Chunk* chunk, boost::shared_ptr<Query>& query);
        virtual Chunk* readChunk(const Address& addr);
        virtual void   compressChunk(Chunk const* chunk, CompressedBuffer& buf);
        virtual bool   containsChunk(const Address& addr);
        virtual bool   isLocal();
        virtual void   setNodeId(NodeID id);
        virtual NodeID getNodeId() const;
        virtual size_t getNumberOfNodes() const;
        virtual void   remove(ArrayID id, bool allVersions);

        static GlobalStorage instance;
    };
}

#endif
