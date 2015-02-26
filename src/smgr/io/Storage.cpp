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
 * @file
 *
 * @brief Storage implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include "util/FileIO.h"

#include <inttypes.h>
#include <map>

#include "boost/make_shared.hpp"
#include "log4cxx/logger.h"
#include "system/Exceptions.h"
#include "smgr/io/InternalStorage.h"
#include "system/SystemCatalog.h"
#include "network/NetworkManager.h"
#include "network/BaseConnection.h"
#include "network/MessageUtils.h"
#include "array/Metadata.h"
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"
#include "query/Statistics.h"
#include "query/Operator.h"
#include "smgr/delta/Delta.h"
#include "system/Cluster.h"

#include <sys/time.h>

#ifdef __APPLE__
#define fdatasync(_fd) fsync(_fd)
#endif

namespace scidb
{
#ifndef SCIDB_NO_DELTA_COMPRESSION
    DeltaVersionControl _deltaVersionControl;
#endif // SCIDB_NO_DELTA_COMPRESSION

    const uint32_t SCIDB_STORAGE_HEADER_MAGIC = 0xDDDDBBBB;
    const uint32_t SCIDB_STORAGE_FORMAT_VERSION = 3;


    // Logger for operator. static to prevent visibility of variable outside of file
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc"));

    using namespace std;
    using namespace boost;

    const size_t DEFAULT_SEGMENT_SIZE = 1024*1024*1024; // default limit of database partition (in megabytes) used for generated storage descriptor file
    const size_t DEFAULT_TRANS_LOG_LIMIT = 1024; // default limit of transaction log file (in megabytes)
    const size_t MAX_CFG_LINE_LENGTH = 1024;
    const int MAX_REDUNDANCY = 8;
    const int MAX_INSTANCE_BITS = 10; // 2^MAX_INSTANCE_BITS = max number of instances
/**
 * Fibonacci hash for a 64 bit key
 * @param key to hash
 * @param fib_B = log2(max_num_of_buckets)
 * @return hash = bucket index
 */
uint64_t fibHash64(const uint64_t key, const uint64_t fib_B)
{
    assert(fib_B < 64);
    const uint64_t fib_A64 = (uint64_t)11400714819323198485U;
    return (key*fib_A64) >> (64-fib_B);
}

    VersionControl* VersionControl::instance;

    bool HashScatter::contains(const Address& addr)
    {
        return addr.hash() % nDomains == domainId - 1;
    }

    HashScatter::HashScatter(size_t nDomains, size_t domainId)
    {
        this->nDomains = nDomains;
        this->domainId = domainId;
    }

    HashScatter::HashScatter()
    {
        this->nDomains = SystemCatalog::getInstance()->getNumberOfInstances();
        this->domainId = (size_t)StorageManager::getInstance().getInstanceId();
    }

    void ChunkDescriptor::getAddress(Address& addr) const
    {
        addr.arrId = hdr.arrId;
        addr.attId = hdr.attId;
        addr.coords.resize(hdr.nCoordinates);
        for (int j = 0; j < hdr.nCoordinates; j++) {
            addr.coords[j] = coords[j];
        }
    }

    //
    // Cached storage methods
    //

    Array const& CachedStorage::getDBArray(ArrayID arrayID) 
    {
        ScopedMutexLock cs(mutex);
        boost::shared_ptr<DBArray>& arr = dbArrayCache[arrayID];
        if (!arr) { 
            boost::shared_ptr<Query> emptyQuery;
            arr = boost::shared_ptr<DBArray>(new DBArray(arrayID, emptyQuery));
        }
        return *arr;
    }

    CachedStorage::CoordinateMapInitializer::~CoordinateMapInitializer()
    {
        ScopedMutexLock cs(storage.mutex);
        cm.raw = false;
        cm.initialized = initialized;
        if (cm.waiting) {
            cm.waiting = false;
            storage.initEvent.signal(); // wakeup all threads waiting for this chunk
        }
    }

    CachedStorage::ChunkInitializer::~ChunkInitializer()
    {
        ScopedMutexLock cs(storage.mutex);
        storage.notifyChunkReady(chunk);
    }

    CachedStorage::CoordinateMap& CachedStorage::getCoordinateMap(string const& indexName,
                                                                  DimensionDesc const& dim,
                                                                  const boost::shared_ptr<Query>& query)
    {
        CoordinateMap* cm;
        {
            ScopedMutexLock cs(mutex);
            mutex.checkForDeadlock();
            cm = &_coordinateMap[indexName];
            if (cm->initialized) {
                assert(!cm->raw);
                return *cm;
            }
            if (cm->raw) { 
                // Some other thread is already constructing this mapping
                do {
                    cm->waiting = true;
                    Event::ErrorChecker noopEc;
                    initEvent.wait(mutex, noopEc);
                } while (cm->raw);
                if (!cm->initialized) { 
                    throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NO_MAPPING_FOR_COORDINATE);
                }
                return *cm;
            }
            cm->attrMap = buildFunctionalMapping(dim);
            if (cm->attrMap) { // functional mapping: do not need to load something from storage
                if (dim.getFlags() & DimensionDesc::COMPLEX_TRANSFORMATION) { 
                    throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_FUNC_MAP_TRANSFORMATION_NOT_POSSIBLE);
                }
                cm->functionalMapping = true;
                cm->initialized = true;
                return *cm;
            }
            cm->raw = true; 
        }
        CoordinateMapInitializer guard(this, cm);
        boost::shared_ptr<Array> indexArray = query->getArray(indexName);
        shared_ptr<ConstArrayIterator> ai = indexArray->getConstIterator(0);
        Coordinates origin(1);
        origin[0] = dim.getStart();
        if (!ai->setPosition(origin)) { 
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NO_MAPPING_FOR_COORDINATE);
        }
        ConstChunk const& chunk = ai->getChunk();
        PinBuffer scope(chunk);
        cm->indexArrayDesc = indexArray->getArrayDesc();
        TypeId attrType = cm->indexArrayDesc.getAttributes()[0].getType();
        DimensionDesc const& mapDim = cm->indexArrayDesc.getDimensions()[0];
        cm->attrMap = shared_ptr<AttributeMultiMap>(new AttributeMultiMap(attrType, mapDim.getStart(), (size_t)mapDim.getLength(),
                                                                          chunk.getData(), chunk.getSize()));
        cm->coordMap = shared_ptr<MemoryBuffer>(new MemoryBuffer(chunk.getData(), chunk.getSize()));
        cm->functionalMapping = false;
        guard.initialized = true;
        return *cm;
    }

    void CachedStorage::removeCoordinateMap(string const& indexName)
    {
       ScopedMutexLock cs(mutex);
       _coordinateMap.erase(indexName);
    }

    Value CachedStorage::reverseMapCoordinate(string const& indexName, DimensionDesc const& dim, Coordinate pos,
                                              const boost::shared_ptr<Query>& query)
    {
        CoordinateMap& cm = getCoordinateMap(indexName, dim, query);
        if (cm.functionalMapping) {
            Value value;
            cm.attrMap->getOriginalCoordinate(value, (pos * dim.getFuncMapScale()) + dim.getFuncMapOffset());
            return value;
        }
        uint8_t* src = (uint8_t*)cm.coordMap->getData();
        AttributeDesc const& idxAttr = cm.indexArrayDesc.getAttributes()[0];
        DimensionDesc const& idxDim = cm.indexArrayDesc.getDimensions()[0];
        size_t index = (size_t)(pos - idxDim.getStart());
        if (index >= idxDim.getLength()) {
            return Value();
        }
        Type type(TypeLibrary::getType(idxAttr.getType()));
        size_t size = type.byteSize();
        if (size == 0) {
            src += size_t(idxDim.getLength())*sizeof(int) + ((int*)src)[index];
            if (*src == 0) {
                size = (src[1] << 24) | (src[2] << 16) | (src[3] << 8) | src[4];
                src += 5;
            } else {
                size = *src++;
            }
        } else {
            src += size*index;
        }
        return Value(src, size, false);
    }

    Coordinate CachedStorage::mapCoordinate(string const& indexName,
                                            DimensionDesc const& dim,
                                            Value const& value,
                                            CoordinateMappingMode mode,
                                            const boost::shared_ptr<Query>& query)
    {
        CoordinateMap& cm = getCoordinateMap(indexName, dim, query);
        Coordinate c = cm.attrMap->get(value, mode);
        if (cm.functionalMapping) {
            c = (c - dim.getFuncMapOffset()) / dim.getFuncMapScale();
        }
        return c;
    }

    void CachedStorage::remove(ArrayID arrId, bool allVersions, uint64_t timestamp)
    {
        Address addr;
        addr.arrId = arrId;
        addr.attId = 0;
        
        ScopedMutexLock cs(mutex);
        
        while (true) {
            map<Address, DBChunk, AddressLess>::iterator i = chunkMap.upper_bound(addr);
            if (i == chunkMap.end() || i->first.arrId != arrId) {
                break;
            }
            DBChunk& chunk = i->second;
            if (chunk.timestamp <= timestamp) { 
                addr.attId = chunk.addr.attId;
                addr.coords = chunk.addr.coords;
                continue;
            }
            DBChunk* original = chunk.cloneOf;
            for (size_t j = 0; j < chunk.clones.size(); j++) {
                DBChunk* clone = chunk.clones[j];
                clone->cloneOf = original;
                 if (original != NULL) {
                     original->clones.push_back(clone);
                 }
            }
            if (original != NULL) {
                assert(original->getDiskChunk() == original);
                vector<DBChunk*>& clones = original->clones;
                for (size_t j = 0; j < clones.size(); j++) {
                    if (clones[j] == &chunk) {
                        clones.erase(clones.begin() + j);
                        break;
                    }
                }
            }
            CachedStorage::freeChunk(chunk);
            chunkMap.erase(i);
        }
        dbArrayCache.erase(arrId);
    }

    void CachedStorage::notifyChunkReady(DBChunk& chunk)
    {
        // This method is invoked with storage mutex locked
        chunk.raw = false;
        if (chunk.waiting) {
            chunk.waiting = false;
            loadEvent.signal(); // wakeup all threads waiting for this chunk
        }
    }

    void CachedStorage::open(size_t cacheSize, const vector<Compressor*>& compressors)
    {
        this->cacheSize = cacheSize;
        this->compressors = compressors;
        cacheUsed = 0;
        strictCacheLimit = Config::getInstance()->getOption<bool>(CONFIG_STRICT_CACHE_LIMIT);
        cacheOverflowFlag = false;
        scatter = NULL;
        timestamp = 1;
        lru.prune();
    }

    void CachedStorage::close()
    {
        for (map<Address, DBChunk, AddressLess>::iterator i = chunkMap.begin(); i != chunkMap.end(); ++i)
        {
            if (i->second.accessCount != 0)
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_PIN_UNPIN_DISBALANCE);
        }
        chunkMap.clear();
    }

    void CachedStorage::setScatter(Scatter* scatter)
    {
        this->scatter = scatter;
    }

    void CachedStorage::pinChunk(Chunk const* aChunk)
    {
        ScopedMutexLock cs(mutex);
        DBChunk& chunk = *(DBChunk*)aChunk;
        chunk.beginAccess();
    }

    bool CachedStorage::containsChunk(const Address& addr)
    {
        ScopedMutexLock cs(mutex);
        return chunkMap.find(addr) != chunkMap.end();
    }

    void CachedStorage::addChunkToCache(DBChunk& chunk)
    {
        // Check amount of memory used by cached chunks and discard least recently used
        // chunks from the cache
        mutex.checkForDeadlock();
        while (cacheUsed + chunk.hdr.size > cacheSize) { 
            if (lru.isEmpty()) {
                if (strictCacheLimit && cacheUsed != 0) { 
                    Event::ErrorChecker noopEc;
                    cacheOverflowFlag = true;
                    cacheOverflowEvent.wait(mutex, noopEc);
                } else { 
                    break;
                }
            }
            freeChunk(*lru.prev);
        }
        cacheUsed += chunk.hdr.size;
    }

    RWLock& CachedStorage::getChunkLatch(DBChunk*)
    {
        return *(RWLock*)0;
    }

    void CachedStorage::loadChunk(Chunk* aChunk)
    {
        ScopedMutexLock cs(mutex);
        DBChunk& chunk = *(DBChunk*)aChunk;
        if (chunk.accessCount < 2) { // Access count>=2 means that this chunk is already pinned and loaded by some upper frame so access to it may not cause deadlock */
            mutex.checkForDeadlock();
        }
        if (chunk.raw) {
            // Some other thread is already loading the chunk: just wait until it completes
            do {
                chunk.waiting = true;
                Semaphore::ErrorChecker ec;
                boost::shared_ptr<Query> query = Query::getQueryByID(Query::getCurrentQueryID(), false, false);
                if (query) { 
                    ec = bind(&Query::validate, query);
                }
                loadEvent.wait(mutex, ec);
            } while (chunk.raw);

            if (chunk.data == NULL) {
                chunk.raw = true;
            }
        } else {
            if (chunk.data == NULL) { 
                mutex.checkForDeadlock();
                chunk.raw = true;
                addChunkToCache(chunk);
            }
        }
    }

   DBChunk* CachedStorage::lookupChunk(const Address& addr, bool create)
   {
        ScopedMutexLock cs(mutex);
        if (!create && chunkMap.find(addr) == chunkMap.end()) {
            return NULL;
        }
        DBChunk& chunk = chunkMap[addr];
        chunk.beginAccess();
        return &chunk;
   }

    void CachedStorage::freeChunk(DBChunk& victim)
    {
        if (victim.data != NULL && victim.hdr.pos.hdrPos != 0) {
            cacheUsed -= victim.getSize();
            if (cacheOverflowFlag) { 
                cacheOverflowFlag = false;
                cacheOverflowEvent.signal();
            }
        }
        if (victim.next != NULL) {
            victim.unlink();
        }
        victim.free();
    }

    void CachedStorage::unpinChunk(Chunk const* aChunk)
    {
        ScopedMutexLock cs(mutex);
        DBChunk& chunk = *(DBChunk*)aChunk;
        assert(chunk.accessCount > 0);
        if (--chunk.accessCount == 0) {
            // Chunk is not accessed any more by any thread, unpin it and include in LRU list
            lru.link(&chunk);
        }
    }

    void CachedStorage::decompressChunk(Chunk* chunk, CompressedBuffer const& buf)
    {
        chunk->allocate(buf.getDecompressedSize());
        PinBuffer scope(buf);
        if (buf.getSize() != buf.getDecompressedSize()) {
            compressors[buf.getCompressionMethod()]->decompress(buf.getData(), buf.getSize(), *chunk);
        } else {
            memcpy(chunk->getData(), buf.getData(), buf.getSize());
        }
    }

    void CachedStorage::compressChunk(Chunk const* aChunk, CompressedBuffer& buf)
    {
        DBChunk& chunk = *(DBChunk*)aChunk;
        int compressionMethod = chunk.getCompressionMethod();
        if (compressionMethod < 0)
            throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_COMPRESS_METHOD_NOT_DEFINED);
        buf.setDecompressedSize(chunk.getSize());
        buf.setCompressionMethod(compressionMethod);
        {
            ScopedMutexLock cs(mutex);
            if (!chunk.isRaw() && chunk.data != NULL) {
                PinBuffer scope(chunk);
                buf.allocate(chunk.getCompressedSize() != 0 ? chunk.getCompressedSize() : chunk.getSize());
                size_t compressedSize = compressors[compressionMethod]->compress(buf.getData(), chunk);
                if (compressedSize == chunk.getSize()) {
                    memcpy(buf.getData(), chunk.data, compressedSize);
                } else if (compressedSize != buf.getSize()) {
                    buf.reallocate(compressedSize);
                }
            }
        }
    }

    void CachedStorage::deleteChunk(Chunk& chunk)
    {
        ScopedMutexLock cs(mutex);
        DBChunk& victim = (DBChunk&)chunk;
        CachedStorage::freeChunk(victim);
        assert(victim.cloneOf == NULL);
        chunkMap.erase(victim.addr);
    }

    Chunk* CachedStorage::newChunk(const Address& addr, int compressionMethod)
    {
        ScopedMutexLock cs(mutex);
        if (chunkMap.find(addr) != chunkMap.end())
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_ALREADY_EXISTS);
        DBChunk& chunk = chunkMap[addr];
        chunk.setAddress(addr, compressionMethod);
        chunk.accessCount = 1; // newly created chunk is pinned
        chunk.nWriters = 0;
        chunk.timestamp = ++timestamp;
        return &chunk;
    }

    Chunk* CachedStorage::cloneChunk(const Address& addr, DBChunk const& srcChunk)
    {
        return NULL;
    }

    void CachedStorage::cloneChunk(Coordinates const& pos, AttributeID attrID, ArrayID originalArrayID, ArrayID cloneArrayID)
    {
        Address addr(originalArrayID, attrID, pos);
        Chunk* origChunk = readChunk(addr);
        addr.arrId = cloneArrayID;
        cloneChunk(addr, *(DBChunk*)origChunk);
        origChunk->unPin();
    }

    int CachedStorage::chooseCompressionMethod(DBChunk& chunk, void* buf)
    {
        if (chunk.hdr.compressionMethod < 0) {
            size_t minSize = chunk.getSize();
            int best = 0;
            for (int i = 0, n = compressors.size(); i < n; i++) {
                size_t compressedSize = compressors[i]->compress(buf, chunk);
                if (compressedSize < minSize) {
                    best = i;
                    minSize = compressedSize;
                }
            }
            chunk.hdr.compressionMethod = best;
            if (chunk.getAttributeDesc().getDefaultCompressionMethod() != best) {
                SystemCatalog::getInstance()->setDefaultCompressionMethod(chunk.addr.arrId, chunk.addr.attId, best);
                chunk.updateArrayDescriptor();
            }
        }
        return chunk.hdr.compressionMethod;
    }

    //
    // Local storage methods
    //

    inline VersionID getVersionID(ArrayDesc const& desc, AttributeID attr)
    {
        if (desc.getAttributes()[attr].getReserve() != 0) {
            char const* at = strchr(desc.getName().c_str(), '@');
            return (at != NULL) ? atol(at+1) : 0;
        }
        return 0;
    }

    inline bool LocalStorage::isResponsibleFor(DBChunk const& chunk,
                                               boost::shared_ptr<Query>& query)
    {
        ScopedMutexLock cs(mutex);
        Query::validateQueryPtr(query);
        assert(chunk.hdr.instanceId < size_t(nInstances));

        if (chunk.hdr.instanceId == hdr.instanceId) {
            return true;
        }
        if (!query->isPhysicalInstanceDead(chunk.hdr.instanceId))  {
            return false;
        }
        if (redundancy == 1) {
            return true;
        }
        InstanceID replicas[MAX_REDUNDANCY+1];
        getReplicasInstanceId(replicas, chunk);
        for (int i = 1; i <= redundancy; i++) {
            if (replicas[i] == hdr.instanceId) {
                return true;
            }
            if (!query->isPhysicalInstanceDead(replicas[i])) {
               // instance with this replica is alive
               return false;
            }
        }
        return false;
    }

    void LocalStorage::remove(ArrayID arrId, bool allVersions, uint64_t timestamp)
    {
        ScopedMutexLock cs(mutex);
        Address addr;
        addr.arrId = arrId;
        addr.attId = 0;

        for (map<Address, DBChunk, AddressLess>::iterator i = chunkMap.upper_bound(addr);
             i != chunkMap.end() && i->first.arrId == arrId;
             ++i)
        {                
            DBChunk& chunk = i->second; 
            if (chunk.hdr.pos.hdrPos != 0 && chunk.timestamp > timestamp) {
                chunk.hdr.arrId = 0;
                LOG4CXX_TRACE(logger, "ChunkDesc: Free chunk descriptor at position " << chunk.hdr.pos.hdrPos);
                File::writeAll(hd, &chunk.hdr, sizeof(ChunkHeader), chunk.hdr.pos.hdrPos);
                assert(chunk.hdr.nCoordinates < MAX_COORDINATES);
                freeHeaders.insert(chunk.hdr.pos.hdrPos);
                
                if (hdr.clusterSize != 0) 
                {
                    ClusterID cluId = getClusterID(i->second.hdr.pos);
                    size_t& nChunks = liveChunksInCluster[cluId];
                    assert(nChunks > 0);
                    if (--nChunks == 0) { 
                        freeCluster(cluId);
                        clusters.erase(arrId);
                    }
                }
            }
        }
        // Update storage header
        File::writeAll(hd, &hdr, HEADER_SIZE, 0);
        CachedStorage::remove(arrId, allVersions, timestamp);
    }

    void LocalStorage::freeCluster(ClusterID id)
    {
        liveChunksInCluster.erase(id);
        DiskPos clusterPos;
        getClusterPos(clusterPos, id);
        LOG4CXX_DEBUG(logger, "Free cluster " << id << " query " << Query::getCurrentQueryID());
        freeClusters[clusterPos.segmentNo].insert(clusterPos.offs);
    }

    int LocalStorage::getRandomSegment()
    {
        int i = 0, n = segments.size()-1;
        if (n != 0) {
            long val = (long)(rand() % totalAvailable);
            do {
                val -= (long)((segments[i].size - hdr.segment[i].used)/GB);
            } while (val > 0 && ++i < n);
        }
        return i;
    }

    void LocalStorage::getReplicasInstanceId(InstanceID* replicas, DBChunk const& chunk) const
    {
        replicas[0] = chunk.hdr.instanceId;
        for (int i = 0; i < redundancy; i++) {
            // A prime number can be used to smear the replicas as follows
            // InstanceID instanceId = (chunk.getArrayDesc().getChunkNumber(chunk.addr.coords) + (i+1)) % PRIME_NUMBER % nInstances;
            // the PRIME_NUMBER needs to be just shy of the number of instances to work, so we would need a table.
            // For Fibonacci no table is required, and it seems to work OK.

            const uint64_t nReplicas = (redundancy+1);
            const uint64_t currReplica = (i+1);
            const uint64_t chunkId = chunk.getArrayDesc().getChunkNumber(chunk.addr.coords)*(nReplicas) + currReplica;
            InstanceID instanceId = fibHash64(chunkId,MAX_INSTANCE_BITS) % nInstances;
            for (int j = 0; j <= i; j++) {
                if (replicas[j] == instanceId) {
                    instanceId = (instanceId + 1) % nInstances;
                    j = -1;
                }
            }
            replicas[i+1] = instanceId;
        }
    }

    void LocalStorage::recover(InstanceID recoveredInstance, boost::shared_ptr<Query>& query)
    {
        ScopedMutexLock cs(mutex);
        NetworkManager* networkManager = NetworkManager::getInstance();
        for (map<Address, DBChunk, AddressLess>::iterator i = chunkMap.begin(); i != chunkMap.end(); ++i) {
            DBChunk const& chunk = i->second;
            if (chunk.hdr.instanceId == recoveredInstance || isResponsibleFor(chunk, query)) {
                if (chunk.hdr.instanceId == hdr.instanceId) {
                    InstanceID replicas[MAX_REDUNDANCY+1];
                    getReplicasInstanceId(replicas, chunk);
                    replicas[0] = recoveredInstance; // barrier
                    int j = redundancy;
                    while (replicas[j] != recoveredInstance) {
                        j -= 1;
                    }
                    if (j == 0) {
                        continue;
                    }
                } else if (chunk.hdr.instanceId != recoveredInstance) {
                    continue;
                }
                size_t compressedSize = chunk.hdr.compressedSize;
                boost::scoped_array<char> data(new char[compressedSize]);
                readAll(chunk.hdr.pos, data.get(), compressedSize);

                boost::shared_ptr<CompressedBuffer> buffer = boost::make_shared<CompressedBuffer>();
                buffer->allocate(compressedSize);
                memcpy(buffer->getData(), data.get(), compressedSize);
                data.reset();

                boost::shared_ptr<MessageDesc> chunkMsg = boost::make_shared<MessageDesc>(mtRecoverChunk, buffer);
                boost::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
                chunkRecord->set_eof(false);
                chunkRecord->set_sparse(chunk.isSparse());
                chunkRecord->set_compression_method(chunk.getCompressionMethod());
                chunkRecord->set_attribute_id(chunk.addr.attId);
                chunkRecord->set_array_id(chunk.addr.arrId);
                chunkRecord->set_decompressed_size(chunk.hdr.size);
                chunkRecord->set_count(0);
                chunkMsg->setQueryID(query->getQueryID());

                for (size_t k = 0; k < chunk.addr.coords.size(); k++)
                {
                    chunkRecord->add_coordinates(chunk.addr.coords[k]);
                }
                LOG4CXX_DEBUG(logger, "Recover chunk of array " << chunk.getArrayDesc().getName() << "(" << chunk.addr.arrId << "), coordinates=" << chunk.addr.coords);

                networkManager->sendMessage(recoveredInstance, chunkMsg); // wait until message is sent to avoid async queue overflow
            }
        }
    }

void LocalStorage::abortReplicas(vector<boost::shared_ptr<ReplicationManager::Item> >* replicasVec)
{
    assert(replicasVec);
    for (size_t i=0; i<replicasVec->size(); ++i) {
        const boost::shared_ptr<ReplicationManager::Item>& item = (*replicasVec)[i];
        assert(_replicationManager);
        _replicationManager->abort(item);
        assert(item->isDone());
    }
}

void LocalStorage::waitForReplicas(vector<boost::shared_ptr<ReplicationManager::Item> >& replicasVec)
{
    for (size_t i=0; i<replicasVec.size(); ++i) {
        const boost::shared_ptr<ReplicationManager::Item>& item = replicasVec[i];
        assert(_replicationManager);
        _replicationManager->wait(item);
        assert(item->isDone());
        assert(item->validate(false));
    }
}
void LocalStorage::replicate(Address const& addr, DBChunk& chunk, void const* data,
                             size_t compressedSize, size_t decompressedSize,
                             boost::shared_ptr<Query>& query,
                             vector<boost::shared_ptr<ReplicationManager::Item> >& replicasVec)
    {
        ScopedMutexLock cs(mutex);
        Query::validateQueryPtr(query);

        if (redundancy <= 0 || chunk.hdr.instanceId != hdr.instanceId || chunk.getArrayDesc().isLocal()) { // self chunk
            return;
        }
            replicasVec.reserve(redundancy);
            InstanceID replicas[MAX_REDUNDANCY+1];
            getReplicasInstanceId(replicas, chunk);

            QueryID queryId = query->getCurrentQueryID();
            assert(queryId != 0);
            boost::shared_ptr<MessageDesc> chunkMsg;
            if (data != NULL) { 
                boost::shared_ptr<CompressedBuffer> buffer = boost::make_shared<CompressedBuffer>();
                buffer->allocate(compressedSize);
                memcpy(buffer->getData(), data, compressedSize);
                chunkMsg = boost::make_shared<MessageDesc>(mtChunkReplica, buffer);
            } else { 
                chunkMsg = boost::make_shared<MessageDesc>(mtChunkReplica);
            }
            boost::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
            chunkRecord->set_eof(false);
            chunkRecord->set_sparse(chunk.isSparse());
            chunkRecord->set_rle(chunk.isRLE());
            chunkRecord->set_compression_method(chunk.getCompressionMethod());
            chunkRecord->set_attribute_id(addr.attId);
            chunkRecord->set_array_id(addr.arrId);
            chunkRecord->set_decompressed_size(decompressedSize);
            chunkRecord->set_count(0);
            chunkMsg->setQueryID(queryId);

            LOG4CXX_TRACE(logger, "Replicate chunk of array ID=" << addr.arrId << " attribute ID=" << addr.attId);

            if (data == NULL) { 
                assert(chunk.cloneOf != NULL);
                chunkRecord->set_cloned_array_id(chunk.cloneOf->addr.arrId);
            }
            for (size_t k = 0; k < addr.coords.size(); k++)
            {
                chunkRecord->add_coordinates(addr.coords[k]);
            }

            for (int i = 1; i <= redundancy; i++) {
                boost::shared_ptr<ReplicationManager::Item> item(new ReplicationManager::Item(replicas[i], chunkMsg, query));
                assert(_replicationManager);
                _replicationManager->send(item);
                replicasVec.push_back(item);
            }
    }

    inline double getTimeSecs()
    {
        struct timeval tv;
        gettimeofday(&tv,0);
        return (((double) tv.tv_sec) * 1000000 + ((double) tv.tv_usec)) / 1000000;
    }

    void LocalStorage::writeAll(const DiskPos& pos, const void* data, size_t size)
    {
        double t0=0, t1=0, writeTime=0;

        if (_writeLogThreshold >= 0)
        {
            t0 = getTimeSecs();
        }
        File::writeAll(sd[pos.segmentNo], data, size, pos.offs);
        if (_writeLogThreshold >= 0)
        {
            t1 = getTimeSecs();
            writeTime = t1 - t0;
        }

        if (_writeLogThreshold>=0 && writeTime*1000 > _writeLogThreshold )
        {
            LOG4CXX_DEBUG(logger, "CWR: pwrite fd "<<sd[pos.segmentNo]<<" size "<<size<<" time "<<writeTime);
        }
    }

    void LocalStorage::readAll(const DiskPos& pos, void* data, size_t size)
    {
        double t0=0, t1=0, readTime=0;
        if (_writeLogThreshold >= 0)
        {
            t0 = getTimeSecs();
        }
        File::readAll(sd[pos.segmentNo], data, size, pos.offs);
        if (_writeLogThreshold >= 0)
        {
            t1 = getTimeSecs();
            readTime = t1 - t0;
        }
        if (_writeLogThreshold>=0 && readTime*1000 > _writeLogThreshold )
        {
            LOG4CXX_DEBUG(logger, "CWR: pwrite fd "<<sd[pos.segmentNo]<<" size "<<size<<" time "<<readTime);
        }
    }

    RWLock& LocalStorage::getChunkLatch(DBChunk* chunk)
    {
        return latches[(size_t)chunk->hdr.pos.offs % N_LATCHES];
    }

    DBChunk* LocalStorage::nextChunk(Address& addr, uint64_t timestamp,
                                     ArrayDesc const& desc, PartitioningSchema ps, 
                                     boost::shared_ptr<Query>& query)
    {
        ScopedMutexLock cs(mutex);
        Query::validateQueryPtr(query);

        while (true) {
            map<Address, DBChunk, AddressLess>::iterator curr = chunkMap.upper_bound(addr);
            if (curr == chunkMap.end() || addr.arrId != curr->first.arrId || addr.attId != curr->first.attId) {
                return NULL;
            }
            DBChunk& chunk = curr->second;
            addr.coords = chunk.addr.coords;
            if (isResponsibleFor(chunk, query)
                && chunk.timestamp <= timestamp
                && (ps != psReplication || desc.getChunkNumber(addr.coords) % nInstances == hdr.instanceId)
                && (scatter == NULL || scatter->contains(chunk.addr)))
            {
#ifdef PIN_CURRENT_CHUNK
                pinChunk(&chunk);
#endif
                return &chunk;
            }
        }
    }

    inline uint32_t calculateCRC32(void const* content, size_t content_length, uint32_t crc = ~0)
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
        
        unsigned char* buffer = (unsigned char*)content;
        
        while (content_length-- != 0) {
            crc = (crc >> 8) ^ table[(crc & 0xFF) ^ *buffer++];
        }
        return crc;
    }
    

    Chunk* LocalStorage::patchChunk(const Address& addr, ConstChunk const& srcChunk,
                                    boost::shared_ptr<Query>& query)
    {
        Chunk* newClone(NULL);
        vector<boost::shared_ptr<ReplicationManager::Item> > replicasVec;
        boost::function<void()> f = boost::bind(&LocalStorage::abortReplicas, this, &replicasVec); 
        Destructor replicasCleaner(f);
        {
            Query::validateQueryPtr(query);
            //
            // "addr" now specifies address of new chunk (which doesn't exist yet). And we need address of the original version of the chunk.
            // The fragment of code below is getting address of chunk which should be patched
            //
            ArrayDesc newVersionDesc;
            SystemCatalog::getInstance()->getArrayDesc(addr.arrId, newVersionDesc);
            string const& dstName = newVersionDesc.getName();
            size_t at = dstName.find('@');
            assert(at != string::npos);
            VersionID dstVersion = atol(&dstName[at+1]);
            assert(dstVersion > 1);

            ArrayDesc currVersionDesc;
            std::stringstream ss;
            ss << dstName.substr(0, at+1) << (dstVersion-1);
            SystemCatalog::getInstance()->getArrayDesc(ss.str(), currVersionDesc);

            Address currVersionAddress = addr;
            currVersionAddress.arrId = currVersionDesc.getId();

            DBChunk* dstChunk = CachedStorage::lookupChunk(currVersionAddress, false);
            if (dstChunk != NULL) {
                if (srcChunk.getDiskChunk() == dstChunk) {
                    // Original chunk was not changed: just clone it
                    newClone = cloneChunk(addr, *dstChunk);
                    replicate(addr, *(DBChunk*)newClone, NULL, 0, 0, query, replicasVec);
                    dstChunk->unPin();
                    goto Epilogue;
                }
                MemChunk tmpChunk;
                tmpChunk.initialize(&dstChunk->getArray(), &dstChunk->getArrayDesc(), currVersionAddress, dstChunk->getCompressionMethod());
                tmpChunk.setSparse(dstChunk->isSparse());
                tmpChunk.setRLE(dstChunk->isRLE());
                tmpChunk.allocate(dstChunk->getSize());
                memcpy(tmpChunk.getData(), dstChunk->getData(), tmpChunk.getSize());
                PinBuffer scope(srcChunk);
                if (VersionControl::instance != NULL
                    && VersionControl::instance->newVersion(tmpChunk, srcChunk, dstVersion, dstChunk->isDelta()))
                {
                    size_t newSize = tmpChunk.getSize();
                    boost::scoped_array<char> buf(new char[newSize]);
                    if (!buf) {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_ALLOCATE_MEMORY);
                    }
                    currentStatistics->allocatedSize += newSize;
                    currentStatistics->allocatedChunks++;
                    size_t compressedSize = compressors[chooseCompressionMethod(*dstChunk, buf.get())]->compress(buf.get(), tmpChunk);
                    assert(compressedSize <= newSize);
                    if (compressedSize <= dstChunk->hdr.allocatedSize) {
                        ScopedMutexLock cs(mutex);
                        void* deflated = (compressedSize == newSize) ? tmpChunk.getData() : buf.get();

                        // Update all cloned chunk headers
                        DBChunk* clone = dstChunk;
                        ChunkHeader oldHdr;
                        while (true) {
                            RWLock::ErrorChecker noopEc;
                            ScopedRWLockWrite cloneWriter(getChunkLatch(clone), noopEc);
                            oldHdr = clone->hdr;
                            if (clone->data != NULL) {
                                clone->allocate(newSize);
                                memcpy(clone->data, tmpChunk.getData(), newSize);
                            }
                            clone->hdr.compressedSize = compressedSize;
                            clone->hdr.size = newSize;
                            clone->hdr.flags |= ChunkHeader::DELTA_CHUNK;
                            clone->hdr.flags &= ~ChunkHeader::RLE_CHUNK;
                            if (clone->cloneOf == NULL) {
                                break;
                            } else {
                                clone = clone->cloneOf;
                            }
                        }
                        ChunkDescriptor cloneDesc;
                        newClone = cloneChunk(addr, *dstChunk, cloneDesc);

                        // Write ahead UNDO log
                        size_t oldChunkSize = oldHdr.compressedSize;
                        boost::scoped_array<char> transLogRecordBuf(new char[sizeof(TransLogRecord)*2 + oldChunkSize]);
                        TransLogRecord* transLogRecord = (TransLogRecord*)transLogRecordBuf.get();
                        ArrayDesc arrayDesc;
                        SystemCatalog::getInstance()->getArrayDesc(dstName.substr(0, at), arrayDesc);
                        transLogRecord->array = arrayDesc.getId();
                        transLogRecord->versionArrayID = addr.arrId;
                        transLogRecord->version = dstVersion;
                        transLogRecord->hdr = oldHdr;
                        transLogRecord->newHdrPos = cloneDesc.hdr.pos.hdrPos;
                        transLogRecord->oldSize = oldChunkSize;
                        transLogRecord->hdrCRC = calculateCRC32(transLogRecord, sizeof(TransLogRecordHeader));
                        readAll(dstChunk->hdr.pos, transLogRecord+1, oldChunkSize);
                        memset((char*)(transLogRecord+1) + oldChunkSize, 0, sizeof(TransLogRecord)); // end of log marker
                        transLogRecord->bodyCRC = calculateCRC32(transLogRecord+1, oldChunkSize);
                        if (logSize + sizeof(TransLogRecord) + oldChunkSize > logSizeLimit) {
                            logSize = 0;
                            currLog ^= 1;
                        }
                        LOG4CXX_TRACE(logger, "ChunkDesc: Write in log chunk " << transLogRecord->hdr.pos.offs
                                      << " size " << oldChunkSize << " at position " << logSize);

                        File::writeAll(log[currLog], transLogRecord, sizeof(TransLogRecord)*2 + oldChunkSize, logSize);
                        logSize += sizeof(TransLogRecord) + oldChunkSize;
                        transLogRecordBuf.reset();

                        // Save new and updated chunk headers to the disk
                        assert(cloneDesc.hdr.pos.hdrPos != 0);
                        if (logger->isTraceEnabled()) {
                            LOG4CXX_TRACE(logger, "ChunkDesc: Create new chunk descriptor clone with size "
                                          << sizeof(ChunkDescriptor) << " at position " << cloneDesc.hdr.pos.hdrPos);
                            LOG4CXX_TRACE(logger, "Clone chunk descriptor to write: " << cloneDesc.toString());
                        }
                        File::writeAll(hd, &cloneDesc, sizeof(ChunkDescriptor), cloneDesc.hdr.pos.hdrPos);
                        assert(clone->hdr.pos.hdrPos != 0);
                        LOG4CXX_TRACE(logger, "ChunkDesc: Update cloned chunk descriptor at position " << clone->hdr.pos.hdrPos);
                        File::writeAll(hd, &clone->hdr, sizeof(ChunkHeader), clone->hdr.pos.hdrPos);

                        replicate(addr, *dstChunk, deflated, compressedSize, newSize, query, replicasVec);

                        // Write chunk data
                        writeAll(dstChunk->hdr.pos, deflated, compressedSize);
                        buf.reset();

                        dstChunk->unPin();
                    }
                    if (!newClone) { buf.reset(); }
                }
                if (!newClone) { dstChunk->unPin(); }
            }
        }
      Epilogue:
        if (newClone) {
            waitForReplicas(replicasVec);
            replicasCleaner.disarm();
        }
        return newClone;
    }

    Chunk* LocalStorage::cloneChunk(const Address& addr, DBChunk const& srcChunk, boost::shared_ptr<Query>& query)
    {
        vector<boost::shared_ptr<ReplicationManager::Item> > replicasVec;
        boost::function<void()> f = boost::bind(&LocalStorage::abortReplicas, this, &replicasVec); 
        Destructor replicasCleaner(f);
        Chunk* clone = cloneChunk(addr, srcChunk);
        replicate(addr, *(DBChunk*)clone, NULL, 0, 0, query, replicasVec);
        waitForReplicas(replicasVec);
        replicasCleaner.disarm();
        return clone;
    }

    Chunk* LocalStorage::cloneChunk(const Address& addr, DBChunk const& srcChunk)
    {
        ChunkDescriptor cloneDesc;
        Chunk* clone = cloneChunk(addr, srcChunk, cloneDesc);
        assert(cloneDesc.hdr.pos.hdrPos != 0);
        LOG4CXX_TRACE(logger, "ChunkDesc: Create new chunk descriptor clone with size " << sizeof(ChunkDescriptor) << " at position " << cloneDesc.hdr.pos.hdrPos);
        File::writeAll(hd, &cloneDesc, sizeof(ChunkDescriptor), cloneDesc.hdr.pos.hdrPos);
        return clone;
    }

    Chunk* LocalStorage::cloneChunk(const Address& addr, DBChunk const& srcChunk, ChunkDescriptor& cloneDesc)
    {
        ScopedMutexLock cs(mutex);
        DBChunk& chunk = *(DBChunk*)newChunk(addr, srcChunk.getCompressionMethod());
        int nCoordinates = addr.coords.size();
        chunk.accessCount -= 1; // newly created chunks has accessCount == 1
        chunk.raw = false;

        // Write chunk descriptor in storage header
        chunk.hdr = srcChunk.hdr;
        chunk.hdr.arrId = addr.arrId;
        chunk.hdr.attId = addr.attId;
        if (freeHeaders.empty()) {
            chunk.hdr.pos.hdrPos = hdr.currPos;
            hdr.currPos += sizeof(ChunkDescriptor);
            hdr.nChunks += 1;
            // Update storage header
            File::writeAll(hd, &hdr, HEADER_SIZE, 0);
        } else  {
            set<uint64_t>::iterator i = freeHeaders.begin();            
            chunk.hdr.pos.hdrPos = *i;
            assert(chunk.hdr.pos.hdrPos != 0);
            freeHeaders.erase(i);
       }
        if (hdr.clusterSize != 0) { 
            liveChunksInCluster[getClusterID(chunk.hdr.pos)] += 1;
        }

        chunk.cloneOf = (DBChunk*)&srcChunk;
        chunk.cloneOf->clones.push_back(&chunk);
        cloneDesc.hdr = chunk.hdr;
        for (int i = 0; i < nCoordinates; i++) {
            cloneDesc.coords[i] = addr.coords[i];
        }

        return &chunk;
    }

    inline char* strtrim(char* buf)
    {
        char* p = buf;
        char ch;
        while ((unsigned char)(ch = *p) <= ' ' && ch != '\0') {
            p += 1;
        }
        char* q = p + strlen(p);
        while (q > p && (unsigned char)q[-1] <= ' ') {
            q -= 1;
        }
        *q = '\0';
        return p;
    }

    inline string relativePath(const string& dir, const string& file)
    {
        return file[0] == '/' ? file : dir + file;
    }

    LocalStorage::LocalStorage() : _replicationManager(NULL)
    {
       hd = -1;
       for (size_t i=0; i < MAX_SEGMENTS; ++i) {
          sd[i] = -1;
       }
       log[0] = log[1] = -1;
    }

    void LocalStorage::open(const string& storageDescriptorFilePath, size_t cacheSize)
    {
        StatisticsScope sScope;
        InjectedErrorListener<WriteChunkInjectedError>::start();
        char buf[MAX_CFG_LINE_LENGTH];
        char const* descPath = storageDescriptorFilePath.c_str();
        string databasePath = "";
        string databaseHeader;
        string databaseLog;
        uint64_t transLogLimit;
        vector<Segment> segments;
        size_t pathEnd = storageDescriptorFilePath.find_last_of('/');
        if (pathEnd != string::npos) {
            databasePath = storageDescriptorFilePath.substr(0, pathEnd+1);
        }
        FILE* f = fopen(descPath, "r");
        if (f == NULL) {
            f = fopen(descPath, "w");
            if (!f)
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) << descPath << errno;
            size_t fileNameBeg = (pathEnd == string::npos) ? 0 : pathEnd+1;
            size_t fileNameEnd = storageDescriptorFilePath.find_last_of('.');
            if (fileNameEnd == string::npos || fileNameEnd < fileNameBeg) {
                fileNameEnd = storageDescriptorFilePath.size();
            }
            string databaseName = storageDescriptorFilePath.substr(fileNameBeg, fileNameEnd - fileNameBeg);
            databaseHeader = databasePath + databaseName + ".header";
            databaseLog = databasePath + databaseName + ".log";
            segments.push_back(Segment(databasePath + databaseName + ".data1", (uint64_t)DEFAULT_SEGMENT_SIZE*MB));
            fprintf(f, "%s.header\n", databaseName.c_str());
            fprintf(f, "%ld %s.log\n", (long)DEFAULT_TRANS_LOG_LIMIT, databaseName.c_str());
            fprintf(f, "%ld %s.data1\n", (long)DEFAULT_SEGMENT_SIZE, databaseName.c_str());
            transLogLimit = (uint64_t)DEFAULT_TRANS_LOG_LIMIT*MB;
        } else {
            int pos;
            long sizeMb;
            if (!fgets(buf, sizeof buf, f))
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT);
            databaseHeader = relativePath(databasePath, strtrim(buf));
            if (!fgets(buf, sizeof buf, f))
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT);
            if (sscanf(buf, "%ld%n", &sizeMb, &pos) != 1)
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT);
            databaseLog = relativePath(databasePath, strtrim(buf+pos));
            transLogLimit = (uint64_t)sizeMb*MB;
            while (fgets(buf, sizeof buf, f)) {
                if (sscanf(buf, "%ld%n", &sizeMb, &pos) != 1)
                    throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT);
                segments.push_back(Segment(relativePath(databasePath, strtrim(buf+pos)), (uint64_t)sizeMb*MB));
            }
        }
        fclose(f);
        open(databaseHeader, databaseLog, transLogLimit, segments, cacheSize, CompressorFactory::getInstance().getCompressors());

        _replicationManager = ReplicationManager::getInstance();
        assert(_replicationManager);
        assert(_replicationManager->isStarted());
    }

    void LocalStorage::rollback(std::map<ArrayID,VersionID> const& undoUpdates)
    {
        ScopedMutexLock cs(mutex);
        for (int i = 0; i < 2; i++) {
            uint64_t pos = 0;
            TransLogRecord transLogRecord;
            while (true) {
                size_t rc = ::pread(log[i], &transLogRecord, sizeof(TransLogRecord), pos);
                if (rc != sizeof(TransLogRecord) || transLogRecord.array == 0) { 
                    LOG4CXX_DEBUG(logger, "End of log at position " << pos << " rc=" << rc);
                    break;
                }
                uint32_t crc = calculateCRC32(&transLogRecord, sizeof(TransLogRecordHeader));
                if (crc != transLogRecord.hdrCRC)
                {
                    LOG4CXX_ERROR(logger, "CRC doesn't match for log record: " << crc << " vs. expected " << transLogRecord.hdrCRC);
                    break;
                }
                pos += sizeof(TransLogRecord);
                std::map<ArrayID,VersionID>::const_iterator it = undoUpdates.find(transLogRecord.array);
                VersionID lastVersionID = -1;
                if (it != undoUpdates.end() && (lastVersionID = it->second) < transLogRecord.version) {
                    if (transLogRecord.oldSize != 0) {
                        if (chunkMap.size() != 0) {
                            Address addr;
                            addr.arrId = transLogRecord.versionArrayID;
                            addr.attId = transLogRecord.hdr.attId;
                            addr.coords.resize(transLogRecord.hdr.nCoordinates);
                            File::readAll(hd, &addr.coords[0], transLogRecord.hdr.nCoordinates*sizeof(Coordinate), transLogRecord.hdr.pos.hdrPos + sizeof(ChunkHeader));
                            map<Address, DBChunk, AddressLess>::iterator it = chunkMap.find(addr);
                            if (it != chunkMap.end()) {
                                DBChunk* clone = &it->second;
                                while (true) {
                                    RWLock::ErrorChecker noopEc;
                                    ScopedRWLockWrite cloneWriter(getChunkLatch(clone), noopEc);
                                    clone->hdr.compressedSize = transLogRecord.hdr.compressedSize;
                                    clone->hdr.size = transLogRecord.hdr.size;
                                    clone->hdr.flags = transLogRecord.hdr.flags;
                                    if (clone->data != NULL) { 
                                        freeChunk(*clone);
                                    }
                                    if (clone->cloneOf == NULL) {
                                        break;
                                    } else {
                                        clone = clone->cloneOf;
                                    }
                                }
                            }
                        }
                        LOG4CXX_DEBUG(logger, "Restore chunk " << transLogRecord.hdr.pos.offs
                                      << " size " << transLogRecord.oldSize << " from position " << (pos-sizeof(TransLogRecord)));
                        boost::scoped_array<char> buf(new char[transLogRecord.oldSize]);
                        File::readAll(log[i], buf.get(), transLogRecord.oldSize, pos);
                        crc = calculateCRC32(buf.get(), transLogRecord.oldSize);
                        if (crc != transLogRecord.bodyCRC) {
                            LOG4CXX_ERROR(logger, "CRC for restored chunk doesn't match at position " << pos << ": " << crc << " vs. expected " << transLogRecord.bodyCRC);
                            break;
                        }
                        writeAll(transLogRecord.hdr.pos, buf.get(), transLogRecord.oldSize);
                        buf.reset();
                        // restore first chunk in clones chain
                        assert(transLogRecord.hdr.pos.hdrPos != 0);
                        LOG4CXX_TRACE(logger, "ChunkDesc: Restore chunk descriptor at position " << transLogRecord.hdr.pos.hdrPos);
                        File::writeAll(hd, &transLogRecord.hdr, sizeof(ChunkHeader), transLogRecord.hdr.pos.hdrPos);
                        transLogRecord.hdr.pos.hdrPos = transLogRecord.newHdrPos;
                    }
                    transLogRecord.hdr.arrId = 0; // mark chunk as free
                    assert(transLogRecord.hdr.pos.hdrPos != 0);
                    LOG4CXX_TRACE(logger, "ChunkDesc: Undo chunk descriptor creation at position " << transLogRecord.hdr.pos.hdrPos);
                    File::writeAll(hd, &transLogRecord.hdr, sizeof(ChunkHeader), transLogRecord.hdr.pos.hdrPos);
                }
                pos += transLogRecord.oldSize;
            }
        }
        flush();
    }


    static void collectArraysToRollback( boost::shared_ptr< std::map<ArrayID,VersionID> >& arrsToRollback,
                                         const VersionID& lastVersion,
                                         const ArrayID& baseArrayId,
                                         const ArrayID& newArrayId)
    {
        assert(arrsToRollback);
        assert(baseArrayId>0);
        (*arrsToRollback.get())[baseArrayId] = lastVersion;
    }
    
    void LocalStorage::doTxnRecoveryOnStartup()
    {
        list<shared_ptr<SystemCatalog::LockDesc> > coordLocks;
        list<shared_ptr<SystemCatalog::LockDesc> > workerLocks;

        SystemCatalog::getInstance()->readArrayLocks(getInstanceId(), coordLocks, workerLocks);

        shared_ptr<map<ArrayID,VersionID> > arraysToRollback(new map<ArrayID,VersionID>());
        UpdateErrorHandler::RollbackWork collector = bind(&collectArraysToRollback, arraysToRollback, _1, _2, _3);

        // Deal with the  SystemCatalog::LockDesc::COORD type locks first
        for (list<shared_ptr<SystemCatalog::LockDesc> >::const_iterator iter = coordLocks.begin();
             iter != coordLocks.end(); ++iter) {

            const shared_ptr<SystemCatalog::LockDesc>& lock = *iter;

            if (lock->getLockMode() == SystemCatalog::LockDesc::RM) {
                const bool checkLock = false;
                RemoveErrorHandler::handleRemoveLock(lock, checkLock);
            } else if (lock->getLockMode() == SystemCatalog::LockDesc::CRT ||
                       lock->getLockMode() == SystemCatalog::LockDesc::WR) {
                UpdateErrorHandler::handleErrorOnCoordinator(lock, collector);
            } else {
                assert(lock->getLockMode() == SystemCatalog::LockDesc::RNF ||
                       lock->getLockMode() == SystemCatalog::LockDesc::RD);
            }
        }
        for (list<shared_ptr<SystemCatalog::LockDesc> >::const_iterator iter = workerLocks.begin();
             iter != workerLocks.end(); ++iter) {
            
            const shared_ptr<SystemCatalog::LockDesc>& lock = *iter;

            if (lock->getLockMode() == SystemCatalog::LockDesc::CRT ||
                lock->getLockMode() == SystemCatalog::LockDesc::WR) {
                const bool checkCoordinatorLock = true;
                UpdateErrorHandler::handleErrorOnWorker(lock, checkCoordinatorLock, collector);
            } else {
                assert(lock->getLockMode() == SystemCatalog::LockDesc::RNF);
            }
        }
        rollback(*arraysToRollback.get());
        SystemCatalog::getInstance()->deleteArrayLocks(getInstanceId());
    }
    
    void LocalStorage::open(const string& headerPath, const string& transactionLogPath,
                            uint64_t transLogLimit, vector<Segment> const& segmentList,
                            size_t cacheSize, const vector<Compressor*>& compressors)
    {
        CachedStorage::open(cacheSize, compressors);
        segments = segmentList;

        int flags = O_LARGEFILE|O_RDWR|O_CREAT;
        hd = ::open(headerPath.c_str(), flags, 0777);
        if (hd < 0)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) << headerPath << errno;

        struct flock flc;
        flc.l_type = F_WRLCK;
        flc.l_whence = SEEK_SET;
        flc.l_start = 0;
        flc.l_len = 1;

        if (fcntl(hd, F_SETLK, &flc))
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_LOCK_DATABASE);

        log[0] = ::open((transactionLogPath + "_1").c_str(), O_LARGEFILE|O_SYNC|O_RDWR|O_CREAT, 0777);
        if (log[0] < 0)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) << (transactionLogPath + "_1") << errno;

        log[1] = ::open((transactionLogPath + "_2").c_str(), O_LARGEFILE|O_SYNC|O_RDWR|O_CREAT, 0777);
        if (log[1] < 0)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) << (transactionLogPath + "_2") << errno;

        logSizeLimit = transLogLimit;
        logSize = 0;
        currLog = 0;

        size_t rc = read(hd, &hdr, sizeof(hdr));
        if (rc != 0 && rc != sizeof(hdr))
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_OPERATION_FAILED) << "read";

        size_t nSegments = segments.size(); 

        clusters.clear();
        liveChunksInCluster.clear();
        freeClusters.resize(nSegments);

        _writeLogThreshold = Config::getInstance()->getOption<int>(CONFIG_IO_LOG_THRESHOLD);

        // Open segments and calculate total amount of available space in the database
        long available = 0;
        for (size_t i = 0; i < nSegments; i++) {
            sd[i] = ::open(segments[i].path.c_str(), flags, 0777);
            if (sd[i] < 0)
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) << segments[i].path << errno;

            if (segments[i].size < hdr.segment[i].used) {
                segments[i].size = hdr.segment[i].used;
            }
            available += (long)((segments[i].size - hdr.segment[i].used)/GB);
        }

        totalAvailable = available;
        readAheadSize = Config::getInstance()->getOption<int>(CONFIG_READ_AHEAD_SIZE);

        nInstances = SystemCatalog::getInstance()->getNumberOfInstances();
        redundancy = 0; // disable replication during rollback: each instance is perfroming rollback locally

        if (rc == 0 || hdr.currPos < HEADER_SIZE) {
            // Database is not initialized
            memset(&hdr, 0, sizeof(hdr));
            hdr.magic = SCIDB_STORAGE_HEADER_MAGIC;
            hdr.version = SCIDB_STORAGE_FORMAT_VERSION;
            hdr.currPos = HEADER_SIZE;
            hdr.instanceId = INVALID_INSTANCE;
            hdr.nChunks = 0;
            hdr.clusterSize = Config::getInstance()->getOption<int>(CONFIG_CHUNK_CLUSTER_SIZE);
        } else {
            if (hdr.magic != SCIDB_STORAGE_HEADER_MAGIC) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_INVALID_STORAGE_HEADER);
            }
            if (hdr.version != SCIDB_STORAGE_FORMAT_VERSION) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_STORAGE_FORMAT_VERSION) << hdr.version << SCIDB_STORAGE_FORMAT_VERSION;
            }

            doTxnRecoveryOnStartup();

            // Database is initialized: read information about all locally available chunks in map
            redundancy = Config::getInstance()->getOption<int>(CONFIG_REDUNDANCY);
            syncReplication = !Config::getInstance()->getOption<bool>(CONFIG_ASYNC_REPLICATION);

            ChunkDescriptor desc;
            uint64_t chunkPos = HEADER_SIZE;
            Address addr;
            map<uint64_t, DBChunk*> clones;
            set<ArrayID> removedArrays;
            for (size_t i = 0; i < hdr.nChunks; i++, chunkPos += sizeof(ChunkDescriptor)) { 
                size_t rc = pread(hd, &desc, sizeof(ChunkDescriptor), chunkPos);
                if (rc != sizeof(ChunkDescriptor)) { 
                    LOG4CXX_ERROR(logger, "Inconsistency in storage header: rc=" << rc << ", chunkPos=" << chunkPos << ", i=" << i << ", hdr.nChunks=" << hdr.nChunks << ", hdr.currPos=" << hdr.currPos);
                    hdr.currPos = chunkPos;
                    hdr.nChunks = i;
                    break;
                }
                if (desc.hdr.pos.hdrPos != chunkPos) { 
                    LOG4CXX_ERROR(logger, "Invalid chunk header " << i << " at position " << chunkPos << " desc.hdr.pos.hdrPos=" << desc.hdr.pos.hdrPos << " arrayID=" << desc.hdr.arrId << " hdr.nChunks=" << hdr.nChunks);
                    freeHeaders.insert(chunkPos);
                } else { 
                    assert(desc.hdr.nCoordinates < MAX_COORDINATES);
                    if (desc.hdr.arrId != 0) {
                        try {
                            if (removedArrays.count(desc.hdr.arrId) != 0) {
                                desc.hdr.arrId = 0;
                                LOG4CXX_TRACE(logger, "ChunkDesc: Remove chunk descriptor for unexisted array at position " << chunkPos);
                                File::writeAll(hd, &desc.hdr, sizeof(ChunkHeader), chunkPos);
                                assert(desc.hdr.nCoordinates < MAX_COORDINATES);
                                freeHeaders.insert(chunkPos);
                                continue;
                            }
                            desc.getAddress(addr);
                            DBChunk& chunk = chunkMap[addr];
                            chunk.setAddress(desc);
                            if (hdr.clusterSize != 0) { 
                                liveChunksInCluster[getClusterID(chunk.hdr.pos)] += 1;
                            }
                            DBChunk*& clone = clones[chunk.hdr.pos.offs * MAX_SEGMENTS + chunk.hdr.pos.segmentNo];
                            chunk.cloneOf = clone;
                            if (clone != NULL) {
                                clone->clones.push_back(&chunk);
                                chunk.hdr.compressedSize = clone->hdr.compressedSize;
                                chunk.hdr.size = clone->hdr.size;
                                chunk.hdr.flags = clone->hdr.flags;
                            }
                            clone = &chunk;
                        } catch (SystemException const& x) {
                            if (x.getLongErrorCode() == SCIDB_LE_ARRAYID_DOESNT_EXIST) {
                                removedArrays.insert(desc.hdr.arrId);
                                desc.hdr.arrId = 0;
                                LOG4CXX_TRACE(logger, "ChunkDesc: Remove chunk descriptor for unexisted array at position " << chunkPos);
                                File::writeAll(hd, &desc.hdr, sizeof(ChunkHeader), chunkPos);
                                freeHeaders.insert(chunkPos);                            
                            } else {
                                throw;
                            }
                        }
                    } else {
                        freeHeaders.insert(chunkPos);
                    }
                }
            }
            if (chunkPos != hdr.currPos) {
                LOG4CXX_ERROR(logger, "Storage header is not consistent: " << chunkPos << " vs. " << hdr.currPos);
                // throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_DATABASE_HEADER_CORRUPTED);
                if (chunkPos > hdr.currPos) {
                    hdr.currPos = chunkPos;
                }
            }
            
            if (hdr.clusterSize != 0) { 
                for (size_t i = 0; i < nSegments; i++) {
                    for (uint64_t offs = 0; offs < hdr.segment[i].used; offs += hdr.clusterSize) {
                        if (liveChunksInCluster[getClusterID(i, offs)] == 0) { 
                            freeClusters[i].insert(offs);
                        }
                    }
                }
            }
        }

        int syncMSeconds = Config::getInstance()->getOption<int>(CONFIG_SYNC_IO_INTERVAL);
        if (syncMSeconds >= 0)
        {
            vector<int> fds(0);
            for (size_t i=0; i<segments.size(); i++)
            {
                fds.push_back(sd[i]);
            }

            BackgroundFileFlusher::getInstance()->start(syncMSeconds, _writeLogThreshold, fds);
        }
    }

    void LocalStorage::close()
    {
        InjectedErrorListener<WriteChunkInjectedError>::stop();
        BackgroundFileFlusher::getInstance()->stop();
        CachedStorage::close();
        std::ostringstream ss;
        
        if (::close(hd) != 0) {
            LOG4CXX_ERROR(logger, "Failed to close header file");
        }
        if (::close(log[0]) != 0) {
            LOG4CXX_ERROR(logger, "Failed to close transaction log file");
        }
        if (::close(log[1]) != 0) {
            LOG4CXX_ERROR(logger, "Failed to close transaction log file");
        }
        for (size_t i = 0, nSegments = segments.size(); i < nSegments; i++) {
           if (::close(sd[i]) != 0) {
               LOG4CXX_ERROR(logger, "Failed to close data file");
           }
        }
        if (!ss.str().empty()) {
            LOG4CXX_ERROR(logger, ss.str());
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_CLOSE_FAILED);
        }
    }

    void LocalStorage::flush()
    {
        if (::fsync(hd))
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_OPERATION_FAILED) << "fsync";
        for (size_t i = 0, nSegments = segments.size(); i < nSegments; i++) {
            if (::fsync(sd[i]))
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_OPERATION_FAILED) << "fsync";
        }
    }

    boost::shared_ptr<ArrayIterator>
    LocalStorage::getArrayIterator(const ArrayDesc& arrDesc,
                                   AttributeID attId,
                                   boost::shared_ptr<Query>& query)
    {
       return boost::shared_ptr<ArrayIterator>(new DBArrayIterator(this, arrDesc, attId, query));
    }

    void LocalStorage::fetchChunk(DBChunk& chunk)
    {
        ChunkInitializer guard(this, chunk);
        if (chunk.hdr.pos.hdrPos == 0) { 
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_ACCESS_TO_RAW_CHUNK) << chunk.getArrayDesc().getName();
        }
        size_t chunkSize = chunk.getSize();
        chunk.allocate(chunkSize);
        if (chunk.getCompressedSize() != chunkSize) {
            const size_t bufSize = chunk.getCompressedSize();
            boost::scoped_array<char> buf(new char[bufSize]);
            currentStatistics->allocatedSize += bufSize;
            currentStatistics->allocatedChunks++;
            if (!buf)
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_ALLOCATE_MEMORY);
            readAll(chunk.hdr.pos, buf.get(), chunk.getCompressedSize());
            chunk.loader = pthread_self();
            size_t rc = compressors[chunk.hdr.compressionMethod]->decompress(buf.get(), chunk.getCompressedSize(), chunk);
            chunk.loader = 0;
            if (rc != chunk.getSize())
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_DECOMPRESS_CHUNK);
            buf.reset();
        } else {
            readAll(chunk.hdr.pos, chunk.data, chunkSize);
        }
    }

    void LocalStorage::loadChunk(Chunk* aChunk)
    {
        CachedStorage::loadChunk(aChunk);
        DBChunk& chunk = *(DBChunk*)aChunk;
        if (chunk.raw && chunk.loader != pthread_self()) {
            fetchChunk(chunk);
            size_t clusterSize = chunk.hdr.compressedSize;
            if (clusterSize < readAheadSize) {
                DiskPos const& pos = chunk.hdr.pos;
                Address const* addr = &chunk.addr;
                while (true) 
                {
                    DBChunk* preloadChunk = NULL;
                    { 
                        ScopedMutexLock cs(mutex);
                        map<Address, DBChunk, AddressLess>::iterator i = chunkMap.upper_bound(*addr); 
                        if (i != chunkMap.end()) {
                            preloadChunk = &i->second;
                            if (preloadChunk->hdr.arrId != chunk.addr.arrId || preloadChunk->hdr.attId != chunk.addr.attId) {
                                // no more chunks of this array in the local storage
                                break;
                            }
                            if (!preloadChunk->raw && preloadChunk->hdr.pos.segmentNo == pos.segmentNo && preloadChunk->hdr.instanceId == hdr.instanceId) {
                                if (preloadChunk->hdr.pos.offs == pos.offs + clusterSize) {
                                    if (preloadChunk->data == NULL && (clusterSize += preloadChunk->hdr.compressedSize) <= readAheadSize) {
                                        preloadChunk->raw = true;
                                        preloadChunk->beginAccess();
                                        addChunkToCache(*preloadChunk);
                                    } else {
                                        // cluster size limit is reached ot chunk is alreader loaded
                                        break;
                                    }
                                } else {
                                    // subsequent chunk in this disk partition belongs to some other array
                                    break;
                                }
                            } else {  
                                addr = &preloadChunk->addr;
                                continue;
                            }
                        } else { 
                            break;
                        }
                    }
                    fetchChunk(*preloadChunk);
                    preloadChunk->unPin();
                    addr = &preloadChunk->addr;
                }
            }
        }
    }

    Chunk* LocalStorage::readChunk(const Address& addr)
    {
        DBChunk* chunk = CachedStorage::lookupChunk(addr, false);
        if (!chunk)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_NOT_FOUND);
        loadChunk(chunk);
        return chunk;
    }


    void LocalStorage::compressChunk(Chunk const* aChunk, CompressedBuffer& buf)
    {
        CachedStorage::compressChunk(aChunk, buf);
        if (buf.getData() == NULL) { // chunk data is not present in the cache so read compressed data from the disk
            DBChunk& chunk = *(DBChunk*)aChunk;
            if (chunk.hdr.pos.hdrPos == 0) { 
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_ACCESS_TO_RAW_CHUNK) << chunk.getArrayDesc().getName();
            }
            buf.allocate(chunk.getCompressedSize());
            readAll(chunk.hdr.pos, buf.getData(), chunk.getCompressedSize());
        }
    }

    void LocalStorage::writeChunk(Chunk* newChunk, boost::shared_ptr<Query>& query)
    {
        //XXX TODO: consider locking mutex here to avoid writing replica chunks for a rolled-back query
        Query::validateQueryPtr(query);
        DBChunk& chunk = *(DBChunk*)newChunk;

        const size_t bufSize = chunk.getSize();
        boost::scoped_array<char> buf(new char[bufSize]);
        currentStatistics->allocatedSize += bufSize;
        currentStatistics->allocatedChunks++;

        ArrayDesc versionDesc;
        SystemCatalog::getInstance()->getArrayDesc(chunk.addr.arrId, versionDesc);
        string const& versionName = versionDesc.getName();
        size_t at = versionName.find('@');
        VersionID dstVersion = 0;
        if (at != string::npos && versionName.find(':') == string::npos) {
            dstVersion = atol(&versionName[at+1]);
        }

        if (!buf)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_ALLOCATE_MEMORY);
        void const* deflated = buf.get();
        int nCoordinates = chunk.addr.coords.size();
        size_t compressedSize = compressors[chooseCompressionMethod(chunk, buf.get())]->compress(buf.get(), chunk);
        assert(compressedSize <= chunk.getSize());
        if (compressedSize == chunk.getSize()) { // no compression
            deflated = chunk.data;
        }
        vector<boost::shared_ptr<ReplicationManager::Item> > replicasVec;
        boost::function<void()> f = boost::bind(&LocalStorage::abortReplicas, this, &replicasVec); 
        Destructor replicasCleaner(f);
        replicate(chunk.addr, chunk, deflated, compressedSize, chunk.getSize(), query, replicasVec);

        {
            ScopedMutexLock cs(mutex);
            assert(chunk.isRaw()); // new chunk is raw
            Query::validateQueryPtr(query);

            chunk.hdr.compressedSize = compressedSize;
            size_t reserve = chunk.getAttributeDesc().getReserve();
            chunk.hdr.allocatedSize = (reserve != 0 && dstVersion > 1) ? compressedSize + compressedSize*reserve/100 : compressedSize;

            if (hdr.clusterSize == 0) { // old no-cluster mode
                chunk.hdr.pos.segmentNo = getRandomSegment();
                if (hdr.segment[chunk.hdr.pos.segmentNo].used + chunk.hdr.allocatedSize > segments[chunk.hdr.pos.segmentNo].size) {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NO_FREE_SPACE);
                }
                chunk.hdr.pos.offs = hdr.segment[chunk.hdr.pos.segmentNo].used;
                hdr.segment[chunk.hdr.pos.segmentNo].used += chunk.hdr.allocatedSize;
            } else {
                if (chunk.hdr.allocatedSize > hdr.clusterSize) {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_SIZE_TOO_LARGE) << chunk.hdr.allocatedSize << hdr.clusterSize;
                }
                // Choose location of the chunk
                Cluster& cluster = clusters[chunk.addr.arrId];
                if (cluster.used == 0 || cluster.used + chunk.hdr.allocatedSize > hdr.clusterSize) {
                    cluster.pos.segmentNo = getRandomSegment();
                    cluster.used = 0;
                    if (!freeClusters[cluster.pos.segmentNo].empty()) {
                        set<uint64_t>::iterator i = freeClusters[cluster.pos.segmentNo].begin();
                        cluster.pos.offs = *i;
                        freeClusters[cluster.pos.segmentNo].erase(i);
                    } else {
                        if (hdr.segment[cluster.pos.segmentNo].used + hdr.clusterSize > segments[cluster.pos.segmentNo].size)
                        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NO_FREE_SPACE);
                        cluster.pos.offs = hdr.segment[cluster.pos.segmentNo].used;
                        hdr.segment[cluster.pos.segmentNo].used += hdr.clusterSize;
                        LOG4CXX_DEBUG(logger, "Allocate new chunk segment " << cluster.pos.offs);
                    }
                    assert(cluster.pos.offs % hdr.clusterSize == 0);
                }
                chunk.hdr.pos.segmentNo = cluster.pos.segmentNo;
                chunk.hdr.pos.offs = cluster.pos.offs + cluster.used;
                cluster.used += chunk.hdr.allocatedSize;
                liveChunksInCluster[getClusterID(chunk.hdr.pos)] += 1;
            }

            if (freeHeaders.empty()) {
                chunk.hdr.pos.hdrPos = hdr.currPos;
                hdr.currPos += sizeof(ChunkDescriptor);
                hdr.nChunks += 1;
            } else  {
                set<uint64_t>::iterator i = freeHeaders.begin();
                chunk.hdr.pos.hdrPos = *i;
                assert(chunk.hdr.pos.hdrPos != 0);
                freeHeaders.erase(i);
            }
            
            // Write ahead UNDO log
            if (dstVersion != 0) {
                TransLogRecord transLogRecord[2];
                ArrayDesc arrayDesc;
                SystemCatalog::getInstance()->getArrayDesc(versionName.substr(0, at), arrayDesc);
                transLogRecord->array = arrayDesc.getId();
                transLogRecord->versionArrayID = chunk.addr.arrId;
                transLogRecord->version = dstVersion;
                transLogRecord->hdr = chunk.hdr;
                transLogRecord->oldSize = 0;
                transLogRecord->hdrCRC = calculateCRC32(transLogRecord, sizeof(TransLogRecordHeader));
                memset(&transLogRecord[1], 0, sizeof(TransLogRecord)); // end of log marker
                
                if (logSize + sizeof(TransLogRecord) > logSizeLimit) {
                    logSize = 0;
                    currLog ^= 1;
                }
                LOG4CXX_TRACE(logger, "ChunkDesc: Write in log chunk header " << transLogRecord->hdr.pos.offs << " at position " << logSize);
                
                File::writeAll(log[currLog], transLogRecord, sizeof(TransLogRecord)*2, logSize);
                logSize += sizeof(TransLogRecord);
            }
            
            // Write chunk data
            writeAll(chunk.hdr.pos, deflated, compressedSize);
            buf.reset();

            if (--chunk.accessCount == 0) { // newly created chunks has accessCount == 1
                lru.link(&chunk);
            }
            // Write chunk descriptor in storage header
            ChunkDescriptor desc;
            desc.hdr = chunk.hdr;
            for (int i = 0; i < nCoordinates; i++) {
                desc.coords[i] = chunk.addr.coords[i];
            }
            assert(chunk.hdr.pos.hdrPos != 0);

            LOG4CXX_TRACE(logger, "ChunkDesc: Write chunk descriptor at position " << chunk.hdr.pos.hdrPos);
            if (logger->isTraceEnabled()) {
                LOG4CXX_TRACE(logger, "Chunk descriptor to write: " << desc.toString());
            }

            File::writeAll(hd, &desc, sizeof(ChunkDescriptor), chunk.hdr.pos.hdrPos);

            // Update storage header
            File::writeAll(hd, &hdr, HEADER_SIZE, 0);

            InjectedErrorListener<WriteChunkInjectedError>::check();

            notifyChunkReady(chunk);
            addChunkToCache(chunk);
        }
        waitForReplicas(replicasVec);
        replicasCleaner.disarm();
    }

    void LocalStorage::getDiskInfo(DiskInfo& info)
    {
        memset(&info, 0, sizeof info);
        info.clusterSize = hdr.clusterSize;
        info.nSegments = segments.size();
        for (size_t i = 0; i < segments.size(); i++) {
            info.used += hdr.segment[i].used;
            info.available += segments[i].size - hdr.segment[i].used;
            info.nFreeClusters += freeClusters[i].size();
        }
    }

    InstanceID LocalStorage::getInstanceId() const
    {
        return hdr.instanceId;
    }

    size_t LocalStorage::getNumberOfInstances() const
    {
        return nInstances;
    }

    void LocalStorage::setInstanceId(InstanceID id)
    {
        hdr.instanceId = id;
        File::writeAll(hd, &hdr, HEADER_SIZE, 0);
    }

    bool LocalStorage::isLocal()
    {
        return true;
    }

    //
    // DeltaChunk methods
    //

    LocalStorage::DeltaChunk::DeltaChunk(DBArrayIterator& iterator) 
    : arrayIterator(iterator), accessCount(0) { }

    void LocalStorage::DeltaChunk::setInputChunk(DBChunk* chunk, VersionID ver)
    {
        inputChunk = chunk;
        version = ver;
        extracted = false;
    }

    const Array& LocalStorage::DeltaChunk::getArray() const
    {
        return inputChunk->getArray();
    }

    const ArrayDesc& LocalStorage::DeltaChunk::getArrayDesc() const
    {
        return inputChunk->getArrayDesc();
    }

    const AttributeDesc& LocalStorage::DeltaChunk::getAttributeDesc() const
    {
        return inputChunk->getAttributeDesc();
    }

    int LocalStorage::DeltaChunk::getCompressionMethod() const
    {
        return inputChunk->getCompressionMethod();
    }

    bool LocalStorage::DeltaChunk::pin() const
    {
        ((DeltaChunk*)this)->accessCount += 1;
        return true;
    }

    void LocalStorage::DeltaChunk::unPin() const
    {
        DeltaChunk& self = *(DeltaChunk*)this;
        if (--self.accessCount == 0) { 
            self.versionChunk.free();
            self.extracted = false;
        }
    }

    Coordinates const& LocalStorage::DeltaChunk::getFirstPosition(bool withOverlap) const
    {
        return inputChunk->getFirstPosition(withOverlap);
    }

    Coordinates const& LocalStorage::DeltaChunk::getLastPosition(bool withOverlap) const
    {
        return inputChunk->getLastPosition(withOverlap);
    }

    boost::shared_ptr<ConstChunkIterator> LocalStorage::DeltaChunk::getConstIterator(int iterationMode) const
    {
        const AttributeDesc* bitmapAttr = arrayIterator.array.getEmptyBitmapAttribute();
        DeltaChunk* self = (DeltaChunk*)this;
        if (bitmapAttr != NULL && bitmapAttr->getId() != inputChunk->addr.attId 
            && (inputChunk->isRLE() || !(iterationMode & ConstChunkIterator::NO_EMPTY_CHECK)))
        {
            Address bitmapAddr(arrayIterator.addr.arrId, bitmapAttr->getId(), inputChunk->addr.coords);
            Chunk* bitmapChunk = arrayIterator.storage->readChunk(bitmapAddr);
            arrayIterator.deltaBitmapChunk.setInputChunk((DBChunk*)bitmapChunk, version);
            self->versionChunk.setBitmapChunk((Chunk*)&arrayIterator.deltaBitmapChunk);
            bitmapChunk->unPin();
        } else {
            self->versionChunk.setBitmapChunk(NULL);
        }
        extract();
        return versionChunk.getConstIterator(iterationMode);
    }

    DBChunk const* LocalStorage::DeltaChunk::getDiskChunk() const
    {
        return inputChunk;
    }

    bool LocalStorage::DeltaChunk::isMaterialized() const
    {
        return true;
    }

    bool LocalStorage::DeltaChunk::isSparse() const
    {
        extract();
        return versionChunk.isSparse();
    }

    bool LocalStorage::DeltaChunk::isRLE() const
    {
        extract();
        return versionChunk.isRLE();
    }
    
    boost::shared_ptr<ConstRLEEmptyBitmap> LocalStorage::DeltaChunk::getEmptyBitmap() const
    {
        const AttributeDesc* bitmapAttr = arrayIterator.array.getEmptyBitmapAttribute();
        boost::shared_ptr<ConstRLEEmptyBitmap> bitmap;
        if (bitmapAttr != NULL && bitmapAttr->getId() != inputChunk->addr.attId && inputChunk->isRLE()) {
            Address bitmapAddr(arrayIterator.addr.arrId, bitmapAttr->getId(), inputChunk->addr.coords);
            DBChunk* bitmapChunk = (DBChunk*)arrayIterator.storage->readChunk(bitmapAddr);

            RWLock::ErrorChecker noopEc;
            ScopedRWLockRead reader(bitmapChunk->getLatch(), noopEc);
            if (bitmapChunk->isDelta()) {
                MemChunk tmpChunk;
                tmpChunk.initialize(&bitmapChunk->getArray(), &bitmapChunk->getArrayDesc(), bitmapChunk->getAddress(), bitmapChunk->getCompressionMethod());
                VersionControl::instance->getVersion(tmpChunk, *bitmapChunk, version);
                bitmap = boost::shared_ptr<ConstRLEEmptyBitmap>(new RLEEmptyBitmap(ConstRLEEmptyBitmap((char*)tmpChunk.getData()))); 
            } else {
                bitmap = boost::shared_ptr<ConstRLEEmptyBitmap>(new RLEEmptyBitmap(ConstRLEEmptyBitmap((char*)bitmapChunk->getData())));
            }
            bitmapChunk->unPin();
        } else { 
            bitmap = ConstChunk::getEmptyBitmap();
        }
        return bitmap;
    }
        

    void* LocalStorage::DeltaChunk::getData() const
    {
        extract();
        return versionChunk.getData();
    }

    size_t LocalStorage::DeltaChunk::getSize() const
    {
        extract();
        return versionChunk.getSize();
    }

    void LocalStorage::DeltaChunk::compress(CompressedBuffer& buf, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const
    {
        extract();
        versionChunk.compress(buf, emptyBitmap);
    }

    void LocalStorage::DeltaChunk::extract() const
    {
        if (!extracted) {
            DeltaChunk* self = (DeltaChunk*)this;
            self->versionChunk.initialize(&inputChunk->getArray(), &inputChunk->getArrayDesc(), inputChunk->getAddress(), inputChunk->getCompressionMethod());
            RWLock::ErrorChecker noopEc;
            ScopedRWLockRead reader(inputChunk->getLatch(), noopEc);
            PinBuffer scope(*inputChunk);
            if (inputChunk->isDelta()) {
                VersionControl::instance->getVersion(self->versionChunk, *inputChunk, version);
            } else {
                self->versionChunk.setSparse(inputChunk->isSparse());
                self->versionChunk.setRLE(inputChunk->isRLE());
                self->versionChunk.allocate(inputChunk->getSize());
                memcpy(self->versionChunk.getData(), inputChunk->getData(), inputChunk->getSize());
            }
            assert(checkChunkMagic(versionChunk));
            self->extracted = true;
        }
    }

    LocalStorage::DBArrayIterator::DBArrayIterator(LocalStorage* storage, const ArrayDesc& arrDesc,
                                                   AttributeID attId, boost::shared_ptr<Query>& query)
    : array(arrDesc),
      deltaChunk(*this),
      deltaBitmapChunk(*this),
      attrDesc(array.getAttributes()[attId]),
      _query(query)
    {
        this->storage = storage;
        addr.arrId = arrDesc.getId();
        addr.attId = attId;
        currChunk = NULL;
        defaultCompressionMethod = attrDesc.getDefaultCompressionMethod();
        version = getVersionID(arrDesc, attId);
        positioned = false;
        ps = arrDesc.getPartitioningSchema();
    }

    LocalStorage::DBArrayIterator::~DBArrayIterator()
    {
#ifdef PIN_CURRENT_CHUNK
        if (currChunk != NULL) {
            currChunk->unPin();
        }
#endif
    }

    ConstChunk const& LocalStorage::DBArrayIterator::getChunk()
    {
        position();
        if (!currChunk)
            throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NO_CURRENT_CHUNK);
        if (version > 1) {
            deltaChunk.setInputChunk((DBChunk*)currChunk, version);
            return deltaChunk;
        }
        return *currChunk;
    }

    bool LocalStorage::DBArrayIterator::end()
    {
        position();
        return currChunk == NULL;
    }

    void LocalStorage::DBArrayIterator::operator ++()
    {
        position();
        setCurrent();
    }

    Coordinates const& LocalStorage::DBArrayIterator::getPosition()
    {
        position();
        if (!currChunk)
            throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NO_CURRENT_CHUNK);
        return currChunk->getFirstPosition(false);
    }

    bool LocalStorage::DBArrayIterator::setPosition(Coordinates const& pos)
    {
#ifdef PIN_CURRENT_CHUNK
        if (currChunk != NULL) {
            currChunk->unPin();
            currChunk = NULL;
        }
#else
        currChunk = NULL;
#endif
        if (!positioned) {
            timestamp = storage->timestamp;
            positioned = true;
        }
        addr.coords = pos;
        array.getChunkPositionFor(addr.coords);
        DBChunk* chunk = storage->lookupChunk(addr, false);
        if (chunk != NULL) {
#ifndef PIN_CURRENT_CHUNK
            chunk->unPin();
#endif
            boost::shared_ptr<Query> query(getQuery());
            if (storage->isResponsibleFor(*chunk, query) && chunk->timestamp <= timestamp) {
                currChunk = chunk;
                return true;
            }
#ifdef PIN_CURRENT_CHUNK
            chunk->unPin();
#endif
        }
        return false;
    }

    void LocalStorage::DBArrayIterator::setCurrent()
    {
#ifdef PIN_CURRENT_CHUNK
        if (currChunk != NULL) {
            currChunk->unPin();
            currChunk = NULL;
        }
#endif
        boost::shared_ptr<Query> query(getQuery());
        currChunk = storage->nextChunk(addr, timestamp, array, ps, query);
    }

    void LocalStorage::DBArrayIterator::reset()
    {
        positioned = true;
        addr.coords.resize(0);
        timestamp = storage->timestamp;
        setCurrent();
    }

    Chunk& LocalStorage::DBArrayIterator::newChunk(Coordinates const& pos)
    {
        if (!array.contains(pos))
            throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES);
        addr.coords = pos;
        array.getChunkPositionFor(addr.coords);
        return *storage->newChunk(addr, defaultCompressionMethod);
    }

    void LocalStorage::DBArrayIterator::deleteChunk(Chunk& chunk)
    {
        storage->deleteChunk(chunk);
    }

    Chunk& LocalStorage::DBArrayIterator::newChunk(Coordinates const& pos, int compressionMethod)
    {
        if (!array.contains(pos))
            throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES);
        addr.coords = pos;
        array.getChunkPositionFor(addr.coords);
        return *storage->newChunk(addr, compressionMethod);
    }

    Chunk& LocalStorage::DBArrayIterator::copyChunk(ConstChunk const& srcChunk, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap)
    {
        if (version > 1) { // first version of delta-array should always be allocated - not cloned
            addr.coords = srcChunk.getFirstPosition(false);
            boost::shared_ptr<Query> query(getQuery());
            Chunk* patch = storage->patchChunk(addr, srcChunk, query);
            if (patch != NULL) { 
                return *patch;
            }
        } else if (version == 0) {
            DBChunk const* diskChunk = srcChunk.getDiskChunk();
            if (diskChunk != NULL) {
                AttributeDesc const& srcAttrDesc = diskChunk->getAttributeDesc();
                if (diskChunk->hdr.allocatedSize == diskChunk->hdr.compressedSize && attrDesc.isNullable() == srcAttrDesc.isNullable()) {
                    boost::shared_ptr<Query> query(getQuery());
                    addr.coords = srcChunk.getFirstPosition(false);
                    Chunk* clone = storage->cloneChunk(addr, *diskChunk, query);
                    if (clone != NULL) {
                        return *clone;
                    }
                }
            }
        }
        return ArrayIterator::copyChunk(srcChunk, emptyBitmap);
    }

    LocalStorage LocalStorage::instance;
    Storage* StorageManager::instance = &LocalStorage::instance;

    //
    // Chunk implementation
    //

    DBChunk::~DBChunk() 
    { 
    }
    
    bool DBChunk::isTemporary() const
    {
        return false;
    }

    size_t DBChunk::count() const
    {
        return hdr.nElems != 0 ? hdr.nElems : ConstChunk::count();
    }

    bool DBChunk::isCountKnown() const
    {
        return hdr.nElems != 0 || ConstChunk::isCountKnown();
    }

    void DBChunk::setCount(size_t count)
    {
        hdr.nElems = count;
    }

    bool DBChunk::isDelta() const
    {
        return (hdr.flags & ChunkHeader::DELTA_CHUNK) != 0;
    }

    bool DBChunk::isSparse() const
    {
        return (hdr.flags & ChunkHeader::SPARSE_CHUNK) != 0;
    }

    bool DBChunk::isRLE() const
    {
        return (hdr.flags & ChunkHeader::RLE_CHUNK) != 0;
    }

    bool DBChunk::isMaterialized() const
    {
        return true;
    }

    DBChunk const* DBChunk::getDiskChunk() const
    {
        return this;
    }


    void DBChunk::setSparse(bool sparse)
    {
        if (sparse) {
            hdr.flags |= ChunkHeader::SPARSE_CHUNK;
        } else {
            hdr.flags &= ~ChunkHeader::SPARSE_CHUNK;
        }
    }

    void DBChunk::setRLE(bool rle)
    {
        if (rle) {
            hdr.flags |= ChunkHeader::RLE_CHUNK;
        } else {
            hdr.flags &= ~ChunkHeader::RLE_CHUNK;
        }
    }

    void DBChunk::updateArrayDescriptor()
    {
        _arrayDesc = SystemCatalog::getInstance()->getArrayDesc(addr.arrId);
    }

    void DBChunk::truncate(Coordinate lastCoord)
    {
        lastPos[0] = lastPosWithOverlaps[0] = lastCoord;
    }

    void DBChunk::write(boost::shared_ptr<Query>& query)
    {
       Query::validateQueryPtr(query);
       if (--nWriters <= 0) {
          storage->writeChunk(this, query);
          nWriters = 0;
       }
    }
    
    DBChunk::DBChunk() 
    {
        data = NULL;
        accessCount = 0;
        next = prev = NULL;
        timestamp = 1;
    }

    Array const& DBChunk::getArray() const
    {
        return storage->getDBArray(addr.arrId);
    }
                   
    void DBChunk::init()
    {
        data = NULL;
        accessCount = 0;
        hdr.nElems = 0;
        nWriters = 0;
        raw = false;
        waiting = false;
        next = prev = NULL;
        storage = (InternalStorage*)&StorageManager::getInstance();
        timestamp = 1;
        cloneOf = NULL;
        loader = 0;
    }

    RWLock& DBChunk:: getLatch()
    {
        return storage->getChunkLatch(this);
    }

    void DBChunk::calculateBoundaries()
    {
        lastPos = lastPosWithOverlaps = firstPosWithOverlaps = addr.coords;
        updateArrayDescriptor();
        const ArrayDesc& ad = getArrayDesc();
        hdr.instanceId = ad.isLocal() ? storage->getInstanceId() : ad.getChunkNumber(addr.coords) % storage->getNumberOfInstances();
        const Dimensions& dims = ad.getDimensions();
        size_t n = dims.size(); 
        assert(addr.coords.size() == n);
        for (size_t i = 0; i < n; i++) {
            if (firstPosWithOverlaps[i] > dims[i].getStart()) {
                firstPosWithOverlaps[i] -= dims[i].getChunkOverlap();
            }
            lastPos[i] = lastPosWithOverlaps[i] += dims[i].getChunkInterval() - 1;
            if (lastPos[i] > dims[i].getEndMax()) {
                lastPos[i] = dims[i].getEndMax();
            }
            if ((lastPosWithOverlaps[i] += dims[i].getChunkOverlap()) > dims[i].getEndMax()) {
                lastPosWithOverlaps[i] = dims[i].getEndMax();
            }
        }
    }

    bool DBChunk::isEmpty()
    {
        return next == this;
    }

    void DBChunk::prune()
    {
        next = prev = this;
    }

    void DBChunk::link(DBChunk* elem)
    {
        assert((elem->next == NULL && elem->prev == NULL) || (elem->next == elem && elem->prev == elem));
        elem->prev = this;
        elem->next = next;
        next = next->prev = elem;
    }

    void DBChunk::unlink()
    {
        next->prev = prev;
        prev->next = next;
        prune();
    }

    void DBChunk::beginAccess()
    {
        if (accessCount++ == 0 && next != NULL) {
            unlink();
        }
    }

    void DBChunk::setAddress(const Address& firstElem, int compressionMethod)
    {
        init();
        addr = firstElem;
        raw = true; // new chunk is not yet initialized
        // initialize disk header of chunk
        hdr.size = 0;
        hdr.compressedSize = 0;
        hdr.compressionMethod = compressionMethod;
        hdr.arrId = addr.arrId;
        hdr.attId = addr.attId;
        hdr.nCoordinates = addr.coords.size();
        hdr.flags = Config::getInstance()->getOption<bool>(CONFIG_RLE_CHUNK_FORMAT) ? ChunkHeader::RLE_CHUNK : 0;
        hdr.pos.hdrPos = 0;
        calculateBoundaries();
    }

    void  DBChunk::setAddress(const ChunkDescriptor& desc)
    {
        init();
        hdr = desc.hdr;
        desc.getAddress(addr);
        calculateBoundaries();
    }

    const ArrayDesc& DBChunk::getArrayDesc() const
    {
        return *_arrayDesc;
    }

    const AttributeDesc& DBChunk::getAttributeDesc() const
    {
        return getArrayDesc().getAttributes()[addr.attId];
    }

    int DBChunk::getCompressionMethod() const
    {
        return hdr.compressionMethod;
    }

    void* DBChunk::getData() const
    {
        if (loader != pthread_self()) {
            if (!accessCount)
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_NOT_PINNED);
            if (hdr.pos.hdrPos != 0) {
                storage->loadChunk((Chunk*)this);
            }
        }
        return data;
    }

    size_t DBChunk::getSize() const
    {
        return hdr.size;
    }

    size_t totalDBChunkAllocatedSize;
    
    void DBChunk::allocate(size_t size)
    {
        if (data) { 
            __sync_sub_and_fetch(&totalDBChunkAllocatedSize, hdr.size);
            ::free(data);
        }            
        hdr.size = size;
        data = ::malloc(size);
        if (!data)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_ALLOCATE_MEMORY);
        __sync_add_and_fetch(&totalDBChunkAllocatedSize, size);
        currentStatistics->allocatedSize += size;
        currentStatistics->allocatedChunks++;
    }

    void DBChunk::reallocate(size_t size)
    {
        data = ::realloc(data, size);
        if (!data)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_REALLOCATE_MEMORY);
        __sync_add_and_fetch(&totalDBChunkAllocatedSize, size - hdr.size);
        hdr.size = size;
        currentStatistics->allocatedSize += size;
        currentStatistics->allocatedChunks++;
    }

    void DBChunk::free()
    {
        //SYSTEM_CHECK(SCIDB_E_INTERNAL_ERROR, accessCount == 0, "Deallocated chunk should not be accessed");
        if (data) { 
            __sync_sub_and_fetch(&totalDBChunkAllocatedSize, hdr.size);
            ::free(data);
        }            
        data = NULL;
    }

    Coordinates const& DBChunk::getFirstPosition(bool withOverlap) const
    {
        return withOverlap ? firstPosWithOverlaps : addr.coords;
    }

    Coordinates const& DBChunk::getLastPosition(bool withOverlap) const
    {
        return withOverlap ? lastPosWithOverlaps : lastPos;
    }

    boost::shared_ptr<ConstChunkIterator> DBChunk::getConstIterator(int iterationMode) const
    {
        const AttributeDesc* bitmapAttr = getArrayDesc().getEmptyBitmapAttribute();
        Chunk* bitmapChunk = NULL;
        if (bitmapAttr != NULL && bitmapAttr->getId() != addr.attId 
            && (isRLE() || !(iterationMode & ConstChunkIterator::NO_EMPTY_CHECK))) 
        {
            Address bitmapAddr(addr.arrId, bitmapAttr->getId(), addr.coords);
            bitmapChunk = storage->readChunk(bitmapAddr);
        }
        pin();
        storage->loadChunk((Chunk*)this);
        boost::shared_ptr<Query> emptyQuery;
        boost::shared_ptr<ConstChunkIterator> iterator = boost::shared_ptr<ConstChunkIterator>
            (isRLE() ? getAttributeDesc().isEmptyIndicator()
                 ? (ConstChunkIterator*)new RLEBitmapChunkIterator(getArrayDesc(), addr.attId, (Chunk*)this, bitmapChunk, iterationMode)
                 : (ConstChunkIterator*)new RLEConstChunkIterator(getArrayDesc(), addr.attId, (Chunk*)this, bitmapChunk, iterationMode)
             : isSparse()
                 ? (ConstChunkIterator*)new SparseChunkIterator(getArrayDesc(), addr.attId, (Chunk*)this, bitmapChunk,
                                                                false, iterationMode, emptyQuery)
                 : (ConstChunkIterator*)new MemChunkIterator(getArrayDesc(), addr.attId, (Chunk*)this, bitmapChunk,
                                                             false, iterationMode, emptyQuery));
        unPin();
        if (bitmapChunk) {
            bitmapChunk->unPin();
        }
        return iterator;
    }

    boost::shared_ptr<ChunkIterator> DBChunk::getIterator(boost::shared_ptr<Query> const& query, int iterationMode)
    {
        const AttributeDesc* bitmapAttr = getArrayDesc().getEmptyBitmapAttribute();
        Chunk* bitmapChunk = NULL;
        if (bitmapAttr != NULL && bitmapAttr->getId() != addr.attId 
            && !(iterationMode & ConstChunkIterator::NO_EMPTY_CHECK)) 
        {
            Address bitmapAddr(addr.arrId, bitmapAttr->getId(), addr.coords);
            bitmapChunk = storage->newChunk(bitmapAddr, bitmapAttr->getDefaultCompressionMethod());
            if ((iterationMode & ChunkIterator::SPARSE_CHUNK) || isSparse()) {
                bitmapChunk->setSparse(true);
            }
        }
        nWriters += 1;
        return boost::shared_ptr<ChunkIterator>
            (isRLE()
             ? (ChunkIterator*)new RLEChunkIterator(getArrayDesc(), addr.attId, this, bitmapChunk, iterationMode, query)
             : ((((iterationMode & ChunkIterator::SPARSE_CHUNK) || isSparse())/* && !getAttributeDesc().isEmptyIndicator()*/)
                ? (ChunkIterator*)new SparseChunkIterator(getArrayDesc(), addr.attId, this, bitmapChunk,
                                                          true, iterationMode, query)
                : (ChunkIterator*)new MemChunkIterator(getArrayDesc(), addr.attId, this, bitmapChunk,
                                                       true, iterationMode, query)));
    }

    boost::shared_ptr<ConstRLEEmptyBitmap> DBChunk::getEmptyBitmap() const 
    {
        const AttributeDesc* bitmapAttr = getArrayDesc().getEmptyBitmapAttribute();
        boost::shared_ptr<ConstRLEEmptyBitmap> bitmap;
        if (bitmapAttr != NULL && bitmapAttr->getId() != addr.attId && isRLE()) {
            Address bitmapAddr(addr.arrId, bitmapAttr->getId(), addr.coords);
            Chunk* bitmapChunk = storage->readChunk(bitmapAddr);
            bitmap = boost::shared_ptr<ConstRLEEmptyBitmap>(new ConstRLEEmptyBitmap(*bitmapChunk));
            bitmapChunk->unPin();
        } else { 
            bitmap = ConstChunk::getEmptyBitmap();
        }
        return bitmap;
    }

    bool DBChunk::pin() const
    {
        storage->pinChunk(this);
        currentStatistics->pinnedSize += getSize();
        currentStatistics->pinnedChunks++;
        return true;
    }

    void DBChunk::unPin() const
    {
        storage->unpinChunk(this);
    }

    void DBChunk::compress(CompressedBuffer& buf, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const
    {
        if (emptyBitmap && isRLE()) { 
            MemChunk closure;
            closure.initialize(*this);
            makeClosure(closure, emptyBitmap);
            closure.compress(buf, emptyBitmap);
        } else {
            PinBuffer scope(*this);
            storage->compressChunk(this, buf);
        }
    }

    void DBChunk::decompress(CompressedBuffer const& buf)
    {
        storage->decompressChunk(this, buf);
    }

    //
    // Global storage implemention
    //
    void GlobalStorage::remove(ArrayID remArrId, bool allVersions, uint64_t timestamp)
    {
        ScopedMutexLock cs(mutex);
        CachedStorage::remove(remArrId, allVersions, timestamp);

        DIR* arrDir = opendir(dfsDir.c_str());
        struct dirent *arrEntry, *chunkEntry;
        while ((arrEntry = readdir(arrDir)) != NULL) {
            ArrayID arrId;
            AttributeID attId;
            if (sscanf(arrEntry->d_name, "%"PRIu64"_%u", &arrId, &attId) == 2 && arrId == remArrId) {
                DIR* chunkDir = opendir((dfsDir + arrEntry->d_name).c_str());
                while ((chunkEntry = readdir(chunkDir)) != NULL) {
                    if (*chunkEntry->d_name != '.') {
                        ::unlink((dfsDir + arrEntry->d_name + '/' + chunkEntry->d_name).c_str());
                    }
                }
                closedir(chunkDir);
            }
        }
        closedir(arrDir);
    }


    void GlobalStorage::open(const string& storageDescriptorFilePath, size_t cacheSize)
    {
        char buf[MAX_CFG_LINE_LENGTH];
        string databasePath = "";
        size_t pathEnd = storageDescriptorFilePath.find_last_of('/');
        if (pathEnd != string::npos) {
            databasePath = storageDescriptorFilePath.substr(0, pathEnd+1);
        }
        FILE* f = fopen(storageDescriptorFilePath.c_str(), "r");
        if (!f)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) << storageDescriptorFilePath << errno;
        if (!fgets(buf, sizeof buf, f))
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT);
        string databaseHeaderPath = relativePath(databasePath, strtrim(buf));
        if (!fgets(buf, sizeof buf, f))
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT);
        string dfsDir = relativePath(databasePath, strtrim(buf));
        fclose(f);
        open(databaseHeaderPath, dfsDir, cacheSize, CompressorFactory::getInstance().getCompressors());
    }

    void GlobalStorage::recover(InstanceID recoveredInstance, boost::shared_ptr<Query>& query)
    {
    }

    void GlobalStorage::getDiskInfo(DiskInfo& info)
    {
        memset(&info, 0, sizeof info);
    }

    void GlobalStorage::rollback(std::map<ArrayID,VersionID> const& undoUpdates)
    {
    }

    void GlobalStorage::open(const string& headerPath, const string& rootDir, size_t cacheSize, const vector<Compressor*>& compressors)
    {
        CachedStorage::open(cacheSize, compressors);
        dfsDir = (rootDir[rootDir.size()-1] == '/') ? rootDir : rootDir + "/";
        headerFilePath = headerPath;
        instanceId = 0;
        nInstances = SystemCatalog::getInstance()->getNumberOfInstances();
        int fd = ::open(headerFilePath.c_str(), O_RDONLY);
        if (fd >= 0) {
            size_t rc = ::read(fd, &instanceId, sizeof(instanceId));
            if (rc != 0 && rc != sizeof(instanceId))
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_OPERATION_FAILED) << "read";
            ::close(fd);
        }
    }

    void GlobalStorage::flush()
    {
    }

    boost::shared_ptr<ArrayIterator> GlobalStorage::getArrayIterator(const ArrayDesc& arrDesc,
                                                                     AttributeID attId,
                                                                     boost::shared_ptr<Query>& query)
    {
       return boost::shared_ptr<ArrayIterator>(new DBArrayIterator(this, arrDesc, attId, query));
    }

    string GlobalStorage::getChunkPath(const Address& addr, bool withCoordinates)
    {
        string path = dfsDir;
        char buf[64];
        sprintf(buf, "%016"PRIi64"_%010u", addr.arrId, addr.attId);
        path.append(buf);
        if (withCoordinates) {
            char sep = '/';
            for (size_t i = 0, n = addr.coords.size(); i < n; i++) {
                sprintf(buf, "%c%016"PRIi64, sep, addr.coords[i]);
                path.append(buf);
                sep = '_';
            }
        }
        return path;
    }

    void GlobalStorage::freeChunk(DBChunk& victim)
    {
        CachedStorage::freeChunk(victim);
        chunkMap.erase(victim.getAddress());
    }

    bool GlobalStorage::containsChunk(const Address& addr)
    {
        return CachedStorage::containsChunk(addr) || ::access(getChunkPath(addr).c_str(), R_OK) == 0;
    }

    Chunk* GlobalStorage::readChunk(const Address& addr)
    {
        DBChunk* chunk = CachedStorage::lookupChunk(addr, true);
        if (chunk->isRaw()) {
            int fd = ::open(getChunkPath(addr).c_str(), O_RDONLY);
            if (fd < 0) // chunk exists in DFS
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) << getChunkPath(addr) << errno;
            ChunkDescriptor desc;
            File::readAll(fd, &desc.hdr, sizeof(desc.hdr), 0);
            for (size_t i = 0, n = addr.coords.size(); i < n; i++) {
                desc.coords[i] = addr.coords[i];
            }
            size_t chunkSize = chunk->getSize();
            chunk->allocate(chunkSize);
            if (chunk->getCompressedSize() != chunkSize) {
                const size_t bufSize = chunk->getCompressedSize();
                boost::scoped_array<char> buf(new char[bufSize]);
                currentStatistics->allocatedSize += bufSize;
                currentStatistics->allocatedChunks++;
                if (!buf)
                    throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_ALLOCATE_MEMORY);
                File::readAll(fd, buf.get(), chunk->getCompressedSize(), sizeof(desc.hdr));
                size_t rc = compressors[chunk->getCompressionMethod()]->decompress(buf.get(), chunk->getCompressedSize(), *chunk);
                if (rc != chunkSize)
                    throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_DECOMPRESS_CHUNK);
                buf.reset();
            } else {
                File::readAll(fd, chunk->getData(), chunkSize, sizeof(desc.hdr));
            }
            ::close(fd);
            {
                // Check if there are some other threads waiting for completion read of this chunk and wake up them
                ScopedMutexLock cs(mutex);
                notifyChunkReady(*chunk);
            }
        }
        return chunk;
    }


    void GlobalStorage::compressChunk(Chunk const* aChunk, CompressedBuffer& buf)
    {
        CachedStorage::compressChunk(aChunk, buf);
        if (buf.getData() == NULL) { // chunk data is not present in the cache so read compressed data from DFS
            DBChunk& chunk = *(DBChunk*)aChunk;
            buf.allocate(chunk.getCompressedSize());

            int fd = ::open(getChunkPath(chunk.getAddress()).c_str(), O_RDONLY);
            if (fd < 0) // chunk exists in DFS
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) << getChunkPath(chunk.getAddress()) << errno;
            File::readAll(fd, buf.getData(), chunk.getCompressedSize(), sizeof(ChunkHeader));
            ::close(fd);
        }
    }

    void GlobalStorage::writeChunk(Chunk* newChunk, boost::shared_ptr<Query>& query)
    {
        ScopedMutexLock cs(mutex);
        Query::validateQueryPtr(query);
        DBChunk& chunk = *(DBChunk*)newChunk;
        assert(chunk.isRaw()); // new chunk is raw

        const size_t bufSize = chunk.getSize();
        boost::scoped_array<char> buf(new char[bufSize]);
        currentStatistics->allocatedSize += bufSize;
        currentStatistics->allocatedChunks++;
        if (!buf)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_ALLOCATE_MEMORY);
        void const* deflated = buf.get();
        Address const& addr = chunk.getAddress();
        chunk.setCompressedSize(compressors[chooseCompressionMethod(chunk, buf.get())]->compress(buf.get(), chunk));
        if (chunk.getCompressedSize() == chunk.getSize()) { // no compression
            deflated = chunk.getData();
        }
        // Write chunk data
        int fd = ::open(getChunkPath(addr).c_str(), O_RDWR|O_CREAT, 0777);
        if (fd < 0) {
            mkdir(getChunkPath(addr, false).c_str(), 0777);
            fd = ::open(getChunkPath(addr).c_str(), O_RDWR|O_CREAT, 0777);
        }
        if (fd < 0)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) << getChunkPath(addr) << errno;
        File::writeAll(fd, &chunk.getHeader(), sizeof(ChunkHeader), 0);
        File::writeAll(fd, deflated, chunk.getCompressedSize(), sizeof(ChunkHeader));
        buf.reset();
        ::close(fd);

        notifyChunkReady(chunk);
    }

    InstanceID GlobalStorage::getInstanceId() const
    {
        return instanceId;
    }

    size_t GlobalStorage::getNumberOfInstances() const
    {
        return nInstances;
    }

    void GlobalStorage::setInstanceId(InstanceID id)
    {
        instanceId = id;
        int fd = ::open(headerFilePath.c_str(), O_RDWR|O_CREAT, 0777);
        if (fd < 0)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) << headerFilePath << errno;
        File::writeAll(fd, &instanceId, sizeof(instanceId), 0);
        ::close(fd);
    }

    bool GlobalStorage::isLocal()
    {
        return false;
    }

    GlobalStorage::DBArrayIterator::DBArrayIterator(GlobalStorage* storage, const ArrayDesc& arrDesc,
                                                    AttributeID attId, boost::shared_ptr<Query>& query)
    : array(arrDesc), _query(query)
    {
        this->storage = storage;
        addr.arrId = arrDesc.getId();
        addr.attId = attId;
        currChunk = NULL;
        defaultCompressionMethod = arrDesc.getAttributes()[attId].getDefaultCompressionMethod();
        reset();
    }

    GlobalStorage::DBArrayIterator::~DBArrayIterator()
    {
        if (dir != NULL) {
            closedir(dir);
        }
        if (currChunk != NULL) {
            currChunk->unPin();
        }
    }

    ConstChunk const& GlobalStorage::DBArrayIterator::getChunk()
    {
        if (!currChunk)
            throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NO_CURRENT_CHUNK);
        return *currChunk;
    }

    bool GlobalStorage::DBArrayIterator::end()
    {
        return currChunk == NULL;
    }

    void GlobalStorage::DBArrayIterator::operator ++()
    {
        setCurrent();
    }

    Coordinates const& GlobalStorage::DBArrayIterator::getPosition()
    {
        if (!currChunk)
            throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NO_CURRENT_CHUNK);
        return currChunk->getFirstPosition(false);
    }

    bool GlobalStorage::DBArrayIterator::setPosition(Coordinates const& pos)
    {
        if (currChunk != NULL) {
            currChunk->unPin();
            currChunk = NULL;
        }
        addr.coords = pos;
        array.getChunkPositionFor(addr.coords);
        if (storage->containsChunk(addr)) {
            currChunk = storage->lookupChunk(addr, true);
            return true;
        } else {
            return false;
        }
    }

    void GlobalStorage::DBArrayIterator::setCurrent()
    {
        if (currChunk != NULL) {
            currChunk->unPin();
            currChunk = NULL;
        }
        while (true) {
            struct dirent* entry = readdir(dir);
            if (entry != NULL) {
                char const* name = entry->d_name;
                if (*name != '.') {
                    for (size_t i = 0, n = addr.coords.size(); i < n; i++) {
                        int pos;
                        if (sscanf(name, "%"PRIi64"%n", &addr.coords[i], &pos) != 1)
                            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_FILE_HAS_INVALID_NAME);
                        name += pos + 1; // skip separator character
                    }
                    if (storage->scatter == NULL || storage->scatter->contains(addr)) {
                        DBChunk* chunk = storage->lookupChunk(addr, true);
                        if (chunk->getTimestamp() <= timestamp) {
                            currChunk = chunk;
                            return;
                        }
                    }
                }
            }
        }
    }

    void GlobalStorage::DBArrayIterator::reset()
    {
        if (dir != NULL) {
            closedir(dir);
        }
        dir = opendir(storage->getChunkPath(addr, false).c_str());
        if (!dir)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_DFS_DIRECTORY_NOT_EXIST);
        timestamp = storage->timestamp;
        setCurrent();
    }

    void GlobalStorage::DBArrayIterator::deleteChunk(Chunk& chunk)
    {
        storage->deleteChunk(chunk);
    }

    Chunk& GlobalStorage::DBArrayIterator::newChunk(Coordinates const& pos)
    {
        if (!array.contains(pos))
            throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES);
        addr.coords = pos;
        array.getChunkPositionFor(addr.coords);
        return *storage->newChunk(addr, defaultCompressionMethod);
    }

    Chunk& GlobalStorage::DBArrayIterator::newChunk(Coordinates const& pos, int compressionMethod)
    {
        if (!array.contains(pos))
            throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES);
        addr.coords = pos;
        array.getChunkPositionFor(addr.coords);
        return *storage->newChunk(addr, compressionMethod);
    }

    GlobalStorage GlobalStorage::instance;
}
