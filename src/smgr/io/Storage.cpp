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
 * @author poliocough@gmail.com
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
#include <query/ops/list/ListArrayBuilder.h>
#include <system/Utils.h>

#include <sys/time.h>

#ifdef __APPLE__
#define fdatasync(_fd) fsync(_fd)
#endif

namespace scidb
{

using namespace std;
using namespace boost;

///////////////////////////////////////////////////////////////////
/// Constants and #defines
///////////////////////////////////////////////////////////////////

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc"));

#ifndef SCIDB_NO_DELTA_COMPRESSION
DeltaVersionControl _deltaVersionControl;
#endif // SCIDB_NO_DELTA_COMPRESSION
const uint32_t SCIDB_STORAGE_HEADER_MAGIC = 0xDDDDBBBB;
const uint32_t SCIDB_STORAGE_FORMAT_VERSION = 3;

const size_t DEFAULT_SEGMENT_SIZE = 1024 * 1024 * 1024; // default limit of database partition (in megabytes) used for generated storage descriptor file
const size_t DEFAULT_TRANS_LOG_LIMIT = 1024; // default limit of transaction log file (in megabytes)
const size_t MAX_CFG_LINE_LENGTH = 1024;
const int MAX_REDUNDANCY = 8;
const int MAX_INSTANCE_BITS = 10; // 2^MAX_INSTANCE_BITS = max number of instances

///////////////////////////////////////////////////////////////////
/// Static helper functions
///////////////////////////////////////////////////////////////////

/**
 * Fibonacci hash for a 64 bit key
 * @param key to hash
 * @param fib_B = log2(max_num_of_buckets)
 * @return hash = bucket index
 */
static uint64_t fibHash64(const uint64_t key, const uint64_t fib_B)
{
    assert(fib_B < 64);
    const uint64_t fib_A64 = (uint64_t) 11400714819323198485U;
    return (key * fib_A64) >> (64 - fib_B);
}

inline static char* strtrim(char* buf)
{
    char* p = buf;
    char ch;
    while ((unsigned char) (ch = *p) <= ' ' && ch != '\0')
    {
        p += 1;
    }
    char* q = p + strlen(p);
    while (q > p && (unsigned char) q[-1] <= ' ')
    {
        q -= 1;
    }
    *q = '\0';
    return p;
}

inline static string relativePath(const string& dir, const string& file)
{
    return file[0] == '/' ? file : dir + file;
}

inline static double getTimeSecs()
{
    struct timeval tv;
    gettimeofday(&tv, 0);
    return (((double) tv.tv_sec) * 1000000 + ((double) tv.tv_usec)) / 1000000;
}

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

static void collectArraysToRollback(boost::shared_ptr<std::map<ArrayID, VersionID> >& arrsToRollback, const VersionID& lastVersion,
                                    const ArrayID& baseArrayId, const ArrayID& newArrayId)
{
    assert(arrsToRollback);
    assert(baseArrayId>0);
    (*arrsToRollback.get())[baseArrayId] = lastVersion;
}

///////////////////////////////////////////////////////////////////
/// ChunkDescriptor
///////////////////////////////////////////////////////////////////

VersionControl* VersionControl::instance;

void ChunkDescriptor::getAddress(StorageAddress& addr) const
{
    addr.arrId = hdr.arrId;
    addr.attId = hdr.attId;
    addr.coords.resize(hdr.nCoordinates);
    for (int j = 0; j < hdr.nCoordinates; j++)
    {
        addr.coords[j] = coords[j];
    }
}

///////////////////////////////////////////////////////////////////
/// CoordinateMapInitializer and ChunkInitializer
///////////////////////////////////////////////////////////////////

CachedStorage::CoordinateMapInitializer::~CoordinateMapInitializer()
{
    ScopedMutexLock cs(storage._mutex);
    cm.raw = false;
    cm.initialized = initialized;
    if (cm.waiting)
    {
        cm.waiting = false;
        storage._initEvent.signal(); // wakeup all threads waiting for this chunk
    }
}

CachedStorage::ChunkInitializer::~ChunkInitializer()
{
    ScopedMutexLock cs(storage._mutex);
    storage.notifyChunkReady(chunk);
}

///////////////////////////////////////////////////////////////////
/// CachedStorage class
///////////////////////////////////////////////////////////////////

CachedStorage::CachedStorage() :
    _replicationManager(NULL)
{
    _hd = -1;
    for (size_t i = 0; i < MAX_SEGMENTS; ++i)
    {
        _sd[i] = -1;
    }
    _log[0] = _log[1] = -1;
}

void CachedStorage::open(const string& storageDescriptorFilePath, size_t cacheSizeBytes)
{
    StatisticsScope sScope;
    InjectedErrorListener<WriteChunkInjectedError>::start();
    char buf[MAX_CFG_LINE_LENGTH];
    char const* descPath = storageDescriptorFilePath.c_str();
    string databasePath = "";
    string databaseHeader;
    string databaseLog;
    uint64_t transLogLimit;
    size_t pathEnd = storageDescriptorFilePath.find_last_of('/');
    if (pathEnd != string::npos)
    {
        databasePath = storageDescriptorFilePath.substr(0, pathEnd + 1);
    }
    FILE* f = fopen(descPath, "r");
    if (f == NULL)
    {
        f = fopen(descPath, "w");
        if (!f)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) << descPath << errno;
        size_t fileNameBeg = (pathEnd == string::npos) ? 0 : pathEnd + 1;
        size_t fileNameEnd = storageDescriptorFilePath.find_last_of('.');
        if (fileNameEnd == string::npos || fileNameEnd < fileNameBeg)
        {
            fileNameEnd = storageDescriptorFilePath.size();
        }
        string databaseName = storageDescriptorFilePath.substr(fileNameBeg, fileNameEnd - fileNameBeg);
        databaseHeader = databasePath + databaseName + ".header";
        databaseLog = databasePath + databaseName + ".log";
        _segments.push_back(Segment(databasePath + databaseName + ".data1", (uint64_t) DEFAULT_SEGMENT_SIZE * MB));
        fprintf(f, "%s.header\n", databaseName.c_str());
        fprintf(f, "%ld %s.log\n", (long) DEFAULT_TRANS_LOG_LIMIT, databaseName.c_str());
        fprintf(f, "%ld %s.data1\n", (long) DEFAULT_SEGMENT_SIZE, databaseName.c_str());
        transLogLimit = (uint64_t) DEFAULT_TRANS_LOG_LIMIT * MB;
    }
    else
    {
        int pos;
        long sizeMb;
        if (!fgets(buf, sizeof buf, f))
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT);
        databaseHeader = relativePath(databasePath, strtrim(buf));
        if (!fgets(buf, sizeof buf, f))
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT);
        if (sscanf(buf, "%ld%n", &sizeMb, &pos) != 1)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT);
        databaseLog = relativePath(databasePath, strtrim(buf + pos));
        transLogLimit = (uint64_t) sizeMb * MB;
        while (fgets(buf, sizeof buf, f))
        {
            if (sscanf(buf, "%ld%n", &sizeMb, &pos) != 1)
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT);
            _segments.push_back(Segment(relativePath(databasePath, strtrim(buf + pos)), (uint64_t) sizeMb * MB));
        }
    }
    fclose(f);

    _cacheSize = cacheSizeBytes;
    _compressors = CompressorFactory::getInstance().getCompressors();
    _cacheUsed = 0;
    _strictCacheLimit = Config::getInstance()->getOption<bool> (CONFIG_STRICT_CACHE_LIMIT);
    _cacheOverflowFlag = false;
    _timestamp = 1;
    _lru.prune();

    int flags = O_LARGEFILE | O_RDWR | O_CREAT;
    _hd = ::open(databaseHeader.c_str(), flags, 0777);
    if (_hd < 0)
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) << databaseHeader << errno;

    struct flock flc;
    flc.l_type = F_WRLCK;
    flc.l_whence = SEEK_SET;
    flc.l_start = 0;
    flc.l_len = 1;

    if (fcntl(_hd, F_SETLK, &flc))
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_LOCK_DATABASE);

    _log[0] = ::open((databaseLog + "_1").c_str(), O_LARGEFILE | O_SYNC | O_RDWR | O_CREAT, 0777);
    if (_log[0] < 0)
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) << (databaseLog + "_1") << errno;

    _log[1] = ::open((databaseLog + "_2").c_str(), O_LARGEFILE | O_SYNC | O_RDWR | O_CREAT, 0777);
    if (_log[1] < 0)
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) << (databaseLog + "_2") << errno;

    _logSizeLimit = transLogLimit;
    _logSize = 0;
    _currLog = 0;

    size_t rc = read(_hd, &_hdr, sizeof(_hdr));
    if (rc != 0 && rc != sizeof(_hdr))
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_OPERATION_FAILED_WITH_ERRNO) << "read" << errno;

    size_t nSegments = _segments.size();

    _clusters.clear();
    _liveChunksInCluster.clear();
    _freeClusters.resize(nSegments);

    _writeLogThreshold = Config::getInstance()->getOption<int> (CONFIG_IO_LOG_THRESHOLD);
    _enableDeltaEncoding = Config::getInstance()->getOption<bool> (CONFIG_ENABLE_DELTA_ENCODING);

    // Open segments and calculate total amount of available space in the database
    long available = 0;
    for (size_t i = 0; i < nSegments; i++)
    {
        _sd[i] = ::open(_segments[i].path.c_str(), flags, 0777);
        if (_sd[i] < 0)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) << _segments[i].path << errno;

        if (_segments[i].size < _hdr.segment[i].used)
        {
            _segments[i].size = _hdr.segment[i].used;
        }
        available += (long) ((_segments[i].size - _hdr.segment[i].used) / GB);
    }

    _totalAvailable = available;

    _nInstances = SystemCatalog::getInstance()->getNumberOfInstances();
    _redundancy = 0; // disable replication during rollback: each instance is perfroming rollback locally

    if (rc == 0 || _hdr.currPos < HEADER_SIZE)
    {
        // Database is not initialized
        memset(&_hdr, 0, sizeof(_hdr));
        _hdr.magic = SCIDB_STORAGE_HEADER_MAGIC;
        _hdr.version = SCIDB_STORAGE_FORMAT_VERSION;
        _hdr.currPos = HEADER_SIZE;
        _hdr.instanceId = INVALID_INSTANCE;
        _hdr.nChunks = 0;
        _hdr.clusterSize = Config::getInstance()->getOption<int> (CONFIG_CHUNK_CLUSTER_SIZE);
    }
    else
    {
        if (_hdr.magic != SCIDB_STORAGE_HEADER_MAGIC)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_INVALID_STORAGE_HEADER);
        }
        if (_hdr.version != SCIDB_STORAGE_FORMAT_VERSION)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_STORAGE_FORMAT_VERSION) << _hdr.version << SCIDB_STORAGE_FORMAT_VERSION;
        }

        doTxnRecoveryOnStartup();

        // Database is initialized: read information about all locally available chunks in map
        _redundancy = Config::getInstance()->getOption<int> (CONFIG_REDUNDANCY);
        _syncReplication = !Config::getInstance()->getOption<bool> (CONFIG_ASYNC_REPLICATION);

        ChunkDescriptor desc;
        uint64_t chunkPos = HEADER_SIZE;
        StorageAddress addr;
        map<uint64_t, DBChunk*> clones;
        set<ArrayID> removedArrays, orphanedArrays;
        for (size_t i = 0; i < _hdr.nChunks; i++, chunkPos += sizeof(ChunkDescriptor))
        {
            size_t rc = pread(_hd, &desc, sizeof(ChunkDescriptor), chunkPos);
            if (rc != sizeof(ChunkDescriptor))
            {
                LOG4CXX_ERROR(logger, "Inconsistency in storage header: rc=" << rc << ", chunkPos=" << chunkPos << ", i=" << i << ", hdr.nChunks=" << _hdr.nChunks << ", hdr.currPos=" << _hdr.currPos);
                _hdr.currPos = chunkPos;
                _hdr.nChunks = i;
                break;
            }
            if (desc.hdr.pos.hdrPos != chunkPos)
            {
                LOG4CXX_ERROR(logger, "Invalid chunk header " << i << " at position " << chunkPos << " desc.hdr.pos.hdrPos=" << desc.hdr.pos.hdrPos << " arrayID=" << desc.hdr.arrId << " hdr.nChunks=" << _hdr.nChunks);
                _freeHeaders.insert(chunkPos);
            }
            else
            {
                assert(desc.hdr.nCoordinates < MAX_NUM_DIMS_SUPPORTED);
                if (desc.hdr.arrId != 0)
                {
                    try
                    {
                        if (removedArrays.count(desc.hdr.arrId) != 0)
                        {
                            desc.hdr.arrId = 0;
                            LOG4CXX_TRACE(logger, "ChunkDesc: Remove chunk descriptor for unexisted array at position " << chunkPos);
                            File::writeAll(_hd, &desc.hdr, sizeof(ChunkHeader), chunkPos);
                            assert(desc.hdr.nCoordinates < MAX_NUM_DIMS_SUPPORTED);
                            _freeHeaders.insert(chunkPos);
                            continue;
                        }

                        if (orphanedArrays.find(desc.hdr.arrId) != orphanedArrays.end())
                        {
                            continue;
                        }

                        ArrayDesc adesc;
                        SystemCatalog::getInstance()->getArrayDesc(desc.hdr.arrId, adesc);
                        ChunkMap::iterator iter = _chunkMap.find(adesc.getUAId());
                        if (iter == _chunkMap.end())
                        {
                            iter = _chunkMap.insert(make_pair(adesc.getUAId(), make_shared <InnerChunkMap> ())).first;
                        }

                        shared_ptr<InnerChunkMap>& innerMap = iter->second;
                        desc.getAddress(addr);

                        shared_ptr<DBChunk>& chunk =(*innerMap)[addr];
                        if( !desc.hdr.is<ChunkHeader::TOMBSTONE>() )
                        {
                            chunk.reset(new DBChunk());
                            chunk->setAddress(desc);
                            if (_hdr.clusterSize != 0)
                            {
                                _liveChunksInCluster[getClusterID(chunk->_hdr.pos)] += 1;
                            }
                            DBChunk*& clone = clones[chunk->_hdr.pos.offs * MAX_SEGMENTS + chunk->_hdr.pos.segmentNo];
                            chunk->_cloneOf = clone;
                            if (clone != NULL)
                            {
                                clone->_clones.push_back(chunk.get());
                                chunk->_hdr.compressedSize = clone->_hdr.compressedSize;
                                chunk->_hdr.size = clone->_hdr.size;
                                chunk->_hdr.flags = clone->_hdr.flags;
                            }
                            clone = chunk.get();
                        }
                    } catch (SystemException const& x)
                    {
                        if (x.getLongErrorCode() == SCIDB_LE_ARRAYID_DOESNT_EXIST)
                        {
                            removedArrays.insert(desc.hdr.arrId);
                            desc.hdr.arrId = 0;
                            LOG4CXX_TRACE(logger, "ChunkDesc: Remove chunk descriptor for unexisted array at position " << chunkPos);
                            File::writeAll(_hd, &desc.hdr, sizeof(ChunkHeader), chunkPos);
                            _freeHeaders.insert(chunkPos);
                        }
                        else if (x.getLongErrorCode() == SCIDB_LE_TYPE_NOT_REGISTERED)
                        {
                            orphanedArrays.insert(desc.hdr.arrId);
                            LOG4CXX_WARN(logger, "Array id:" << desc.hdr.arrId << " is orphan. (" << x.getErrorMessage() << ")");
                        }
                        else
                        {
                            throw;
                        }
                    }
                }
                else
                {
                    _freeHeaders.insert(chunkPos);
                }
            }
        }
        if (chunkPos != _hdr.currPos)
        {
            LOG4CXX_ERROR(logger, "Storage header is not consistent: " << chunkPos << " vs. " << _hdr.currPos);
            // throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_DATABASE_HEADER_CORRUPTED);
            if (chunkPos > _hdr.currPos)
            {
                _hdr.currPos = chunkPos;
            }
        }

        if (_hdr.clusterSize != 0)
        {
            for (size_t i = 0; i < nSegments; i++)
            {
                for (uint64_t offs = 0; offs < _hdr.segment[i].used; offs += _hdr.clusterSize)
                {
                    if (_liveChunksInCluster[getClusterID(i, offs)] == 0)
                    {
                        _freeClusters[i].insert(offs);
                    }
                }
            }
        }
    }

    int syncMSeconds = Config::getInstance()->getOption<int> (CONFIG_SYNC_IO_INTERVAL);
    if (syncMSeconds >= 0)
    {
        vector<int> fds(0);
        for (size_t i = 0; i < _segments.size(); i++)
        {
            fds.push_back(_sd[i]);
        }

        BackgroundFileFlusher::getInstance()->start(syncMSeconds, _writeLogThreshold, fds);
    }

    _replicationManager = ReplicationManager::getInstance();
    assert(_replicationManager);
    assert(_replicationManager->isStarted());
}

void CachedStorage::close()
{
    InjectedErrorListener<WriteChunkInjectedError>::stop();
    BackgroundFileFlusher::getInstance()->stop();

    for (ChunkMap::iterator i = _chunkMap.begin(); i != _chunkMap.end(); ++i)
    {
        shared_ptr<InnerChunkMap> & innerMap = i->second;
        for (InnerChunkMap::iterator j = innerMap->begin(); j != innerMap->end(); ++j)
        {
            if (j->second && j->second->_accessCount != 0)
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_PIN_UNPIN_DISBALANCE);
        }
    }
    _chunkMap.clear();

    std::ostringstream ss;

    if (::close(_hd) != 0)
    {
        LOG4CXX_ERROR(logger, "Failed to close header file");
    }
    if (::close(_log[0]) != 0)
    {
        LOG4CXX_ERROR(logger, "Failed to close transaction log file");
    }
    if (::close(_log[1]) != 0)
    {
        LOG4CXX_ERROR(logger, "Failed to close transaction log file");
    }
    for (size_t i = 0, nSegments = _segments.size(); i < nSegments; i++)
    {
        if (::close(_sd[i]) != 0)
        {
            LOG4CXX_ERROR(logger, "Failed to close data file");
        }
    }
    if (!ss.str().empty())
    {
        LOG4CXX_ERROR(logger, ss.str());
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_CLOSE_FAILED);
    }
}

CachedStorage::CoordinateMap& CachedStorage::getCoordinateMap(string const& indexName, DimensionDesc const& dim,
                                                              const boost::shared_ptr<Query>& query)
{
    CoordinateMap* cm;
    {
        ScopedMutexLock cs(_mutex);
        _mutex.checkForDeadlock();
        cm = &_coordinateMap[indexName];
        if (cm->initialized)
        {
            assert(!cm->raw);
            return *cm;
        }
        if (cm->raw)
        {
            // Some other thread is already constructing this mapping
            do
            {
                cm->waiting = true;
                Semaphore::ErrorChecker ec = bind(&Query::validate, query);
                _initEvent.wait(_mutex, ec);
            } while (cm->raw);
            if (!cm->initialized)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NO_MAPPING_FOR_COORDINATE);
            }
            return *cm;
        }
        cm->attrMap = buildFunctionalMapping(dim);
        if (cm->attrMap)
        { // functional mapping: do not need to load something from storage
            if (dim.getFlags() & DimensionDesc::COMPLEX_TRANSFORMATION)
            {
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
    if (!ai->setPosition(origin))
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NO_MAPPING_FOR_COORDINATE);
    }
    ConstChunk const& chunk = ai->getChunk();
    PinBuffer scope(chunk);
    cm->indexArrayDesc = indexArray->getArrayDesc();
    TypeId attrType = cm->indexArrayDesc.getAttributes()[0].getType();
    DimensionDesc const& mapDim = cm->indexArrayDesc.getDimensions()[0];
    assert(mapDim.getEndMax() != MAX_COORDINATE);
    cm->attrMap = make_shared <AttributeMultiMap> (attrType, mapDim.getStart(), (size_t) mapDim.getLength(), chunk.getData(), chunk.getSize());
    cm->coordMap = make_shared <MemoryBuffer> (chunk.getData(), chunk.getSize());
    cm->functionalMapping = false;
    guard.initialized = true;
    return *cm;
}

void CachedStorage::removeCoordinateMap(string const& indexName)
{
    ScopedMutexLock cs(_mutex);
    _coordinateMap.erase(indexName);
}

Value CachedStorage::reverseMapCoordinate(string const& indexName, DimensionDesc const& dim, Coordinate pos, const boost::shared_ptr<Query>& query)
{
    CoordinateMap& cm = getCoordinateMap(indexName, dim, query);
    if (cm.functionalMapping)
    {
        Value value;
        cm.attrMap->getOriginalCoordinate(value, (pos * dim.getFuncMapScale()) + dim.getFuncMapOffset());
        return value;
    }
    uint8_t* src = (uint8_t*) cm.coordMap->getData();
    AttributeDesc const& idxAttr = cm.indexArrayDesc.getAttributes()[0];
    DimensionDesc const& idxDim = cm.indexArrayDesc.getDimensions()[0];
    size_t index = (size_t) (pos - idxDim.getStart());
    if (index >= idxDim.getLength())
    {
        return Value();
    }
    Type type(TypeLibrary::getType(idxAttr.getType()));
    size_t size = type.byteSize();
    if (size == 0)
    {
        src += size_t(idxDim.getLength()) * sizeof(int) + ((int*) src)[index];
        if (*src == 0)
        {
            size = (src[1] << 24) | (src[2] << 16) | (src[3] << 8) | src[4];
            src += 5;
        }
        else
        {
            size = *src++;
        }
    }
    else
    {
        src += size * index;
    }
    return Value(src, size, false);
}

Coordinate CachedStorage::mapCoordinate(string const& indexName, DimensionDesc const& dim, Value const& value, CoordinateMappingMode mode,
                                        const boost::shared_ptr<Query>& query)
{
    CoordinateMap& cm = getCoordinateMap(indexName, dim, query);
    Coordinate c = cm.attrMap->get(value, mode);
    if (cm.functionalMapping)
    {
        c = (c - dim.getFuncMapOffset()) / dim.getFuncMapScale();
    }
    return c;
}

void CachedStorage::notifyChunkReady(DBChunk& chunk)
{
    // This method is invoked with storage mutex locked
    chunk._raw = false;
    if (chunk._waiting)
    {
        chunk._waiting = false;
        _loadEvent.signal(); // wakeup all threads waiting for this chunk
    }
}

void CachedStorage::pinChunk(Chunk const* aChunk)
{
    ScopedMutexLock cs(_mutex);
    DBChunk& chunk = *(DBChunk*) aChunk;
    chunk.beginAccess();
}

void CachedStorage::unpinChunk(Chunk const* aChunk)
{
    ScopedMutexLock cs(_mutex);
    DBChunk& chunk = *(DBChunk*) aChunk;
    assert(chunk._accessCount > 0);
    if (--chunk._accessCount == 0)
    {
        // Chunk is not accessed any more by any thread, unpin it and include in LRU list
        _lru.link(&chunk);
    }
}

void CachedStorage::addChunkToCache(DBChunk& chunk)
{
    // Check amount of memory used by cached chunks and discard least recently used
    // chunks from the cache
    _mutex.checkForDeadlock();
    while (_cacheUsed + chunk._hdr.size > _cacheSize)
    {
        if (_lru.isEmpty())
        {
            if (_strictCacheLimit && _cacheUsed != 0)
            {
                Event::ErrorChecker noopEc;
                _cacheOverflowFlag = true;
                _cacheOverflowEvent.wait(_mutex, noopEc);
            }
            else
            {
                break;
            }
        }
        freeChunk(*_lru._prev);
    }
    _cacheUsed += chunk._hdr.size;
}

DBChunk* CachedStorage::lookupChunk(ArrayDesc const& desc, StorageAddress const& addr)
{
    ScopedMutexLock cs(_mutex);
    ChunkMap::const_iterator iter = _chunkMap.find(desc.getUAId());
    if (iter != _chunkMap.end())
    {
        shared_ptr<InnerChunkMap> const& innerMap = iter->second;
        InnerChunkMap::const_iterator innerIter = innerMap->find(addr);
        if (innerIter != innerMap->end())
        {
            shared_ptr<DBChunk>const& chunk = innerIter->second;
            if(chunk)
            {
                chunk->beginAccess();
                return chunk.get();
            }
        }
    }
    return NULL;
}

void CachedStorage::decompressChunk(Chunk* chunk, CompressedBuffer const& buf)
{
    chunk->allocate(buf.getDecompressedSize());
    PinBuffer scope(buf);
    if (buf.getSize() != buf.getDecompressedSize())
    {
        _compressors[buf.getCompressionMethod()]->decompress(buf.getData(), buf.getSize(), *chunk);
    }
    else
    {
        memcpy(chunk->getData(), buf.getData(), buf.getSize());
    }
}

void CachedStorage::compressChunk(Chunk const* aChunk, CompressedBuffer& buf)
{
    DBChunk& chunk = *(DBChunk*) aChunk;
    int compressionMethod = chunk.getCompressionMethod();
    if (compressionMethod < 0)
        throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_COMPRESS_METHOD_NOT_DEFINED);
    buf.setDecompressedSize(chunk.getSize());
    buf.setCompressionMethod(compressionMethod);
    {
        ScopedMutexLock cs(_mutex);
        if (!chunk.isRaw() && chunk._data != NULL)
        {
            PinBuffer scope(chunk);
            buf.allocate(chunk.getCompressedSize() != 0 ? chunk.getCompressedSize() : chunk.getSize());
            size_t compressedSize = _compressors[compressionMethod]->compress(buf.getData(), chunk);
            if (compressedSize == chunk.getSize())
            {
                memcpy(buf.getData(), chunk._data, compressedSize);
            }
            else if (compressedSize != buf.getSize())
            {
                buf.reallocate(compressedSize);
            }
        }
    }

    if (buf.getData() == NULL)
    { // chunk data is not present in the cache so read compressed data from the disk
        DBChunk const& chunk = *(static_cast<DBChunk const*> (aChunk));
        if (chunk._hdr.pos.hdrPos == 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_ACCESS_TO_RAW_CHUNK) << chunk.getArrayDesc().getName();
        }
        buf.allocate(chunk.getCompressedSize());
        readAll(chunk._hdr.pos, buf.getData(), chunk.getCompressedSize());
    }
}

int CachedStorage::chooseCompressionMethod(DBChunk& chunk, void* buf)
{
    if (chunk._hdr.compressionMethod < 0)
    {
        size_t minSize = chunk.getSize();
        int best = 0;
        for (int i = 0, n = _compressors.size(); i < n; i++)
        {
            size_t compressedSize = _compressors[i]->compress(buf, chunk);
            if (compressedSize < minSize)
            {
                best = i;
                minSize = compressedSize;
            }
        }
        chunk._hdr.compressionMethod = best;
        if (chunk.getAttributeDesc().getDefaultCompressionMethod() != best)
        {
            SystemCatalog::getInstance()->setDefaultCompressionMethod(chunk._addr.arrId, chunk._addr.attId, best);
            chunk.updateArrayDescriptor();
        }
    }
    return chunk._hdr.compressionMethod;
}

inline bool CachedStorage::isResponsibleFor(DBChunk const& chunk, boost::shared_ptr<Query> const& query)
{
    ScopedMutexLock cs(_mutex);
    Query::validateQueryPtr(query);
    assert(chunk._hdr.instanceId < size_t(_nInstances));

    if (chunk._hdr.instanceId == _hdr.instanceId)
    {
        return true;
    }
    if (!query->isPhysicalInstanceDead(chunk._hdr.instanceId))
    {
        return false;
    }
    if (_redundancy == 1)
    {
        return true;
    }
    InstanceID replicas[MAX_REDUNDANCY + 1];
    getReplicasInstanceId(replicas, chunk.getArrayDesc(), chunk.getAddress());
    for (int i = 1; i <= _redundancy; i++)
    {
        if (replicas[i] == _hdr.instanceId)
        {
            return true;
        }
        if (!query->isPhysicalInstanceDead(replicas[i]))
        {
            // instance with this replica is alive
            return false;
        }
    }
    return false;
}

Chunk* CachedStorage::createChunk(ArrayDesc const& desc, StorageAddress const& addr, int compressionMethod)
{
    ScopedMutexLock cs(_mutex);

    assert(desc.getUAId()!=0);
    ChunkMap::iterator iter = _chunkMap.find(desc.getUAId());
    if (iter == _chunkMap.end())
    {
        iter = _chunkMap.insert(make_pair(desc.getUAId(), make_shared <InnerChunkMap> ())).first;
    }
    else if (iter->second->find(addr) != iter->second->end())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_ALREADY_EXISTS);
    }

    shared_ptr<DBChunk>& chunk = (*(iter->second))[addr];
    chunk.reset(new DBChunk());
    chunk->setAddress(addr, compressionMethod);
    chunk->_accessCount = 1; // newly created chunk is pinned
    chunk->_nWriters = 0;
    chunk->_timestamp = ++_timestamp;
    return chunk.get();
}

void CachedStorage::deleteChunk(Chunk& chunk)
{
    ScopedMutexLock cs(_mutex);
    DBChunk& victim = (DBChunk&) chunk;
    CachedStorage::freeChunk(victim);
    assert(victim._cloneOf == NULL);

    ChunkMap::const_iterator iter = _chunkMap.find(victim.getArrayDesc().getUAId());
    if (iter != _chunkMap.end())
    {
        iter->second->erase(victim._addr);
    }
}

void CachedStorage::freeChunk(DBChunk& victim)
{
    if (victim._data != NULL && victim._hdr.pos.hdrPos != 0)
    {
        _cacheUsed -= victim.getSize();
        if (_cacheOverflowFlag)
        {
            _cacheOverflowFlag = false;
            _cacheOverflowEvent.signal();
        }
    }
    if (victim._next != NULL)
    {
        victim.unlink();
    }
    victim.free();
}

void CachedStorage::unlinkChunkClones(DBChunk& chunk)
{
    DBChunk* original = chunk._cloneOf;
    for (size_t j = 0; j < chunk._clones.size(); j++)
    {
        DBChunk* clone = chunk._clones[j];
        clone->_cloneOf = original;
        if (original != NULL)
        {
            original->_clones.push_back(clone);
        }
    }
    if (original != NULL)
    {
        assert(original->getDiskChunk() == original);
        vector<DBChunk*>& clones = original->_clones;
        for (size_t j = 0; j < clones.size(); j++)
        {
            if (clones[j] == &chunk)
            {
                clones.erase(clones.begin() + j);
                break;
            }
        }
    }
}

void CachedStorage::remove(ArrayUAID uaId, ArrayID arrId, uint64_t timestamp)
{
    if (uaId == arrId)
    {
        removeImmutableArray(uaId, timestamp);
    }
    else
    {
        removeMutableArray(uaId, arrId);
    }
}

void CachedStorage::removeImmutableArray(ArrayID arrId, uint64_t timestamp)
{
    ScopedMutexLock cs(_mutex);
    shared_ptr<InnerChunkMap> innerMap;
    ChunkMap::const_iterator iter = _chunkMap.find(arrId);
    if (iter == _chunkMap.end())
    {
        return;
    }
    else
    {
        innerMap = iter->second;
    }
    set<StorageAddress> victims;
    for (InnerChunkMap::iterator i = innerMap->begin(); i != innerMap->end(); ++i)
    {
        StorageAddress const& address = i->first;
        SCIDB_ASSERT(address.arrId == arrId);
        shared_ptr<DBChunk>& chunk = i->second;
        assert(chunk.get());  //there should be no tombstone chunks in an immutable array!
        if(chunk->_timestamp > timestamp)
        {
            if (chunk->_hdr.pos.hdrPos != 0)
            {
                chunk->_hdr.arrId = 0;
                LOG4CXX_TRACE(logger, "ChunkDesc: Free chunk descriptor at position " << chunk->_hdr.pos.hdrPos);
                File::writeAll(_hd, &chunk->_hdr, sizeof(ChunkHeader), chunk->_hdr.pos.hdrPos);
                assert(chunk->_hdr.nCoordinates < MAX_NUM_DIMS_SUPPORTED);
                _freeHeaders.insert(chunk->_hdr.pos.hdrPos);
                if (_hdr.clusterSize != 0)
                {
                    ClusterID cluId = getClusterID(chunk->_hdr.pos);
                    size_t& nChunks = _liveChunksInCluster[cluId];
                    assert(nChunks > 0);
                    if (--nChunks == 0)
                    {
                        freeCluster(cluId);
                        _clusters.erase(arrId);
                    }
                }
            }
            unlinkChunkClones(*chunk);
            CachedStorage::freeChunk(*chunk);
            victims.insert(address);
        }
    }
    File::writeAll(_hd, &_hdr, HEADER_SIZE, 0);
    for(set<StorageAddress>::iterator i = victims.begin(); i != victims.end(); ++i)
    {
        StorageAddress const& address = *i;
        innerMap->erase(address);
    }
    _dbArrayCache.erase(arrId);
    if (innerMap->size() == 0)
    {
        _chunkMap.erase(arrId);
    }
}

void CachedStorage::removeMutableArray(ArrayUAID uaId, ArrayID arrId)
{
    ScopedMutexLock cs(_mutex);
    shared_ptr<InnerChunkMap> innerMap;
    ChunkMap::const_iterator iter = _chunkMap.find(uaId);
    if (iter == _chunkMap.end())
    {
        return;
    }
    else
    {
        innerMap = iter->second;
    }
    vector<StorageAddress> victims;
    for (InnerChunkMap::iterator i = innerMap->begin(); i != innerMap->end(); ++i)
    {
        StorageAddress const& addr = i->first;
        if (addr.arrId != arrId)
        {
            continue;
        }
        shared_ptr<DBChunk>& chunk = i->second;
        if ( chunk )
        {
            unlinkChunkClones(*chunk);
            CachedStorage::freeChunk(*chunk);
        }
        victims.push_back(addr);
    }
    for(vector<StorageAddress>::iterator i = victims.begin(); i != victims.end(); ++i)
    {
       StorageAddress const& address = *i;
       innerMap->erase(address);
    }
    _dbArrayCache.erase(arrId);
    if (innerMap->size() == 0)
    {
       _chunkMap.erase(uaId);
    }
    deleteDescriptorsFor(arrId);
}

void CachedStorage::deleteDescriptorsFor(ArrayID arrId)
{
    //we estimate that 1TB hard drive at 5MB per chunk contains about 200K chunks, which is 200K ChunkDescriptors * 852 bytes per descriptor = 170MB
    //Ideally that's how big the header file can get.
    //But... if there are two hard drives for the instance, with average 1MB chunk size and a bunch of tombstones - we can easily go into the
    //gigabyte range for the size of this file

    //Don't allow this routine to allocate more than MEM_ARRAY_THRESHOLD megabytes. In the future this can be replaced with a more dynamic
    //query->getMemoryLimit() or something like that.
    size_t memLimit = static_cast<size_t>(Config::getInstance()->getOption<int>(CONFIG_MEM_ARRAY_THRESHOLD)) * MB;

    //Allocate and iterate thru the file at most memLimit bytes at a time.
    size_t startEntry = 0;
    size_t endEntry = std::max<size_t> ( std::min<size_t>(_hdr.nChunks, memLimit / sizeof(ChunkDescriptor)), 1);
    while (startEntry != _hdr.nChunks)
    {
        scoped_array<ChunkDescriptor> chunkDescriptors(new ChunkDescriptor[endEntry - startEntry] );
        if (!chunkDescriptors)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_ALLOCATE_MEMORY);
        }
        File::readAll(_hd, &chunkDescriptors[0], (endEntry - startEntry) * sizeof(ChunkDescriptor), HEADER_SIZE + (startEntry * sizeof(ChunkDescriptor)));
        for (size_t i = 0; i < (endEntry - startEntry); i++)
        {
            ChunkDescriptor& chunkDesc = chunkDescriptors[i];
            if(chunkDesc.hdr.arrId == arrId )
            {
                chunkDesc.hdr.arrId = 0;
                assert(chunkDesc.hdr.nCoordinates < MAX_NUM_DIMS_SUPPORTED);
                _freeHeaders.insert(chunkDesc.hdr.pos.hdrPos);
                if ( chunkDesc.hdr.is<ChunkHeader::TOMBSTONE>() == false && _hdr.clusterSize != 0)
                {
                    ClusterID cluId = getClusterID(chunkDesc.hdr.pos);
                    size_t& nChunks = _liveChunksInCluster[cluId];
                    assert(nChunks > 0);
                    if (--nChunks == 0)
                    {
                        freeCluster(cluId);
                        _clusters.erase(arrId);
                    }
                }
            }
        }
        File::writeAll(_hd, &chunkDescriptors[0], (endEntry - startEntry) * sizeof(ChunkDescriptor), HEADER_SIZE + (startEntry * sizeof(ChunkDescriptor)));
        startEntry = endEntry;
        endEntry = std::max( std::min<size_t>(_hdr.nChunks, endEntry + (memLimit / sizeof(ChunkDescriptor))), startEntry + 1);
    }
    File::writeAll(_hd, &_hdr, HEADER_SIZE, 0);
}

void CachedStorage::freeCluster(ClusterID id)
{
    _liveChunksInCluster.erase(id);
    DiskPos clusterPos;
    getClusterPos(clusterPos, id);
    LOG4CXX_DEBUG(logger, "Free cluster " << id << " query " << Query::getCurrentQueryID());
    _freeClusters[clusterPos.segmentNo].insert(clusterPos.offs);
}

int CachedStorage::getRandomSegment()
{
    int i = 0, n = _segments.size() - 1;
    if (n != 0)
    {
        long val = (long) (rand() % _totalAvailable);
        do
        {
            val -= (long) ((_segments[i].size - _hdr.segment[i].used) / GB);
        } while (val > 0 && ++i < n);
    }
    return i;
}

InstanceID CachedStorage::getPrimaryInstanceId(ArrayDesc const& desc, StorageAddress const& address) const
{
    //in this context we have to be careful to use nInstances which was set at the beginning of system lifetime
    //this method must return the same value regardless of whether or not there were failures
    return desc.isLocal() ? getInstanceId() : desc.getChunkNumber(address.coords) % _nInstances;
}

void CachedStorage::getReplicasInstanceId(InstanceID* replicas, ArrayDesc const& desc, StorageAddress const& address) const
{
    replicas[0] = getPrimaryInstanceId(desc, address);
    for (int i = 0; i < _redundancy; i++)
    {
        // A prime number can be used to smear the replicas as follows
        // InstanceID instanceId = (chunk.getArrayDesc().getChunkNumber(chunk.addr.coords) + (i+1)) % PRIME_NUMBER % nInstances;
        // the PRIME_NUMBER needs to be just shy of the number of instances to work, so we would need a table.
        // For Fibonacci no table is required, and it seems to work OK.

        const uint64_t nReplicas = (_redundancy + 1);
        const uint64_t currReplica = (i + 1);
        const uint64_t chunkId = desc.getChunkNumber(address.coords) * (nReplicas) + currReplica;
        InstanceID instanceId = fibHash64(chunkId, MAX_INSTANCE_BITS) % _nInstances;
        for (int j = 0; j <= i; j++)
        {
            if (replicas[j] == instanceId)
            {
                instanceId = (instanceId + 1) % _nInstances;
                j = -1;
            }
        }
        replicas[i + 1] = instanceId;
    }
}

void CachedStorage::recover(InstanceID recoveredInstance, boost::shared_ptr<Query>& query)
{
    ScopedMutexLock cs(_mutex);
    NetworkManager* networkManager = NetworkManager::getInstance();

    for (ChunkMap::const_iterator i = _chunkMap.begin(); i != _chunkMap.end(); ++i)
    {
        shared_ptr<InnerChunkMap> const& innerMap = i->second;
        for (InnerChunkMap::const_iterator j = innerMap->begin(); j != innerMap->end(); ++j)
        {
            shared_ptr<DBChunk> const& chunk = j->second;
            if (chunk && (chunk->_hdr.instanceId == recoveredInstance || isResponsibleFor(*chunk, query)))
            {
                if (chunk->_hdr.instanceId == _hdr.instanceId)
                {
                    InstanceID replicas[MAX_REDUNDANCY + 1];
                    getReplicasInstanceId(replicas, chunk->getArrayDesc(), chunk->getAddress());
                    replicas[0] = recoveredInstance; // barrier
                    int k = _redundancy;
                    while (replicas[k] != recoveredInstance)
                    {
                        k -= 1;
                    }
                    if (k == 0)
                    {
                        continue;
                    }
                }
                else if (chunk->_hdr.instanceId != recoveredInstance)
                {
                    continue;
                }
                size_t compressedSize = chunk->_hdr.compressedSize;
                boost::scoped_array<char> data(new char[compressedSize]);
                readAll(chunk->_hdr.pos, data.get(), compressedSize);

                boost::shared_ptr<CompressedBuffer> buffer = boost::make_shared<CompressedBuffer>();
                buffer->allocate(compressedSize);
                memcpy(buffer->getData(), data.get(), compressedSize);
                data.reset();

                boost::shared_ptr<MessageDesc> chunkMsg = boost::make_shared<MessageDesc>(mtRecoverChunk, buffer);
                boost::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk> ();
                chunkRecord->set_eof(false);
                chunkRecord->set_sparse(chunk->isSparse());
                chunkRecord->set_compression_method(chunk->getCompressionMethod());
                chunkRecord->set_attribute_id(chunk->_addr.attId);
                chunkRecord->set_array_id(chunk->_addr.arrId);
                chunkRecord->set_decompressed_size(chunk->_hdr.size);
                chunkRecord->set_count(0);
                chunkMsg->setQueryID(query->getQueryID());

                for (size_t k = 0; k < chunk->_addr.coords.size(); k++)
                {
                    chunkRecord->add_coordinates(chunk->_addr.coords[k]);
                }
                LOG4CXX_DEBUG(logger, "Recover chunk of array " << chunk->getArrayDesc().getName() << "(" << chunk->_addr.arrId << "), coordinates=" << chunk->_addr.coords);

                networkManager->sendMessage(recoveredInstance, chunkMsg); // wait until message is sent to avoid async queue overflow
            }
        }
    }
}

void CachedStorage::replicate(ArrayDesc const& desc, StorageAddress const& addr, DBChunk* chunk, void const* data, size_t compressedSize, size_t decompressedSize,
                              boost::shared_ptr<Query>& query, vector<boost::shared_ptr<ReplicationManager::Item> >& replicasVec)
{
    ScopedMutexLock cs(_mutex);
    Query::validateQueryPtr(query);

    if (_redundancy <= 0 || (chunk && !isPrimaryReplica(chunk)) || desc.isLocal())
    { // self chunk
        return;
    }
    replicasVec.reserve(_redundancy);
    InstanceID replicas[MAX_REDUNDANCY + 1];
    getReplicasInstanceId(replicas, desc, addr);

    QueryID queryId = query->getCurrentQueryID();
    assert(queryId != 0);
    boost::shared_ptr<MessageDesc> chunkMsg;
    if (chunk && data)
    {
        boost::shared_ptr<CompressedBuffer> buffer = boost::make_shared<CompressedBuffer>();
        buffer->allocate(compressedSize);
        memcpy(buffer->getData(), data, compressedSize);
        chunkMsg = boost::make_shared<MessageDesc>(mtChunkReplica, buffer);
    }
    else
    {
        chunkMsg = boost::make_shared<MessageDesc>(mtChunkReplica);
    }
    chunkMsg->setQueryID(queryId);
    boost::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk> ();
    chunkRecord->set_attribute_id(addr.attId);
    chunkRecord->set_array_id(addr.arrId);
    for (size_t k = 0; k < addr.coords.size(); k++)
    {
        chunkRecord->add_coordinates(addr.coords[k]);
    }
    chunkRecord->set_eof(false);

    if(chunk)
    {
        chunkRecord->set_sparse(chunk->isSparse());
        chunkRecord->set_rle(chunk->isRLE());
        chunkRecord->set_compression_method(chunk->getCompressionMethod());
        chunkRecord->set_decompressed_size(decompressedSize);
        chunkRecord->set_count(0);
        LOG4CXX_TRACE(logger, "Replicate chunk of array ID=" << addr.arrId << " attribute ID=" << addr.attId);
        if (data == NULL)
        {
            assert(chunk->_cloneOf != NULL);
            chunkRecord->set_source_array_id(chunk->_cloneOf->_addr.arrId);
            chunkRecord->set_source_attribute_id(chunk->_cloneOf->_addr.attId);
        }
    }
    else
    {
        chunkRecord->set_tombstone(true);
    }

    for (int i = 1; i <= _redundancy; i++)
    {
        boost::shared_ptr<ReplicationManager::Item> item = make_shared <ReplicationManager::Item>(replicas[i], chunkMsg, query);
        assert(_replicationManager);
        _replicationManager->send(item);
        replicasVec.push_back(item);
    }
}

void CachedStorage::abortReplicas(vector<boost::shared_ptr<ReplicationManager::Item> >* replicasVec)
{
    assert(replicasVec);
    for (size_t i = 0; i < replicasVec->size(); ++i)
    {
        const boost::shared_ptr<ReplicationManager::Item>& item = (*replicasVec)[i];
        assert(_replicationManager);
        _replicationManager->abort(item);
        assert(item->isDone());
    }
}

void CachedStorage::waitForReplicas(vector<boost::shared_ptr<ReplicationManager::Item> >& replicasVec)
{
    for (size_t i = 0; i < replicasVec.size(); ++i)
    {
        const boost::shared_ptr<ReplicationManager::Item>& item = replicasVec[i];
        assert(_replicationManager);
        _replicationManager->wait(item);
        assert(item->isDone());
        assert(item->validate(false));
    }
}

void CachedStorage::writeAll(const DiskPos& pos, const void* data, size_t size)
{
    double t0 = 0, t1 = 0, writeTime = 0;

    if (_writeLogThreshold >= 0)
    {
        t0 = getTimeSecs();
    }
    File::writeAll(_sd[pos.segmentNo], data, size, pos.offs);
    if (_writeLogThreshold >= 0)
    {
        t1 = getTimeSecs();
        writeTime = t1 - t0;
    }

    if (_writeLogThreshold >= 0 && writeTime * 1000 > _writeLogThreshold)
    {
        LOG4CXX_DEBUG(logger, "CWR: pwrite fd "<<_sd[pos.segmentNo]<<" size "<<size<<" time "<<writeTime);
    }
}

void CachedStorage::readAll(const DiskPos& pos, void* data, size_t size)
{
    double t0 = 0, t1 = 0, readTime = 0;
    if (_writeLogThreshold >= 0)
    {
        t0 = getTimeSecs();
    }
    File::readAll(_sd[pos.segmentNo], data, size, pos.offs);
    if (_writeLogThreshold >= 0)
    {
        t1 = getTimeSecs();
        readTime = t1 - t0;
    }
    if (_writeLogThreshold >= 0 && readTime * 1000 > _writeLogThreshold)
    {
        LOG4CXX_DEBUG(logger, "CWR: pwrite fd "<<_sd[pos.segmentNo]<<" size "<<size<<" time "<<readTime);
    }
}

RWLock& CachedStorage::getChunkLatch(DBChunk* chunk)
{
    return _latches[(size_t) chunk->_hdr.pos.offs % N_LATCHES];
}

void CachedStorage::getChunkPositions(ArrayDesc const& desc, boost::shared_ptr<Query> const& query, CoordinateSet& chunkPositions)
{
    StorageAddress readAddress (desc.getId(), 0, Coordinates());
    while(findNextChunk(desc, query, readAddress))
    {
        chunkPositions.insert(readAddress.coords);
    }
}

bool CachedStorage::findNextChunk(ArrayDesc const& desc, boost::shared_ptr<Query> const& query, StorageAddress& address)
{
    ScopedMutexLock cs(_mutex);
    assert(address.attId < desc.getAttributes().size() && address.arrId <= desc.getId());
    Query::validateQueryPtr(query);
    ChunkMap::const_iterator iter = _chunkMap.find(desc.getUAId());
    if (iter == _chunkMap.end())
    {
        address.coords.clear();
        return false;
    }
    shared_ptr<InnerChunkMap> const& innerMap = iter->second;
    if(address.coords.size())
    {
        address.coords[address.coords.size()-1] += desc.getDimensions()[desc.getDimensions().size() - 1].getChunkInterval();
    }
    address.arrId = desc.getId();
    InnerChunkMap::const_iterator innerIter = innerMap->lower_bound(address);
    while (true)
    {
        if (innerIter == innerMap->end() || innerIter->first.attId != address.attId)
        {
            address.coords.clear();
            return false;
        }
        if(innerIter->first.arrId <= desc.getId())
        {
            if(innerIter->second && isResponsibleFor( *(innerIter->second), query))
            {
                address.arrId = innerIter->first.arrId;
                address.coords = innerIter->first.coords;
                return true;
            }
            else
            {
                address.arrId = desc.getId();
                address.coords = innerIter->first.coords;
                address.coords[address.coords.size()-1] += desc.getDimensions()[desc.getDimensions().size() - 1].getChunkInterval();
                innerIter = innerMap->lower_bound(address);
            }
        }
        while(innerIter != innerMap->end() && innerIter->first.arrId > address.arrId && innerIter->first.attId == address.attId)
        {
            ++innerIter;
        }
    }
}

bool CachedStorage::findChunk(ArrayDesc const& desc, boost::shared_ptr<Query> const& query, StorageAddress& address)
{
    ScopedMutexLock cs(_mutex);
    ChunkMap::const_iterator iter = _chunkMap.find(desc.getUAId());
    if (iter == _chunkMap.end())
    {
        address.coords.clear();
        return false;
    }
    shared_ptr<InnerChunkMap> const& innerMap = iter->second;
    address.arrId = desc.getId();
    InnerChunkMap::const_iterator innerIter = innerMap->lower_bound(address);
    if (innerIter == innerMap->end() || innerIter->first.coords != address.coords || innerIter->first.attId != address.attId)
    {
        address.coords.clear();
        return false;
    }

    assert(innerIter->first.arrId <= address.arrId && innerIter->first.coords == address.coords);
    if(innerIter->second && (!query || isResponsibleFor( *(innerIter->second), query)))
    {
        address.arrId = innerIter->first.arrId;
        return true;
    }
    else
    {
        address.coords.clear();
        return false;
    }
}

void CachedStorage::cloneChunk(Coordinates const& pos, ArrayDesc const& targetDesc, AttributeID targetAttrID,
                               ArrayDesc const& sourceDesc, AttributeID sourceAttrID)
{
    StorageAddress addr(sourceDesc.getId(), sourceAttrID, pos);
    Chunk* origChunk = readChunk(sourceDesc, addr);
    UnPinner scope(origChunk);
    addr.arrId = targetDesc.getId();
    addr.attId = targetAttrID;
    cloneChunk(targetDesc, addr, *(DBChunk*) origChunk);
}

Chunk* CachedStorage::cloneChunk(ArrayDesc const& dstDesc, StorageAddress const& addr, DBChunk const& srcChunk, boost::shared_ptr<Query>& query)
{
    vector<boost::shared_ptr<ReplicationManager::Item> > replicasVec;
    boost::function<void()> f = boost::bind(&CachedStorage::abortReplicas, this, &replicasVec);
    Destructor<boost::function<void()> > replicasCleaner(f);
    DBChunk* clone = static_cast<DBChunk*>( cloneChunk(dstDesc, addr, srcChunk));
    replicate(clone->getArrayDesc(), addr, clone, NULL, 0, 0, query, replicasVec);
    waitForReplicas(replicasVec);
    replicasCleaner.disarm();
    return clone;
}

Chunk* CachedStorage::cloneChunk(ArrayDesc const& dstDesc, StorageAddress const& addr, DBChunk const& srcChunk)
{
    ChunkDescriptor cloneDesc;
    Chunk* clone = cloneChunk(dstDesc, addr, srcChunk, cloneDesc);
    assert(cloneDesc.hdr.pos.hdrPos != 0);
    LOG4CXX_TRACE(logger, "ChunkDesc: Create new chunk descriptor clone with size " << sizeof(ChunkDescriptor) << " at position " << cloneDesc.hdr.pos.hdrPos);
    File::writeAll(_hd, &cloneDesc, sizeof(ChunkDescriptor), cloneDesc.hdr.pos.hdrPos);
    return clone;
}

Chunk* CachedStorage::cloneChunk(ArrayDesc const& dstDesc, StorageAddress const& addr, DBChunk const& srcChunk, ChunkDescriptor& cloneDesc)
{
    ScopedMutexLock cs(_mutex);
    DBChunk& chunk = *(DBChunk*) createChunk(dstDesc, addr, srcChunk.getCompressionMethod());
    int nCoordinates = addr.coords.size();
    chunk._accessCount -= 1; // newly created chunks has accessCount == 1
    chunk._raw = false;

    // Write chunk descriptor in storage header
    chunk._hdr = srcChunk._hdr;
    chunk._hdr.arrId = addr.arrId;
    chunk._hdr.attId = addr.attId;
    if (_freeHeaders.empty())
    {
        chunk._hdr.pos.hdrPos = _hdr.currPos;
        _hdr.currPos += sizeof(ChunkDescriptor);
        _hdr.nChunks += 1;
        // Update storage header
        File::writeAll(_hd, &_hdr, HEADER_SIZE, 0);
    }
    else
    {
        set<uint64_t>::iterator i = _freeHeaders.begin();
        chunk._hdr.pos.hdrPos = *i;
        assert(chunk._hdr.pos.hdrPos != 0);
        _freeHeaders.erase(i);
    }
    if (_hdr.clusterSize != 0)
    {
        _liveChunksInCluster[getClusterID(chunk._hdr.pos)] += 1;
    }

    chunk._cloneOf = (DBChunk*) &srcChunk;
    chunk._cloneOf->_clones.push_back(&chunk);
    cloneDesc.hdr = chunk._hdr;
    for (int i = 0; i < nCoordinates; i++)
    {
        cloneDesc.coords[i] = addr.coords[i];
    }

    return &chunk;
}

void CachedStorage::cleanChunk(DBChunk* chunk)
{
    ScopedMutexLock cs(_mutex);
    if ((--chunk->_accessCount) == 0) {
        chunk->free();
    }
    notifyChunkReady(*chunk);
}

void CachedStorage::writeChunk(Chunk* newChunk, boost::shared_ptr<Query>& query)
{
    //XXX TODO: consider locking mutex here to avoid writing replica chunks for a rolled-back query
    DBChunk& chunk = *(DBChunk*) newChunk;

    // To deal with exceptions: unpin and free
    boost::function<void()> func = boost::bind(&CachedStorage::cleanChunk, this, &chunk);
    Destructor<boost::function<void()> > chunkCleaner(func);

    Query::validateQueryPtr(query);

    const size_t bufSize = chunk.getSize();
    boost::scoped_array<char> buf(new char[bufSize]);
    if (!buf) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_ALLOCATE_MEMORY);
    }
    currentStatistics->allocatedSize += bufSize;
    currentStatistics->allocatedChunks++;

    ArrayDesc const& desc = chunk.getArrayDesc();
    VersionID dstVersion = desc.getVersionId();
    void const* deflated = buf.get();
    int nCoordinates = chunk._addr.coords.size();
    size_t compressedSize = _compressors[chooseCompressionMethod(chunk, buf.get())]->compress(buf.get(), chunk);
    assert(compressedSize <= chunk.getSize());
    if (compressedSize == chunk.getSize())
    { // no compression
        deflated = chunk._data;
    }
    vector<boost::shared_ptr<ReplicationManager::Item> > replicasVec;
    func = boost::bind(&CachedStorage::abortReplicas, this, &replicasVec);
    Destructor<boost::function<void()> > replicasCleaner(func);
    func.clear();
    replicate(desc, chunk._addr, &chunk, deflated, compressedSize, chunk.getSize(), query, replicasVec);

    {
        ScopedMutexLock cs(_mutex);
        assert(chunk.isRaw()); // new chunk is raw
        Query::validateQueryPtr(query);

        chunk._hdr.compressedSize = compressedSize;
        size_t reserve = chunk.getAttributeDesc().getReserve();
        chunk._hdr.allocatedSize = (reserve != 0 && dstVersion > 1) ? compressedSize + compressedSize * reserve / 100 : compressedSize;

        if (_hdr.clusterSize == 0)
        { // old no-cluster mode
            chunk._hdr.pos.segmentNo = getRandomSegment();
            if (_hdr.segment[chunk._hdr.pos.segmentNo].used + chunk._hdr.allocatedSize > _segments[chunk._hdr.pos.segmentNo].size)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NO_FREE_SPACE);
            }
            chunk._hdr.pos.offs = _hdr.segment[chunk._hdr.pos.segmentNo].used;
            _hdr.segment[chunk._hdr.pos.segmentNo].used += chunk._hdr.allocatedSize;
        }
        else
        {
            if (chunk._hdr.allocatedSize > _hdr.clusterSize)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_SIZE_TOO_LARGE) << chunk._hdr.allocatedSize << _hdr.clusterSize;
            }
            // Choose location of the chunk
            Cluster& cluster = _clusters[chunk._addr.arrId];
            if (cluster.used == 0 || cluster.used + chunk._hdr.allocatedSize > _hdr.clusterSize)
            {
                cluster.pos.segmentNo = getRandomSegment();
                cluster.used = 0;
                if (!_freeClusters[cluster.pos.segmentNo].empty())
                {
                    set<uint64_t>::iterator i = _freeClusters[cluster.pos.segmentNo].begin();
                    cluster.pos.offs = *i;
                    _freeClusters[cluster.pos.segmentNo].erase(i);
                }
                else
                {
                    if (_hdr.segment[cluster.pos.segmentNo].used + _hdr.clusterSize > _segments[cluster.pos.segmentNo].size)
                        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NO_FREE_SPACE);
                    cluster.pos.offs = _hdr.segment[cluster.pos.segmentNo].used;
                    _hdr.segment[cluster.pos.segmentNo].used += _hdr.clusterSize;
                    LOG4CXX_DEBUG(logger, "Allocate new chunk segment " << cluster.pos.offs);
                }
                assert(cluster.pos.offs % _hdr.clusterSize == 0);
            }
            chunk._hdr.pos.segmentNo = cluster.pos.segmentNo;
            chunk._hdr.pos.offs = cluster.pos.offs + cluster.used;
            cluster.used += chunk._hdr.allocatedSize;
            _liveChunksInCluster[getClusterID(chunk._hdr.pos)] += 1;
        }

        if (_freeHeaders.empty())
        {
            chunk._hdr.pos.hdrPos = _hdr.currPos;
            _hdr.currPos += sizeof(ChunkDescriptor);
            _hdr.nChunks += 1;
        }
        else
        {
            set<uint64_t>::iterator i = _freeHeaders.begin();
            chunk._hdr.pos.hdrPos = *i;
            assert(chunk._hdr.pos.hdrPos != 0);
            _freeHeaders.erase(i);
        }

        // Write ahead UNDO log
        if (dstVersion != 0)
        {
            TransLogRecord transLogRecord[2];
            transLogRecord->arrayUAID = desc.getUAId();
            transLogRecord->arrayId = chunk._addr.arrId;
            transLogRecord->version = dstVersion;
            transLogRecord->hdr = chunk._hdr;
            transLogRecord->oldSize = 0;
            transLogRecord->hdrCRC = calculateCRC32(transLogRecord, sizeof(TransLogRecordHeader));
            memset(&transLogRecord[1], 0, sizeof(TransLogRecord)); // end of log marker

            if (_logSize + sizeof(TransLogRecord) > _logSizeLimit)
            {
                _logSize = 0;
                _currLog ^= 1;
            }
            LOG4CXX_TRACE(logger, "ChunkDesc: Write in log chunk header " << transLogRecord->hdr.pos.offs << " at position " << _logSize);

            File::writeAll(_log[_currLog], transLogRecord, sizeof(TransLogRecord) * 2, _logSize);
            _logSize += sizeof(TransLogRecord);
        }

        // Update value count in Chunk Header
        if(chunk.isRLE() &&
           chunk.getCompressionMethod() == CompressorFactory::NO_COMPRESSION) {
            if (chunk.getAttributeDesc().isEmptyIndicator()) {
                ConstRLEEmptyBitmap bitmap(static_cast<const char*>(deflated));
                chunk._hdr.nElems = bitmap.count();
            } else {
                ConstRLEPayload payload(static_cast<const char*>(deflated));
                chunk._hdr.nElems = payload.count();
            }
            // Update sparse flag
            chunk.setSparse(chunk._hdr.nElems <
                            chunk.getNumberOfElements(true) *
                            Config::getInstance()->
                            getOption<double>(CONFIG_SPARSE_CHUNK_THRESHOLD));
        }

        // Write chunk data
        writeAll(chunk._hdr.pos, deflated, compressedSize);
        buf.reset();

        // Write chunk descriptor in storage header
        ChunkDescriptor desc;
        desc.hdr = chunk._hdr;
        for (int i = 0; i < nCoordinates; i++)
        {
            desc.coords[i] = chunk._addr.coords[i];
        }
        assert(chunk._hdr.pos.hdrPos != 0);

        LOG4CXX_TRACE(logger, "ChunkDesc: Write chunk descriptor at position " << chunk._hdr.pos.hdrPos);
        if (logger->isTraceEnabled())
        {
            LOG4CXX_TRACE(logger, "Chunk descriptor to write: " << desc.toString());
        }

        File::writeAll(_hd, &desc, sizeof(ChunkDescriptor), chunk._hdr.pos.hdrPos);

        // Update storage header
        File::writeAll(_hd, &_hdr, HEADER_SIZE, 0);

        InjectedErrorListener<WriteChunkInjectedError>::check();

        if (isPrimaryReplica(&chunk)) {
            chunkCleaner.disarm();
            chunk.unPin();
            notifyChunkReady(chunk);
            addChunkToCache(chunk);
        } // else chunkCleaner will dec accessCount and free
    }
    waitForReplicas(replicasVec);
    replicasCleaner.disarm();
}

void CachedStorage::removeDeadChunks(ArrayDesc const& arrayDesc, set<Coordinates, CoordinatesLess> const& liveChunks, boost::shared_ptr<Query>& query)
{
    ScopedMutexLock cs(_mutex);
    StorageAddress readAddress (arrayDesc.getId(), 0, Coordinates());
    while(findNextChunk(arrayDesc, query, readAddress))
    {
        if(liveChunks.count(readAddress.coords) == 0)
        {
            SCIDB_ASSERT( getPrimaryInstanceId(arrayDesc, readAddress) == _hdr.instanceId );
            removeChunkVersion(arrayDesc, readAddress.coords, query);
        }
    }
}

void CachedStorage::removeChunkVersion(ArrayDesc const& arrayDesc, Coordinates const& coords, shared_ptr<Query>& query)
{
    vector<boost::shared_ptr<ReplicationManager::Item> > replicasVec;
    boost::function<void()> f = boost::bind(&CachedStorage::abortReplicas, this, &replicasVec);
    Destructor<boost::function<void()> > replicasCleaner(f);
    StorageAddress addr(arrayDesc.getId(), 0, coords);
    replicate(arrayDesc, addr, 0, 0, 0, 0, query, replicasVec);
    removeLocalChunkVersion(arrayDesc, coords);
    waitForReplicas(replicasVec);
    replicasCleaner.disarm();
}

void CachedStorage::removeLocalChunkVersion(ArrayDesc const& arrayDesc, Coordinates const& coords)
{
    ScopedMutexLock cs(_mutex);
    assert(arrayDesc.getUAId() != arrayDesc.getId()); //Immutable arrays NEVER have tombstones
    VersionID dstVersion = arrayDesc.getVersionId();
    ChunkDescriptor tombstoneDesc;
    tombstoneDesc.hdr.flags = 0;
    tombstoneDesc.hdr.set<ChunkHeader::TOMBSTONE>(true);
    tombstoneDesc.hdr.arrId = arrayDesc.getId();
    tombstoneDesc.hdr.nCoordinates = coords.size();
    tombstoneDesc.hdr.instanceId = getPrimaryInstanceId(arrayDesc, StorageAddress(arrayDesc.getId(), 0, coords));
    tombstoneDesc.hdr.allocatedSize = 0;
    tombstoneDesc.hdr.compressedSize = 0;
    tombstoneDesc.hdr.size = 0;
    tombstoneDesc.hdr.nElems = 0;
    tombstoneDesc.hdr.compressionMethod = 0;
    tombstoneDesc.hdr.pos.segmentNo = 0;
    tombstoneDesc.hdr.pos.offs = 0;
    for (int i = 0; i <  tombstoneDesc.hdr.nCoordinates; i++)
    {
        tombstoneDesc.coords[i] = coords[i];
    }
    //WAL
    TransLogRecord transLogRecord[2];
    transLogRecord->arrayUAID = arrayDesc.getUAId();
    transLogRecord->arrayId = arrayDesc.getId();
    transLogRecord->version = dstVersion;
    transLogRecord->oldSize = 0;
    memset(&transLogRecord[1], 0, sizeof(TransLogRecord)); // end of log marker
    ChunkMap::iterator iter = _chunkMap.find(arrayDesc.getUAId());
    if(iter == _chunkMap.end())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Attempt to create tombstone for unexistent array";
    }
    shared_ptr<InnerChunkMap> inner = iter->second;
    for (AttributeID i =0; i<arrayDesc.getAttributes().size(); i++)
    {
        tombstoneDesc.hdr.attId = i;
        StorageAddress addr (arrayDesc.getId(), i, coords);
        if( (*inner)[addr].get() != NULL)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_ALREADY_EXISTS);
        }
        if (_freeHeaders.empty())
        {
            tombstoneDesc.hdr.pos.hdrPos = _hdr.currPos;
            _hdr.currPos += sizeof(ChunkDescriptor);
            _hdr.nChunks += 1;
        }
        else
        {
            set<uint64_t>::iterator i = _freeHeaders.begin();
            tombstoneDesc.hdr.pos.hdrPos = *i;
            assert( tombstoneDesc.hdr.pos.hdrPos != 0);
            _freeHeaders.erase(i);
        }
        transLogRecord->hdr = tombstoneDesc.hdr;
        transLogRecord->hdrCRC = calculateCRC32(transLogRecord, sizeof(TransLogRecordHeader));
        if (_logSize + sizeof(TransLogRecord) > _logSizeLimit)
        {
            _logSize = 0;
            _currLog ^= 1;
        }
        LOG4CXX_TRACE(logger, "ChunkDesc: Write in log chunk tombstone header " << transLogRecord->hdr.pos.offs << " at position " << _logSize);
        File::writeAll(_log[_currLog], transLogRecord, sizeof(TransLogRecord) * 2, _logSize);
        _logSize += sizeof(TransLogRecord);
        LOG4CXX_TRACE(logger, "ChunkDesc: Write chunk tombstone descriptor at position " <<  tombstoneDesc.hdr.pos.hdrPos);
        if (logger->isTraceEnabled())
        {
            LOG4CXX_TRACE(logger, "Chunk tombstone descriptor to write: " << tombstoneDesc.toString());
        }
        File::writeAll(_hd, &tombstoneDesc, sizeof(ChunkDescriptor), tombstoneDesc.hdr.pos.hdrPos);
    }
    File::writeAll(_hd, &_hdr, HEADER_SIZE, 0);
    InjectedErrorListener<WriteChunkInjectedError>::check();
}

void CachedStorage::rollback(std::map<ArrayID, VersionID> const& undoUpdates)
{
    ScopedMutexLock cs(_mutex);
    for (int i = 0; i < 2; i++)
    {
        uint64_t pos = 0;
        TransLogRecord transLogRecord;
        while (true)
        {
            size_t rc = ::pread(_log[i], &transLogRecord, sizeof(TransLogRecord), pos);
            if (rc != sizeof(TransLogRecord) || transLogRecord.arrayUAID == 0)
            {
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
            std::map<ArrayID, VersionID>::const_iterator it = undoUpdates.find(transLogRecord.arrayUAID);
            VersionID lastVersionID = -1;
            if (it != undoUpdates.end() && (lastVersionID = it->second) < transLogRecord.version)
            {
                if (transLogRecord.oldSize != 0)
                {
                    if (_chunkMap.size() != 0)
                    {
                        ChunkMap::iterator iter = _chunkMap.find(transLogRecord.arrayUAID);
                        if (iter != _chunkMap.end())
                        {
                            StorageAddress addr;
                            addr.arrId = transLogRecord.arrayId;
                            addr.attId = transLogRecord.hdr.attId;
                            addr.coords.resize(transLogRecord.hdr.nCoordinates);
                            File::readAll(_hd, &addr.coords[0], transLogRecord.hdr.nCoordinates * sizeof(Coordinate), transLogRecord.hdr.pos.hdrPos
                                    + sizeof(ChunkHeader));

                            shared_ptr<InnerChunkMap> & innerMap = iter->second;
                            InnerChunkMap::iterator it = innerMap->find(addr);
                            if (it != innerMap->end())
                            {
                                DBChunk* clone = it->second.get();
                                while (true)
                                {
                                    RWLock::ErrorChecker noopEc;
                                    ScopedRWLockWrite cloneWriter(getChunkLatch(clone), noopEc);
                                    clone->_hdr.compressedSize = transLogRecord.hdr.compressedSize;
                                    clone->_hdr.size = transLogRecord.hdr.size;
                                    clone->_hdr.flags = transLogRecord.hdr.flags;
                                    if (clone->_data != NULL)
                                    {
                                        freeChunk(*clone);
                                    }
                                    if (clone->_cloneOf == NULL)
                                    {
                                        break;
                                    }
                                    else
                                    {
                                        clone = clone->_cloneOf;
                                    }
                                }
                            }
                        }
                    }
                    LOG4CXX_DEBUG(logger, "Restore chunk " << transLogRecord.hdr.pos.offs
                            << " size " << transLogRecord.oldSize << " from position " << (pos-sizeof(TransLogRecord)));
                    boost::scoped_array<char> buf(new char[transLogRecord.oldSize]);
                    File::readAll(_log[i], buf.get(), transLogRecord.oldSize, pos);
                    crc = calculateCRC32(buf.get(), transLogRecord.oldSize);
                    if (crc != transLogRecord.bodyCRC)
                    {
                        LOG4CXX_ERROR(logger, "CRC for restored chunk doesn't match at position " << pos << ": " << crc << " vs. expected " << transLogRecord.bodyCRC);
                        break;
                    }
                    writeAll(transLogRecord.hdr.pos, buf.get(), transLogRecord.oldSize);
                    buf.reset();
                    // restore first chunk in clones chain
                    assert(transLogRecord.hdr.pos.hdrPos != 0);
                    LOG4CXX_TRACE(logger, "ChunkDesc: Restore chunk descriptor at position " << transLogRecord.hdr.pos.hdrPos);
                    File::writeAll(_hd, &transLogRecord.hdr, sizeof(ChunkHeader), transLogRecord.hdr.pos.hdrPos);
                    transLogRecord.hdr.pos.hdrPos = transLogRecord.newHdrPos;
                }
                transLogRecord.hdr.arrId = 0; // mark chunk as free
                assert(transLogRecord.hdr.pos.hdrPos != 0);
                LOG4CXX_TRACE(logger, "ChunkDesc: Undo chunk descriptor creation at position " << transLogRecord.hdr.pos.hdrPos);
                File::writeAll(_hd, &transLogRecord.hdr, sizeof(ChunkHeader), transLogRecord.hdr.pos.hdrPos);
            }
            pos += transLogRecord.oldSize;
        }
    }
    flush();
}

void CachedStorage::doTxnRecoveryOnStartup()
{
    list<shared_ptr<SystemCatalog::LockDesc> > coordLocks;
    list<shared_ptr<SystemCatalog::LockDesc> > workerLocks;

    SystemCatalog::getInstance()->readArrayLocks(getInstanceId(), coordLocks, workerLocks);

    shared_ptr<map<ArrayID, VersionID> > arraysToRollback = make_shared <map<ArrayID, VersionID> > ();
    UpdateErrorHandler::RollbackWork collector = bind(&collectArraysToRollback, arraysToRollback, _1, _2, _3);

    // Deal with the  SystemCatalog::LockDesc::COORD type locks first
    for (list<shared_ptr<SystemCatalog::LockDesc> >::const_iterator iter = coordLocks.begin(); iter != coordLocks.end(); ++iter)
    {

        const shared_ptr<SystemCatalog::LockDesc>& lock = *iter;

        if (lock->getLockMode() == SystemCatalog::LockDesc::RM)
        {
            const bool checkLock = false;
            RemoveErrorHandler::handleRemoveLock(lock, checkLock);
        }
        else if (lock->getLockMode() == SystemCatalog::LockDesc::CRT || lock->getLockMode() == SystemCatalog::LockDesc::WR)
        {
            UpdateErrorHandler::handleErrorOnCoordinator(lock, collector);
        }
        else
        {
            assert(lock->getLockMode() == SystemCatalog::LockDesc::RNF ||
                    lock->getLockMode() == SystemCatalog::LockDesc::RD);
        }
    }
    for (list<shared_ptr<SystemCatalog::LockDesc> >::const_iterator iter = workerLocks.begin(); iter != workerLocks.end(); ++iter)
    {

        const shared_ptr<SystemCatalog::LockDesc>& lock = *iter;

        if (lock->getLockMode() == SystemCatalog::LockDesc::CRT || lock->getLockMode() == SystemCatalog::LockDesc::WR)
        {
            const bool checkCoordinatorLock = true;
            UpdateErrorHandler::handleErrorOnWorker(lock, checkCoordinatorLock, collector);
        }
        else
        {
            assert(lock->getLockMode() == SystemCatalog::LockDesc::RNF);
        }
    }
    rollback(*arraysToRollback.get());
    SystemCatalog::getInstance()->deleteArrayLocks(getInstanceId());
}

void CachedStorage::flush()
{
    int rc;
    do
    {
        rc = ::fsync(_hd);
    } while (rc != 0 && errno == EINTR);
    if (rc != 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_OPERATION_FAILED_WITH_ERRNO) << "fsync" << errno;
    }

    for (size_t i = 0, nSegments = _segments.size(); i < nSegments; i++)
    {
        do
        {
            rc = ::fsync(_sd[i]);
        } while (rc != 0 && errno == EINTR);
        if (rc != 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_OPERATION_FAILED_WITH_ERRNO) << "fsync" << errno;
        }
    }
}

Array const& CachedStorage::getDBArray(ArrayID arrayID)
{
    ScopedMutexLock cs(_mutex);
    boost::shared_ptr<DBArray>& arr = _dbArrayCache[arrayID];
    if (!arr)
    {
        boost::shared_ptr<Query> emptyQuery;
        arr = make_shared<DBArray> (arrayID, emptyQuery);
    }
    return *arr;
}

boost::shared_ptr<ArrayIterator> CachedStorage::getArrayIterator(const ArrayDesc& arrDesc, AttributeID attId, boost::shared_ptr<Query>& query)
{
    return boost::shared_ptr<ArrayIterator>(new DBArrayIterator(this, arrDesc, attId, query, true));
}

boost::shared_ptr<ConstArrayIterator> CachedStorage::getConstArrayIterator(const ArrayDesc& arrDesc, AttributeID attId, boost::shared_ptr<Query>& query)
{
    return boost::shared_ptr<ConstArrayIterator>(new DBArrayIterator(this, arrDesc, attId, query, false));
}

void CachedStorage::fetchChunk(DBChunk& chunk)
{
    ChunkInitializer guard(this, chunk);
    if (chunk._hdr.pos.hdrPos == 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_ACCESS_TO_RAW_CHUNK) << chunk.getArrayDesc().getName();
    }
    size_t chunkSize = chunk.getSize();
    chunk.allocate(chunkSize);
    if (chunk.getCompressedSize() != chunkSize)
    {
        const size_t bufSize = chunk.getCompressedSize();
        boost::scoped_array<char> buf(new char[bufSize]);
        currentStatistics->allocatedSize += bufSize;
        currentStatistics->allocatedChunks++;
        if (!buf)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_ALLOCATE_MEMORY);
        readAll(chunk._hdr.pos, buf.get(), chunk.getCompressedSize());
        chunk._loader = pthread_self();
        size_t rc = _compressors[chunk._hdr.compressionMethod]->decompress(buf.get(), chunk.getCompressedSize(), chunk);
        chunk._loader = 0;
        if (rc != chunk.getSize())
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_DECOMPRESS_CHUNK);
        buf.reset();
    }
    else
    {
        readAll(chunk._hdr.pos, chunk._data, chunkSize);
    }
}

void CachedStorage::loadChunk(ArrayDesc const& desc, Chunk* aChunk)
{
    {
        ScopedMutexLock cs(_mutex);
        DBChunk& chunk = *(DBChunk*) aChunk;
        if (chunk._accessCount < 2)
        { // Access count>=2 means that this chunk is already pinned and loaded by some upper frame so access to it may not cause deadlock */
            _mutex.checkForDeadlock();
        }
        if (chunk._raw)
        {
            // Some other thread is already loading the chunk: just wait until it completes
            do
            {
                chunk._waiting = true;
                Semaphore::ErrorChecker ec;
                boost::shared_ptr<Query> query = Query::getQueryByID(Query::getCurrentQueryID(), false, false);
                if (query)
                {
                    ec = bind(&Query::validate, query);
                }
                _loadEvent.wait(_mutex, ec);
            } while (chunk._raw);

            if (chunk._data == NULL)
            {
                chunk._raw = true;
            }
        }
        else
        {
            if (chunk._data == NULL)
            {
                _mutex.checkForDeadlock();
                chunk._raw = true;
                addChunkToCache(chunk);
            }
        }
    }

    DBChunk& chunk = *(static_cast<DBChunk*>(aChunk));
    if (chunk._raw && chunk._loader != pthread_self())
    {
        fetchChunk(chunk);
    }
}

Chunk* CachedStorage::readChunk(ArrayDesc const& desc, StorageAddress const& addr)
{
    DBChunk* chunk = CachedStorage::lookupChunk(desc, addr);
    if (!chunk) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_NOT_FOUND);
    }
    loadChunk(desc, chunk);
    return chunk;
}

InstanceID CachedStorage::getInstanceId() const
{
    return _hdr.instanceId;
}

size_t CachedStorage::getNumberOfInstances() const
{
    return _nInstances;
}

void CachedStorage::setInstanceId(InstanceID id)
{
    _hdr.instanceId = id;
    File::writeAll(_hd, &_hdr, HEADER_SIZE, 0);
}

void CachedStorage::getDiskInfo(DiskInfo& info)
{
    memset(&info, 0, sizeof info);
    info.clusterSize = _hdr.clusterSize;
    info.nSegments = _segments.size();
    for (size_t i = 0; i < _segments.size(); i++)
    {
        info.used += _hdr.segment[i].used;
        info.available += _segments[i].size - _hdr.segment[i].used;
        info.nFreeClusters += _freeClusters[i].size();
    }
}

void CachedStorage::listChunkDescriptors(ListChunkDescriptorsArrayBuilder& builder)
{
    ScopedMutexLock cs(_mutex);
    pair<ChunkDescriptor, bool> element;
    uint64_t chunkPos = HEADER_SIZE;
    for (size_t i = 0; i < _hdr.nChunks; i++, chunkPos += sizeof(ChunkDescriptor))
    {
        File::readAll(_hd, &element.first, sizeof(ChunkDescriptor), chunkPos);
        element.second = _freeHeaders.count(chunkPos);
        builder.listElement(element);
    }
}

void CachedStorage::listChunkMap(ListChunkMapArrayBuilder& builder)
{
    ScopedMutexLock cs(_mutex);
    for (ChunkMap::iterator i = _chunkMap.begin(); i != _chunkMap.end(); ++i)
    {
        ArrayUAID uaid = i->first;
        for (InnerChunkMap::iterator j = i->second->begin(); j != i->second->end(); ++j)
        {
            builder.listElement(ChunkMapEntry(uaid, j->first, j->second.get()));
        }
    }
}

///////////////////////////////////////////////////////////////////
/// DBArrayIterator
///////////////////////////////////////////////////////////////////

CachedStorage::DBArrayIterator::DBArrayIterator(CachedStorage* storage, const ArrayDesc& arrDesc, AttributeID attId, boost::shared_ptr<Query>& query, bool writeMode):
    _currChunk(NULL),
    _storage(storage),
    _arrayDesc(arrDesc),
    _deltaChunk(*this),
    _deltaBitmapChunk(*this),
    _attrDesc(arrDesc.getAttributes()[attId]),
    _address(arrDesc.getId(), attId, Coordinates()),
    _query(query),
    _writeMode(writeMode)
{
    reset();
}

CachedStorage::DBArrayIterator::~DBArrayIterator()
{}

ConstChunk const& CachedStorage::DBArrayIterator::getChunk()
{
    if (end())
    {
        throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NO_CURRENT_CHUNK);
    }
    if (_currChunk == NULL)
    {
        DBChunk* chunk = _storage->lookupChunk(_arrayDesc, _address);
        UnPinner scope(chunk);
        assert(chunk);
        _currChunk = chunk;
    }
    if (_arrayDesc.getVersionId() > 1)
    {
        _deltaChunk.setInputChunk(static_cast<DBChunk*>(_currChunk), _arrayDesc.getVersionId());
        return _deltaChunk;
    }
    return *_currChunk;
}

bool CachedStorage::DBArrayIterator::end()
{
    return _address.coords.size() == 0;
}

void CachedStorage::DBArrayIterator::operator ++()
{
    _currChunk = NULL;
    if (end())
    {
        throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NO_CURRENT_CHUNK);
    }
    bool ret = _storage->findNextChunk(_arrayDesc, _query.lock(), _address);
    if (_writeMode)
    {   //in _writeMode we iterate only over chunks from this exact version
        while (ret && _address.arrId != _arrayDesc.getId())
        {
            ret = _storage->findNextChunk(_arrayDesc, _query.lock(), _address);
        }
    }
}

Coordinates const& CachedStorage::DBArrayIterator::getPosition()
{
    if (end())
    {
        throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NO_CURRENT_CHUNK);
    }
    return _address.coords;
}

bool CachedStorage::DBArrayIterator::setPosition(Coordinates const& pos)
{
    _currChunk = NULL;
    _address.coords = pos;
    _arrayDesc.getChunkPositionFor(_address.coords);
    bool ret = _storage->findChunk(_arrayDesc, _query.lock(), _address);
    if ( !ret || (_writeMode && _address.arrId != _arrayDesc.getId()))
    {
        _address.coords.clear();
        return false;
    }
    return true;
}

void CachedStorage::DBArrayIterator::reset()
{
    _currChunk = NULL;
    _address.coords.clear();
    bool ret = _storage->findNextChunk(_arrayDesc, _query.lock(), _address);
    if (_writeMode)
    {   //in _writeMode we iterate only over chunks from this exact version
        while ( ret && _address.arrId != _arrayDesc.getId())
        {
            ret = _storage->findNextChunk(_arrayDesc, _query.lock(), _address);
        }
    }
}

Chunk& CachedStorage::DBArrayIterator::newChunk(Coordinates const& pos)
{
    assert(_writeMode);
    return newChunk(pos, _attrDesc.getDefaultCompressionMethod());
}

Chunk& CachedStorage::DBArrayIterator::newChunk(Coordinates const& pos, int compressionMethod)
{
    assert(_writeMode);
    _currChunk = NULL;
    _address.coords = pos;
    if (!_arrayDesc.contains(_address.coords))
    {
        _address.coords.clear();
        throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES);
    }
    _arrayDesc.getChunkPositionFor(_address.coords);
    bool ret = _storage->findChunk(_arrayDesc, _query.lock(), _address);
    if(ret && _address.arrId == _arrayDesc.getId())
    {
        _address.coords.clear();
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_ALREADY_EXISTS);
    }
    _address.arrId = _arrayDesc.getId();
    _address.coords = pos;
    _arrayDesc.getChunkPositionFor(_address.coords);
    _currChunk = _storage->createChunk(_arrayDesc, _address, compressionMethod);
    return *_currChunk;
}

void CachedStorage::DBArrayIterator::deleteChunk(Chunk& chunk)
{
    assert(_writeMode);
    _currChunk = NULL;
    _address.coords.clear();
    _storage->deleteChunk(chunk);
}

Chunk& CachedStorage::DBArrayIterator::copyChunk(ConstChunk const& srcChunk, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap)
{
    assert(_writeMode);
    _address.coords = srcChunk.getFirstPosition(false);
    if(_arrayDesc.getVersionId() > 1)
    {
        if(_storage->findChunk(_arrayDesc, _query.lock(), _address))
        {
            if(_address.arrId == _arrayDesc.getId())
            {
                _address.coords.clear();
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_ALREADY_EXISTS);
            }
            else
            {
                DBChunk* dstChunk = _storage->lookupChunk(_arrayDesc, _address);
                UnPinner scope(dstChunk);
                if (srcChunk.getDiskChunk() == dstChunk)
                {
                    // Original chunk was not changed: no need to do anything!
                    return *dstChunk;
                }
                //else new delta code goes here!
            }
        }
    }
    else if (_arrayDesc.getVersionId() == 0)
    { //immutable
        DBChunk const* diskChunk = srcChunk.getDiskChunk();
        if (diskChunk != NULL)
        {
            AttributeDesc const& srcAttrDesc = diskChunk->getAttributeDesc();
            if (diskChunk->_hdr.allocatedSize == diskChunk->_hdr.compressedSize && _attrDesc.isNullable() == srcAttrDesc.isNullable())
            {
                _address.arrId = _arrayDesc.getId();
                shared_ptr<Query> query = getQuery();
                Chunk* clone = _storage->cloneChunk(_arrayDesc, _address, *diskChunk, query);
                if (clone != NULL)
                {
                    _currChunk = clone;
                    return *clone;
                }
            }
        }
    }
    _currChunk = &ArrayIterator::copyChunk(srcChunk, emptyBitmap);
    _address.arrId = _arrayDesc.getId();
    return *_currChunk;
}

///////////////////////////////////////////////////////////////////
/// DeltaChunk
///////////////////////////////////////////////////////////////////

CachedStorage::DeltaChunk::DeltaChunk(DBArrayIterator& iterator) :
    _arrayIterator(iterator), _accessCount(0)
{
}

void CachedStorage::DeltaChunk::setInputChunk(DBChunk* chunk, VersionID ver)
{
    _inputChunk = chunk;
    _version = ver;
    _extracted = false;
}

const Array& CachedStorage::DeltaChunk::getArray() const
{
    return _inputChunk->getArray();
}

const ArrayDesc& CachedStorage::DeltaChunk::getArrayDesc() const
{
    return _inputChunk->getArrayDesc();
}

const AttributeDesc& CachedStorage::DeltaChunk::getAttributeDesc() const
{
    return _inputChunk->getAttributeDesc();
}

int CachedStorage::DeltaChunk::getCompressionMethod() const
{
    return _inputChunk->getCompressionMethod();
}

bool CachedStorage::DeltaChunk::pin() const
{
    ((DeltaChunk*) this)->_accessCount += 1;
    return true;
}

void CachedStorage::DeltaChunk::unPin() const
{
    DeltaChunk& self = *(DeltaChunk*) this;
    if (--self._accessCount == 0)
    {
        self._versionChunk.free();
        self._extracted = false;
    }
}

Coordinates const& CachedStorage::DeltaChunk::getFirstPosition(bool withOverlap) const
{
    return _inputChunk->getFirstPosition(withOverlap);
}

Coordinates const& CachedStorage::DeltaChunk::getLastPosition(bool withOverlap) const
{
    return _inputChunk->getLastPosition(withOverlap);
}

boost::shared_ptr<ConstChunkIterator> CachedStorage::DeltaChunk::getConstIterator(int iterationMode) const
{
    const AttributeDesc* bitmapAttr = _arrayIterator._arrayDesc.getEmptyBitmapAttribute();
    DeltaChunk* self = (DeltaChunk*) this;
    if (bitmapAttr != NULL && bitmapAttr->getId() != _inputChunk->_addr.attId && (_inputChunk->isRLE() || !(iterationMode
            & ConstChunkIterator::NO_EMPTY_CHECK)))
    {
        StorageAddress bitmapAddr(_arrayIterator._arrayDesc.getId(), bitmapAttr->getId(), _inputChunk->_addr.coords);
        _arrayIterator._storage->findChunk(_arrayIterator._arrayDesc, shared_ptr<Query>(), bitmapAddr);
        Chunk* bitmapChunk = _arrayIterator._storage->readChunk(_arrayIterator._arrayDesc, bitmapAddr);
        UnPinner scope(bitmapChunk);
        _arrayIterator._deltaBitmapChunk.setInputChunk((DBChunk*) bitmapChunk, _version);
        self->_versionChunk.setBitmapChunk((Chunk*) &_arrayIterator._deltaBitmapChunk);
    }
    else
    {
        self->_versionChunk.setBitmapChunk(NULL);
    }
    extract();
    return _versionChunk.getConstIterator(iterationMode);
}

DBChunk const* CachedStorage::DeltaChunk::getDiskChunk() const
{
    return _inputChunk;
}

bool CachedStorage::DeltaChunk::isMaterialized() const
{
    return true;
}

bool CachedStorage::DeltaChunk::isSparse() const
{
    extract();
    return _versionChunk.isSparse();
}

bool CachedStorage::DeltaChunk::isRLE() const
{
    extract();
    return _versionChunk.isRLE();
}

boost::shared_ptr<ConstRLEEmptyBitmap> CachedStorage::DeltaChunk::getEmptyBitmap() const
{
    const AttributeDesc* bitmapAttr = _arrayIterator._arrayDesc.getEmptyBitmapAttribute();
    boost::shared_ptr<ConstRLEEmptyBitmap> bitmap;
    if (bitmapAttr != NULL && bitmapAttr->getId() != _inputChunk->_addr.attId && _inputChunk->isRLE())
    {
        StorageAddress bitmapAddr(_arrayIterator._arrayDesc.getId(), bitmapAttr->getId(), _inputChunk->_addr.coords);
        _arrayIterator._storage->findChunk(_arrayIterator._arrayDesc, shared_ptr<Query>(), bitmapAddr);
        DBChunk* bitmapChunk = (DBChunk*) _arrayIterator._storage->readChunk(_arrayIterator._arrayDesc, bitmapAddr);
        UnPinner scope(bitmapChunk);
        RWLock::ErrorChecker noopEc;
        ScopedRWLockRead reader(bitmapChunk->getLatch(), noopEc);
        if (bitmapChunk->isDelta())
        {
            MemChunk tmpChunk;
            tmpChunk.initialize(&bitmapChunk->getArray(), &bitmapChunk->getArrayDesc(), bitmapChunk->getAddress(),
                                bitmapChunk->getCompressionMethod());
            VersionControl::instance->getVersion(tmpChunk, *bitmapChunk, _version);
            bitmap = make_shared <ConstRLEEmptyBitmap> (ConstRLEEmptyBitmap((char*) tmpChunk.getData()));
        }
        else
        {
            bitmap = make_shared <ConstRLEEmptyBitmap> (ConstRLEEmptyBitmap((char*) bitmapChunk->getData()));
        }
    }
    else
    {
        bitmap = ConstChunk::getEmptyBitmap();
    }
    return bitmap;
}

void* CachedStorage::DeltaChunk::getData() const
{
    extract();
    return _versionChunk.getData();
}

size_t CachedStorage::DeltaChunk::getSize() const
{
    extract();
    return _versionChunk.getSize();
}

void CachedStorage::DeltaChunk::compress(CompressedBuffer& buf, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const
{
    extract();
    _versionChunk.compress(buf, emptyBitmap);
}

void CachedStorage::DeltaChunk::extract() const
{
    if (!_extracted)
    {
        DeltaChunk* self = (DeltaChunk*) this;
        self->_versionChunk.initialize(&_inputChunk->getArray(), &_inputChunk->getArrayDesc(), _inputChunk->getAddress(),
                                      _inputChunk->getCompressionMethod());
        RWLock::ErrorChecker noopEc;
        ScopedRWLockRead reader(_inputChunk->getLatch(), noopEc);
        PinBuffer scope(*_inputChunk);
        if (_inputChunk->isDelta())
        {
            VersionControl::instance->getVersion(self->_versionChunk, *_inputChunk, _version);
        }
        else
        {
            self->_versionChunk.setSparse(_inputChunk->isSparse());
            self->_versionChunk.setRLE(_inputChunk->isRLE());
            self->_versionChunk.allocate(_inputChunk->getSize());
            memcpy(self->_versionChunk.getData(), _inputChunk->getData(), _inputChunk->getSize());
        }
        assert(checkChunkMagic(_versionChunk));
        self->_extracted = true;
    }
}

CachedStorage CachedStorage::instance;
Storage* StorageManager::instance = &CachedStorage::instance;


///////////////////////////////////////////////////////////////////
/// DBChunk
///////////////////////////////////////////////////////////////////

DBChunk::DBChunk()
{
    _data = NULL;
    _accessCount = 0;
    _next = _prev = NULL;
    _timestamp = 1;
}

DBChunk::~DBChunk()
{
    free();
}

bool DBChunk::isTemporary() const
{
    return false;
}

size_t DBChunk::count() const
{
    return _hdr.nElems != 0 ? _hdr.nElems : ConstChunk::count();
}

bool DBChunk::isCountKnown() const
{
    return _hdr.nElems != 0 || ConstChunk::isCountKnown();
}

void DBChunk::setCount(size_t count)
{
    _hdr.nElems = count;
}

bool DBChunk::isDelta() const
{
    return _hdr.is<ChunkHeader::DELTA_CHUNK> ();
}

bool DBChunk::isSparse() const
{
    return _hdr.is<ChunkHeader::SPARSE_CHUNK> ();
}

bool DBChunk::isRLE() const
{
    return _hdr.is<ChunkHeader::RLE_CHUNK> ();
}

void DBChunk::setSparse(bool sparse)
{
    _hdr.set<ChunkHeader::SPARSE_CHUNK> (sparse);
}

void DBChunk::setRLE(bool rle)
{
    _hdr.set<ChunkHeader::RLE_CHUNK> (rle);
}

bool DBChunk::isMaterialized() const
{
    return true;
}

DBChunk const* DBChunk::getDiskChunk() const
{
    return this;
}

void DBChunk::updateArrayDescriptor()
{
    _arrayDesc = SystemCatalog::getInstance()->getArrayDesc(_addr.arrId);
}

void DBChunk::truncate(Coordinate lastCoord)
{
    _lastPos[0] = _lastPosWithOverlaps[0] = lastCoord;
}

void DBChunk::write(boost::shared_ptr<Query>& query)
{
    Query::validateQueryPtr(query);
    if (--_nWriters <= 0)
    {
        _storage->writeChunk(this, query);
        _nWriters = 0;
    }
}

Array const& DBChunk::getArray() const
{
    return _storage->getDBArray(_addr.arrId);
}

void DBChunk::init()
{
    _data = NULL;
    _accessCount = 0;
    _hdr.nElems = 0;
    _nWriters = 0;
    _raw = false;
    _waiting = false;
    _next = _prev = NULL;
    _storage = &StorageManager::getInstance();
    _timestamp = 1;
    _cloneOf = NULL;
    _loader = 0;
}

RWLock& DBChunk::getLatch()
{
    return _storage->getChunkLatch(this);
}

void DBChunk::calculateBoundaries()
{
    _lastPos = _lastPosWithOverlaps = _firstPosWithOverlaps = _addr.coords;
    updateArrayDescriptor();
    const ArrayDesc& ad = getArrayDesc();
    _hdr.instanceId = _storage->getPrimaryInstanceId(ad, _addr);
    const Dimensions& dims = ad.getDimensions();
    size_t n = dims.size();
    assert(_addr.coords.size() == n);
    for (size_t i = 0; i < n; i++)
    {
        if (_firstPosWithOverlaps[i] > dims[i].getStart())
        {
            _firstPosWithOverlaps[i] -= dims[i].getChunkOverlap();
        }
        _lastPos[i] = _lastPosWithOverlaps[i] += dims[i].getChunkInterval() - 1;
        if (_lastPos[i] > dims[i].getEndMax())
        {
            _lastPos[i] = dims[i].getEndMax();
        }
        if ((_lastPosWithOverlaps[i] += dims[i].getChunkOverlap()) > dims[i].getEndMax())
        {
            _lastPosWithOverlaps[i] = dims[i].getEndMax();
        }
    }
}

bool DBChunk::isEmpty()
{
    return _next == this;
}

void DBChunk::prune()
{
    _next = _prev = this;
}

void DBChunk::link(DBChunk* elem)
{
    assert((elem->_next == NULL && elem->_prev == NULL) || (elem->_next == elem && elem->_prev == elem));
    elem->_prev = this;
    elem->_next = _next;
    _next = _next->_prev = elem;
}

void DBChunk::unlink()
{
    _next->_prev = _prev;
    _prev->_next = _next;
    prune();
}

void DBChunk::beginAccess()
{
    if (_accessCount++ == 0 && _next != NULL)
    {
        unlink();
    }
}

void DBChunk::setAddress(const StorageAddress& firstElem, int compressionMethod)
{
    init();
    _addr = firstElem;
    _raw = true; // new chunk is not yet initialized
    // initialize disk header of chunk
    _hdr.size = 0;
    _hdr.compressedSize = 0;
    _hdr.compressionMethod = compressionMethod;
    _hdr.arrId = _addr.arrId;
    _hdr.attId = _addr.attId;
    _hdr.nCoordinates = _addr.coords.size();
    _hdr.flags = Config::getInstance()->getOption<bool> (CONFIG_RLE_CHUNK_FORMAT) ? ChunkHeader::RLE_CHUNK : 0;
    _hdr.pos.hdrPos = 0;
    calculateBoundaries();
}

void DBChunk::setAddress(const ChunkDescriptor& desc)
{
    init();
    _hdr = desc.hdr;
    desc.getAddress(_addr);
    calculateBoundaries();
}

const ArrayDesc& DBChunk::getArrayDesc() const
{
    return *_arrayDesc;
}

const AttributeDesc& DBChunk::getAttributeDesc() const
{
    return getArrayDesc().getAttributes()[_addr.attId];
}

int DBChunk::getCompressionMethod() const
{
    return _hdr.compressionMethod;
}

void* DBChunk::getData() const
{
    if (_loader != pthread_self())
    {
        if (!_accessCount)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_NOT_PINNED);
        if (_hdr.pos.hdrPos != 0)
        {
            _storage->loadChunk(getArrayDesc(), (Chunk*) this);
        }
    }
    return _data;
}

size_t DBChunk::getSize() const
{
    return _hdr.size;
}

size_t totalDBChunkAllocatedSize;

void DBChunk::allocate(size_t size)
{
    if (_data)
    {
        __sync_sub_and_fetch(&totalDBChunkAllocatedSize, _hdr.size);
        ::free(_data);
    }
    _hdr.size = size;
    _data = ::malloc(size);
    if (!_data)
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_ALLOCATE_MEMORY);
    __sync_add_and_fetch(&totalDBChunkAllocatedSize, size);
    currentStatistics->allocatedSize += size;
    currentStatistics->allocatedChunks++;
}

void DBChunk::reallocate(size_t size)
{
    _data = ::realloc(_data, size);
    if (!_data)
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_REALLOCATE_MEMORY);
    __sync_add_and_fetch(&totalDBChunkAllocatedSize, size - _hdr.size);
    _hdr.size = size;
    currentStatistics->allocatedSize += size;
    currentStatistics->allocatedChunks++;
}

void DBChunk::free()
{
    //SYSTEM_CHECK(SCIDB_E_INTERNAL_ERROR, accessCount == 0, "Deallocated chunk should not be accessed");
    if (_data)
    {
        __sync_sub_and_fetch(&totalDBChunkAllocatedSize, _hdr.size);
        ::free(_data);
    }
    _data = NULL;
}

Coordinates const& DBChunk::getFirstPosition(bool withOverlap) const
{
    return withOverlap ? _firstPosWithOverlaps : _addr.coords;
}

Coordinates const& DBChunk::getLastPosition(bool withOverlap) const
{
    return withOverlap ? _lastPosWithOverlaps : _lastPos;
}

boost::shared_ptr<ConstChunkIterator> DBChunk::getConstIterator(int iterationMode) const
{
    const AttributeDesc* bitmapAttr = getArrayDesc().getEmptyBitmapAttribute();
    Chunk* bitmapChunk = NULL;
    UnPinner bitmapScope(NULL);
    if (bitmapAttr != NULL && bitmapAttr->getId() != _addr.attId && (isRLE() || !(iterationMode & ConstChunkIterator::NO_EMPTY_CHECK)))
    {
        StorageAddress bitmapAddr(getArrayDesc().getId(), bitmapAttr->getId(), _addr.coords);
        _storage->findChunk(getArrayDesc(), shared_ptr<Query>(), bitmapAddr);
        bitmapChunk = _storage->readChunk(getArrayDesc(), bitmapAddr);
        bitmapScope.set(bitmapChunk);
    }
    pin();
    UnPinner selfScope(const_cast<DBChunk*> (this));
    _storage->loadChunk(getArrayDesc(), (Chunk*) this);
    boost::shared_ptr<Query> emptyQuery;
    boost::shared_ptr<ConstChunkIterator> iterator = boost::shared_ptr<ConstChunkIterator>(
            isRLE() ?
                getAttributeDesc().isEmptyIndicator() ?
                    (ConstChunkIterator*) new RLEBitmapChunkIterator(getArrayDesc(), _addr.attId, (Chunk*) this, bitmapChunk, iterationMode) :
                    (ConstChunkIterator*) new RLEConstChunkIterator(getArrayDesc(), _addr.attId, (Chunk*) this, bitmapChunk, iterationMode)
                :
            isSparse() ?
                (ConstChunkIterator*) new SparseChunkIterator(getArrayDesc(), _addr.attId, (Chunk*) this, bitmapChunk, false, iterationMode, emptyQuery) :
                (ConstChunkIterator*) new MemChunkIterator(getArrayDesc(), _addr.attId, (Chunk*) this, bitmapChunk, false, iterationMode, emptyQuery)
    );
    return iterator;
}

boost::shared_ptr<ChunkIterator> DBChunk::getIterator(boost::shared_ptr<Query> const& query, int iterationMode)
{
    const AttributeDesc* bitmapAttr = getArrayDesc().getEmptyBitmapAttribute();
    Chunk* bitmapChunk = NULL;
    UnPinner bitmapScope(NULL);
    if (bitmapAttr != NULL && bitmapAttr->getId() != _addr.attId && !(iterationMode & ConstChunkIterator::NO_EMPTY_CHECK))
    {
        StorageAddress bitmapAddr(getArrayDesc().getId(), bitmapAttr->getId(), _addr.coords);
        bitmapChunk = _storage->createChunk(getArrayDesc(), bitmapAddr, bitmapAttr->getDefaultCompressionMethod());
        bitmapScope.set(bitmapChunk);
        if ((iterationMode & ChunkIterator::SPARSE_CHUNK) || isSparse())
        {
            bitmapChunk->setSparse(true);
        }
    }
    _nWriters += 1;
    boost::shared_ptr<ChunkIterator> iterator = boost::shared_ptr<ChunkIterator>(
        isRLE() ?
            (ChunkIterator*) new RLEChunkIterator(getArrayDesc(), _addr.attId, this, bitmapChunk, iterationMode, query) :
            ((((iterationMode & ChunkIterator::SPARSE_CHUNK) || isSparse())/* && !getAttributeDesc().isEmptyIndicator()*/) ?
                (ChunkIterator*) new SparseChunkIterator(getArrayDesc(), _addr.attId, this, bitmapChunk, true, iterationMode, query) :
                (ChunkIterator*) new MemChunkIterator(getArrayDesc(), _addr.attId, this, bitmapChunk, true, iterationMode, query)));
    return iterator;
}

boost::shared_ptr<ConstRLEEmptyBitmap> DBChunk::getEmptyBitmap() const
{
    const AttributeDesc* bitmapAttr = getArrayDesc().getEmptyBitmapAttribute();
    boost::shared_ptr<ConstRLEEmptyBitmap> bitmap;
    if (bitmapAttr != NULL && bitmapAttr->getId() != _addr.attId && isRLE())
    {
        StorageAddress bitmapAddr(getArrayDesc().getId(), bitmapAttr->getId(), _addr.coords);
        _storage->findChunk(getArrayDesc(), shared_ptr<Query>(), bitmapAddr);
        Chunk* bitmapChunk = _storage->readChunk(getArrayDesc(), bitmapAddr);
        UnPinner scope(bitmapChunk);
        bitmap = make_shared<ConstRLEEmptyBitmap> (*bitmapChunk);
    }
    else
    {
        bitmap = ConstChunk::getEmptyBitmap();
    }
    return bitmap;
}

bool DBChunk::pin() const
{
    _storage->pinChunk(this);
    currentStatistics->pinnedSize += getSize();
    currentStatistics->pinnedChunks++;
    return true;
}

void DBChunk::unPin() const
{
    _storage->unpinChunk(this);
}

void DBChunk::compress(CompressedBuffer& buf, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const
{
    if (emptyBitmap && isRLE())
    {
        MemChunk closure;
        closure.initialize(*this);
        makeClosure(closure, emptyBitmap);
        closure.compress(buf, emptyBitmap);
    }
    else
    {
        PinBuffer scope(*this);
        _storage->compressChunk(this, buf);
    }
}

void DBChunk::decompress(CompressedBuffer const& buf)
{
    _storage->decompressChunk(this, buf);
}

}
