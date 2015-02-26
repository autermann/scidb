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
 * PhysicalFlipStore.cpp
 *
 *  Created on: Apr 16, 2010
 *      Author: Knizhnik
 */

//
// The macro defintions below are used two switch on 64-bit IO mode
//
#include <util/FileIO.h>
#include <boost/make_shared.hpp>

#include "query/Operator.h"
#include "query/QueryProcessor.h"
#include "query/TypeSystem.h"
#include "query/FunctionLibrary.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "array/DBArray.h"
#include "system/SystemCatalog.h"
#include "network/NetworkManager.h"
#include "smgr/io/Storage.h"
#include "smgr/io/DimensionIndex.h"
#include "util/iqsort.h"
#include <log4cxx/logger.h>

namespace scidb {

using namespace std;
using namespace boost;

#define FLIP        (1U << 31)
#define SYNTHETIC   (1U << 30)
//#define SEND_ACK    true

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.redimension_store"));

struct TupleHeader
{
    uint32_t   size;
    uint16_t   overlapMask;
    uint16_t   overlapDir;
    Coordinate coords[1];
};

struct PartialChunk
{
    size_t segmentNo;
    size_t size;

    PartialChunk(size_t segNo, size_t firstElemSize) {
        segmentNo = segNo;
        size = firstElemSize;
    }

    PartialChunk() {
        size = 0;
    }
};

class ChunkCoordComparator
{
    Dimensions const& dims;

  public:
    ChunkCoordComparator(Dimensions const& dimensions) : dims(dimensions) {}

    int operator()(TupleHeader* t1, TupleHeader* t2) const {
        for (size_t i = 0, n = dims.size(); i < n; i++) {
            Coordinate c1 = (t1->coords[i] - dims[i].getStart()) / dims[i].getChunkInterval();
            Coordinate c2 = (t2->coords[i] - dims[i].getStart()) / dims[i].getChunkInterval();
            if (c1 != c2) {
                return c1 < c2 ? -1 : 1;
            }
        }
        return 0;
    }
};

class ChunkWithOverlapCoordComparator
{
    Dimensions const& dims;

  public:
    ChunkWithOverlapCoordComparator(Dimensions const& dimensions) : dims(dimensions) {}

    int operator()(TupleHeader* t1, TupleHeader* t2) const {
        for (size_t i = 0, n = dims.size(); i < n; i++) {
            Coordinate c1 = (t1->coords[i] - dims[i].getStart()) / dims[i].getChunkInterval();
            Coordinate c2 = (t2->coords[i] - dims[i].getStart()) / dims[i].getChunkInterval();
            if (t1->overlapMask & (1 << i)) { 
                // may be more efficient form of c1 += (t1->overlapDir & (1 << i)) ? 1 : -1
                c1 += int(((t1->overlapDir >> i) & 1) << 1) - 1;
            } 
            if (t2->overlapMask & (1 << i)) { 
                // may be more efficient form of c2 += (t2->overlapDir & (1 << i)) ? 1 : -1
                c2 += int(((t2->overlapDir >> i) & 1) << 1) - 1;
            }
            if (c1 != c2) {
                return c1 < c2 ? -1 : 1;
            }
        }
        return 0;
    }
};

class TupleCoordComparator
{
    size_t nDims;

  public:
    TupleCoordComparator(size_t nDimensions) : nDims(nDimensions) {}

    int operator()(TupleHeader* t1, TupleHeader* t2) const {
        for (size_t i = 0, n = nDims; i < n; i++) {
            Coordinate c1 = t1->coords[i];
            Coordinate c2 = t2->coords[i];
            if (c1 != c2) {
                return c1 < c2 ? -1 : 1;
            }
        }
        return 0;
    }
};

static const size_t sortBufSize = 256*1024*1024;
static const size_t writeBufSize = 1024*1024;
static const size_t tupleHeaderSize = sizeof(uint64_t);

void writeSegment(string const& baseName, char* buf, size_t segmentNo, TupleHeader** tuples, size_t nTuples, Dimensions const& dims, char* writeBuf, vector<int>& segments, bool hasOverlap)
{
    std::stringstream ss;
    ss << baseName << segmentNo << ".XXXXXX";
    string path = ss.str();
    char const* fileName = path.c_str();
    int fd  = ::mkstemp((char*)fileName);
    //int fd  = ::open(fileName, O_RDWR|O_TRUNC|O_CREAT|O_LARGEFILE, 0666);
    if (fd < 0) {
        int error = errno;
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CANT_OPEN_FILE) << fileName << error;
    }
    ::unlink(fileName);

    BackgroundFileFlusher::getInstance()->addDescriptor(fd);
    try
    {
        if (hasOverlap) { 
            ChunkWithOverlapCoordComparator comparator(dims);
            iqsort(tuples, nTuples, comparator);
        } else {
            ChunkCoordComparator comparator(dims);
            iqsort(tuples, nTuples, comparator);
        }
        size_t writeBufUsed = 0;
        int64_t segPos = 0;
        for (size_t i = 0; i < nTuples; i++) {
            TupleHeader* hdr = tuples[i];
            size_t tupleSize = hdr->size;
            size_t available = writeBufSize - writeBufUsed >= tupleSize ? tupleSize : writeBufSize - writeBufUsed;
            memcpy(writeBuf + writeBufUsed, hdr, available);
            writeBufUsed += available;
            if (available < tupleSize) {
                assert(writeBufUsed == writeBufSize);
                File::writeAll(fd, writeBuf, writeBufSize, segPos);
                segPos += writeBufSize;
                memcpy(writeBuf, (char*)hdr + available, tupleSize - available);
                writeBufUsed = tupleSize - available;
            }
        }
        File::writeAll(fd, writeBuf, writeBufUsed, segPos);
    }
    catch (...)
    {
        BackgroundFileFlusher::getInstance()->dropDescriptor(fd);
    }
    segments.push_back(fd);
}

void checkChunkElements(ArrayDesc const& dstArrayDesc, Coordinates const& chunkPos, char* src, size_t size)
{
    char* end = src + size;
    size_t nDims = dstArrayDesc.getDimensions().size();
    Attributes const& dstAttrs = dstArrayDesc.getAttributes();
    size_t nAttrs = dstAttrs.size();
    Coordinates dstPos(nDims);
    while (src < end) {
        for (size_t i = 0; i < nDims; i++) {
            dstPos[i] = *(Coordinate*)src;
            src += sizeof(Coordinate);
        }
        dstArrayDesc.getChunkPositionFor(dstPos);
        assert(chunkPos == dstPos);
        for (size_t i = 0; i < nAttrs; i++) {
            if (!dstAttrs[i].isEmptyIndicator()) {
                bool isNull = false;
                if (dstAttrs[i].isNullable()) {
                    isNull = *src++ != 0;
                }
                size_t attrSize = TypeLibrary::getType(dstAttrs[i].getType()).byteSize();
                if (attrSize == 0) {
                    if (isNull) {
                        src += sizeof(int);
                    } else {
                        attrSize = *(uint32_t*)src;
                        src += sizeof(uint32_t);
                        src += attrSize;
                    }
                } else {
                    src += attrSize;
                }
            }
        }
    }
    assert(src == end);
}



struct SegmentContainer
{
    vector<int> segments;
    SegmentContainer(): segments(0)
    {}

    ~SegmentContainer()
    {
        BackgroundFileFlusher::getInstance()->dropDescriptors(segments);
        for (size_t i = 0; i < segments.size(); i++)
        {
            ::close(segments[i]);
        }
    }
};

void transformArray(ArrayDesc const& dstArrayDesc, shared_ptr<Array> dstArray,
                    ArrayDesc const& srcArrayDesc, shared_ptr<Array> srcArray,
                    vector<size_t> const& attrMap, vector<size_t> const& dimMap,
                    vector<AggregatePtr> const& aggregates, shared_ptr<Query> query)
{
    time_t start = time(NULL);
    Config* cfg = Config::getInstance();
    size_t bufSize = (size_t)cfg->getOption<int>(CONFIG_MERGE_SORT_BUFFER) << 20;
    if (bufSize == 0) {
        bufSize = sortBufSize;
    }
    vector<char> buf(bufSize);
    vector<char> writeBuf(writeBufSize);

    Attributes srcAttrs = srcArrayDesc.getAttributes();
    Attributes tupleAttrs = dstArrayDesc.getAttributes();
    Dimensions dstDims = dstArrayDesc.getDimensions();
    size_t nAttrs = tupleAttrs.size();
    size_t nDims = dstDims.size();
    size_t nSrcAttrs = srcArrayDesc.getAttributes().size();
    size_t tupleFixedSize = tupleHeaderSize + nDims*sizeof(Coordinate);
    vector<Type> attrTypes(nAttrs);
    vector<Value> attrValues(nAttrs);
    vector<Value> aggregateStates(nAttrs);
    vector<Value> aggregateResults(nAttrs);

    Coordinates dstPos(nDims);

    for (size_t i = 0; i < nAttrs; i++)
    {
        if (aggregates[i].get())
        {
            size_t j = attrMap[i];
            assert(j != SYNTHETIC && !(j & FLIP));
            tupleAttrs[i] = srcAttrs[j];

            aggregateStates[i].setNull(0);
            aggregateResults[i] = Value(aggregates[i]->getResultType());
        }

        attrTypes[i] = TypeLibrary::getType(tupleAttrs[i].getType());
        attrValues[i] = Value(attrTypes[i]);

        if (!tupleAttrs[i].isEmptyIndicator())
        {
            tupleFixedSize += attrTypes[i].byteSize();
            if (tupleAttrs[i].isNullable()) {
                tupleFixedSize += sizeof(bool);
            }
            if (attrTypes[i].variableSize()) {
                tupleFixedSize += sizeof(uint32_t);
            }
        }
    }
    size_t denseChunkSize = tupleFixedSize;
    bool hasOverlap = false;
    for (size_t i = 0; i < nDims; i++) {
        denseChunkSize *= dstDims[i].getChunkInterval();
        hasOverlap |= dstDims[i].getChunkOverlap() != 0;
    }

    NetworkManager* networkManager = NetworkManager::getInstance();
    const size_t nInstances = (size_t)query->getInstancesCount();
    const size_t myInstanceId = (size_t)query->getInstanceID();
    size_t newDim = INVALID_DIMENSION_ID;
    Coordinate newDimStart = MIN_COORDINATE;
    Coordinate newDimEnd = MAX_COORDINATE;

    vector< shared_ptr<AttributeMultiMap> > coordinateMultiIndices(nDims);
    vector< shared_ptr<AttributeMap> > coordinateIndices(nDims);
    for (size_t i = 0; i < nDims; i++)
    {
        size_t j = dimMap[i];
        if (j == SYNTHETIC) 
        {
            newDim = i;
            newDimStart = dstDims[i].getStart();
            newDimEnd = newDimStart + dstDims[i].getChunkInterval() - 1;
        }
        else if ((j & FLIP) && (dstDims[i].getType() != TID_INT64))
        {
            AttributeID attID = (j & ~FLIP);
            string indexMapName = dstArrayDesc.getMappingArrayName(i);
            if (!dstDims[i].isDistinct()) { 
                coordinateMultiIndices[i] = buildSortedMultiIndex(srcArray, attID, query, indexMapName, dstDims[i].getStart(), dstDims[i].getLength());
            } else { 
                coordinateMultiIndices[i] = buildFunctionalMapping(dstDims[i]);
                if (!coordinateMultiIndices[i]) {
                    coordinateIndices[i] = buildSortedIndex(srcArray, attID, query, indexMapName, dstDims[i].getStart(), dstDims[i].getLength());
                }
            }
        }
    }
    LOG4CXX_DEBUG(logger, "Time for building coordinate indices: " << (time(NULL) - start) << " seconds");
    start = time(NULL);

    vector<TupleHeader*> tuples(bufSize/tupleFixedSize);
    size_t nSegments = 0;
    size_t nTuples = 0;
    size_t segmentSize = 0;

    std::stringstream ss;
    string const& tmpDir = cfg->getOption<string>(CONFIG_TMP_DIR);
    ss << tmpDir;
    if (tmpDir.length() != 0 && tmpDir[tmpDir.length()-1] != '/') {
        ss << '/';
    }
    ss << dstArrayDesc.getName();
    ss << '-';
    ss << myInstanceId;
    ss << ':';
    string baseName = ss.str();

    size_t iterAttr = 0;
    bool coordinatesOnly = true;

    vector< shared_ptr<ConstArrayIterator> > arrayIterators(nSrcAttrs);
    vector< shared_ptr<ConstChunkIterator> > chunkIterators(nSrcAttrs);

    vector< shared_ptr<ArrayIterator> > dstArrayIterators(nAttrs);
    vector< shared_ptr<ChunkIterator> > dstChunkIterators(nAttrs);

    for (size_t i = 0; i < nAttrs; i++) {
        size_t j = attrMap[i];

        if (j != SYNTHETIC) {
            if (!(j & FLIP)) {
                if (coordinatesOnly) {
                    coordinatesOnly = false;
                    iterAttr = j;
                }
                arrayIterators[j] = srcArray->getConstIterator(j);
            }
            if (!tupleAttrs[i].isEmptyIndicator()) {
               dstArrayIterators[i] = dstArray->getIterator(i);
            }
        }
    }
    for (size_t i = 0; i < nDims; i++) {
        int j = dimMap[i];
        if (j & FLIP) {
            j &= ~FLIP;
            if (coordinatesOnly) {
                coordinatesOnly = false;
                iterAttr = j;
            }
            arrayIterators[j] = srcArray->getConstIterator(j);
        }
    }
    shared_ptr<ConstArrayIterator> srcArrayIterator =
        coordinatesOnly ? srcArray->getConstIterator(0) : arrayIterators[iterAttr];
    shared_ptr<ConstChunkIterator> srcChunkIterator;
    map<Coordinates, vector<PartialChunk>, CoordinatesLess> myChunks;
    Coordinates chunkPos(nDims);
    Coordinates overlapChunkPos(nDims);
    SegmentContainer c;

    //
    // Loop through input array: merge, partially sort data and store them in segments (files)
    //
    int iterationMode = ConstChunkIterator::IGNORE_OVERLAPS|ConstChunkIterator::IGNORE_EMPTY_CELLS;
    while (!srcArrayIterator->end())
    {
        //
        // Initialize chunk iterators
        //
        if (coordinatesOnly) {
            srcChunkIterator = srcArrayIterator->getChunk().getConstIterator(iterationMode);
        } else {
            for (size_t i = 0; i < nSrcAttrs; i++) {
                if (arrayIterators[i]) {
                    chunkIterators[i] = arrayIterators[i]->getChunk().getConstIterator(iterationMode);
                }
            }
            srcChunkIterator = chunkIterators[iterAttr];
        }

        //
        // Loop through the chunks content
        //
        while (!srcChunkIterator->end()) {
            size_t tupleSize = tupleFixedSize;
            Coordinates const& srcPos = srcChunkIterator->getPosition();
            for (size_t i = 0; i < nAttrs; i++) {
                if (attrTypes[i].variableSize()) {
                    size_t j = attrMap[i];
                    tupleSize += (j & FLIP)
                       ? srcArrayDesc.getOriginalCoordinate(j & ~FLIP, srcPos[j & ~FLIP], query).size()
                       : chunkIterators[j]->getItem().size();
                }
            }
            for (size_t i = 0; i < nDims; i++) {
                size_t j = dimMap[i];
                if (j & FLIP) {
                    Value const& value = chunkIterators[j & ~FLIP]->getItem();
                    if (value.isNull())
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CANT_REDIMENSION_NULL);
                    if (dstDims[i].getType() == TID_INT64) { 
                        dstPos[i] = value.getInt64();
                    } else { 
                        if (coordinateIndices[i]) { 
                            dstPos[i] = coordinateIndices[i]->get(value);
                        } else {
                            dstPos[i] = coordinateMultiIndices[i]->get(value);
                        }
                    }
                } else if (j == SYNTHETIC) {
                    dstPos[i] = newDimStart;
                } else {
                    dstPos[i] = srcPos[j];
                }
            }
            for (size_t i = 0; i < nDims; i++) {
                chunkPos[i] = dstPos[i];
            }
            dstArrayDesc.getChunkPositionFor(chunkPos);

            size_t nOverlappedChunks = 1; 
            uint16_t overlapMask = 0;
            uint16_t overlapDir = 0;
            if (hasOverlap) { 
                size_t nOverlaps = 0;
                for (size_t i = 0; i < nDims; i++) {
                    assert(chunkPos[i] <= dstPos[i]);
                    if (dstPos[i] - chunkPos[i] + dstDims[i].getChunkOverlap() >= dstDims[i].getChunkInterval()
                        && chunkPos[i] + dstDims[i].getChunkInterval() <= dstDims[i].getEndMax()) 
                    {
                        nOverlaps += 1;
                        overlapMask |= 1 << i;                        
                        overlapDir |= 1 << i;                        
                    } else if (dstPos[i] - chunkPos[i] < dstDims[i].getChunkOverlap()
                        && chunkPos[i] > dstDims[i].getStart()) 
                    {
                        nOverlaps += 1;
                        overlapMask |= 1 << i;                        
                    }
                }
                nOverlappedChunks = 1 << nOverlaps;
            }
            if (tupleSize*nOverlappedChunks + segmentSize > bufSize) {
                writeSegment(baseName, &buf[0], ++nSegments, &tuples[0], nTuples, dstDims, &writeBuf[0], c.segments, hasOverlap);
                nTuples = 0;
                segmentSize = 0;
                if (buf.size() < tupleSize*nOverlappedChunks) { 
                    buf.resize(buf.size()*2 < tupleSize*nOverlappedChunks ? tupleSize*nOverlappedChunks : buf.size()*2);
                }
            }
            char* dst = &buf[segmentSize];
            TupleHeader* hdr = (TupleHeader*)dst;
            hdr->size = tupleSize;
            hdr->overlapMask = 0;
            hdr->overlapDir = overlapDir;

            if (tuples.size() < nTuples + nOverlappedChunks) { 
                tuples.resize(tuples.size()*2 < nTuples + nOverlappedChunks ? nTuples + nOverlappedChunks : tuples.size()*2);
            }
            tuples[nTuples++] = hdr;
            segmentSize += tupleSize;

            for (size_t i = 0; i < nDims; i++) {
                hdr->coords[i] = dstPos[i];
            }
            dst += tupleHeaderSize + nDims*sizeof(Coordinate);

            for (size_t i = 0; i < nAttrs; i++) {
                size_t j = attrMap[i];
                if (!tupleAttrs[i].isEmptyIndicator()) {
                    if ((j & FLIP) && tupleAttrs[i].getType() == TID_INT64) {
                        *(Coordinate*)dst = srcPos[j & ~FLIP];
                        dst += sizeof(Coordinate);
                    } else {
                        Value const& value = (j & FLIP)
                            ? srcArrayDesc.getOriginalCoordinate(j & ~FLIP, srcPos[j & ~FLIP], query)
                            : chunkIterators[j]->getItem();
                        bool isNull = false;
                        if (tupleAttrs[i].isNullable()) {
                            *dst++ = isNull = value.isNull();
                        }
                        if (attrTypes[i].variableSize()) {
                            if (isNull) {
                                *(int*)dst = value.getMissingReason();
                                dst += sizeof(int);
                                continue;
                            }
                            *(uint32_t*)dst = value.size();
                            dst += sizeof(uint32_t);
                        } else { 
                            if (isNull) {
                                if (attrTypes[i].byteSize() >= sizeof(int)) {
                                    *(int*)dst = value.getMissingReason();
                                } else {
                                    *dst = (char)value.getMissingReason();
                                }
                            } else {
                                if (value.size() > attrTypes[i].byteSize()) {
                                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TRUNCATION) << value.size() << attrTypes[i].byteSize();
                                }
                            }
                        }
                        if (!isNull) {
                            memcpy(dst, value.data(), value.size());
                            dst += attrTypes[i].variableSize() ? value.size() : attrTypes[i].byteSize();
                        } else {
                            dst += attrTypes[i].byteSize();
                        }
                    }
                }
            }
            assert(tupleSize == size_t(dst - (char*)hdr));

            vector<PartialChunk>& parts = myChunks[chunkPos];
            if (parts.size() == 0 || parts.back().segmentNo != nSegments) {
                parts.push_back(PartialChunk(nSegments, tupleSize));
            } else {
                parts.back().size += tupleSize;
            }

            uint32_t mask = 0;
            while (--nOverlappedChunks != 0) { 
                uint32_t bit = 1 << nDims;
                do { 
                    bit >>= 1;                    
                } while (!((overlapMask&~mask) & bit));
                assert(bit != 0);
                mask &= bit - 1;
                mask |= bit;
                TupleHeader* overlapHdr = (TupleHeader*)&buf[segmentSize];
                memcpy(overlapHdr, hdr, tupleSize);
                tuples[nTuples++] = overlapHdr;
                overlapHdr->overlapMask = mask;
                segmentSize += tupleSize;

                for (size_t i = 0; i < nDims; i++) { 
                    overlapChunkPos[i] = chunkPos[i] + ((mask & (1 << i)) ? (overlapDir & (1 << i)) ? int(dstDims[i].getChunkInterval()) : -int(dstDims[i].getChunkInterval()) : 0);
                }
                vector<PartialChunk>& parts = myChunks[overlapChunkPos];
                if (parts.size() == 0 || parts.back().segmentNo != nSegments) {
                    parts.push_back(PartialChunk(nSegments, tupleSize));
                } else {
                    parts.back().size += tupleSize;
                }
            }
            assert(mask == overlapMask);

            //
            // Advance chunk iterators
            //
            if (coordinatesOnly) {
                ++(*srcChunkIterator);
            } else {
                for (size_t i = 0; i < nSrcAttrs; i++) {
                    if (chunkIterators[i]) {
                        ++(*chunkIterators[i]);
                    }
                }
            }
        }

        //
        // Advance array iterators
        //
        srcChunkIterator.reset();
        if (coordinatesOnly) {
            ++(*srcArrayIterator);
        } else {
            for (size_t i = 0; i < nSrcAttrs; i++) {
                if (arrayIterators[i]) {
                    chunkIterators[i].reset();
                    ++(*arrayIterators[i]);
                }
            }
        }
    }
    if (nTuples != 0) {
        writeSegment(baseName, &buf[0], ++nSegments, &tuples[0], nTuples, dstDims, &writeBuf[0], c.segments, hasOverlap);
    }
    LOG4CXX_DEBUG(logger, "Partial sort time: " << (time(NULL) - start) << " seconds");
    start = time(NULL);

    vector<Coordinate> myChunkCoordinates(myChunks.size()*nDims);
    size_t nc = 0;
    for (map<Coordinates, vector<PartialChunk>, CoordinatesLess>::iterator iter = myChunks.begin();
         iter != myChunks.end();
         ++iter)
    {
        for (size_t i = 0; i < nDims; i++) {
            myChunkCoordinates[nc++] = iter->first[i];
        }
    }
    assert(nc == myChunkCoordinates.size());

    for (size_t i = 0; i < nInstances; i++) {
        if (i != myInstanceId) {
            networkManager->send(i, shared_ptr<SharedBuffer>(new MemoryBuffer(&myChunkCoordinates[0], nc*sizeof(Coordinate))), query);
        }
    }
    map<Coordinates, vector<InstanceID>, CoordinatesLess> chunkInstanceMap;
    for (size_t i = 0; i < nInstances; i++) {
        Coordinate* coords;
        size_t nChunks;
        shared_ptr<SharedBuffer> buf;
        if (i != myInstanceId) {
             buf = networkManager->receive(i, query);
             if (!buf) {
                 continue;
             }
             nChunks = buf->getSize() / (sizeof(Coordinate)*nDims);
             coords = (Coordinate*)buf->getData();
         } else {
             nChunks = nc/nDims;
             coords = &myChunkCoordinates[0];
         }
         while (nChunks-- != 0) {
             chunkInstanceMap[Coordinates(coords, coords+nDims)].push_back(i);
             coords += nDims;
         }
    }
    LOG4CXX_DEBUG(logger, "Time for exchanging chunk map: " << (time(NULL) - start) << " seconds");
    start = time(NULL);

    Coordinates lowBoundary(nDims, MAX_COORDINATE);
    Coordinates highBoundary(nDims, MIN_COORDINATE);
    TupleCoordComparator comparator(nDims);
    Coordinate newDimMax = newDimStart;
            
    for (map<Coordinates, vector<InstanceID>, CoordinatesLess>::iterator iter = chunkInstanceMap.begin();
         iter != chunkInstanceMap.end();
         ++iter)
    {
        size_t targetInstanceId = dstArrayDesc.getChunkNumber(iter->first) % nInstances;
        if (targetInstanceId == myInstanceId) { // will be my chunk
            size_t chunkSize = 0;
            for (size_t i = 0; i < iter->second.size(); i++) {
                if (iter->second[i] == myInstanceId) { // local data
                    vector<PartialChunk>& parts = myChunks[iter->first];
                    for (size_t j = 0; j < parts.size(); j++) {
                        if (chunkSize + parts[j].size > buf.size()) { 
                            buf.resize(chunkSize + parts[j].size);
                        }
                        size_t rc = ::read(c.segments[parts[j].segmentNo], &buf[chunkSize], parts[j].size);
                        if (rc != parts[j].size)
                            throw SYSTEM_EXCEPTION(SCIDB_SE_IO, SCIDB_LE_OPERATION_FAILED) << "read";
                        //checkChunkElements(dstArrayDesc, iter->first, &buf[chunkSize], parts[j].size);
                        chunkSize += rc;
                    }
                } else {
                    shared_ptr<SharedBuffer> cb = networkManager->receive(iter->second[i], query);
#ifdef SEND_ACK
                    networkManager->send(iter->second[i], shared_ptr<SharedBuffer>(new MemoryBuffer(NULL, 0)), query); // send confirmation
#endif
                    if (chunkSize + cb->getSize() > buf.size()) { 
                        buf.resize(chunkSize + cb->getSize());
                    }
                    memcpy(&buf[chunkSize],  cb->getData(), cb->getSize());
                    chunkSize += cb->getSize();
                }
            }
            for (size_t i = 0; i < nDims; i++) {
                dstPos[i] = iter->first[i];
                if (iter->first[i] > highBoundary[i]) {
                    highBoundary[i] = iter->first[i];
                }
                if (iter->first[i] < lowBoundary[i]) {
                    lowBoundary[i] = iter->first[i];
                }
            }
            double density = (double)chunkSize/denseChunkSize;
            bool isSparse = density < cfg->getOption<double>(CONFIG_SPARSE_CHUNK_THRESHOLD);
            int mode = isSparse ? ChunkIterator::SPARSE_CHUNK : 0;
            if (newDim != INVALID_DIMENSION_ID) { 
                mode |= ChunkIterator::SEQUENTIAL_WRITE;
            }
            Coordinate newDimPos = newDimStart;

            for (size_t i = 0; i < nAttrs; i++)
            {
                aggregateStates[i].setNull(0);
                if (dstArrayIterators[i])
                {
                    Chunk& chunk = dstArrayIterators[i]->newChunk(iter->first);
                    chunk.setExpectedDensity(density);
                    dstChunkIterators[i] = chunk.getIterator(query, mode);
                    mode |= ChunkIterator::NO_EMPTY_CHECK;
                }
            }

            size_t count = 0;
            char* beg = &buf[0];
            char* end = beg + chunkSize;

            nTuples = 0;
            for (char* src = beg; src < end; src += ((TupleHeader*)src)->size) {
                if (tuples.size() <= nTuples) { 
                    tuples.resize(tuples.size() * 2);
                }
                tuples[nTuples++] = (TupleHeader*)src;
            }
            iqsort(&tuples[0], nTuples, comparator);

            vector<size_t> aggCt(nAttrs,0);
            for (size_t t = 0; t < nTuples; t++)
            {
                TupleHeader* hdr = tuples[t];
                bool newPosition = count == 0;
                for (size_t i = 0; i < nDims; i++)
                {
                    if (i != newDim) { 
                        Coordinate c = hdr->coords[i];
                        if (c != dstPos[i])
                        {
                            newPosition = true;
                            if (c > highBoundary[i]) { 
                                highBoundary[i] = c;
                            }
                            if (newDimPos-1 > newDimMax) { 
                                newDimMax = newDimPos-1;
                            }
                            newDimPos = newDimStart;
                        }
                        dstPos[i] = c;
                    }
                }

                if (count != 0 && newPosition)
                {
                    for (size_t j = 0; j < nAttrs; j++)
                    {
                        if(aggregates[j].get() && aggCt[j] > 0)
                        {
                            aggregates[j]->finalResult(aggregateResults[j], aggregateStates[j]);
                            dstChunkIterators[j]->writeItem(aggregateResults[j]);
                            aggregateStates[j].setNull();
                            aggCt[j] = 0;
                        }
                    }
                }

                char* src = (char*)hdr + tupleHeaderSize + nDims*sizeof(Coordinate);
                count += 1;
                if (newDim != INVALID_DIMENSION_ID) { 
                    if (newDimPos > newDimEnd) { 
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_REDIMENSION_STORE_ERROR7);                        
                    }
                    dstPos[newDim] = newDimPos++;
                    newPosition = true;
                }
                for (size_t i = 0; i < nAttrs; i++)
                {
                    if (!tupleAttrs[i].isEmptyIndicator())
                    {
                        bool isNull = false;
                        if (tupleAttrs[i].isNullable()) {
                            isNull = *src++ != 0;
                        }
                        size_t attrSize = attrTypes[i].byteSize();
                        if (attrSize == 0) {
                            if (isNull) {
                                attrValues[i].setNull(*(int*)src);
                                src += sizeof(int);
                            } else {
                                attrSize = *(uint32_t*)src;
                                src += sizeof(uint32_t);
                                attrValues[i].setData(src, attrSize);
                                src += attrSize;
                            }
                        } else {
                            if (isNull) {
                                attrValues[i].setNull(attrSize >= sizeof(int) ? *(int*)src : *src);
                                assert(attrValues[i].getMissingReason() >= 0);
                            } else {
                                attrValues[i].setData(src, attrSize);
                            }
                            src += attrSize;
                        }

                        if (!dstChunkIterators[i]->setPosition(dstPos))
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";

                        if (aggregates[i].get())
                        {
                            if ( !((aggregates[i]->ignoreNulls() && attrValues[i].isNull()) ||
                                   (aggregates[i]->ignoreZeroes() && attrValues[i].isZero())))
                            {
                                if (aggregateStates[i].getMissingReason()==0)
                                {
                                    aggregates[i]->initializeState(aggregateStates[i]);
                                }
                                aggregates[i]->accumulate(aggregateStates[i], attrValues[i]);
                            }
                            aggCt[i]++;
                        }
                        else
                        {
                            if (newPosition) {
                                dstChunkIterators[i]->writeItem(attrValues[i]);
                            }
                        }
                    }
                }
            }
            for (size_t i = 0; i < nAttrs; i++)
            {
                if (dstChunkIterators[i])
                {
                    if (aggregates[i].get())
                    {
                        aggregates[i]->finalResult(aggregateResults[i], aggregateStates[i]);
                        dstChunkIterators[i]->writeItem(aggregateResults[i]);
                        aggregateStates[i].setNull(0);
                        aggCt[i] = 0;
                    }

                    ((Chunk&)dstChunkIterators[i]->getChunk()).setCount(count);
                    dstChunkIterators[i]->flush();
                    dstChunkIterators[i].reset();
                }
            }
        } else { // chunk will belong to other instance
            for (size_t i = 0; i < iter->second.size(); i++) {
                if (iter->second[i] == myInstanceId) { // local data
                    vector<PartialChunk>& parts = myChunks[iter->first];
                    size_t chunkSize = 0;
                    for (size_t j = 0; j < parts.size(); j++) {
                        if (chunkSize + parts[j].size > buf.size()) { 
                            buf.resize(chunkSize + parts[j].size);
                        }
                        size_t rc = ::read(c.segments[parts[j].segmentNo], &buf[chunkSize], parts[j].size);
                        if (rc != parts[j].size)
                            throw SYSTEM_EXCEPTION(SCIDB_SE_IO, SCIDB_LE_OPERATION_FAILED) << "read";
                        //checkChunkElements(dstArrayDesc, iter->first, &buf[chunkSize], parts[j].size);
                        chunkSize += rc;
                    }
                    networkManager->send(targetInstanceId, shared_ptr<SharedBuffer>(new MemoryBuffer(&buf[0], chunkSize)), query);
#ifdef SEND_ACK
                    networkManager->receive(targetInstanceId, query); // receive confirmation
#endif
                    break;
                }
            }
        }
    }
    for (size_t i = 0; i < nDims; i++) {
        assert(highBoundary[i] <= dstDims[i].getEndMax());
        if (coordinateIndices[i]) { 
            assert(highBoundary[i] < Coordinate(dstDims[i].getStart() + coordinateIndices[i]->size()));
            highBoundary[i] = dstDims[i].getStart() + coordinateIndices[i]->size() - 1;
        } else if (coordinateMultiIndices[i]) { 
            assert(highBoundary[i] < Coordinate(dstDims[i].getStart() + coordinateMultiIndices[i]->size()));
            highBoundary[i] = dstDims[i].getStart() + coordinateMultiIndices[i]->size() - 1;
        } else if (i == newDim && highBoundary[i] > newDimMax) { 
            highBoundary[i] = newDimMax;
        }
    }

    SystemCatalog::getInstance()->updateArrayBoundaries(dstArrayDesc.getId(), lowBoundary, highBoundary);
    LOG4CXX_DEBUG(logger, "Time for merging data: " << (time(NULL) - start) << " seconds");
}




class PhysicalFlipStore: public PhysicalOperator
{
  private:
    ArrayID   _arrayID;   /**< ID of new array */
    ArrayID   _updateableArrayID;   /**< ID of new array */
    Dimensions _updateableDims;
    shared_ptr<SystemCatalog::LockDesc> _lock;

  public:
    PhysicalFlipStore(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema), _arrayID((ArrayID)~0), _updateableArrayID((ArrayID)~0)
    {
    }


    void preSingleExecute(shared_ptr<Query> query)
    {
        ArrayDesc desc;
        shared_ptr<const InstanceMembership> membership(Cluster::getInstance()->getInstanceMembership());
        assert(membership);

        if (((membership->getViewId() != query->getCoordinatorLiveness()->getViewId()) ||
             (membership->getInstances().size() != query->getInstancesCount()))) {
           throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM2);
        }

        _lock = shared_ptr<SystemCatalog::LockDesc>(new SystemCatalog::LockDesc(_schema.getName(),
                                                                                       query->getQueryID(),
                                                                                       Cluster::getInstance()->getLocalInstanceId(),
                                                                                       SystemCatalog::LockDesc::COORD,
                                                                                       SystemCatalog::LockDesc::WR));
        shared_ptr<Query::ErrorHandler> ptr(new UpdateErrorHandler(_lock));
        query->pushErrorHandler(ptr);

        bool rc = false;
        if (!SystemCatalog::getInstance()->getArrayDesc(_schema.getName(), desc, false)) {

            if (_schema.getId() != 0) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST) << _schema.getName();
            }
            _lock->setLockMode(SystemCatalog::LockDesc::CRT);
            rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
            assert(rc);

            ArrayID newArrayID = SystemCatalog::getInstance()->addArray(_schema, psRoundRobin);
            desc = _schema;
            desc.setId(newArrayID);
        } else if (_schema.getId() != desc.getId()) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST) << _schema.getName();
        }

        if (desc.isImmutable()) {
            _lock->setArrayId(desc.getId());
            rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
            assert(rc);            
            return;
        }
        _updateableArrayID = desc.getId();
        VersionID lastVersion = SystemCatalog::getInstance()->getLastVersion(_updateableArrayID);

        _lock->setArrayId(_updateableArrayID);
        _lock->setArrayVersion(lastVersion+1);
        rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
        assert(rc);

        string const& arrayName = desc.getName();
        _updateableDims = _schema.getDimensions();
        std::stringstream ss;
        ss << arrayName << "@" << (lastVersion+1);
        _schema = ArrayDesc(ss.str(), _schema.getAttributes(), _schema.grabDimensions(lastVersion+1));

        _arrayID = SystemCatalog::getInstance()->addArray(_schema, psRoundRobin);
        _lock->setArrayVersionId(_arrayID);
        rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
        assert(rc);
        rc = rc; // Eliminate warnings
    }

    virtual void postSingleExecute(shared_ptr<Query> query)
    {
        if (_updateableArrayID != (ArrayID)~0) {
            SystemCatalog::getInstance()->createNewVersion(_updateableArrayID, _arrayID);
        }
        assert(_lock);
    }

    virtual bool isDistributionPreserving(const std::vector< ArrayDesc> & inputSchemas) const
    {
        return false;
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return PhysicalBoundaries::createFromFullSchema(_schema);
    }

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                 const std::vector< ArrayDesc> & inputSchemas) const
    {
        return ArrayDistribution(psRoundRobin);
    }

    void populateAggregates(ArrayDesc const& srcArrayDesc, vector<AggregatePtr> & aggregates, vector<size_t>& attributesMapping)
    {
        for(size_t i =1; i<_parameters.size(); i++)
        {
            assert(_parameters[i]->getParamType() == PARAM_AGGREGATE_CALL);
            {
                AttributeID inputAttId;
                string aggOutputName;
                AggregatePtr agg = resolveAggregate((shared_ptr<OperatorParamAggregateCall>&) _parameters[i],
                                                    srcArrayDesc.getAttributes(),
                                                    &inputAttId,
                                                    &aggOutputName);

                bool found = false;
                if (inputAttId == (AttributeID) -1)
                {
                    inputAttId = 0;
                }

                for (size_t j = 0; j<_schema.getAttributes().size(); j++)
                {
                    if (_schema.getAttributes()[j].getName() == aggOutputName)
                    {
                        aggregates[j] = agg;
                        attributesMapping[j] = inputAttId;
                        found = true;
                        break;
                    }
                }
                if (!found) { 
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REDIMENSION_STORE_ERROR6) << aggOutputName;
                }
            }
        }
    }

    shared_ptr<Array> execute(vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);

           VersionID version(0);
           string baseArrayName = splitArrayNameVersion(_schema.getName(), version);

        query->exclusiveLock(baseArrayName);

        if (!_lock) {
           _lock = shared_ptr<SystemCatalog::LockDesc>(new SystemCatalog::LockDesc(baseArrayName,
                                                            query->getQueryID(),
                                                            Cluster::getInstance()->getLocalInstanceId(),
                                                            SystemCatalog::LockDesc::WORKER,
                                                            SystemCatalog::LockDesc::WR));
           _lock->setArrayVersion(version);
           shared_ptr<Query::ErrorHandler> ptr(new UpdateErrorHandler(_lock));
           query->pushErrorHandler(ptr);

           Query::Finalizer f = bind(&UpdateErrorHandler::releaseLock,
                                     _lock, _1);
           query->pushFinalizer(f);
           SystemCatalog::ErrorChecker errorChecker = bind(&Query::validate, query);
           bool rc = SystemCatalog::getInstance()->lockArray(_lock, errorChecker);
           if (!rc) {
              throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_INCREMENT_LOCK)
              << baseArrayName;
           }
        }

        shared_ptr<Array> srcArray = inputArrays[0];
        shared_ptr<Array> dstArray = shared_ptr<Array>(new DBArray(_schema.getName(), query)); // We can't use _arrayID because it's not initialized on remote instances

        ArrayDesc  const& srcArrayDesc = srcArray->getArrayDesc();
        ArrayDesc  const& dstArrayDesc = _schema;
        Attributes const& dstAttrs = dstArrayDesc.getAttributes();
        Attributes const& srcAttrs = srcArrayDesc.getAttributes();
        Dimensions const& dstDims = dstArrayDesc.getDimensions();
        Dimensions const& srcDims = srcArrayDesc.getDimensions();
        size_t nAttrs = dstAttrs.size();
        size_t nDims = dstDims.size();
        size_t nSrcAttrs = srcAttrs.size();
        size_t nSrcDims = srcDims.size();

        vector<AggregatePtr> aggregates (nAttrs);
        vector<size_t> attributesMapping(nAttrs);
        vector<size_t> dimensionsMapping(nDims);

        populateAggregates(srcArrayDesc, aggregates, attributesMapping);

        for (size_t i = 0; i < nAttrs; i++)
        {
            if (aggregates[i].get())
            {//already populated
                continue;
            }

            for (size_t j = 0; j < nSrcAttrs; j++) {
                if (srcAttrs[j].getName() == dstAttrs[i].getName()) {
                    attributesMapping[i] = j;
                    goto NextAttr;
                }
            }
            for (size_t j = 0; j < nSrcDims; j++) {
                if (srcDims[j].hasNameOrAlias(dstAttrs[i].getName())) {
                    attributesMapping[i] = j | FLIP;
                    goto NextAttr;
                }
            }
            attributesMapping[i] = SYNTHETIC;
          NextAttr:;
        }
        for (size_t i = 0; i < nDims; i++) {
            for (size_t j = 0; j < nSrcDims; j++) {
                if (srcDims[j].hasNameOrAlias(dstDims[i].getBaseName())) {
                    dimensionsMapping[i] = j;
                    goto NextDim;
                }
            }
            for (size_t j = 0; j < nSrcAttrs; j++) {
                if (dstDims[i].hasNameOrAlias(srcAttrs[j].getName())) {
                    dimensionsMapping[i] = j | FLIP;
                    goto NextDim;
                }
            }
            dimensionsMapping[i] = SYNTHETIC; 
          NextDim:;
        }
        if (_updateableArrayID != (ArrayID)~0) {
            string baseName = _schema.getName().substr(0, _schema.getName().find('@'));
            SystemCatalog::getInstance()->updateArray(ArrayDesc(_updateableArrayID, baseName, _schema.getAttributes(), _updateableDims, _schema.getFlags()));
        }
        _schema.setId(dstArray->getHandle());
        SystemCatalog::getInstance()->updateArray(_schema);

        transformArray(dstArrayDesc, dstArray, srcArrayDesc, srcArray, attributesMapping, dimensionsMapping, aggregates, query);
        StorageManager::getInstance().flush();
        query->replicationBarrier();
        getInjectedErrorListener().check();
        return dstArray;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalFlipStore, "redimension_store", "PhysicalFlipStore")

}  // namespace ops
