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
 * @file StreamArray.cpp
 *
 * @brief Array receiving chunks from abstract stream
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include <stdio.h>
#include <vector>
#include "array/StreamArray.h"
#include "system/Exceptions.h"
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"

namespace scidb
{
    using namespace boost;
    using namespace std;

    //
    // StreamArray
    //
    StreamArray::StreamArray(const StreamArray& other)
    : desc(other.desc), emptyCheck(other.emptyCheck), iterators(other.iterators.size()), currentBitmapChunk(NULL)
    {
    }

    StreamArray::StreamArray(ArrayDesc const& arr, bool emptyable)
    : desc(arr), emptyCheck(emptyable), iterators(arr.getAttributes().size()), currentBitmapChunk(NULL)
    {
    }

    string const& StreamArray::getName() const
    {
        return desc.getName();
    }

    ArrayID StreamArray::getHandle() const
    {
        return desc.getId();
    }

    ArrayDesc const& StreamArray::getArrayDesc() const
    {
        return desc;
    }

    boost::shared_ptr<ArrayIterator> StreamArray::getIterator(AttributeID attId)
    {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "StreamArray::getIterator()";
    }

    boost::shared_ptr<ConstArrayIterator> StreamArray::getConstIterator(AttributeID attId) const
    {
        if (!iterators[attId]) {
            ((StreamArray*)this)->iterators[attId] = boost::shared_ptr<ConstArrayIterator>(new StreamArrayIterator(*(StreamArray*)this, attId));
        }
        return iterators[attId];
    }

    //
    // Stream array iterator
    //
    StreamArrayIterator::StreamArrayIterator(StreamArray& arr, AttributeID attr)
    : array(arr), attId(attr)
    {
        moveNext();
    }

    void StreamArrayIterator::moveNext()
    {
        if (array.currentBitmapChunk == NULL || attId == 0 || array.currentBitmapChunk->getAttributeDesc().getId() != attId) {
            currentChunk = array.nextChunk(attId, dataChunk);
            if (currentChunk != NULL && array.emptyCheck) {
                AttributeDesc const* bitmapAttr = array.desc.getEmptyBitmapAttribute();
                if (bitmapAttr != NULL && bitmapAttr->getId() != attId) {
                    if (array.currentBitmapChunk == NULL || array.currentBitmapChunk->getFirstPosition(false) != currentChunk->getFirstPosition(false)) {
                        array.currentBitmapChunk = array.nextChunk(bitmapAttr->getId(), bitmapChunk);
                        if (!array.currentBitmapChunk)
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_BITMAP_CHUNK);
                    }
                    dataChunk.setBitmapChunk((Chunk*)array.currentBitmapChunk);
                }
            }
        } else {
            currentChunk = array.currentBitmapChunk;
        }
    }

    ConstChunk const& StreamArrayIterator::getChunk()
    {
        if (!currentChunk)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        return *currentChunk;
    }

    bool StreamArrayIterator::end()
    {
        return currentChunk == NULL;
    }

    void StreamArrayIterator::operator ++()
    {
        if (currentChunk != NULL) {
            moveNext();
        }
    }

    Coordinates const& StreamArrayIterator::getPosition()
    {
        if (!currentChunk)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        return currentChunk->getFirstPosition(false);
    }

    //
    // AccumulatorArray
    //
    AccumulatorArray::AccumulatorArray(boost::shared_ptr<Array> array) : StreamArray(array->getArrayDesc(), false), pipe(array), iterators(array->getArrayDesc().getAttributes().size())
    {
    }

    ConstChunk const* AccumulatorArray::nextChunk(AttributeID attId, MemChunk& chunk)
    {
        if (!iterators[attId]) {
            iterators[attId] = pipe->getConstIterator(attId);
        } else {
            ++(*iterators[attId]);
        }
        if (iterators[attId]->end()) {
            return NULL;
        }
        ConstChunk const& inputChunk = iterators[attId]->getChunk();
        if (inputChunk.isMaterialized()) {
            return &inputChunk;
        }
        Address addr(attId, inputChunk.getFirstPosition(false));
        chunk.initialize(this, &desc, addr, inputChunk.getCompressionMethod());
        chunk.setBitmapChunk((Chunk*)&inputChunk);
        boost::shared_ptr<ConstChunkIterator> src = inputChunk.getConstIterator(ChunkIterator::INTENDED_TILE_MODE|ChunkIterator::IGNORE_EMPTY_CELLS);
        boost::shared_ptr<Query> emptyQuery;
        boost::shared_ptr<ChunkIterator> dst = chunk.getIterator(emptyQuery,
                                                                 (src->getMode() & ChunkIterator::TILE_MODE)|ChunkIterator::NO_EMPTY_CHECK|(inputChunk.isSparse()?ChunkIterator::SPARSE_CHUNK:0)|ChunkIterator::SEQUENTIAL_WRITE);
        bool vectorMode = src->supportsVectorMode() && dst->supportsVectorMode();
        src->setVectorMode(vectorMode);
        dst->setVectorMode(vectorMode);
        size_t count = 0;
        while (!src->end()) {
            if (dst->setPosition(src->getPosition())) {
                dst->writeItem(src->getItem());
                count += 1;
            }
            ++(*src);
        }
        if (!vectorMode && !desc.containsOverlaps()) {
            chunk.setCount(count);
        }
        dst->flush();
        return &chunk;
    }

    //
    // Multistream array
    //
    MultiStreamArray::MultiStreamArray(size_t n, ArrayDesc const& arr, bool emptyCheck)
    : StreamArray(arr, emptyCheck),
      nStreams(n),
      chunkPos(arr.getAttributes().size())
    {}

    MultiStreamArray::MultiStreamArray(MultiStreamArray const& other)
    : StreamArray(other),
      nStreams(other.nStreams),
      chunkPos(other.desc.getAttributes().size())
    {}

    ConstChunk const* MultiStreamArray::nextChunk(AttributeID attId, MemChunk& chunk)
    {
        size_t i;
        if (chunkPos[attId].size() == 0) {
            chunkPos[attId].resize(nStreams);
            for (i = 0; i < nStreams; i++) {
                nextChunkPos(i, attId, chunkPos[attId][i]);
            }
        }
        for (i = 0; i < nStreams && chunkPos[attId][i].size() == 0; i++);
        if (i == nStreams) {
            return NULL;
        }
        size_t min = i;
        CoordinatesLess less;
        while (++i < nStreams) {
            if (chunkPos[attId][i].size() != 0 && less(chunkPos[attId][i], chunkPos[attId][min])) {
                min = i;
            }
        }
        Coordinates minPos = chunkPos[attId][min];

        ConstChunk const* next = nextChunkBody(min, attId, chunk, minPos);
        assert(minPos == next->getFirstPosition(false));
        if (!next)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CANT_FETCH_CHUNK_BODY);
        boost::shared_ptr<ChunkIterator> dstIterator;
        for (i = 0; i < nStreams; i++) {
            if (chunkPos[attId][i] == minPos) {
                if (i != min) {
                    ConstChunk const* merge = nextChunkBody(i, attId, mergeChunk, minPos);
                    if (!merge)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CANT_FETCH_CHUNK_BODY);
                    assert(minPos == merge->getFirstPosition(false));
                    chunk.setCount(0); // unknown
                    AttributeDesc const& attr = merge->getAttributeDesc();
                    Value const& defaultValue = attr.getDefaultValue();
                    if (next->isRLE() || merge->isRLE() || next->isSparse() || merge->isSparse() || attr.isNullable() || !defaultValue.isZero() || TypeLibrary::getType(attr.getType()).variableSize()) {
                        int sparseMode = next->isSparse() ? ChunkIterator::SPARSE_CHUNK : 0;
                        if (!dstIterator) {
                           boost::shared_ptr<Query> emptyQuery;
                           dstIterator = ((Chunk*)next)->getIterator(emptyQuery,
                                                                     sparseMode|ChunkIterator::APPEND_CHUNK|ChunkIterator::NO_EMPTY_CHECK);
                        }
                        boost::shared_ptr<ConstChunkIterator> srcIterator = merge->getConstIterator(ChunkIterator::IGNORE_DEFAULT_VALUES|ChunkIterator::IGNORE_NULL_VALUES|ChunkIterator::IGNORE_EMPTY_CELLS);
                        assert(minPos == merge->getFirstPosition(false));
                        if (next->getArrayDesc().getEmptyBitmapAttribute() != NULL) {
                            while (!srcIterator->end()) {
                                if (!dstIterator->setPosition(srcIterator->getPosition()))
                                    throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                                Value const& value = srcIterator->getItem();
                                dstIterator->writeItem(value);
                                ++(*srcIterator);
                            }
                        } else { 
                            while (!srcIterator->end()) {
                                Value const& value = srcIterator->getItem();
                                if (value != defaultValue) {
                                    if (!dstIterator->setPosition(srcIterator->getPosition()))
                                        throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                                    dstIterator->writeItem(value);
                                }
                                ++(*srcIterator);
                            }
                        }
                    } else {
                        if (next->getSize() != merge->getSize())
                            throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CANT_MERGE_CHUNKS_WITH_VARYING_SIZE);
                        PinBuffer scope(*merge);
                        char* dst = (char*)next->getData();
                        char* src = (char*)merge->getData();
                        for (size_t j = 0, n = next->getSize(); j < n; j++) {
                            dst[j] |= src[j];
                        }
                    }
                }
                if (!nextChunkPos(i, attId, chunkPos[attId][i])) {
                    chunkPos[attId][i].clear();
                }
            }
        }
        if (dstIterator) {
           dstIterator->flush();
        }
        return next;
    }

    //
    // Merge stream
    //
    MergeStreamArray::MergeStreamArray(ArrayDesc const& array, vector< boost::shared_ptr<Array> > input)
    : StreamArray(array, false),
      inputArrays(input),
      inputIterators(array.getAttributes().size())
    {}

    void MergeStreamArray::merge(boost::shared_ptr<ChunkIterator> dst, boost::shared_ptr<ConstChunkIterator> src)
    {
        while (!src->end()) {
            if (dst->setPosition(src->getPosition())) {
                dst->writeItem(src->getItem());
            }
            ++(*src);
        }
    }

    ConstChunk const* MergeStreamArray::nextChunk(AttributeID attId, MemChunk& chunk)
    {
        size_t i;
        size_t nStreams = inputArrays.size();
        if (inputIterators[attId].size() == 0) {
            inputIterators[attId].resize(nStreams);
            for (i = 0; i < nStreams; i++) {
                inputIterators[attId][i] = inputArrays[i]->getConstIterator(attId);
            }
        }
        for (i = 0; i < nStreams && inputIterators[attId][i]->end(); i++);
        if (i == nStreams) {
            return NULL;
        }
        size_t min = i;
        CoordinatesLess less;
        while (++i < nStreams) {
            if (!inputIterators[attId][i]->end() && less(inputIterators[attId][i]->getPosition(), inputIterators[attId][min]->getPosition())) {
                min = i;
            }
        }
        Coordinates minPos = inputIterators[attId][min]->getPosition();
        ConstChunk const& inputChunk = inputIterators[attId][min]->getChunk();

        Address addr(attId, minPos);
        chunk.initialize(this, &desc, addr, inputChunk.getCompressionMethod());
        chunk.setBitmapChunk((Chunk*)&inputChunk);
        boost::shared_ptr<Query> emptyQuery;
        boost::shared_ptr<ChunkIterator> dst = chunk.getIterator(emptyQuery,
                                                                 ChunkIterator::NO_EMPTY_CHECK|(inputChunk.isSparse()?ChunkIterator::SPARSE_CHUNK:0));
        {
            boost::shared_ptr<ConstChunkIterator> src = inputChunk.getConstIterator(ChunkIterator::NO_EMPTY_CHECK|ChunkIterator::IGNORE_EMPTY_CELLS|ChunkIterator::IGNORE_DEFAULT_VALUES);
            MergeStreamArray::merge(dst, src);
        }
        for (i = 0; i < nStreams; i++) {
            if (!inputIterators[attId][i]->end() && inputIterators[attId][i]->getPosition() == minPos) {
                if (i != min) {
                    merge(dst, inputIterators[attId][i]->getChunk().getConstIterator(ChunkIterator::NO_EMPTY_CHECK|ChunkIterator::IGNORE_EMPTY_CELLS|ChunkIterator::IGNORE_DEFAULT_VALUES));
                }
                ++(*inputIterators[attId][i]);
            }
        }
        dst->flush();
        return &chunk;
    }
}
