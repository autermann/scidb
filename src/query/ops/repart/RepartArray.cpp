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
 * @file RepartArray.cpp
 *
 * @brief Repart array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include "system/Exceptions.h"
#include "RepartArray.h"

namespace scidb {

    using namespace boost;
    using namespace std;

    //
    // Repart chunk iterator methods
    //
    int RepartChunkIterator::getMode()
    {
        return outMode;
    }

    void RepartChunkIterator::reset()
    {
        pos = first;
        pos[pos.size()-1] -= 1;
        hasCurrent = true;
        ++(*this);
    }

    void RepartChunkIterator::operator++()
    {
        size_t nDims = pos.size();
        while (true) {
            size_t i = nDims-1;
            while (++pos[i] > last[i]) {
                if (i == 0) {
                    hasCurrent = false;
                    return;
                }
                pos[i] = first[i];
                i -= 1;
            }
            if (!inputIterator || !inputIterator->getChunk().contains(pos, !(inMode & IGNORE_OVERLAPS))) {
                inputIterator.reset();
                if (arrayIterator->setPosition(pos)) {
                    ConstChunk const& inputChunk = arrayIterator->getChunk();
                    chunk.sparse |= inputChunk.isSparse();
                    inputIterator = inputChunk.getConstIterator(inMode);
                    if (inputIterator->setPosition(pos)) {
                        hasCurrent = true;
                        return;
                    }
                }
            } else if (inputIterator->setPosition(pos)) {
                hasCurrent = true;
                return;
            }
        }
    }

    bool RepartChunkIterator::setPosition(Coordinates const& newPos)
    {
        pos = newPos;
        inputIterator.reset();
        if (arrayIterator->setPosition(newPos)) {
            ConstChunk const& inputChunk = arrayIterator->getChunk();
            chunk.sparse |= inputChunk.isSparse();
            inputIterator = inputChunk.getConstIterator(inMode);
            if (inputIterator->setPosition(newPos)) {
                return hasCurrent = true;
            }
        }
        return hasCurrent = false;
    }

    Coordinates const& RepartChunkIterator::getPosition()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return pos;
    }

    Value& RepartChunkIterator::getItem()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return inputIterator->getItem();
    }

    bool RepartChunkIterator::end()
    {
        return !hasCurrent;
    }

    bool RepartChunkIterator::isEmpty()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return inputIterator->isEmpty();
    }

    ConstChunk const& RepartChunkIterator::getChunk()
    {
        return chunk;
    }

    RepartChunkIterator::RepartChunkIterator(RepartChunk& chk, int iterationMode)
    : chunk(chk),
      pos(chk.getArrayDesc().getDimensions().size()),
      first(chunk.getFirstPosition(!(iterationMode & IGNORE_OVERLAPS))),
      last(chunk.getLastPosition(!(iterationMode & IGNORE_OVERLAPS))),
      arrayIterator(chunk.getArrayIterator().getInputIterator()),
      inMode((iterationMode|chunk.overlapMode)&~(TILE_MODE|INTENDED_TILE_MODE)),
      outMode(iterationMode)
    {
        
        reset();
    }

    //
    // Repart chunk methods
    //

    RepartChunk::RepartChunk(RepartArray const& arr, DelegateArrayIterator const& iterator, AttributeID attrID)
    : DelegateChunk(arr, iterator, attrID, false),
      overlapMode(arr.overlapMode),
      sparse(false)
    {
    }

    bool RepartChunk::isSparse() const
    {
        return sparse;
    }

    boost::shared_ptr<ConstChunkIterator> RepartChunk::getConstIterator(int iterationMode) const
    {
        return boost::shared_ptr<ConstChunkIterator>(new RepartChunkIterator(*(RepartChunk*)this, iterationMode));
    }

    void RepartChunk::initialize(Coordinates const& pos, bool isSparse)
    {
        ArrayDesc const& desc = array.getArrayDesc();
        Address addr(desc.getId(), attrID, pos);
        chunk.initialize(&array, &desc, addr, desc.getAttributes()[attrID].getDefaultCompressionMethod());
        sparse = isSparse;
        setInputChunk(chunk);
    }

    //
    // Repart array iterator
    //
    void RepartArrayIterator::fillSparseChunk(Coordinates const& chunkPos)
    {
        ArrayDesc const& desc = array.getArrayDesc();
        Address addr(desc.getId(), attr, chunkPos);
        repartChunk.initialize(&array, &desc, addr, desc.getAttributes()[attr].getDefaultCompressionMethod());
        repartChunk.setSparse(true);

        boost::shared_ptr<Query> query(getQuery());
        boost::shared_ptr<ChunkIterator> dst = repartChunk.getIterator(query, ChunkIterator::NO_EMPTY_CHECK);

        Coordinates const& firstPos = repartChunk.getFirstPosition(true);
        Coordinates lastPos = repartChunk.getLastPosition(true);
        Coordinates pos = firstPos;
        size_t nDims = pos.size();
        pos[nDims-1] -= chunkInterval[nDims-1];
        for (size_t j = 0; j < nDims; j++) {
            lastPos[j] += chunkInterval[j];
        }
        while (true) {
            size_t i = nDims-1;
            while ((pos[i] += chunkInterval[i]) >= lastPos[i]) {
                if (i == 0) {
                    dst->flush();
                    if (emptyIterator) {
                        if (!emptyIterator->setPosition(chunkPos))
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                        repartChunk.setBitmapChunk((Chunk*)&emptyIterator->getChunk());
                    }
                    return;
                }
                pos[i] = firstPos[i];
                i -= 1;
            }
            if (inputIterator->setPosition(pos)) {
                boost::shared_ptr<ConstChunkIterator> src = inputIterator->getChunk().getConstIterator(overlapMode|ChunkIterator::IGNORE_DEFAULT_VALUES|ChunkIterator::IGNORE_EMPTY_CELLS);
                while (!src->end()) {
                    if (dst->setPosition(src->getPosition())) {
                        dst->writeItem(src->getItem());
                    }
                    ++(*src);
                }
            }
        }
    }

    bool RepartArrayIterator::containsChunk(Coordinates const& chunkPos)
    {
        Dimensions const& dims = array.getArrayDesc().getDimensions();
        size_t nDims = dims.size();
        Coordinates firstPos(nDims);
        Coordinates lastPos(nDims);
        Coordinates pos(nDims);

        for (size_t i = 0; i < nDims; i++) {
            pos[i] = firstPos[i] = chunkPos[i] - chunkOverlap[i];
            lastPos[i] = chunkPos[i] + chunkOverlap[i] + dims[i].getChunkInterval();
            if (chunkOverlap[i] != 0 || chunkInterval[i] != dims[i].getChunkInterval()) { 
                lastPos[i] += chunkInterval[i];
            }
        }
        pos[nDims-1] -= chunkInterval[nDims-1];
        while (true) {
            size_t i = nDims-1;
            while ((pos[i] += chunkInterval[i]) >= lastPos[i]) {
                if (i == 0) {
                    return false;
                }
                pos[i] = firstPos[i];
                i -= 1;
            }
            if (inputIterator->setPosition(pos)) {
                return true;
            }
        }
    }

    ConstChunk const& RepartArrayIterator::getChunk()
    {
        if (currChunk == NULL) { 
            if (!hasCurrent)
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
            if (isSparseArray) {
                if (pos != repartChunk.getAddress().coords) {
                    fillSparseChunk(pos);
                }
                currChunk = &repartChunk;
            } else {
                ((RepartChunk&)*chunk).initialize(pos, isSparseArray);
                currChunk = chunk.get();
            }
        } 
        return *currChunk;
    }

    Coordinates const& RepartArrayIterator::getPosition()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return pos;
    }

    bool RepartArrayIterator::setPosition(Coordinates const& newPos)
    {
        Dimensions const& dims = array.getArrayDesc().getDimensions();
        for (size_t i = 0, nDims = dims.size(); i < nDims; i++) {
            if (newPos[i] < dims[i].getStart() || newPos[i] > upperBound[i]) {
                return hasCurrent = false;
            }
        }
        currChunk = NULL;
        pos = newPos;
        array.getArrayDesc().getChunkPositionFor(pos);
        inputIterator->setPosition(pos);
        return hasCurrent = true;
    }

    void RepartArrayIterator::operator ++()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        Dimensions const& dims = array.getArrayDesc().getDimensions();
        do {
            size_t i = dims.size()-1;
            while ((pos[i] += dims[i].getChunkInterval()) > upperBound[i]) {
                if (i == 0) {
                    hasCurrent = false;
                    return;
                }
                pos[i] = dims[i].getStart();
                i -= 1;
            }
        } while (!containsChunk(pos));

        currChunk = NULL;
        inputIterator->setPosition(pos);
    }

    bool RepartArrayIterator::end()
    {
        return !hasCurrent;
    }

    void RepartArrayIterator::reset()
    {
        Dimensions const& dims = array.getArrayDesc().getDimensions();
        size_t nDims = dims.size();
        for (size_t i = 0; i < nDims; i++) {
            pos[i] = dims[i].getStart();
        }
        pos[nDims-1] -= dims[nDims-1].getChunkInterval();
        inputIterator->reset();
        isSparseArray = !inputIterator->end() && inputIterator->getChunk().isSparse();
        hasCurrent = true;
        currChunk = NULL;
        ++(*this);
    }

    RepartArrayIterator::RepartArrayIterator(RepartArray const& arr, AttributeID attrID,
                                             boost::shared_ptr<ConstArrayIterator> inputIterator,
                                             const boost::shared_ptr<Query>& query)
    : DelegateArrayIterator(arr, attrID, inputIterator), _query(query), overlapMode(arr.overlapMode),
      currChunk(NULL)
    {
        Dimensions const& dims = arr.getInputArray()->getArrayDesc().getDimensions();
        Dimensions const& outDims = arr.getArrayDesc().getDimensions();
        size_t nDims = dims.size();
        pos.resize(nDims);
        chunkInterval.resize(nDims);
        chunkOverlap.resize(nDims);
        upperBound.resize(nDims);
        for (size_t i = 0; i < nDims; i++) {
            upperBound[i] = dims[i].getStart() + dims[i].getCurrLength() - 1;
            chunkInterval[i] = dims[i].getChunkInterval();
            chunkOverlap[i] = outDims[i].getChunkOverlap();
        }
        reset();
        if (isSparseArray) {
            AttributeDesc const* emptyAttr = arr.getArrayDesc().getEmptyBitmapAttribute();
            if (emptyAttr != NULL && emptyAttr->getId() != attrID) {
                emptyIterator = arr.getConstIterator(emptyAttr->getId());
            }
        }
    }

    //
    // Repart array methods
    //

    DelegateChunk* RepartArray::createChunk(DelegateArrayIterator const* iterator, AttributeID id) const
    {
       return new RepartChunk(*this, *iterator, id);
    }

    DelegateArrayIterator* RepartArray::createArrayIterator(AttributeID id) const
    {
       boost::shared_ptr<Query> query(_query.lock());
       return new RepartArrayIterator(*this, id, inputArray->getConstIterator(id), query);
    }

    RepartArray::RepartArray(ArrayDesc const& desc,
                             boost::shared_ptr<Array> const& array,
                             const boost::shared_ptr<Query>& query)
    : DelegateArray(desc, array), _query(query)
    {
        overlapMode = query->getNodesCount() > 1 ? ConstChunkIterator::IGNORE_OVERLAPS : 0;
    }
}

