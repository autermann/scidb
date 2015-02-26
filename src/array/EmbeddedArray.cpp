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
 * @file EmbeddedArray.cpp
 *
 * @brief Embedded array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include "array/EmbeddedArray.h"
#include "system/Exceptions.h"
#include "array/Compressor.h"
#include "system/SystemCatalog.h"

namespace scidb
{
    using namespace boost;
    using namespace std;

    //
    // Embedded array methods
    //
    EmbeddedArray::EmbeddedArray(const EmbeddedArray& other)
    :  physicalArray(other.physicalArray), embeddedDesc(other.embeddedDesc), basePos(other.basePos)
    {
    }

    EmbeddedArray::EmbeddedArray(Array& physicalArrayImpl, ArrayDesc const& embeddedArrayDesc, Coordinates const& positionInParentArray)
    : physicalArray(physicalArrayImpl), embeddedDesc(embeddedArrayDesc), basePos(positionInParentArray)
    {
    }

    string const& EmbeddedArray::getName() const
    {
        return embeddedDesc.getName();
    }

    ArrayID EmbeddedArray::getHandle() const
    {
        return physicalArray.getHandle();
    }

    ArrayDesc const& EmbeddedArray::getArrayDesc() const
    {
        return embeddedDesc;
    }

    boost::shared_ptr<ArrayIterator> EmbeddedArray::getIterator(AttributeID attId)
    {
        return boost::shared_ptr<ArrayIterator>(new EmbeddedArrayIterator(*this, attId));
    }

    boost::shared_ptr<ConstArrayIterator> EmbeddedArray::getConstIterator(AttributeID attId) const
    {
        return ((EmbeddedArray*)this)->getIterator(attId);
    }

    void EmbeddedArray::physicalToVirtual(Coordinates& pos) const
    {
        pos.erase(pos.end() - basePos.size(), pos.end());
    }

    void EmbeddedArray::virtualToPhysical(Coordinates& pos) const
    {
        pos.insert(pos.end(), basePos.begin(), basePos.end());
    }

    bool EmbeddedArray::contains(Coordinates const& physicalPos) const
    {
        size_t i = physicalPos.size();
        size_t j = basePos.size();
        while (physicalPos[--i] == basePos[--j]) {
            if (j == 0) {
                return true;
            }
        }
        return false;
    }

    //
    // Embedded array chunk methods
    //
    EmbeddedChunk::EmbeddedChunk(EmbeddedArray& arr) : array(arr)
    {
        chunk = NULL;
    }

    Array const& EmbeddedChunk::getArray() const
    {
        return array;
    }


    void EmbeddedChunk::setChunk(Chunk& physicalChunk)
    {
        chunk = &physicalChunk;
        firstPos = physicalChunk.getFirstPosition(false);
        firstPosWithOverlaps = physicalChunk.getFirstPosition(true);
        lastPos = physicalChunk.getLastPosition(false);
        lastPosWithOverlaps = physicalChunk.getLastPosition(true);
        array.physicalToVirtual(firstPos);
        array.physicalToVirtual(firstPosWithOverlaps);
        array.physicalToVirtual(lastPos);
        array.physicalToVirtual(lastPosWithOverlaps);
    }

    const ArrayDesc& EmbeddedChunk::getArrayDesc() const
    {
        return array.embeddedDesc;
    }

    const AttributeDesc& EmbeddedChunk::getAttributeDesc() const
    {
        return chunk->getAttributeDesc();
    }

    int EmbeddedChunk::getCompressionMethod() const
    {
        return chunk->getCompressionMethod();
    }

    Coordinates const& EmbeddedChunk::getFirstPosition(bool withOverlap) const
    {
        return withOverlap ? firstPosWithOverlaps : firstPos;
    }

    Coordinates const& EmbeddedChunk::getLastPosition(bool withOverlap) const
    {
        return withOverlap ? lastPosWithOverlaps : lastPos;
    }

    boost::shared_ptr<ChunkIterator> EmbeddedChunk::getIterator(boost::shared_ptr<Query> const& query, int iterationMode)
    {
       return boost::shared_ptr<ChunkIterator>(new EmbeddedChunkIterator(array, this, chunk->getIterator(query, iterationMode)));
    }

    boost::shared_ptr<ConstChunkIterator> EmbeddedChunk::getConstIterator(int iterationMode) const
    {
        return boost::shared_ptr<ConstChunkIterator>(new EmbeddedChunkIterator(array, this, boost::shared_ptr<ChunkIterator>((ChunkIterator*)chunk->getConstIterator(iterationMode).get())));
    }

    bool EmbeddedChunk::pin() const
    {
        return chunk->pin();
    }

    void EmbeddedChunk::unPin() const
    {
        chunk->unPin();
    }

    //
    // Embedded array iterator methods
    //

    EmbeddedArrayIterator::EmbeddedArrayIterator(EmbeddedArray& arr, AttributeID attId)
    : array(arr), attribute(attId), chunk(arr)
    {
       iterator = arr.getIterator(attId);
    }

    ConstChunk const& EmbeddedArrayIterator::getChunk()
    {
        if (end())
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        chunk.setChunk((Chunk&)iterator->getChunk());
        return chunk;
    }

    bool EmbeddedArrayIterator::end()
    {
        return iterator->end() || !array.contains(iterator->getPosition());
    }

    void EmbeddedArrayIterator::operator ++()
    {
        ++(*iterator);
    }

    Coordinates const& EmbeddedArrayIterator::getPosition()
    {
        pos = iterator->getPosition();
        array.physicalToVirtual(pos);
        return pos;
    }

    bool EmbeddedArrayIterator::setPosition(Coordinates const& pos)
    {
        Coordinates physicalPos = pos;
        array.virtualToPhysical(physicalPos);
        return iterator->setPosition(physicalPos);
    }

    void EmbeddedArrayIterator::reset()
    {
        iterator->reset();
    }

    Chunk& EmbeddedArrayIterator::newChunk(Coordinates const& pos)
    {
        return newChunk(pos, array.embeddedDesc.getAttributes()[attribute].getDefaultCompressionMethod());
    }

    Chunk& EmbeddedArrayIterator::newChunk(Coordinates const& pos, int compressionMethod)
    {
        Coordinates physicalPos = pos;
        array.virtualToPhysical(physicalPos);
        if (!iterator->setPosition(physicalPos)) {
            chunk.setChunk(iterator->newChunk(physicalPos, compressionMethod));
        } else {
            chunk.setChunk((Chunk&)iterator->getChunk());
        }
        return chunk;
    }

    //
    // Embedded array chunk iterator methods
    //
    int EmbeddedChunkIterator::getMode()
    {
        return iterator->getMode();
    }

    ConstChunk const& EmbeddedChunkIterator::getChunk()
    {
        return *chunk;
    }

     Value& EmbeddedChunkIterator::getItem()
    {
         if (end())
             throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        return iterator->getItem();
    }

    bool EmbeddedChunkIterator::isEmpty()
    {
        return iterator->isEmpty();
    }

    bool EmbeddedChunkIterator::end()
    {
        return iterator->end() || !array.contains(iterator->getPosition());
    }

    void EmbeddedChunkIterator::operator ++()
    {
        ++(*iterator);
    }

    Coordinates const& EmbeddedChunkIterator::getPosition()
    {
        pos = iterator->getPosition();
        array.physicalToVirtual(pos);
        return pos;
    }

    bool EmbeddedChunkIterator::setPosition(Coordinates const& pos)
    {
        Coordinates physicalPos = pos;
        array.virtualToPhysical(physicalPos);
        return iterator->setPosition(physicalPos);
    }

    void EmbeddedChunkIterator::reset()
    {
        iterator->reset();
        pos = iterator->getPosition();
        array.physicalToVirtual(pos);
        array.virtualToPhysical(pos);
        iterator->setPosition(pos);
    }

    void EmbeddedChunkIterator::writeItem(const  Value& item)
    {
        if (end())
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        iterator->writeItem(item);
    }

    void EmbeddedChunkIterator::flush()
    {
        iterator->flush();
    }

    EmbeddedChunkIterator::EmbeddedChunkIterator(EmbeddedArray& arr, EmbeddedChunk const* aChunk, boost::shared_ptr<ChunkIterator> chunkIterator)
    : array(arr), chunk(aChunk), iterator(chunkIterator)
    {
    }
}


