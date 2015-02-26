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
 * @file SubArray.cpp
 *
 * @brief SubArray implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include "SubArray.h"
#include "system/Exceptions.h"

namespace scidb
{
    using namespace boost;
    

    void subarrayMappingArray(string const& dimName, string const& mappingArrayName, string const& tmpMappingArrayName, 
                              Coordinate from, Coordinate till, boost::shared_ptr<Query> const& query)
    {
        shared_ptr<Array> srcMappingArray = query->getArray(mappingArrayName);
        ArrayDesc const& srcMappingArrayDesc = srcMappingArray->getArrayDesc();
        shared_ptr<ConstArrayIterator> srcArrayIterator = srcMappingArray->getConstIterator(0);
        Coordinates origin(1);
        origin[0] = srcMappingArrayDesc.getDimensions()[0].getStart();
        srcArrayIterator->setPosition(origin);
        ConstChunk const& srcChunk = srcArrayIterator->getChunk();
        shared_ptr<ConstChunkIterator> srcChunkIterator = srcChunk.getConstIterator();
        
        Dimensions indexMapDim(1);
        indexMapDim[0] = DimensionDesc("no", 0, 0, till-from, till-from, till-from+1, 0);                
        ArrayDesc dstMappingArrayDesc(tmpMappingArrayName,
                                      srcMappingArrayDesc.getAttributes(), 
                                      indexMapDim, ArrayDesc::LOCAL|ArrayDesc::TEMPORARY); 
        shared_ptr<Array> dstMappingArray = boost::shared_ptr<Array>(new MemArray(dstMappingArrayDesc));
        if (till >= from) { 
            shared_ptr<ArrayIterator> dstArrayIterator = dstMappingArray->getIterator(0);
            Coordinates pos(1);
            Chunk& dstChunk = dstArrayIterator->newChunk(pos, 0);
            dstChunk.setRLE(false);
            shared_ptr<ChunkIterator> dstChunkIterator = dstChunk.getIterator(query);
            
            pos[0] = from;
            if (srcChunkIterator->setPosition(pos)) {
                while (from <= till && !srcChunkIterator->end()) { 
                    dstChunkIterator->writeItem(srcChunkIterator->getItem());
                    ++(*dstChunkIterator);
                    ++(*srcChunkIterator);
                    from += 1;
                }
            }
            dstChunkIterator->flush();
        }
        query->setTemporaryArray(dstMappingArray);
    }

    //
    // SubArray chunk methods
    //
    Coordinates const& SubArrayChunk::getFirstPosition(bool withOverlap) const
    {
        return withOverlap ? firstPosWithOverlap : firstPos;
    }
        
    Coordinates const& SubArrayChunk::getLastPosition(bool withOverlap) const
    {
       return withOverlap ? lastPosWithOverlap : lastPos;
     }

    boost::shared_ptr<ConstChunkIterator> SubArrayChunk::getConstIterator(int iterationMode) const
    {
        return boost::shared_ptr<ConstChunkIterator>(fullyBelongs 
                                              ? (ConstChunkIterator*)new SubArrayDirectChunkIterator(*this, iterationMode)
                                              : (ConstChunkIterator*)new SubArrayChunkIterator(*this, iterationMode));
    }

    SubArrayChunk::SubArrayChunk(SubArray const& subarray, DelegateArrayIterator const& iterator, AttributeID attrID)
    : DelegateChunk(subarray, iterator, attrID, false),
      array(subarray),
      firstPos(array.dims.size()),
      firstPosWithOverlap(firstPos.size()),
      lastPos(firstPos.size()),
      lastPosWithOverlap(firstPos.size())
    {    
    }
     
    void SubArrayChunk::setPosition(Coordinates const& pos)
    {
        firstPos = pos;        
        fullyBelongs = array.aligned;
        Dimensions const& dims = array.dims;
        for (size_t i = 0, nDims = dims.size(); i < nDims; i++) { 
            firstPosWithOverlap[i] = firstPos[i] - dims[i].getChunkOverlap();
            if (firstPosWithOverlap[i] < 0) {
                firstPosWithOverlap[i] = 0;
            }
            lastPos[i] = firstPos[i] + dims[i].getChunkInterval() - 1;
            if (lastPos[i] > dims[i].getEndMax()) { 
                lastPos[i] = dims[i].getEndMax();
                if (array.subarrayHighPos[i] != array.inputDims[i].getEndMax()) { 
                    fullyBelongs = false;
                }
            }
            lastPosWithOverlap[i] = lastPos[i] + dims[i].getChunkOverlap();
            if (lastPosWithOverlap[i] > dims[i].getEndMax()) { 
                lastPosWithOverlap[i] = dims[i].getEndMax();
            }
        }
        isClone = fullyBelongs && !isSparse();
    }


    //
    // SubArray chunk iterator methods
    //
    int SubArrayChunkIterator::getMode()
    {
        return mode;
    }

     Value& SubArrayChunkIterator::getItem()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return inputIterator->getItem();
    }

    bool SubArrayChunkIterator::isEmpty()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return inputIterator->isEmpty();
    }

    bool SubArrayChunkIterator::end()
    {
        return !hasCurrent;
    }

    void SubArrayChunkIterator::operator ++()
    {
        bool withOverlaps = (mode & IGNORE_OVERLAPS) == 0;
        Coordinates const& first = chunk.getFirstPosition(withOverlaps);
        Coordinates const& last = chunk.getLastPosition(withOverlaps);
        size_t nDims = outPos.size();
        while (true) {
            size_t i = nDims-1;
            while (++outPos[i] > last[i]) { 
                outPos[i] = first[i];
                if (i-- == 0) { 
                    hasCurrent = false;
                    return;
                }
            }
            array.out2in(outPos, inPos);
            if (!inputIterator || !inputChunk->contains(inPos, withOverlaps)) {
                inputIterator.reset();
                boost::shared_ptr<ConstArrayIterator> inputArrayIterator = chunk.getArrayIterator().getInputIterator();
                if (inputArrayIterator->setPosition(inPos)) { 
                    inputChunk = &inputArrayIterator->getChunk();
                    inputIterator = inputChunk->getConstIterator(mode);
                } else { 
                    continue;
                }
            }
            if (inputIterator && inputIterator->setPosition(inPos)) {
                hasCurrent = true;
                return;
            }
        }
    }

    Coordinates const& SubArrayChunkIterator::getPosition() 
    {
        return outPos;
    }

    bool SubArrayChunkIterator::setPosition(Coordinates const& pos)
    {
        outPos = pos;
        array.out2in(outPos, inPos);
        if (!inputIterator || !inputChunk->contains(inPos, (mode & IGNORE_OVERLAPS) == 0)) {
            inputIterator.reset();
            boost::shared_ptr<ConstArrayIterator> inputArrayIterator = chunk.getArrayIterator().getInputIterator();
            if (inputArrayIterator->setPosition(inPos)) { 
                inputChunk = &inputArrayIterator->getChunk();
                inputIterator = inputChunk->getConstIterator(mode);
            } else { 
                return hasCurrent = false;
            }
        }
        return hasCurrent = inputIterator->setPosition(inPos);
    }

    void SubArrayChunkIterator::reset()
    {
        outPos = chunk.getFirstPosition((mode & IGNORE_OVERLAPS) == 0);
        outPos[outPos.size()-1] -= 1; 
        ++(*this);
    }

    ConstChunk const& SubArrayChunkIterator::getChunk()
    {
        return chunk;
    }

    SubArrayChunkIterator::SubArrayChunkIterator(SubArrayChunk const& aChunk, int iterationMode)
    : array(aChunk.array),
      chunk(aChunk),
      inputChunk(&aChunk.getInputChunk()),
      inputIterator(inputChunk->getConstIterator(iterationMode & ~INTENDED_TILE_MODE)),
      outPos(array.dims.size()),
      inPos(outPos.size()),
      hasCurrent(false),
      mode(iterationMode & ~INTENDED_TILE_MODE)
    {
        reset();
    }

    //
    // SubArray direct chunk iterator methods
    //
    Coordinates const& SubArrayDirectChunkIterator::getPosition()
    {
        array.in2out(inputIterator->getPosition(), currPos);
        return currPos;
    }

    bool SubArrayDirectChunkIterator::setPosition(Coordinates const& outPos)
    {
        array.out2in(outPos, currPos);
        return inputIterator->setPosition(currPos);
    }

    SubArrayDirectChunkIterator::SubArrayDirectChunkIterator(SubArrayChunk const& chunk, int iterationMode)
    : DelegateChunkIterator(&chunk, iterationMode),
      array(chunk.array),
      currPos(array.dims.size())
    {
    }
      

    //
    // SubArray iterator methods
    //
    SubArrayIterator::SubArrayIterator(SubArray const& subarray, AttributeID attrID) 
    : DelegateArrayIterator(subarray, attrID, subarray.inputArray->getConstIterator(attrID)),
      array(subarray), 
      outPos(subarray.subarrayLowPos.size()),
      inPos(outPos.size()),
      hasCurrent(false),
      positioned(false),
      outChunkPos(outPos.size())
    {
	}

    inline void SubArrayIterator::checkState() 
    { 
        if (!positioned) { 
            reset();
        }
    }

	bool SubArrayIterator::end()
	{
        checkState();
        return !hasCurrent;
    }

    void SubArrayIterator::fillSparseChunk(size_t i) 
    {
        Dimensions const& dims = array.dims;
        chunkInitialized = false;
        if (i == dims.size()) {
            if (inputIterator->setPosition(inPos)) { 
                ConstChunk const& inChunk = inputIterator->getChunk();
                boost::shared_ptr<ConstChunkIterator> inIterator = inChunk.getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS|ConstChunkIterator::IGNORE_EMPTY_CELLS);
                
                while (!inIterator->end()) {
                    Coordinates const& inChunkPos = inIterator->getPosition();                    
                    array.in2out(inChunkPos, outChunkPos);
                    if (outIterator->setPosition(outChunkPos)) { 
                        outIterator->writeItem(inIterator->getItem());
                    }
                    ++(*inIterator);
                }
            }
        } else { 
            fillSparseChunk(i+1);
            
            size_t interval = dims[i].getChunkInterval() - 1;
            inPos[i] += interval;
            fillSparseChunk(i+1);
            inPos[i] -= interval;
        }
    }
         
    ConstChunk const& SubArrayIterator::getChunk() 
    { 
        checkState();
        if (!chunkInitialized) { 
            chunkInitialized = true;
            ConstChunk const& inChunk = inputIterator->getChunk();
            if (inChunk.isSparse()) {
                ArrayDesc const& desc = array.getArrayDesc();
                Address addr(desc.getId(), attr, outPos);
                sparseChunk.initialize(&array, &desc, addr, 0);
                sparseChunk.setSparse(true);
                if (sparseChunk.isRLE() && !emptyIterator) { 
                    AttributeDesc const* emptyAttr = desc.getEmptyBitmapAttribute();
                    if (emptyAttr != NULL && emptyAttr->getId() != attr) {
                        emptyIterator = array.getConstIterator(emptyAttr->getId());
                    }
                }
                if (emptyIterator) {
                    if (!emptyIterator->setPosition(outPos))
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                    sparseChunk.setBitmapChunk((Chunk*)&emptyIterator->getChunk());
                }
                boost::shared_ptr<Query> dummyQuery;
                outIterator = sparseChunk.getIterator(dummyQuery, ChunkIterator::NO_EMPTY_CHECK);
                fillSparseChunk(0);
                outIterator->flush();
                return sparseChunk;
            }
            chunk->setInputChunk(inChunk);
            ((SubArrayChunk&)*chunk).setPosition(outPos);
        }
        return *chunk;
    }

    bool SubArrayIterator::setInputPosition(size_t i) 
    { 
        Dimensions const& dims = array.dims;
        chunkInitialized = false;
        if (i == dims.size()) {
            return inputIterator->setPosition(inPos);
        }
        if (setInputPosition(i+1)) { 
            return true;
        }
        size_t interval = dims[i].getChunkInterval() - 1;
        inPos[i] += interval;
        bool rc = setInputPosition(i+1);
        inPos[i] -= interval;
        return rc;
    }

	void SubArrayIterator::operator ++()
    {
        checkState();
        const Dimensions& dims = array.dims;
        size_t nDims = dims.size();
        chunkInitialized = false;
        while (true) { 
            size_t i = nDims-1;
            while ((outPos[i] += dims[i].getChunkInterval()) > dims[i].getEndMax()) { 
                if (i == 0) { 
                    hasCurrent = false;
                    return;
                }
                outPos[i--] = 0;
            }
            array.out2in(outPos, inPos);
            if (setInputPosition(0)) { 
                hasCurrent = true;
                return;
            }
        }        
    }

	Coordinates const& SubArrayIterator::getPosition()
	{ 
        checkState();
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return outPos;
	}

	bool SubArrayIterator::setPosition(Coordinates const& pos)
	{
        positioned = true;
        outPos = pos;
        array.getArrayDesc().getChunkPositionFor(outPos);
        array.out2in(outPos, inPos); 
        return hasCurrent = setInputPosition(0);
	}

	void SubArrayIterator::reset()
	{
        positioned = true;
        const Dimensions& dims = array.dims;
        size_t nDims = dims.size();
 		for (size_t i = 0; i < nDims; i++) {
            outPos[i] = 0;
        }
        chunkInitialized = false;
        outPos[nDims-1] -= dims[nDims-1].getChunkInterval();
        ++(*this);
	}
    
    //
    // SubArray methods
    //
    SubArray::SubArray(ArrayDesc& array, Coordinates lowPos, Coordinates highPos, boost::shared_ptr<Array> input)
    : DelegateArray(array, input), 
      subarrayLowPos(lowPos), 
      subarrayHighPos(highPos),
      dims(desc.getDimensions()),
      inputDims(input->getArrayDesc().getDimensions())
	{
        aligned = true;
        for (size_t i = 0, n = dims.size(); i < n; i++) { 
            if ((lowPos[i] - inputDims[i].getStart()) % dims[i].getChunkInterval() != 0) { 
                aligned = false;
                break;
            }
        }
    }
    
    DelegateArrayIterator* SubArray::createArrayIterator(AttributeID attrID) const
    {
        return new SubArrayIterator(*this, attrID);
    }


    DelegateChunk* SubArray::createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
    {
        return new SubArrayChunk(*this, *iterator, attrID);       
    }

    void SubArray::out2in(Coordinates const& out, Coordinates& in) const
    {
        for (size_t i = 0, n = out.size(); i < n; i++) { 
            in[i] = out[i] + subarrayLowPos[i];
        }
    }

    void SubArray::in2out(Coordinates const& in, Coordinates& out) const
    {
        for (size_t i = 0, n = in.size(); i < n; i++) { 
            out[i] = in[i] - subarrayLowPos[i];
        }
    }
}
