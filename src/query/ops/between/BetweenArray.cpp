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
 * @file BetweenArray.cpp
 *
 * @brief Between array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include "BetweenArray.h"
#include "system/Exceptions.h"

namespace scidb
{
    using namespace boost;
    
    //
    // Between chunk methods
    //
    boost::shared_ptr<ConstChunkIterator> BetweenChunk::getConstIterator(int iterationMode) const
    {
        AttributeDesc const& attr = getAttributeDesc();
        if (tileMode) { 
            iterationMode |= ChunkIterator::TILE_MODE;
        } else { 
            if (iterationMode & ChunkIterator::TILE_MODE) {
                BetweenChunk* self = (BetweenChunk*)this;
                self->tileMode = true;
                AttributeDesc const* emptyAttr = array.getInputArray()->getArrayDesc().getEmptyBitmapAttribute();
                if (emptyAttr != NULL) { 
                    self->emptyBitmapIterator = array.getInputArray()->getConstIterator(emptyAttr->getId());
                }
            }
            // iterationMode &= ~ChunkIterator::TILE_MODE;
        }        
        iterationMode &= ~ChunkIterator::INTENDED_TILE_MODE;
        return boost::shared_ptr<ConstChunkIterator>(
            attr.isEmptyIndicator()
            ? (attrID >= array.getInputArray()->getArrayDesc().getAttributes().size())
               ? fullyInside
                  ? (ConstChunkIterator*)new EmptyBitmapBetweenChunkIterator(*this, iterationMode & ~ConstChunkIterator::IGNORE_DEFAULT_VALUES)          
                  : (ConstChunkIterator*)new NewBitmapBetweenChunkIterator(*this, iterationMode & ~ConstChunkIterator::IGNORE_DEFAULT_VALUES)
               : fullyInside
                  ? (ConstChunkIterator*)new DelegateChunkIterator(this, iterationMode & ~ConstChunkIterator::IGNORE_DEFAULT_VALUES)                                
                  : (ConstChunkIterator*)new ExistedBitmapBetweenChunkIterator(*this, iterationMode & ~ConstChunkIterator::IGNORE_DEFAULT_VALUES)        
            : fullyInside 
                ? (ConstChunkIterator*)new DelegateChunkIterator(this, iterationMode)          
                : (ConstChunkIterator*)new BetweenChunkIterator(*this, iterationMode));            
    }

    BetweenChunk::BetweenChunk(BetweenArray const& arr, DelegateArrayIterator const& iterator, AttributeID attrID)
    : DelegateChunk(arr, iterator, attrID, false),
      array(arr)
    {    
        if (array.tileMode) {
            tileMode = true;
            AttributeDesc const* emptyAttr = array.getInputArray()->getArrayDesc().getEmptyBitmapAttribute();
            if (emptyAttr != NULL) { 
                emptyBitmapIterator = array.getInputArray()->getConstIterator(emptyAttr->getId());
            }
        } else { 
            tileMode = false;
        }
    }
     
    void BetweenChunk::setInputChunk(ConstChunk const& inputChunk)
    {
        DelegateChunk::setInputChunk(inputChunk);
        firstPos = inputChunk.getFirstPosition(true);        
        lastPos = inputChunk.getLastPosition(true);     
        fullyInside = true;
        fullyOutside = false;
        Dimensions const& dims = array.dims;
        for (size_t i = 0, nDims = dims.size(); i < nDims; i++) { 
            if (firstPos[i] < array.lowPos[i]) {
                firstPos[i] = array.lowPos[i];
                fullyInside = false;
                if (firstPos[i] > lastPos[i]) { 
                    fullyOutside = true;
                }
            }
            if (lastPos[i] > array.highPos[i]) {
                lastPos[i] = array.highPos[i];
                fullyInside = false;
                if (lastPos[i] < firstPos[i]) {
                    fullyOutside = true;
                }
            }
        }
        isClone = fullyInside && attrID < array.getInputArray()->getArrayDesc().getAttributes().size();
        if (emptyBitmapIterator) { 
            if (!emptyBitmapIterator->setPosition(inputChunk.getFirstPosition(false)))
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
        }
    }


    //
    // Between chunk iterator methods
    //
    int BetweenChunkIterator::getMode()
    {
        return mode;
    }

    Value& BetweenChunkIterator::buildBitmap()
    {
        Coordinates coord = inputIterator->getPosition();
        if (!emptyBitmapIterator->setPosition(coord)) 
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";

        if (chunk.fullyInside) { 
            return emptyBitmapIterator->getItem();
        }
        RLEPayload* emptyBitmap = emptyBitmapIterator->getItem().getTile();
        RLEPayload::append_iterator appender(tileValue.getTile(type));
        Value trueVal, falseVal;
        trueVal.setBool(true);
        falseVal.setBool(false);

        if (chunk.fullyOutside) {
            appender.add(falseVal, emptyBitmap->count());                    
        } else { 
            position_t pos = coord2pos(coord);
            size_t nDims = coord.size();
        
            RLEPayload::iterator ei(emptyBitmap);
            while (!ei.end()) {
                uint64_t count = ei.getRepeatCount();
                Value* boolVal = &falseVal;
                if (ei.checkBit()) { 
                    pos2coord(pos, coord); 
                    size_t i = 0; 
                    while (i < nDims && coord[i] >= chunk.firstPos[i]) { 
                        if (coord[i] > chunk.lastPos[i]) { 
                            do { 
                                if (i == 0) {
                                    position_t count = emptyBitmap->count();
                                    if (count > pos) {
                                        appender.add(falseVal, count - pos);
                                    }
                                    goto EndOfTile;
                                }
                                i -= 1;
                            } while (++coord[i] > chunk.lastPos[i]);
                            i += 1;
                            break;
                        }
                        i += 1;
                    }
                    while (i < nDims) {
                        coord[i] = chunk.firstPos[i];
                        i += 1;
                    }
                    position_t nextPos = coord2pos(coord);
                    assert(nextPos >= pos);
                    uint64_t skip = nextPos - pos;
                    if (skip != 0) {
                        count = min(count, skip);
                    } else {
                        assert(chunk.lastPos[nDims-1] >= coord[nDims-1]);
                        uint64_t tail = chunk.lastPos[nDims-1] - coord[nDims-1] + 1;
                        count = min(tail, count);
                        boolVal = &trueVal;
                    }
                }
                appender.add(*boolVal, count);                    
                ei += count;
                pos += count;
            }
        }
      EndOfTile:
        appender.flush();
        return tileValue;
    }

    Value& BetweenChunkIterator::getItem()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        Value& value = inputIterator->getItem();
        if (mode & TILE_MODE) {  
            if (chunk.fullyInside) {
                return value;
            }
            RLEPayload* inputPayload = value.getTile();
            RLEPayload::append_iterator appender(tileValue.getTile(type));
            if (!chunk.fullyOutside) {
                RLEPayload::iterator vi(inputPayload);
                Coordinates coord = inputIterator->getPosition();
                if (!emptyBitmapIterator->setPosition(coord)) 
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                RLEPayload* emptyBitmap = emptyBitmapIterator->getItem().getTile();
                RLEPayload::iterator ei(emptyBitmap);
                Value v;
                position_t pos = coord2pos(coord);
                size_t nDims = coord.size();

                while (!ei.end()) {
                    uint64_t count = ei.getRepeatCount();
                    if (ei.checkBit()) { 
                        pos2coord(pos, coord); 
                        size_t i = 0; 
                        while (i < nDims && coord[i] >= chunk.firstPos[i]) { 
                            if (coord[i] > chunk.lastPos[i]) { 
                                do { 
                                    if (i == 0) { 
                                        goto EndOfTile;
                                    }
                                    i -= 1;
                                } while (++coord[i] > chunk.lastPos[i]);
                                i += 1;
                                break;
                            }
                            i += 1;
                        }
                        while (i < nDims) {
                            coord[i] = chunk.firstPos[i];
                            i += 1;
                        }
                        position_t nextPos = coord2pos(coord);
                        assert(nextPos >= pos);
                        uint64_t skip = nextPos - pos;
                        if (skip != 0) {
                            pos += skip;
                            vi += ei.skip(skip);
                            continue;
                        }
                        assert(chunk.lastPos[nDims-1] >= coord[nDims-1]);
                        uint64_t tail = chunk.lastPos[nDims-1] - coord[nDims-1] + 1;
                        nextPos += tail;
                        count = appender.add(vi, min(tail, count));
                        
                        while (position_t(pos + count) < nextPos) { 
                            ei += count;
                            pos += count;
                            tail -= count;
                            if (ei.end()) { 
                                goto EndOfTile;
                            }
                            count = ei.getRepeatCount();
                            if (ei.checkBit()) { 
                                count = appender.add(vi, min(tail, count));
                            }
                        }
                    }
                    ei += count;
                    pos += count;
                }
            }
          EndOfTile:
            appender.flush();
            return tileValue;
        }
        return value;
    }

    inline bool BetweenChunkIterator::between() const
    { 
        for (size_t i = 0, n = array.dims.size(); i < n; i++) { 
            if (currPos[i] < array.lowPos[i] || currPos[i] > array.highPos[i]) { 
                return false;
            }
        }
        return true;
    }

    bool BetweenChunkIterator::isEmpty()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return inputIterator->isEmpty() || !between();
    }

    bool BetweenChunkIterator::end()
    {
        return !hasCurrent;
    }

    void BetweenChunkIterator::operator ++()
    {
        if ((mode & (IGNORE_EMPTY_CELLS|TILE_MODE)) == IGNORE_EMPTY_CELLS) { 
            if (isSparse) {
                while (true) { 
                    ++(*inputIterator);
                    if (!inputIterator->end()) { 
                        currPos = inputIterator->getPosition();
                        if (between()) { 
                            hasCurrent = true;
                            return;
                        }
                    } else { 
                        break;
                    }
                }  
                hasCurrent = false;                                                    
            } else { 
                Coordinates const& first = chunk.firstPos;
                Coordinates const& last = chunk.lastPos;
                size_t nDims = currPos.size();
                while (true) {
                    size_t i = nDims-1;
                    while (++currPos[i] > last[i]) { 
                        currPos[i] = first[i];
                        if (i-- == 0) { 
                            hasCurrent = false;
                            return;
                        }
                    }
                    if (inputIterator->setPosition(currPos)) { 
                        hasCurrent = true;
                        return;
                    }
                }
            }
        } else { 
            ++(*inputIterator);
            hasCurrent = !inputIterator->end();
        }
    }

    Coordinates const& BetweenChunkIterator::getPosition() 
    {
        return (mode & (TILE_MODE|IGNORE_EMPTY_CELLS)) == IGNORE_EMPTY_CELLS ? currPos : inputIterator->getPosition();
    }

    bool BetweenChunkIterator::setPosition(Coordinates const& pos)
    {
        if ((mode & (TILE_MODE|IGNORE_EMPTY_CELLS)) == IGNORE_EMPTY_CELLS) { 
            for (size_t i = 0, n = currPos.size(); i < n; i++) { 
                if (pos[i] < chunk.firstPos[i] || pos[i] > chunk.lastPos[i]) { 
                    return hasCurrent = false;
                }
            }
            currPos = pos;
        }
        return hasCurrent = inputIterator->setPosition(pos);
    }

    void BetweenChunkIterator::reset()
    {
        if ((mode & (TILE_MODE|IGNORE_EMPTY_CELLS)) == IGNORE_EMPTY_CELLS) { 
            if (isSparse) { 
                inputIterator->reset();
                if (!inputIterator->end()) { 
                    currPos = inputIterator->getPosition();
                    if (!between()) { 
                        ++(*this);
                    } else { 
                        hasCurrent = true;
                    }
                } else { 
                    hasCurrent = false;
                }
            } else { 
                currPos = chunk.firstPos;
                currPos[currPos.size()-1] -= 1; 
                ++(*this);
            }
        } else { 
            inputIterator->reset();
            hasCurrent = !inputIterator->end();
        }
    }

    ConstChunk const& BetweenChunkIterator::getChunk()
    {
        return chunk;
    }

    BetweenChunkIterator::BetweenChunkIterator(BetweenChunk const& aChunk, int iterationMode)
    : CoordinatesMapper(aChunk),
      array(aChunk.array),
      chunk(aChunk),
      inputIterator(aChunk.getInputChunk().getConstIterator(iterationMode & ~INTENDED_TILE_MODE)),
      currPos(array.dims.size()),
      isSparse(inputIterator->getChunk().isSparse()),
      mode(iterationMode),
      type(chunk.getAttributeDesc().getType())
    {
        if (iterationMode & TILE_MODE) { 
            if (chunk.emptyBitmapIterator) {
                emptyBitmapIterator = chunk.emptyBitmapIterator->getChunk().getConstIterator((iterationMode & IGNORE_OVERLAPS)|TILE_MODE|IGNORE_EMPTY_CELLS);
            } else { 
                ArrayDesc const& arrayDesc = chunk.getArrayDesc();
                Address addr(arrayDesc.getEmptyBitmapAttribute()->getId(), chunk.getFirstPosition(false));
                shapeChunk.initialize(&array, &arrayDesc, addr, 0);
                emptyBitmapIterator = shapeChunk.getConstIterator((iterationMode & IGNORE_OVERLAPS)|TILE_MODE|IGNORE_EMPTY_CELLS);
            }
            hasCurrent = !inputIterator->end();
        } else {
            reset();
        }
    }

    //
    // Exited bitmap chunk iterator methods
    //
     Value& ExistedBitmapBetweenChunkIterator::getItem()
    { 
        if (mode & TILE_MODE) { 
            return buildBitmap();
        }
        _value.setBool(inputIterator->getItem().getBool() && between());
        return _value;
    }

    ExistedBitmapBetweenChunkIterator::ExistedBitmapBetweenChunkIterator(BetweenChunk const& chunk, int iterationMode)
    : BetweenChunkIterator(chunk, iterationMode), _value(TypeLibrary::getType(TID_BOOL))
    {
    }
    
    //
    // New bitmap chunk iterator methods
    //
    Value& NewBitmapBetweenChunkIterator::getItem()
    { 
        if (mode & TILE_MODE) { 
            return buildBitmap();
        }
        _value.setBool(between());
        return _value;
    }

    NewBitmapBetweenChunkIterator::NewBitmapBetweenChunkIterator(BetweenChunk const& chunk, int iterationMode) 
    : BetweenChunkIterator(chunk, iterationMode), _value(TypeLibrary::getType(TID_BOOL))
    {
        ((DelegateChunk&)chunk).overrideSparse();
    }

    //
    // Empty bitmap chunk iterator methods
    //
    Value& EmptyBitmapBetweenChunkIterator::getItem()
    { 
        if (mode & TILE_MODE) { 
            if (!emptyBitmapIterator->setPosition(inputIterator->getPosition())) 
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            return emptyBitmapIterator->getItem();
        }
        return _value;
    }

    bool EmptyBitmapBetweenChunkIterator::isEmpty()
    {
        return false;
    }

    EmptyBitmapBetweenChunkIterator::EmptyBitmapBetweenChunkIterator(BetweenChunk const& chunk, int iterationMode) 
    : NewBitmapBetweenChunkIterator(chunk, iterationMode)
    {
        _value.setBool(true);
    }

    //
    // Between array iterator methods
    //
    BetweenArrayIterator::BetweenArrayIterator(BetweenArray const& arr, AttributeID attrID, AttributeID inputAttrID, bool doReset)
    : DelegateArrayIterator(arr, attrID, arr.inputArray->getConstIterator(inputAttrID)),
      array(arr), 
      lowPos(arr.lowPos),
      highPos(arr.highPos),
      pos(arr.dims.size())
      
    {
        ArrayDesc const& desc = arr.getArrayDesc();
        desc.getChunkPositionFor(lowPos);
        desc.getChunkPositionFor(highPos);
        if ( doReset )
        {
            reset();
        }
	}

	bool BetweenArrayIterator::end()
	{
        return !hasCurrent;
    }

	bool BetweenArrayIterator::insideBox(Coordinates const& coords) const
	{
	    Dimensions const& dims = array.dims;
	    size_t nDims = dims.size();
	    for(size_t i=0; i<nDims; i++)
	    {
	        if(coords[i]<lowPos[i] || coords[i] >= highPos[i] + dims[i].getChunkInterval())
	        {
	            return false;
	        }
	    }
	    return true;
	}

	void BetweenArrayIterator::operator ++()
    {
        Dimensions const& dims = array.dims;
        size_t nDims = dims.size();
        chunkInitialized = false;
        hasCurrent = false;
        ++(*inputIterator);
        if(inputIterator->end()) { return; }

        Coordinates const& currPos = inputIterator->getPosition();
        if (insideBox(currPos))
        {
            pos=currPos;
            hasCurrent = true;
            return;
        }
        while (true)
        { 
            size_t i = nDims-1;
            while ((pos[i] += dims[i].getChunkInterval()) > highPos[i])
            {
                if (i == 0)
                {
                    hasCurrent = false;
                    return;
                }
                pos[i]  = lowPos[i];
                i -= 1;
            }
            if (inputIterator->setPosition(pos))
            {
                hasCurrent = true;
                return;
            }
        }
    }

    Coordinates const& BetweenArrayIterator::getPosition()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return pos;
    }

	bool BetweenArrayIterator::setPosition(Coordinates const& newPos)
	{
        const Dimensions& dims = array.dims;
        for (size_t i = 0, n = array.dims.size(); i < n; i++) { 
            if (newPos[i] < lowPos[i] || newPos[i] >= highPos[i] + dims[i].getChunkInterval()) { 
                return hasCurrent = false;
            }
        }
        chunkInitialized = false;
        pos = newPos;
        array.getArrayDesc().getChunkPositionFor(pos);
        return hasCurrent = inputIterator->setPosition(pos);
	}

	void BetweenArrayIterator::reset()
	{
 		chunkInitialized = false;
        hasCurrent = false;
        const Dimensions& dims = array.dims;
        size_t nDims = dims.size();
        for (size_t i = 0; i < nDims; i++) {
            pos[i] = lowPos[i];
        }
        pos[nDims-1] -= dims[nDims-1].getChunkInterval();
        if(!inputIterator->end())
        {
            Coordinates const& currPos = inputIterator->getPosition();
            hasCurrent = insideBox(currPos);
            if(!hasCurrent)
            {
                ++(*this);
            }
            else
            {
                pos = currPos;
            }
        }
	}
    
	BetweenArraySequentialIterator::BetweenArraySequentialIterator(BetweenArray const& between, AttributeID attrID, AttributeID inputAttrID):
        BetweenArrayIterator(between, attrID, inputAttrID, false)
    {
	    //need to call this class's reset, not parent's.
	    reset();
    }
    
    void BetweenArraySequentialIterator::operator ++()
    {
        hasCurrent = false;
        chunkInitialized = false;
        while (!hasCurrent && !inputIterator->end())
        {
            ++(*inputIterator);
            if(inputIterator->end()) { return; }
            pos = inputIterator->getPosition();
            hasCurrent = insideBox(pos);
        }
    }
    
    void BetweenArraySequentialIterator::reset()
    {
        chunkInitialized = false;
        inputIterator->reset();

        if (inputIterator->end())
        {
            hasCurrent = false;
        }
        else
        {
            pos = inputIterator->getPosition();
            hasCurrent = insideBox(pos);
            if(!hasCurrent)
            {
                ++(*this);
            }
        }
    }
    
    //
    // Between array methods
    //
    BetweenArray::BetweenArray(ArrayDesc& array, Coordinates const& from, Coordinates const& till, boost::shared_ptr<Array> input, bool tile)
    : DelegateArray(array, input), 
      lowPos(from), 
      highPos(till),
      dims(desc.getDimensions()),
      tileMode(tile)
	{
        double numChunksInBox = 1;
        ArrayDesc const& inputDesc = input->getArrayDesc();
        for (size_t i=0, n = inputDesc.getDimensions().size(); i<n; i++)
        {
            numChunksInBox *= inputDesc.getNumChunksAlongDimension(i, lowPos[i], highPos[i]);
        }
        useSequentialIterator = (numChunksInBox > BETWEEN_SEQUENTIAL_ITERATOR_THRESHOLD );
    }
    
    DelegateArrayIterator* BetweenArray::createArrayIterator(AttributeID attrID) const
    {
        AttributeID inputAttrID = attrID;
        if (inputAttrID >= inputArray->getArrayDesc().getAttributes().size()) {
            inputAttrID = 0;
        }

        if(useSequentialIterator)
        {
            return new BetweenArraySequentialIterator(*this, attrID, inputAttrID);
        }
        else
        {
            return new BetweenArrayIterator(*this, attrID, inputAttrID);
        }
    }


    DelegateChunk* BetweenArray::createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
    {
        return new BetweenChunk(*this, *iterator, attrID);       
    }
}
