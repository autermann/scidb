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
 * @file CrossArray.cpp
 *
 * @brief Cross array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include "CrossArray.h"
#include "system/Exceptions.h"

namespace scidb
{
    using namespace boost;

    //
    // Cross chunk methods
    //
    Array const& CrossChunk::getArray() const 
    { 
        return array;
    }

    const ArrayDesc& CrossChunk::getArrayDesc() const
    {
        return array.desc;
    }
    
    const AttributeDesc& CrossChunk::getAttributeDesc() const
    {
        return array.desc.getAttributes()[attr];
    }

    int CrossChunk::getCompressionMethod() const
    {
        return leftChunk->getCompressionMethod();
    }


    Coordinates const& CrossChunk::getFirstPosition(bool withOverlap) const
    {
        return withOverlap ? firstPosWithOverlap : firstPos;
    }
        
    Coordinates const& CrossChunk::getLastPosition(bool withOverlap) const
    {
       return withOverlap ? lastPosWithOverlap : lastPos;
     }

    boost::shared_ptr<ConstChunkIterator> CrossChunk::getConstIterator(int iterationMode) const
    {
        return boost::shared_ptr<ConstChunkIterator>(new CrossChunkIterator(*this, iterationMode));
    }

    CrossChunk::CrossChunk(CrossArray const& cross, AttributeID attrID)
    : array(cross), attr(attrID)
    {    
    }
     
    void CrossChunk::setInputChunk(ConstChunk const* left, ConstChunk const* right)
    {
        leftChunk = left;
        rightChunk = right;

        Coordinates const& leftFirstPos = left->getFirstPosition(false);
        Coordinates const& rightFirstPos = right->getFirstPosition(false);
        firstPos.assign(leftFirstPos.begin(), leftFirstPos.end());
        firstPos.insert(firstPos.end(), rightFirstPos.begin(), rightFirstPos.end());

        Coordinates const& leftFirstPosWithOverlap = left->getFirstPosition(true);
        Coordinates const& rightFirstPosWithOverlap = right->getFirstPosition(true);
        firstPosWithOverlap.assign(leftFirstPosWithOverlap.begin(), leftFirstPosWithOverlap.end());
        firstPosWithOverlap.insert(firstPosWithOverlap.end(), rightFirstPosWithOverlap.begin(), rightFirstPosWithOverlap.end());

        Coordinates const& leftLastPos = left->getLastPosition(false);
        Coordinates const& rightLastPos = right->getLastPosition(false);
        lastPos.assign(leftLastPos.begin(), leftLastPos.end());
        lastPos.insert(lastPos.end(), rightLastPos.begin(), rightLastPos.end());

        Coordinates const& leftLastPosWithOverlap = left->getLastPosition(true);
        Coordinates const& rightLastPosWithOverlap = right->getLastPosition(true);
        lastPosWithOverlap.assign(leftLastPosWithOverlap.begin(), leftLastPosWithOverlap.end());
        lastPosWithOverlap.insert(lastPosWithOverlap.end(), rightLastPosWithOverlap.begin(), rightLastPosWithOverlap.end());
    }

    bool CrossChunk::isMaterialized() const
    {
        return false;
    }

    bool CrossChunk::isSparse() const
    {
        return leftChunk->isSparse() || rightChunk->isSparse();
    }

    //
    // Cross chunk iterator methods
    //
    int CrossChunkIterator::getMode()
    {
        return inputIterator->getMode();
    }

    Value& CrossChunkIterator::getItem()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        if (attrID >= array.nLeftAttrs + array.nRightAttrs) {
            return trueValue;
        }
        return inputIterator->getItem();
    }

    bool CrossChunkIterator::isEmpty()
    {
        return leftIterator->isEmpty() || rightIterator->isEmpty();
    }

    bool CrossChunkIterator::end()
    {
        return !hasCurrent;
    }

    void CrossChunkIterator::operator ++()
    {
        ++(*rightIterator);
        if (rightIterator->end()) { 
            rightIterator->reset();
            ++(*leftIterator);
            hasCurrent = !leftIterator->end() && !rightIterator->end();            
        } else { 
            hasCurrent = true;
        }
    }
    
    Coordinates const& CrossChunkIterator::getPosition() 
    {
        Coordinates const& leftPos = leftIterator->getPosition();
        Coordinates const& rightPos = rightIterator->getPosition();
        currentPos.assign(leftPos.begin(), leftPos.end());
        currentPos.insert(currentPos.end(), rightPos.begin(), rightPos.end());
        return currentPos;        
    }

    bool CrossChunkIterator::setPosition(Coordinates const& pos)
    {
        return hasCurrent = (leftIterator->setPosition(Coordinates(pos.begin(), pos.begin() + array.nLeftDims))            
                             & rightIterator->setPosition(Coordinates(pos.begin() + array.nLeftDims, pos.end())));
    }

    void CrossChunkIterator::reset()
    {
        leftIterator->reset();
        rightIterator->reset();
        hasCurrent = !leftIterator->end() && !rightIterator->end();
    }

    ConstChunk const& CrossChunkIterator::getChunk()
    {
        return chunk;
    }

    CrossChunkIterator::CrossChunkIterator(CrossChunk const& aChunk, int iterationMode)
    : array(aChunk.array),
        chunk(aChunk),
        leftIterator(aChunk.leftChunk->getConstIterator(iterationMode & ~INTENDED_TILE_MODE)),
        rightIterator(aChunk.rightChunk->getConstIterator(iterationMode & ~INTENDED_TILE_MODE)),
        inputIterator(aChunk.attr < array.nLeftAttrs ? leftIterator : rightIterator),
        attrID(aChunk.getAttributeDesc().getId())
    {
        trueValue.setBool(true);
        reset();
    }

    //
    // Cross array iterator methods
    //
    CrossArrayIterator::CrossArrayIterator(CrossArray const& cross, AttributeID attrID) 
    : array(cross), 
      attr(attrID), 
      chunk(cross, attrID),
      chunkInitialized(false)
    {
        ArrayDesc const& leftDesc = cross.leftArray->getArrayDesc();
        ArrayDesc const& rightDesc = cross.rightArray->getArrayDesc();
        AttributeID leftAttrID = 0, rightAttrID = 0;
        if(attrID < cross.nLeftAttrs)
        {
            leftAttrID = attrID;
            if(rightDesc.getEmptyBitmapAttribute())
            {
                rightAttrID = rightDesc.getEmptyBitmapAttribute()->getId();
            }
        }
        else
        {
            if(leftDesc.getEmptyBitmapAttribute())
            {
                leftAttrID = leftDesc.getEmptyBitmapAttribute()->getId();
            }

            //This happens UNLESS left is emptyable and right is not
            if(attrID - cross.nLeftAttrs < cross.nRightAttrs)
            {
                rightAttrID = attrID - cross.nLeftAttrs;
            }
        }

        leftIterator = cross.leftArray->getConstIterator(leftAttrID);
        rightIterator = cross.rightArray->getConstIterator(rightAttrID);
        reset();
	}

    ConstChunk const& CrossArrayIterator::getChunk()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        if (!chunkInitialized) { 
            chunk.setInputChunk(&leftIterator->getChunk(), &rightIterator->getChunk());
            chunkInitialized = true;
        }
        return chunk;
    }

	bool CrossArrayIterator::end()
	{
        return !hasCurrent;
    }

	void CrossArrayIterator::operator ++()
	{
        if (hasCurrent) {  
            chunkInitialized = false;
            ++(*rightIterator);
            if (rightIterator->end()) { 
                rightIterator->reset();
                ++(*leftIterator);
                hasCurrent = !leftIterator->end();            
            }
        }
    }

	Coordinates const& CrossArrayIterator::getPosition()
	{
        Coordinates const& leftPos = leftIterator->getPosition();
        Coordinates const& rightPos = rightIterator->getPosition();
        currentPos.assign(leftPos.begin(), leftPos.end());
        currentPos.insert(currentPos.end(), rightPos.begin(), rightPos.end());
		return currentPos;
	}

	bool CrossArrayIterator::setPosition(Coordinates const& pos)
	{
        chunkInitialized = false;
        return hasCurrent = (leftIterator->setPosition(Coordinates(pos.begin(), pos.begin() + array.nLeftDims))            
                             && rightIterator->setPosition(Coordinates(pos.begin() + array.nLeftDims, pos.end())));
	}

	void CrossArrayIterator::reset()
	{
        chunkInitialized = false;
        leftIterator->reset();
        rightIterator->reset();
        hasCurrent = !leftIterator->end() && !rightIterator->end();
	}
    
    //
    // Cross array methods
    //
    CrossArray::CrossArray(ArrayDesc& d, boost::shared_ptr<Array> left, boost::shared_ptr<Array> right)
    : desc(d), 
      leftArray(left),
      rightArray(right),
      nLeftDims(left->getArrayDesc().getDimensions().size()),
      nLeftAttrs(left->getArrayDesc().getAttributes(true).size()),
      nRightAttrs(right->getArrayDesc().getAttributes().size())
    {
    }
    
    const ArrayDesc& CrossArray::getArrayDesc() const
    { 
        return desc; 
    }

    boost::shared_ptr<ConstArrayIterator> CrossArray::getConstIterator(AttributeID id) const
	{
		return boost::shared_ptr<CrossArrayIterator>(new CrossArrayIterator(*this, id));
	}   
}
