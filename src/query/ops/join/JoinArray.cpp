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
 * JoinArray.h
 *
 *  Created on: Oct 22, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "JoinArray.h"


using namespace std;
using namespace boost;

namespace scidb 
{
    //
    // Chunk iterator
    //
    inline bool JoinChunkIterator::join() 
    { 
        return joinIterator->setPosition(inputIterator->getPosition());
    }

    bool JoinChunkIterator::isEmpty()
    {    
        return inputIterator->isEmpty() || !join();
    }

    bool JoinChunkIterator::end()
    {
        return !hasCurrent;
    }

    void JoinChunkIterator::alignIterators() 
    { 
        while (!inputIterator->end()) {
            if (!(mode & IGNORE_EMPTY_CELLS) || join()) { 
                hasCurrent = true;
                return;
            }
            ++(*inputIterator);
        }
        hasCurrent = false;
    }

    void JoinChunkIterator::reset()
    {
        inputIterator->reset();
        joinIterator->reset();
        alignIterators();
    }

    bool JoinChunkIterator::setPosition(Coordinates const& pos)
    {
        if (inputIterator->setPosition(pos)) { 
            return hasCurrent = !(mode & IGNORE_EMPTY_CELLS) || join();
        }
        return hasCurrent = false;
    }

    void JoinChunkIterator::operator ++()
    {
        ++(*inputIterator);
        alignIterators();
    }

    JoinChunkIterator::JoinChunkIterator(JoinEmptyableArrayIterator const& arrayIterator, DelegateChunk const* chunk, int iterationMode)
    : DelegateChunkIterator(chunk, iterationMode),
      joinIterator(arrayIterator.joinIterator->getChunk().getConstIterator(iterationMode)),
      mode(iterationMode)
    {
        alignIterators();
    }
    
     Value& JoinBitmapChunkIterator::getItem()
    {
        value.setBool(inputIterator->getItem().getBool() && joinIterator->getItem().getBool());
        return value;
    }

    JoinBitmapChunkIterator::JoinBitmapChunkIterator(JoinEmptyableArrayIterator const& arrayIterator, DelegateChunk const* chunk, int iterationMode)
    : JoinChunkIterator(arrayIterator, chunk, iterationMode),
      value(TypeLibrary::getType(TID_BOOL))
    {
    }

    //
    // Array iterator
    //
    bool JoinEmptyableArrayIterator::setPosition(Coordinates const& pos)
    {
        chunkInitialized = false;        
        hasCurrent = inputIterator->setPosition(pos) && (!joinIterator || joinIterator->setPosition(pos));
        return hasCurrent;
    }

    void JoinEmptyableArrayIterator::reset()
    {
        inputIterator->reset();
        if (joinIterator) { 
            joinIterator->reset();
        }
        alignIterators();
    }

    void JoinEmptyableArrayIterator::operator ++()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_POSITION);
        ++(*inputIterator);
        alignIterators();
    }

    bool JoinEmptyableArrayIterator::end()
    {
        return !hasCurrent;
    }

    void JoinEmptyableArrayIterator::alignIterators()
    {
        hasCurrent = false;
        chunkInitialized = false;
        while (!inputIterator->end()) { 
            if (!joinIterator || joinIterator->setPosition(inputIterator->getPosition())) { 
                hasCurrent = true;
                break;
            }
            ++(*inputIterator);
        }
    }

    ConstChunk const& JoinEmptyableArrayIterator::getChunk()
    {
        chunk->overrideClone(!joinIterator);
        return DelegateArrayIterator::getChunk();
    }

    JoinEmptyableArrayIterator::JoinEmptyableArrayIterator(JoinEmptyableArray const& array, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> input, boost::shared_ptr<ConstArrayIterator> join)
    : DelegateArrayIterator(array, attrID, input),
      joinIterator(join)
    {
        alignIterators();
    }



    DelegateChunkIterator* JoinEmptyableArray::createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        JoinEmptyableArrayIterator const& arrayIterator = (JoinEmptyableArrayIterator const&)chunk->getArrayIterator();
        AttributeDesc const& attr = chunk->getAttributeDesc();
        return arrayIterator.joinIterator
            ? attr.isEmptyIndicator()
                ? (DelegateChunkIterator*)new JoinBitmapChunkIterator(arrayIterator, chunk, iterationMode)
                : (DelegateChunkIterator*)new JoinChunkIterator(arrayIterator, chunk, iterationMode)
            : new DelegateChunkIterator(chunk, iterationMode);
    }

    DelegateArrayIterator* JoinEmptyableArray::createArrayIterator(AttributeID attrID) const
    {
        boost::shared_ptr<ConstArrayIterator> inputIterator;
        boost::shared_ptr<ConstArrayIterator> joinIterator;
        AttributeID inputAttrID = attrID;

        if (leftEmptyTagPosition >= 0) { // left array is emptyable
            if (rightEmptyTagPosition >= 0) { // right array is also emptyable: ignore left empty-tag attribute
                if ((int)inputAttrID >= leftEmptyTagPosition) { 
                    inputAttrID += 1;
                }
                if (inputAttrID >= nLeftAttributes) { 
                    inputIterator = right->getConstIterator(inputAttrID - nLeftAttributes);
                    joinIterator = left->getConstIterator(leftEmptyTagPosition);
                } else { 
                    inputIterator = left->getConstIterator(inputAttrID);
                    joinIterator = right->getConstIterator(rightEmptyTagPosition);
                }
            } else { // emptyable array only from left side
                if (inputAttrID >= nLeftAttributes) { 
                    inputIterator = right->getConstIterator(inputAttrID - nLeftAttributes);
                    joinIterator = left->getConstIterator(leftEmptyTagPosition);
                } else { 
                    inputIterator = left->getConstIterator(inputAttrID);
                } 
            }
        } else { // only right array is emptyable
            assert(rightEmptyTagPosition >= 0); 
            if (inputAttrID >= nLeftAttributes) { 
                inputIterator = right->getConstIterator(inputAttrID - nLeftAttributes);
            } else { 
                inputIterator = left->getConstIterator(inputAttrID);
                joinIterator = right->getConstIterator(rightEmptyTagPosition);
            }             
        }
        return new JoinEmptyableArrayIterator(*this, attrID, inputIterator, joinIterator);
    }

    JoinEmptyableArray::JoinEmptyableArray(ArrayDesc const& desc, boost::shared_ptr<Array> leftArr, boost::shared_ptr<Array> rightArr)
    : DelegateArray(desc, leftArr), left(leftArr), right(rightArr)
    {
        ArrayDesc const& leftDesc = left->getArrayDesc();
        ArrayDesc const& rightDesc = right->getArrayDesc();
        nLeftAttributes = leftDesc.getAttributes().size();
        emptyTagPosition = desc.getEmptyBitmapAttribute()->getId();
        leftEmptyTagPosition = leftDesc.getEmptyBitmapAttribute() != NULL ? leftDesc.getEmptyBitmapAttribute()->getId() : -1;
        rightEmptyTagPosition = rightDesc.getEmptyBitmapAttribute() != NULL ? rightDesc.getEmptyBitmapAttribute()->getId() : -1;
    }
}
