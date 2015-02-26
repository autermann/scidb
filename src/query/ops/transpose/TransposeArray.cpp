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
 * @file UnorderedTransposeArray.cpp
 *
 * @brief UnorderedTranspose array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include "TransposeArray.h"

namespace scidb {

    using namespace boost;
    using namespace std;

    inline void invert(Coordinates const& src, Coordinates& dst) 
    { 
        for (size_t i = 0, n = src.size(); i < n; i++) { 
            dst[n - i - 1] = src[i];
        }
    }
            

    //
    // UnorderedTranspose chunk iterator methods
    //
    Coordinates const& UnorderedTransposeChunkIterator::getPosition()
    {
        Coordinates const& inPos = DelegateChunkIterator::getPosition();
        invert(inPos, outPos);
        return outPos;
    }

    bool UnorderedTransposeChunkIterator::setPosition(Coordinates const& outPos)
    {
        invert(outPos, inPos);
        return DelegateChunkIterator::setPosition(inPos);
    }

    UnorderedTransposeChunkIterator::UnorderedTransposeChunkIterator(DelegateChunk const* chunk, int iterationMode)
    : DelegateChunkIterator(chunk, iterationMode),
      inPos(chunk->getArrayDesc().getDimensions().size()),
      outPos(inPos.size())
    {
    }

    //
    // OrderedTranspose chunk methods
    //
    Coordinates const& UnorderedTransposeChunk::getFirstPosition(bool withOverlap) const
    {
        Coordinates const& inPos = DelegateChunk::getFirstPosition(withOverlap);
        invert(inPos, ((UnorderedTransposeChunk*)this)->outPos);
        return outPos;
    }

    Coordinates const& UnorderedTransposeChunk::getLastPosition(bool withOverlap) const
    {
        Coordinates const& inPos = DelegateChunk::getLastPosition(withOverlap);
        invert(inPos, ((UnorderedTransposeChunk*)this)->outPos);
        return outPos;
    }

     UnorderedTransposeChunk::UnorderedTransposeChunk(UnorderedTransposeArray const& arr, DelegateArrayIterator const& iterator, AttributeID attrID, bool isClone)
    : DelegateChunk(arr, iterator, attrID, isClone),
      outPos(arr.getArrayDesc().getDimensions().size())
    {
    }
      
    //
    // UnorderedTranspose array iterator
    //
    Coordinates const& UnorderedTransposeArrayIterator::getPosition()
    {
        Coordinates const& inPos = DelegateArrayIterator::getPosition();
        invert(inPos, outPos);
        return outPos;
    }

    bool UnorderedTransposeArrayIterator::setPosition(Coordinates const& outPos)
    {
        invert(outPos, inPos);
        return DelegateArrayIterator::setPosition(inPos);
    }

    UnorderedTransposeArrayIterator::UnorderedTransposeArrayIterator(UnorderedTransposeArray const& arr, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> inputIterator)
    : DelegateArrayIterator(arr, attrID, inputIterator),
      inPos(arr.getArrayDesc().getDimensions().size()),
      outPos(inPos.size())
    {
    }

    //
    // UnorderedTranspose array methods
    //

    DelegateChunk* UnorderedTransposeArray::createChunk(DelegateArrayIterator const* iterator, AttributeID id) const
    {
       return new UnorderedTransposeChunk(*this, *iterator, id, false);
    }

    DelegateChunkIterator* UnorderedTransposeArray::createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        return new UnorderedTransposeChunkIterator(chunk, iterationMode);
    }

    DelegateArrayIterator* UnorderedTransposeArray::createArrayIterator(AttributeID id) const
    {
        return new UnorderedTransposeArrayIterator(*this, id, inputArray->getConstIterator(id));
    }    

    UnorderedTransposeArray::UnorderedTransposeArray(ArrayDesc const& desc, boost::shared_ptr<Array> const& array)
    : DelegateArray(desc, array)
    {
    } 

    //
    // OrderedTransposeChunkIterator methods
    //
    void OrderedTransposeChunkIterator::operator ++()
    {
        moveNext();
    }

    void OrderedTransposeChunkIterator::moveNext()
    {
        do { 
            size_t i;
            size_t nDims = pos.size();;
            for (i = 0; i < nDims; i++) { 
                if (++pos[i] > inputIterator->getLastPosition()[i]) { 
                    pos[i] = inputIterator->getFirstPosition()[i];
                } else { 
                    break;
                }
            }
            if (i == nDims) { 
                hasCurrent = false;
                return;
            }
        } while (!inputIterator->setPosition(pos));

        hasCurrent = true;
    }

    bool OrderedTransposeChunkIterator::end()
    {
        return !hasCurrent;
    }

    void OrderedTransposeChunkIterator::reset()
    {
        pos = inputIterator->getFirstPosition();
        pos[0] -= 1;
        moveNext();            
    }

    OrderedTransposeChunkIterator::OrderedTransposeChunkIterator(DelegateChunk const* chunk, int iterationMode)
    : UnorderedTransposeChunkIterator(chunk, iterationMode) 
    {
        reset();
    }

    //
    // OrderedTransposeArrayIterator methods
    //
    Coordinates const& OrderedTransposeArrayIterator::getPosition()
    {
        if (!hasCurrent) {             
            buildSetOfChunks();
            if (!hasCurrent) { 
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
            }
        }
        return outPos;
    }

    bool OrderedTransposeArrayIterator::setPosition(Coordinates const& pos)
    {
        hasCurrent = UnorderedTransposeArrayIterator::setPosition(pos);
        if (hasCurrent) { 
            outPos = pos;
            array.getArrayDesc().getChunkPositionFor(outPos);
            if (setConstructed) {
                currPos = chunks.find(outPos);
                if (currPos == chunks.end()) { 
                    return false;
                }
            }
            return true;
        }
        return false;
    }


    inline void OrderedTransposeArrayIterator::setFirst()
    {
        currPos = chunks.begin();
        if (currPos != chunks.end()) { 
            hasCurrent = true;
            outPos = *currPos;
            invert(outPos, inPos);
            if (!inputIterator->setPosition(inPos)) { 
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
            }
        } else { 
            hasCurrent = false;
        }
    }

    bool OrderedTransposeArrayIterator::buildSetOfChunks()
    {
        if (!setConstructed) {
            inputIterator->reset();
            while (!inputIterator->end()) {
                invert(inputIterator->getPosition(), outPos);
                chunks.insert(outPos);                    
                ++(*inputIterator);
            }
            setConstructed = true;
            if (hasCurrent) { 
                currPos = chunks.find(outPos);
                if (currPos == chunks.end()) { 
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
                }
            } else { 
                setFirst();
            }                
            return true;
        }
        return false;
    }

    void OrderedTransposeArrayIterator::operator ++()
    {
        buildSetOfChunks();
        if (!hasCurrent) { 
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        }
        ++currPos;
        if (currPos != chunks.end()) { 
            outPos = *currPos;
            invert(outPos, inPos);
            if (!inputIterator->setPosition(inPos)) { 
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
            }
        } else { 
            hasCurrent = false;
        }
    }

    bool OrderedTransposeArrayIterator::end()
    {
        if (!hasCurrent) { 
            buildSetOfChunks();
        }
        return !hasCurrent;
    }

    void OrderedTransposeArrayIterator::reset()
    {
        hasCurrent = false;
        if (!buildSetOfChunks()) { 
            setFirst();
        }
    }

	OrderedTransposeArrayIterator::OrderedTransposeArrayIterator(OrderedTransposeArray const& arr, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> inputIterator)
    :  UnorderedTransposeArrayIterator(arr, attrID, inputIterator),
       hasCurrent(false), setConstructed(false)
    {
    }         
    
    //
    // OrderedTransposeArray methods
    //
    DelegateChunkIterator* OrderedTransposeArray::createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        return new OrderedTransposeChunkIterator(chunk, iterationMode);
    }

    DelegateArrayIterator* OrderedTransposeArray::createArrayIterator(AttributeID id) const
    {
        return new OrderedTransposeArrayIterator(*this, id, inputArray->getConstIterator(id));
    }

    OrderedTransposeArray::OrderedTransposeArray(ArrayDesc const& desc, boost::shared_ptr<Array> const& array)
    : UnorderedTransposeArray(desc, array)
    {
    }
}

