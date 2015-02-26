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
 * @file MapJoinArray.cpp
 *
 * @brief MapJoin array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include "MapJoinArray.h"
#include "system/Exceptions.h"

namespace scidb
{
    using namespace boost;

    //
    // MapJoin chunk iterator methods
    //
    Value& MapJoinChunkIterator::getItem()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        if (isEmptyIndicator) {
            boolValue.setBool(!isEmpty());
            return boolValue;
        }
        return isLeftAttribute ? inputIterator->getItem() : rightIterator->getItem();
    }

    bool MapJoinChunkIterator::isEmpty()
    {
        return inputIterator->isEmpty() || rightIterator->isEmpty() || notMatched;
    }

    bool MapJoinChunkIterator::end()
    {
        return !hasCurrent;
    }

    bool MapJoinChunkIterator::setRightPosition()
    {
        Coordinates pos = inputIterator->getPosition();
        if (array.mapPosition(pos)) {
            rightIterator.reset();
            if (rightArrayIterator->setPosition(pos)) {
                rightIterator = rightArrayIterator->getChunk().getConstIterator(mode);
                if (rightIterator->setPosition(pos)) {
                    notMatched = false;
                    return true;
                }
            }
        }
        notMatched = true;
        return false;//(mode & IGNORE_EMPTY_CELLS) == 0;
    }

    void MapJoinChunkIterator::operator ++()
    {
        ++(*inputIterator);
        while (!inputIterator->end()) {
            if (setRightPosition()) {
                hasCurrent = true;
                return;
            }
            ++(*inputIterator);
        }
        hasCurrent = false;
    }

    bool MapJoinChunkIterator::setPosition(Coordinates const& pos)
    {
        return hasCurrent = inputIterator->setPosition(pos) && setRightPosition();
    }

    void MapJoinChunkIterator::reset()
    {
        inputIterator->reset();
        while (!inputIterator->end())  {
            if (setRightPosition()) {
                hasCurrent = true;
                return;
            }
            ++(*inputIterator);
        }
        hasCurrent = false;
        }

    MapJoinChunkIterator::MapJoinChunkIterator(MapJoinArray const& arr, DelegateChunk const* chunk, int iterationMode)
    : DelegateChunkIterator(chunk, iterationMode),
      array(arr),
      attr(chunk->getAttributeDesc().getId()),
      mode(iterationMode & ~INTENDED_TILE_MODE)
    {
        MapJoinArrayIterator const& arrayIterator = (MapJoinArrayIterator const&)chunk->getArrayIterator();
        rightArrayIterator = arrayIterator.rightIterator;
        isEmptyIndicator = !arrayIterator.inputIterator || arrayIterator.inputIterator->end() || arrayIterator.inputIterator->getChunk().getAttributeDesc().isEmptyIndicator();
        isLeftAttribute = arrayIterator.inputIterator == arrayIterator.leftIterator;
        reset();
    }

    //
    // MapJoin array iterator methods
    //
    MapJoinArrayIterator::MapJoinArrayIterator(DelegateArray const& delegate, AttributeID attrID,
                                               shared_ptr<ConstArrayIterator> left,
                                               shared_ptr<ConstArrayIterator> right,
                                               shared_ptr<ConstArrayIterator> input)
    : DelegateArrayIterator(delegate, attrID, left),
      leftIterator(left),
      rightIterator(right),
      inputIterator(input)
    {
        reset();
    }

        void MapJoinArrayIterator::operator ++()
    {
        ++(*leftIterator);
        while (!leftIterator->end()) {
            if (!getChunk().getConstIterator()->end()) {
                break;
            }
            ++(*leftIterator);
        }
    }

        void MapJoinArrayIterator::reset()
    {
        leftIterator->reset();
        while (!leftIterator->end()) {
            if (!getChunk().getConstIterator()->end()) {
                break;
            }
            ++(*leftIterator);
        }
    }

    //
    // MapJoin array methods
    //
MapJoinArray::MapJoinArray(ArrayDesc& d, shared_ptr<Array> leftArray, shared_ptr<Array> rightArray,
                           const boost::shared_ptr<Query>& query)
    : DelegateArray(d, leftArray),
      leftDesc(leftArray->getArrayDesc()),
      rightDesc(rightArray->getArrayDesc()),
      left(leftArray),
      right(rightArray),
      nLeftAttrs(leftDesc.getAttributes().size()),
      nRightAttrs(rightDesc.getAttributes().size()),
      _query(query)
    {
        leftEmptyTagPosition = leftDesc.getEmptyBitmapAttribute() != NULL ? leftDesc.getEmptyBitmapAttribute()->getId() : -1;
        rightEmptyTagPosition = rightDesc.getEmptyBitmapAttribute() != NULL ? rightDesc.getEmptyBitmapAttribute()->getId() : -1;
    }

    DelegateChunkIterator* MapJoinArray::createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        return new MapJoinChunkIterator(*this, chunk, iterationMode);
    }


    DelegateArrayIterator*  MapJoinArray::createArrayIterator(AttributeID attrID) const
    {
        shared_ptr<ConstArrayIterator> leftIterator;
        shared_ptr<ConstArrayIterator> rightIterator;
        shared_ptr<ConstArrayIterator> inputIterator;
        AttributeID inputAttrID = attrID;

        if (leftEmptyTagPosition >= 0) { // left array is emptyable
            if (rightEmptyTagPosition >= 0) { // right array is also emptyable: ignore left empty-tag attribute
                if ((int)inputAttrID >= leftEmptyTagPosition) {
                    inputAttrID += 1;
                }
                if (inputAttrID >= nLeftAttrs) {
                    leftIterator = left->getConstIterator(leftEmptyTagPosition);
                    inputIterator = rightIterator = right->getConstIterator(inputAttrID - nLeftAttrs);
                } else {
                    inputIterator = leftIterator = left->getConstIterator(inputAttrID);
                    rightIterator = right->getConstIterator(rightEmptyTagPosition);
                }
            } else { // emptyable array only from left side
                if (inputAttrID >= nLeftAttrs) {
                    leftIterator = left->getConstIterator(leftEmptyTagPosition);
                    inputIterator = rightIterator = right->getConstIterator(inputAttrID - nLeftAttrs);
                } else {
                    inputIterator = leftIterator = left->getConstIterator(inputAttrID);
                    rightIterator = right->getConstIterator(0);
                }
            }
        } else if (rightEmptyTagPosition >= 0) { // only right array is emptyable
            if (inputAttrID >= nLeftAttrs) {
                leftIterator = left->getConstIterator(0);
                inputIterator = rightIterator = right->getConstIterator(inputAttrID - nLeftAttrs);
            } else {
                inputIterator = leftIterator = left->getConstIterator(inputAttrID);
                rightIterator = right->getConstIterator(rightEmptyTagPosition);
            }
        } else { // both input arrays are non-emptyable
            if (inputAttrID >= nLeftAttrs) {
                leftIterator = left->getConstIterator(0);
                if (inputAttrID == nLeftAttrs + nRightAttrs) {
                    rightIterator = right->getConstIterator(0);
                } else {
                    inputIterator = rightIterator = right->getConstIterator(inputAttrID - nLeftAttrs);
                }
            } else {
                inputIterator = leftIterator = left->getConstIterator(inputAttrID);
                rightIterator = right->getConstIterator(0);
            }
        }
        return new MapJoinArrayIterator(*this, attrID, leftIterator, rightIterator, inputIterator);
    }

    bool MapJoinArray::mapPosition(Coordinates& pos) const
    {
       boost::shared_ptr<Query> query(_query.lock());
        for (size_t i = 0, n = pos.size(); i < n; i++) {
           Coordinate c = rightDesc.getOrdinalCoordinate(i,
                                                         leftDesc.getOriginalCoordinate(i, pos[i], query),
                                                         cmTest, query);
            if (c < MIN_COORDINATE) {
                return false;
            }
            pos[i] = c;
        }
        return true;
    }
}
