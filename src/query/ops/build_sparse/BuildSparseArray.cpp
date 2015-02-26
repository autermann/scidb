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
 * BuildSparseArray.cpp
 *
 *  Created on: Apr 11, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/MemArray.h"
#include "network/NetworkManager.h"
#include "BuildSparseArray.h"


using namespace std;
using namespace boost;

namespace scidb {

    //
    // Build chunk iterator methods
    //
    int BuildSparseChunkIterator::getMode()
    {
        return _iterationMode;
    }

    Value& BuildSparseChunkIterator::getItem()
    {
        if (!_hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        if (_emptyIndicator) {
            _bool.setBool(_selected);
            return _bool;
        }
        if (!_selected) {
            return _defaultValue;
        }
        const size_t nBindings =  _array._expressionBindings.size();

        for (size_t i = 0; i < nBindings; i++) {
            switch (_array._expressionBindings[i].kind) {
              case BindInfo::BI_COORDINATE:
                  _params[i] = _array._desc.getOriginalCoordinate(_array._expressionBindings[i].resolvedId,
                                                             _currPos[_array._expressionBindings[i].resolvedId],
                                                             _array._query.lock());
                  break;
              case BindInfo::BI_VALUE:
                  _params[i] = _array._expressionBindings[i].value;
                  break;
              default:
                  assert(false);
            }
        }
        if (_converter) {
            const Value* v = &_expression.evaluate(_params);
            _converter(&v, &_value, NULL);
        }
        else {
            _value = _expression.evaluate(_params);
        }
        if (!_nullable && _value.isNull())
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ASSIGNING_NULL_TO_NON_NULLABLE);
        return _value;
    }

    void BuildSparseChunkIterator::operator ++()
    {
        if (!_hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
      Forward:
        for (int i = _currPos.size(); --i >= 0;) {
            if (++_currPos[i] > _lastPos[i]) {
                _currPos[i] = _firstPos[i];
            } else {
                if (isSelected()) {
                    _hasCurrent = true;
                    return;
                } else {
                    goto Forward;
                }
            }
        }
        _hasCurrent = false;
    }

    bool BuildSparseChunkIterator::end()
    {
        return !_hasCurrent;
    }

    bool BuildSparseChunkIterator::isEmpty()
    {
        return _array.emptyable ? !_selected : false;
    }

    bool BuildSparseChunkIterator::isSelected()
    {
        const size_t nBindings =  _array._predicateBindings.size();
        ExpressionContext params(*_array._predicate);

        for (size_t i = 0; i < nBindings; i++) {
            switch (_array._predicateBindings[i].kind) {
              case BindInfo::BI_COORDINATE:
              params[i] = _array._desc.getOriginalCoordinate(_array._predicateBindings[i].resolvedId,
                                                             _currPos[_array._predicateBindings[i].resolvedId],
                                                             _array._query.lock());
                  break;
              case BindInfo::BI_VALUE:
                  params[i] = _array._predicateBindings[i].value;
                  break;
              default:
                  assert(false);
            }
        }
        _selected = _array._predicate->evaluate(params).getBool();
        return !_array.emptyable || !(_iterationMode & IGNORE_EMPTY_CELLS) || _selected;
    }

    Coordinates const& BuildSparseChunkIterator::getPosition()
    {
        if (!_hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return _currPos;
    }

    bool BuildSparseChunkIterator::setPosition(Coordinates const& pos)
    {
        for (size_t i = 0, n = _currPos.size(); i < n; i++) {
            if (pos[i] < _firstPos[i] || pos[i] > _lastPos[i]) {
                return _hasCurrent = false;
            }
        }
        _currPos = pos;
        return _hasCurrent = isSelected();
    }

    void BuildSparseChunkIterator::reset()
    {
        _currPos = _firstPos;
        _hasCurrent = true;
        if (!isSelected()) {
            ++(*this);
        }
    }

    ConstChunk const& BuildSparseChunkIterator::getChunk()
    {
        return *_chunk;
    }

    BuildSparseChunkIterator::BuildSparseChunkIterator(BuildSparseArray& outputArray, ConstChunk const* chunk, AttributeID attr, int mode)
    : _iterationMode(mode),
      _array(outputArray),
      _firstPos(chunk->getFirstPosition((mode & IGNORE_OVERLAPS) == 0)),
      _lastPos(chunk->getLastPosition((mode & IGNORE_OVERLAPS) == 0)),
      _currPos(_firstPos.size()),
      _attrID(attr),
      _chunk(chunk),
      _converter(outputArray._converter),
      _defaultValue(chunk->getAttributeDesc().getDefaultValue()),
      _expression(*_array._expression),
      _params(_expression),
      _nullable(chunk->getAttributeDesc().isNullable())
    {
        _value = _defaultValue;
        _emptyIndicator = chunk->getAttributeDesc().isEmptyIndicator();
        reset();
    }

    //
    // Build chunk methods
    //
    Array const& BuildSparseChunk::getArray() const 
    { 
        return array;
    }

    bool BuildSparseChunk::isSparse() const
    {
        return true;
    }

    const ArrayDesc& BuildSparseChunk::getArrayDesc() const
    {
        return array._desc;
    }

    const AttributeDesc& BuildSparseChunk::getAttributeDesc() const
    {
        return array._desc.getAttributes()[attrID];
    }

        Coordinates const& BuildSparseChunk::getFirstPosition(bool withOverlap) const
    {
        return withOverlap ? firstPosWithOverlap : firstPos;
    }

        Coordinates const& BuildSparseChunk::getLastPosition(bool withOverlap) const
    {
        return withOverlap ? lastPosWithOverlap : lastPos;
    }

        boost::shared_ptr<ConstChunkIterator> BuildSparseChunk::getConstIterator(int iterationMode) const
    {
        return boost::shared_ptr<ConstChunkIterator>(new BuildSparseChunkIterator(array, this, attrID, iterationMode));
    }

    int BuildSparseChunk::getCompressionMethod() const
    {
        return array._desc.getAttributes()[attrID].getDefaultCompressionMethod();
    }

    void BuildSparseChunk::setPosition(Coordinates const& pos)
    {
        firstPos = pos;
        Dimensions const& dims = array._desc.getDimensions();
        for (size_t i = 0, n = dims.size(); i < n; i++) {
            firstPosWithOverlap[i] = firstPos[i] - dims[i].getChunkOverlap();
            if (firstPosWithOverlap[i] < dims[i].getStart()) {
                firstPosWithOverlap[i] = dims[i].getStart();
            }
            lastPos[i] = firstPos[i] + dims[i].getChunkInterval() - 1;
            lastPosWithOverlap[i] = lastPos[i] + dims[i].getChunkOverlap();
            if (lastPos[i] > dims[i].getEndMax()) {
                lastPos[i] = dims[i].getEndMax();
            }
            if (lastPosWithOverlap[i] > dims[i].getEndMax()) {
                lastPosWithOverlap[i] = dims[i].getEndMax();
            }
        }
    }

    BuildSparseChunk::BuildSparseChunk(BuildSparseArray& arr, AttributeID attr):
            array(arr),
            firstPos(arr._desc.getDimensions().size()),
            lastPos(firstPos.size()),
            firstPosWithOverlap(firstPos.size()),
            lastPosWithOverlap(firstPos.size()),
            attrID(attr)
    {
    }


    //
    // Build array iterator methods
    //

    void BuildSparseArrayIterator::operator ++()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        nextChunk();
    }

    bool BuildSparseArrayIterator::end()
    {
        return !hasCurrent;
    }

    Coordinates const& BuildSparseArrayIterator::getPosition()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return currPos;
    }

    void BuildSparseArrayIterator::nextChunk()
    {
        hasCurrent = true;
        while (true) { 
            int i = dims.size() - 1;
            while ((currPos[i] += dims[i].getChunkInterval()) > dims[i].getEndMax()) { 
                if (i == 0) { 
                    hasCurrent = false;
                    return;
                }
                currPos[i] = dims[i].getStart();
                i -= 1;
            }
            chunkInitialized = false;
            if (array._desc.getChunkNumber(currPos) % array.nInstances == array.instanceID
                && !getChunk().getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS|ChunkIterator::IGNORE_OVERLAPS)->end())
            {
                return;
            }
        }
    }

    bool BuildSparseArrayIterator::setPosition(Coordinates const& pos)
    {
        for (size_t i = 0, n = currPos.size(); i < n; i++) {
            if (pos[i] < dims[i].getStart() || pos[i] > dims[i].getEndMax()) {
                return hasCurrent = false;
            }
        }
        currPos = pos;
        array._desc.getChunkPositionFor(currPos);
        chunkInitialized = false;
        hasCurrent = array._desc.getChunkNumber(currPos) % array.nInstances == array.instanceID;
        if (hasCurrent && getChunk().getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS|ChunkIterator::IGNORE_OVERLAPS)->end()) {
            hasCurrent = false;
        }
        return hasCurrent;
                                                                                                                                    
    }

    void BuildSparseArrayIterator::reset()
    {
        size_t nDims = currPos.size(); 
        for (size_t i = 0; i < nDims; i++) {
            currPos[i] = dims[i].getStart();
        }
        currPos[nDims-1] -= dims[nDims-1].getChunkInterval();
        nextChunk();
    }

    ConstChunk const& BuildSparseArrayIterator::getChunk()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        if (!chunkInitialized) { 
            chunk.setPosition(currPos);
            chunkInitialized = true;
        }
        return chunk;
    }


    BuildSparseArrayIterator::BuildSparseArrayIterator(BuildSparseArray& arr, AttributeID attrID)
    : array(arr),
      chunk(arr, attrID),
      dims(arr._desc.getDimensions()),
      currPos(dims.size())
    {
        reset();
    }


    //
    // Build array methods
    //

    ArrayDesc const& BuildSparseArray::getArrayDesc() const
    {
        return _desc;
    }

    boost::shared_ptr<ConstArrayIterator> BuildSparseArray::getConstIterator(AttributeID attr) const
    {
        return boost::shared_ptr<ConstArrayIterator>(new BuildSparseArrayIterator(*(BuildSparseArray*)this, attr));
    }

BuildSparseArray::BuildSparseArray(boost::shared_ptr<Query>& query,
                                   ArrayDesc const& desc, boost::shared_ptr<Expression> expression, boost::shared_ptr<Expression> predicate)
    : _desc(desc),
      _expression(expression),
      _expressionBindings(expression->getBindings()),
      _predicate(predicate),
      _predicateBindings(predicate->getBindings()),
      _converter(NULL),
      nInstances( 0),
      instanceID(INVALID_INSTANCE),
      emptyable(desc.getEmptyBitmapAttribute() != NULL),
      _query(query)
    {
       assert(query);
       nInstances = query->getInstancesCount();
       instanceID = query->getInstanceID();
       assert(nInstances>0);
       assert(instanceID < nInstances);
         TypeId attrType = _desc.getAttributes()[0].getType();
         TypeId exprType = expression->getType();
        if (attrType != exprType) {
            _converter = FunctionLibrary::getInstance()->findConverter(exprType, attrType);
        }
        for (size_t i = 0; i < _expressionBindings.size(); i++) {
            if (_expressionBindings[i].kind == BindInfo::BI_ATTRIBUTE)
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_BUILD_SPARSE_ERROR1);
        }
        for (size_t i = 0; i < _predicateBindings.size(); i++) {
            if (_predicateBindings[i].kind == BindInfo::BI_ATTRIBUTE)
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_BUILD_SPARSE_ERROR2);
        }
    }
}
