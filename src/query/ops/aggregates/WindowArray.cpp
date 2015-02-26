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
 * @file WindowArray.cpp
 *
 * @brief Window array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>, poliocough@gmail.com
 */

#include <log4cxx/logger.h>
#include <math.h>

#include "WindowArray.h"

using namespace std;
using namespace boost;

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.window"));

const size_t MATERIALIZED_WINDOW_THRESHOLD = 1000;

namespace scidb
{

    // Materialized Window Chunk Iterator

    MaterializedWindowChunkIterator::MaterializedWindowChunkIterator(WindowArrayIterator const* arrayIterator, WindowChunk const* chunk, int mode)
   : _array(arrayIterator->array),
     _chunk(chunk),
     _aggregate(_array._aggregates[chunk->attrID]->clone()),
     _defaultValue(chunk->getAttributeDesc().getDefaultValue()),
     _iterationMode(mode),
     _nextValue(TypeLibrary::getType(chunk->getAttributeDesc().getType())),
     _stateMap(&chunk->_stateMap),
     _inputMap(&chunk->_inputMap),
     _iterativeFill(chunk->_iterativeFill),
     _nDims(chunk->_nDims),
     _coords(_nDims)
    {
       if ((_iterationMode & IGNORE_EMPTY_CELLS) == false)
       {
           //the client will ALWAYS use IGNORE_EMPTY_CELLS, right? Let's make sure they do.
           throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CHUNK_WRONG_ITERATION_MODE);
       }

       reset();
    }

    int MaterializedWindowChunkIterator::getMode()
    {
        return _iterationMode;
    }

    void MaterializedWindowChunkIterator::calculateNextValue()
    {
        Coordinates const& currPos = getPosition();
        Coordinates windowStart(_nDims);
        Coordinates windowEnd(_nDims);

        for (size_t i = 0; i < _nDims; i++)
        {
            windowStart[i] = std::max(currPos[i] - _chunk->array._window[i]._boundaries.first, _chunk->array._dimensions[i].getStartMin());
            windowEnd[i] = std::min(currPos[i] + _chunk->array._window[i]._boundaries.second, _chunk->array._dimensions[i].getEndMax());
        }

        uint64_t wsIpos = _chunk->coord2pos(windowStart);
        uint64_t wsEpos = _chunk->coord2pos(windowEnd);

        Value state;
        state.setNull(0);
        Coordinates probePos(_nDims);
        if(_iterativeFill)
        {
            map<uint64_t, Value>::const_iterator winIter = _inputMap->lower_bound(wsIpos);
            map<uint64_t, Value>::const_iterator winEnd = _inputMap->upper_bound(wsEpos);
            while(winIter != winEnd)
            {
                uint64_t pos = winIter->first;
                _chunk->pos2coord(pos,probePos);

                for(size_t i=0; i<_nDims; i++)
                {
                    if (probePos[i]<windowStart[i] || probePos[i]>windowEnd[i])
                    {
                        goto nextIter;
                    }
                }

                if(state.getMissingReason()==0)
                {
                    _aggregate->initializeState(state);
                }
                _aggregate->accumulate(state, winIter->second);
                nextIter:
                winIter++;
            }
            _aggregate->finalResult(_nextValue, state);
        }
        else
        {
            probePos = windowStart;
            probePos[_nDims-1] -= 1;
            map<uint64_t, Value>::const_iterator inputIter;
            while (true)
            {
                for (size_t i = _nDims-1; ++probePos[i] > windowEnd[i]; i--)
                {
                    if (i == 0)
                    {
                        _aggregate->finalResult(_nextValue, state);
                        return;
                    }
                    probePos[i] = windowStart[i];
                }

                uint64_t pos = _chunk->coord2pos(probePos);
                inputIter = _inputMap->find(pos);
                if (inputIter != _inputMap->end())
                {
                    if(state.getMissingReason()==0)
                    {
                        _aggregate->initializeState(state);
                    }
                    _aggregate->accumulate(state, inputIter->second);
                }
            }
        }
    }

    Value& MaterializedWindowChunkIterator::getItem()
    {
        if (end())
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return _nextValue;
    }

    Coordinates const& MaterializedWindowChunkIterator::getPosition()
    {
        if (end())
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        uint64_t pos = _iter->first;
        _chunk->pos2coord(pos,_coords);
        return _coords;
    }

    bool MaterializedWindowChunkIterator::setPosition(Coordinates const& pos)
    {
        uint64_t iPos = _chunk->coord2pos(pos);
        _iter = _stateMap->find(iPos);
        if(end())
        {
            return false;
        }

        calculateNextValue();
        if (_iterationMode & IGNORE_NULL_VALUES && _nextValue.isNull())
        {
            return false;
        }
        if (_iterationMode & IGNORE_DEFAULT_VALUES && _nextValue == _defaultValue)
        {
            return false;
        }

        return true;
    }

    bool MaterializedWindowChunkIterator::isEmpty()
    {
        return false;
    }

    void MaterializedWindowChunkIterator::reset()
    {
        _iter = _stateMap->begin();
        if(end())
        {
            return;
        }

        calculateNextValue();
        if ( (_iterationMode & IGNORE_NULL_VALUES && _nextValue.isNull()) ||
             (_iterationMode & IGNORE_DEFAULT_VALUES && _nextValue == _defaultValue))
        {
            ++(*this);
        }
    }

    void MaterializedWindowChunkIterator::operator ++()
    {
        ++_iter;
        if (end())
        {
            return;
        }

        calculateNextValue();
        while( ((_iterationMode & IGNORE_NULL_VALUES && _nextValue.isNull()) ||
                (_iterationMode & IGNORE_DEFAULT_VALUES && _nextValue == _defaultValue)) && !end())
        {
            ++_iter;
            calculateNextValue();
        }
    }

    bool MaterializedWindowChunkIterator::end()
    {
        return _iter == _stateMap->end();
    }

    ConstChunk const& MaterializedWindowChunkIterator::getChunk()
    {
        return *_chunk;
    }

    // Window Chunk Iterator

    int WindowChunkIterator::getMode()
    {
        return _iterationMode;
    }

    inline Coordinate max(Coordinate c1, Coordinate c2) { return c1 < c2 ? c2 : c1; }
    inline Coordinate min(Coordinate c1, Coordinate c2) { return c1 > c2 ? c2 : c1; }

    Value& WindowChunkIterator::calculateNextValue()
    {
        size_t nDims = _currPos.size();
        Coordinates firstGridPos(nDims);
        Coordinates lastGridPos(nDims);
        Coordinates currGridPos(nDims);

        for (size_t i = 0; i < nDims; i++) {
            currGridPos[i] = firstGridPos[i] = std::max(_currPos[i] - _chunk->array._window[i]._boundaries.first,
                _chunk->array._dimensions[i].getStartMin());
            lastGridPos[i] = std::min(_currPos[i] + _chunk->array._window[i]._boundaries.second,
                _chunk->array._dimensions[i].getEndMax());
        }

        currGridPos[nDims-1] -= 1;
        Value state;
        state.setNull(0);

        while (true)
        {
            for (size_t i = nDims-1; ++currGridPos[i] > lastGridPos[i]; i--)
            {
                if (i == 0)
                {
                    _aggregate->finalResult(_nextValue, state);
                    return _nextValue;
                }
                currGridPos[i] = firstGridPos[i];
            }

            if (_inputIterator->setPosition(currGridPos))
            {
                Value& v = _inputIterator->getItem();

                if (_noNullsCheck)
                {
                    if (v.isNull()) { 
                        continue;
                    }
#if 0 // K&K: IGNORE_NULL_VALUE is not supported any more
                    SYSTEM_CHECK(SCIDB_E_INTERNAL_ERROR,
                                  v.isNull() == false,
                                  "Aggregate has requested no nulls, but iterator emits nulls anyway. Please fix the operator that is input to the aggregate!");
#endif
                }
                if (state.getMissingReason()==0)
                {
                    _aggregate->initializeState(state);
                }
                _aggregate->accumulate(state, v);
            }
        }
    }

    Value& WindowChunkIterator::getItem()
    {
        if (!_hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return _nextValue;
    }

    Coordinates const& WindowChunkIterator::getPosition()
    {
        return _currPos;
    }

    bool WindowChunkIterator::setPosition(Coordinates const& pos)
    {
        for (size_t i = 0, n = _currPos.size(); i < n; i++)
        {
            if (pos[i] < _firstPos[i] || pos[i] > _lastPos[i])
            {
                return false;
            }
        }
        _currPos = pos;

        if (_emptyTagIterator.get() && !_emptyTagIterator->setPosition(_currPos))
        {
            return false;
        }

        calculateNextValue();
        if (_iterationMode & IGNORE_NULL_VALUES && _nextValue.isNull())
        {
            return false;
        }
        if (_iterationMode & IGNORE_DEFAULT_VALUES && _nextValue == _defaultValue)
        {
            return false;
        }

        return true;
    }

    bool WindowChunkIterator::isEmpty()
    {
        return false;
    }

    void WindowChunkIterator::reset()
    {
        if (setPosition(_firstPos))
        {
            _hasCurrent = true;
            return;
        }
        ++(*this);
    }

    void WindowChunkIterator::operator ++()
    {
        bool done = false;
        while (!done)
        {
            size_t nDims = _firstPos.size();
            for (size_t i = nDims-1; ++_currPos[i] > _lastPos[i]; i--)
            {
                if (i == 0)
                {
                    _hasCurrent = false;
                    return;
                }
                _currPos[i] = _firstPos[i];
            }

            if (_emptyTagIterator.get() && !_emptyTagIterator->setPosition(_currPos))
            {
                continue;
            }

            calculateNextValue();
            if (_iterationMode & IGNORE_NULL_VALUES && _nextValue.isNull())
            {
                continue;
            }
            if (_iterationMode & IGNORE_DEFAULT_VALUES && _nextValue == _defaultValue)
            {
                continue;
            }

            done = true;
            _hasCurrent = true;
        }
    }

    bool WindowChunkIterator::end()
    {
        return !_hasCurrent;
    }

    ConstChunk const& WindowChunkIterator::getChunk()
    {
        return *_chunk;
    }

    WindowChunkIterator::WindowChunkIterator(WindowArrayIterator const* arrayIterator, WindowChunk const* chunk, int mode)
    : _array(arrayIterator->array),
      _firstPos(chunk->getFirstPosition(false)),
      _lastPos(chunk->getLastPosition(false)),
      _currPos(_firstPos.size()),
      _attrID(chunk->attrID),
      _chunk(chunk),
      _aggregate(_array._aggregates[_attrID]->clone()),
      _defaultValue(chunk->getAttributeDesc().getDefaultValue()),
      _iterationMode(mode),
      _nextValue(TypeLibrary::getType(chunk->getAttributeDesc().getType()))
    {
        if ((_iterationMode & IGNORE_EMPTY_CELLS) == false)
        {
            //the client will ALWAYS use IGNORE_EMPTY_CELLS, right? Let's make sure they do.
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CHUNK_WRONG_ITERATION_MODE);
        }

        int iterMode = IGNORE_EMPTY_CELLS;
        if (_aggregate->ignoreNulls())
        {
            _noNullsCheck = true;
            iterMode |= IGNORE_NULL_VALUES;
        }
        else
        {
            _noNullsCheck = false;
        }

        if (_aggregate->ignoreZeroes() && _array._inputArray->getArrayDesc().getAttributes()[_array._inputAttrIDs[_attrID]].getDefaultValue().isZero())
        {
            iterMode |= IGNORE_DEFAULT_VALUES;
        }

        _inputIterator = arrayIterator->iterator->getChunk().getConstIterator(iterMode);

        if (_array.getArrayDesc().getEmptyBitmapAttribute())
        {
            AttributeID eAttrId = _array._inputArray->getArrayDesc().getEmptyBitmapAttribute()->getId();
            _emptyTagArrayIterator = _array._inputArray->getConstIterator(eAttrId);

            if (! _emptyTagArrayIterator->setPosition(_firstPos))
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            _emptyTagIterator = _emptyTagArrayIterator->getChunk().getConstIterator(IGNORE_EMPTY_CELLS);
        }

        reset();
    }

    //Window Chunk

    WindowChunk::WindowChunk(WindowArray const& arr, AttributeID attr)
    : array(arr),
      arrayIterator(NULL),
      _nDims(arr._desc.getDimensions().size()),
      oFirstPos(_nDims),
      arrSize(_nDims),
      firstPos(_nDims),
      lastPos(_nDims),
      attrID(attr),
      _materialized(false)
    {
        if (arr._desc.getEmptyBitmapAttribute() == 0 || attr!=arr._desc.getEmptyBitmapAttribute()->getId())
        {
            _aggregate = arr._aggregates[attrID]->clone();
        }
    }

    Array const& WindowChunk::getArray() const 
    { 
        return array;
    }

    const ArrayDesc& WindowChunk::getArrayDesc() const
    {
        return array._desc;
    }

    const AttributeDesc& WindowChunk::getAttributeDesc() const
    {
        return array._desc.getAttributes()[attrID];
    }

    Coordinates const& WindowChunk::getFirstPosition(bool withOverlap) const
    {
        return firstPos;
    }

    Coordinates const& WindowChunk::getLastPosition(bool withOverlap) const
    {
        return lastPos;
    }

    boost::shared_ptr<ConstChunkIterator> WindowChunk::getConstIterator(int iterationMode) const
    {
        ConstChunk const& inputChunk = arrayIterator->iterator->getChunk();
        if (array.getArrayDesc().getEmptyBitmapAttribute() && attrID == array.getArrayDesc().getEmptyBitmapAttribute()->getId())
        {
            return inputChunk.getConstIterator((iterationMode & ~ChunkIterator::INTENDED_TILE_MODE) | ChunkIterator::IGNORE_OVERLAPS);
        }
        if (_materialized) { 
            return boost::shared_ptr<ConstChunkIterator>(new MaterializedWindowChunkIterator(arrayIterator, this, iterationMode));
        }

        return boost::shared_ptr<ConstChunkIterator>(new WindowChunkIterator(arrayIterator, this, iterationMode));
    }

    bool WindowChunk::isSparse() const
    {
        return arrayIterator->iterator->getChunk().isSparse();
    }

    int WindowChunk::getCompressionMethod() const
    {
        return array._desc.getAttributes()[attrID].getDefaultCompressionMethod();
    }


    inline uint64_t WindowChunk::coord2pos(Coordinates const& coord) const
    {
        uint64_t pos = 0;
        for (size_t i = 0; i < _nDims; i++) {
            pos *= arrSize[i];
            pos += coord[i] - oFirstPos[i];
        }
        return pos;
    }

    inline void WindowChunk::pos2coord(uint64_t pos, Coordinates& coord) const
    {
        for (int i = _nDims; --i >= 0;) {
            coord[i] = oFirstPos[i] + (pos % arrSize[i]);
            pos /= arrSize[i];
        }
        assert(pos == 0);
    }

    void WindowChunk::materialize()
    {
        _materialized = true;
        _stateMap.clear();
       _inputMap.clear();

       double nInputElements = 0;
       double nResultElements = 0;
       double maxInputElements = 0;

       Coordinates oLastPos;
       {
           int iterMode = ChunkIterator::IGNORE_EMPTY_CELLS;
           ConstChunk const& chunk = arrayIterator->iterator->getChunk();

           maxInputElements = chunk.getNumberOfElements(true);
           oFirstPos = chunk.getFirstPosition(true);
           oLastPos = chunk.getLastPosition(true);
           for(size_t i =0; i<_nDims; i++)
           {
               arrSize[i] = oLastPos[i]-oFirstPos[i]+1;
           }

           Coordinates const& firstPos = chunk.getFirstPosition(false);
           Coordinates const& lastPos =  chunk.getLastPosition(false);

           shared_ptr<ConstChunkIterator> chunkIter = chunk.getConstIterator(iterMode);
           while(!chunkIter->end())
           {
               Coordinates const& currPos = chunkIter->getPosition();
               Value const& currVal = chunkIter->getItem();

               bool insideOlap=true;
               for (size_t i=0; i<_nDims; i++)
               {
                   if(currPos[i]<firstPos[i] || currPos[i]>lastPos[i])
                   {
                       insideOlap=false;
                       break;
                   }
               }

               uint64_t pos = coord2pos(currPos);

               if (insideOlap)
               {
                   //every cell in input will have an output
                   _stateMap[pos] = true;
                   nResultElements +=1;
               }

               //but if the agg ignores nulls, we can "filter them out" at this stage
               if (!((currVal.isNull() && _aggregate->ignoreNulls()) || (currVal.isZero() && _aggregate->ignoreZeroes())))
               {
                   _inputMap[pos]=currVal;
                   nInputElements +=1;
               }

               ++(*chunkIter);
           }
       }

       if (nInputElements == 0)
       {
           _iterativeFill = true;
           return;
       }

       //Calculate iterative fill cost:
       //  for each element in the result, incrementing the iterator costs log(n) + nDims coordinate comparisons
       //  the constant 2 was arrived at through tuning
       double perElementCost =  2* log(nInputElements) / log(2.0) + 2 * _nDims;
       for (size_t i =0; i<_nDims; i++)
       {
           if (array._window[i]._boundaries.first + array._window[i]._boundaries.second + 1 != 1)
           {
               //we have to iterate over window size in the last dimension
               if(i==_nDims-1)
               {
                   perElementCost *= array._window[i]._boundaries.first + array._window[i]._boundaries.second + 1;
               }
               else //and over the chunk size in all other dimensions
               {
                   perElementCost *= (oLastPos[i] - oFirstPos[i] + 1);
               }
           }
       }
       //add two binary searches per element
       perElementCost += 2*log(nInputElements) / log(2.0);
       //but empty elements are free
       double iterativeFillCost = nResultElements * perElementCost * (nInputElements / maxInputElements);

       //Calculate probe fill cost:
       // for each element in the result we have to probe the entire window
       double windowArea = 1;
       for (size_t i = 0; i<_nDims; i++)
       {
           windowArea *= array._window[i]._boundaries.first + array._window[i]._boundaries.second + 1;
       }
       // where each probe is a binary search over the input tree
       double probeFillCost = nResultElements * windowArea * log(nInputElements) / log(2.0);

       if(iterativeFillCost < probeFillCost)
       {
           _iterativeFill = true;
       }
       else
       {
           _iterativeFill = false;
       }
    }


    void WindowChunk::setPosition(WindowArrayIterator const* iterator, Coordinates const& pos)
    {
        arrayIterator = iterator;
        firstPos = pos;
        Dimensions const& dims = array._desc.getDimensions();
        for (size_t i = 0, n = dims.size(); i < n; i++) {
            lastPos[i] = firstPos[i] + dims[i].getChunkInterval() - 1;
            if (lastPos[i] > dims[i].getEndMax()) {
                lastPos[i] = dims[i].getEndMax();
            }
        }
        _materialized = false;
        if (_aggregate.get() == 0)
        {
            return;
        }
 
        if (array._desc.getEmptyBitmapAttribute()) {
            ConstChunk const& inputChunk = arrayIterator->iterator->getChunk();
            if (/*!inputChunk.isCountKnown() || */inputChunk.count()*MATERIALIZED_WINDOW_THRESHOLD < inputChunk.getNumberOfElements(false))  
            {
                materialize();
            }
        }
    }
    

    void WindowArrayIterator::operator ++()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        chunkInitialized = false;
        ++(*iterator);
        hasCurrent = !iterator->end();
        if (hasCurrent)
        {
            currPos = iterator->getPosition();
        }            
    }

    bool WindowArrayIterator::end()
    {
        return !hasCurrent;
    }

    Coordinates const& WindowArrayIterator::getPosition()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return currPos;
    }

    bool WindowArrayIterator::setPosition(Coordinates const& pos)
    {
        chunkInitialized = false;
        if (!iterator->setPosition(pos))
        {
            return hasCurrent = false;
        }

        currPos = pos;
        return hasCurrent = true;
    }

    void WindowArrayIterator::reset()
    {
        chunkInitialized = false;
        iterator->reset();
        hasCurrent = !iterator->end();
        if (hasCurrent)
        {
            currPos = iterator->getPosition();
        }
    }

    ConstChunk const& WindowArrayIterator::getChunk()
    {
        if (!chunkInitialized) { 
            chunk.setPosition(this, currPos);
            chunkInitialized = true;
        }
        return chunk;
    }


    WindowArrayIterator::WindowArrayIterator(WindowArray const& arr, AttributeID attrID, AttributeID input)
    : array(arr),
      iterator(arr._inputArray->getConstIterator(input)),
      currPos(arr._dimensions.size()),
      chunk(arr, attrID)
    {
        reset();
    }

    
    ArrayDesc const& WindowArray::getArrayDesc() const
    {
        return _desc;
    }

    boost::shared_ptr<ConstArrayIterator> WindowArray::getConstIterator(AttributeID attr) const
    {
        if (_desc.getEmptyBitmapAttribute() && attr == _desc.getEmptyBitmapAttribute()->getId())
        {
            return boost::shared_ptr<ConstArrayIterator>(new WindowArrayIterator(*this, attr, _inputArray->getArrayDesc().getEmptyBitmapAttribute()->getId()));
        }

        return boost::shared_ptr<ConstArrayIterator>(new WindowArrayIterator(*this, attr, _inputAttrIDs[attr]));
    }

    WindowArray::WindowArray(ArrayDesc const& desc, boost::shared_ptr<Array> const& inputArray,
                             vector<WindowBoundaries> const& window, vector<AttributeID> const& inputAttrIDs, vector<AggregatePtr> const& aggregates):
      _desc(desc),
      _inputDesc(inputArray->getArrayDesc()),
      _window(window),
      _dimensions(_desc.getDimensions()),
      _inputArray(inputArray),
      _inputAttrIDs(inputAttrIDs),
      _aggregates(aggregates)
    {}
}
