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
 * @file WindowArray.h
 *
 * @brief The implementation of the array iterator for the window operator
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>, poliocough@gmail.com
 */

#ifndef WINDOW_ARRAY_H_
#define WINDOW_ARRAY_H_

#include <string>
#include <vector>
#include "array/DelegateArray.h"
#include "array/Metadata.h"
#include "query/FunctionDescription.h"
#include "query/Expression.h"
#include "query/Aggregate.h"
#include "array/MemArray.h"

namespace scidb
{

using namespace std;
using namespace boost;

class WindowArray;
class WindowArrayIterator;
class MaterializedWindowChunkIterator;

struct WindowBoundaries
{
    WindowBoundaries()
    {
        _boundaries.first = _boundaries.second = 0;
    }

    WindowBoundaries(Coordinate preceding, Coordinate following)
    {
        assert(preceding >= 0);
        assert(following >= 0);

        _boundaries.first = preceding;
        _boundaries.second = following;
    }

    std::pair<Coordinate, Coordinate> _boundaries;
};

class WindowChunk : public ConstChunk
{
    friend class MaterializedWindowChunkIterator;
    friend class WindowChunkIterator;
  public:
    WindowChunk(WindowArray const& array, AttributeID attrID);

    virtual const ArrayDesc& getArrayDesc() const;
    virtual const AttributeDesc& getAttributeDesc() const;
	virtual Coordinates const& getFirstPosition(bool withOverlap) const;
	virtual Coordinates const& getLastPosition(bool withOverlap) const;
	virtual boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
    virtual int getCompressionMethod() const;
    virtual bool isSparse() const;
    virtual Array const& getArray() const;

    void setPosition(WindowArrayIterator const* iterator, Coordinates const& pos);
    
  private:
    void materialize();
    void pos2coord(uint64_t pos, Coordinates& coord) const;
    uint64_t coord2pos(Coordinates const& coord) const;

    WindowArray const& array;     
    WindowArrayIterator const* arrayIterator;     
    size_t _nDims;
    Coordinates oFirstPos;
    Coordinates arrSize;
    Coordinates firstPos; 
    Coordinates lastPos; 
    AttributeID attrID;
    AggregatePtr _aggregate;
    std::map<uint64_t, bool> _stateMap;
    std::map<uint64_t, Value> _inputMap;
    bool _iterativeFill;
    bool _materialized;
};

class WindowChunkIterator : public ConstChunkIterator
{
public:
    virtual int getMode();
    virtual bool isEmpty();
    virtual  Value& getItem();
    virtual void operator ++();
    virtual bool end();
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos);
    virtual void reset();
    ConstChunk const& getChunk();

    WindowChunkIterator(WindowArrayIterator const* arrayIterator, WindowChunk const* aChunk, int mode);

private:
    Value& calculateNextValue();

    WindowArray const& _array;
    Coordinates const& _firstPos;
    Coordinates const& _lastPos;
    Coordinates _currPos;
    bool _hasCurrent;
    AttributeID _attrID;
    WindowChunk const* _chunk;
    AggregatePtr _aggregate;
    Value _defaultValue;
    int _iterationMode;
    shared_ptr<ConstChunkIterator> _inputIterator;
    shared_ptr<ConstArrayIterator> _emptyTagArrayIterator;
    shared_ptr<ConstChunkIterator> _emptyTagIterator;
    Value _nextValue;
    bool _noNullsCheck;
};

class MaterializedWindowChunkIterator : public ConstChunkIterator
{    
public:
    MaterializedWindowChunkIterator(WindowArrayIterator const* arrayIterator, WindowChunk const* aChunk, int mode);

    virtual int getMode();
    virtual bool isEmpty();
    virtual Value& getItem();
    virtual void operator ++();
    virtual bool end();
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos);
    virtual void reset();
    ConstChunk const& getChunk();
    
private:
    void calculateNextValue();

    WindowArray const& _array;
    WindowChunk const* _chunk;
    AggregatePtr _aggregate;
    Value _defaultValue;
    int _iterationMode;
    Value _nextValue;
    std::map<uint64_t, bool>const* _stateMap;
    std::map<uint64_t, bool>::const_iterator _iter;
    std::map<uint64_t, Value>const* _inputMap;
    bool _iterativeFill;
    size_t _nDims;
    Coordinates _coords;
};
   
class WindowArrayIterator : public ConstArrayIterator
{
    friend class WindowChunk;
    friend class MaterializedWindowChunkIterator;
    friend class WindowChunkIterator;
  public:
    virtual ConstChunk const& getChunk();
    virtual bool end();
    virtual void operator ++(); 
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos);
    virtual void reset();
    
    WindowArrayIterator(WindowArray const& array, AttributeID id, AttributeID input);

  private:
    WindowArray const& array;
    boost::shared_ptr<ConstArrayIterator> iterator;
    Coordinates currPos;
    bool hasCurrent;
    WindowChunk chunk;
    bool chunkInitialized;
};

class WindowArray : public Array
{
    friend class WindowArrayIterator;
    friend class MaterializedWindowChunkIterator;
    friend class WindowChunkIterator;
    friend class WindowChunk;

  public:
	virtual ArrayDesc const& getArrayDesc() const;
	virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attr) const;

    WindowArray(ArrayDesc const& desc,
                boost::shared_ptr<Array> const& inputArray,
                vector<WindowBoundaries> const& window,
                vector<AttributeID> const& inputAttrIDs,
                vector <AggregatePtr> const& aggregates);

  private:
    ArrayDesc _desc;
    ArrayDesc _inputDesc;
    vector<WindowBoundaries> _window;
    Dimensions _dimensions;
    boost::shared_ptr<Array> _inputArray;
    vector<AttributeID> _inputAttrIDs;
    vector <AggregatePtr> _aggregates;
};

}

#endif
