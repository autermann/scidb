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
 * @file BuildSparseArray.h
 *
 * @brief The implementation of the array iterator for the build operator
 *
 */

#ifndef BUILD_SPARSE_ARRAY_H_
#define BUILD_SPARSE_ARRAY_H_

#include <string>
#include <vector>
#include "array/DelegateArray.h"
#include "array/Metadata.h"
#include "query/FunctionDescription.h"
#include "query/Expression.h"

namespace scidb
{

using namespace std;
using namespace boost;

class BuildSparseArray;
class BuildSparseArrayIterator;
class BuildSparseChunkIterator;

class BuildSparseChunk : public ConstChunk
{
  public:
    virtual const ArrayDesc& getArrayDesc() const;
    virtual const AttributeDesc& getAttributeDesc() const;
    virtual Coordinates const& getFirstPosition(bool withOverlap) const;
    virtual Coordinates const& getLastPosition(bool withOverlap) const;
    virtual boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
    virtual int getCompressionMethod() const;
    virtual bool isSparse() const;
    Array const& getArray() const;

    void setPosition(Coordinates const& pos);

    BuildSparseChunk(BuildSparseArray& array, AttributeID attrID);

  private:
    BuildSparseArray& array;
    Coordinates firstPos;
    Coordinates lastPos;
    Coordinates firstPosWithOverlap;
    Coordinates lastPosWithOverlap;
    AttributeID attrID;
};

class BuildSparseChunkIterator : public ConstChunkIterator
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

    BuildSparseChunkIterator(BuildSparseArray& array, ConstChunk const* chunk, AttributeID attrID, int iterationMode);

  private:
    bool isSelected();

    int _iterationMode;
    BuildSparseArray& _array;
    Coordinates const& _firstPos;
    Coordinates const& _lastPos;
    Coordinates _currPos;
    bool _hasCurrent;
    bool _selected;
    bool _emptyIndicator;
    AttributeID _attrID;
    ConstChunk const* _chunk;
    FunctionPointer _converter;
    Value _bool;
    Value _value;
    Value _defaultValue;
    Expression _expression;
    ExpressionContext _params;
    bool _nullable;
};

class BuildSparseArrayIterator : public ConstArrayIterator
{
    friend class BuildSparseChunkIterator;
  public:
    virtual ConstChunk const& getChunk();
    virtual bool end();
    virtual void operator ++();
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos);
    virtual void reset();

    BuildSparseArrayIterator(BuildSparseArray& array, AttributeID id);

  private:
    void nextChunk();

    BuildSparseArray& array;
    bool hasCurrent;
    bool chunkInitialized;
    BuildSparseChunk chunk;
    Dimensions const& dims;
    Coordinates currPos;
};

class BuildSparseArray : public Array
{
    friend class BuildSparseArrayIterator;
    friend class BuildSparseChunkIterator;
    friend class BuildSparseChunk;

  public:
    virtual ArrayDesc const& getArrayDesc() const;
    virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attr) const;
    
    BuildSparseArray(boost::shared_ptr<Query>& query, ArrayDesc const& desc, boost::shared_ptr<Expression> expression, boost::shared_ptr<Expression> predicate);

  private:
    ArrayDesc _desc;
    boost::shared_ptr<Expression> _expression;
    vector<BindInfo> _expressionBindings;
    boost::shared_ptr<Expression> _predicate;
    vector<BindInfo> _predicateBindings;
    FunctionPointer _converter;
    size_t nInstances;
    size_t instanceID;
    bool emptyable;
    boost::weak_ptr<Query> _query;
};

}

#endif
