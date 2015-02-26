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
 * @file TransposeArray.h
 *
 * @brief Transpose array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#ifndef TRANSPOSE_ARRAY_H
#define TRANSPOSE_ARRAY_H

#include "array/DelegateArray.h"
#include <map>

namespace scidb {

using namespace boost;
using namespace std;

class UnorderedTransposeArray;
class UnorderedTransposeArrayIterator;
class UnorderedTransposeChunk;
class UnorderedTransposeChunkIterator;

//
// Unordered transpose array implementation
//

class UnorderedTransposeChunkIterator : public DelegateChunkIterator
{  
    Coordinates inPos;
    Coordinates outPos;

  public:
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos); 

    UnorderedTransposeChunkIterator(DelegateChunk const* chunk, int iterationMode);
};

class UnorderedTransposeChunk : public DelegateChunk
{
    Coordinates outPos;

  public:
	virtual Coordinates const& getFirstPosition(bool withOverlap) const;
	virtual Coordinates const& getLastPosition(bool withOverlap) const;

    UnorderedTransposeChunk(UnorderedTransposeArray const& array, DelegateArrayIterator const& iterator, AttributeID attrID, bool isClone);
};

class UnorderedTransposeArrayIterator : public DelegateArrayIterator
{
  protected:
    Coordinates inPos;
    Coordinates outPos;

  public:
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos);

	UnorderedTransposeArrayIterator(UnorderedTransposeArray const& array, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> inputIterator);
};

class UnorderedTransposeArray : public DelegateArray
{
    friend class UnorderedTransposeChunk;
    friend class UnorderedTransposeChunkIterator;
    friend class UnorderedTransposeArrayIterator;

  public:
    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const;
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;

    UnorderedTransposeArray(ArrayDesc const& desc, boost::shared_ptr<Array> const& array);
};

//
// Ordered transpose array implementation
//

class OrderedTransposeArray;

class OrderedTransposeChunkIterator : public UnorderedTransposeChunkIterator
{  
    Coordinates pos;
    bool hasCurrent;
    void moveNext();

  public:
    virtual void operator ++();
    virtual bool end();
    virtual void reset();

    OrderedTransposeChunkIterator(DelegateChunk const* chunk, int iterationMode);
};

class OrderedTransposeArrayIterator : public UnorderedTransposeArrayIterator
{
    Coordinates pos;
    bool hasCurrent;
    bool setConstructed;
    set<Coordinates, CoordinatesLess> chunks;
    set<Coordinates, CoordinatesLess>::const_iterator currPos;

    bool buildSetOfChunks();
    void setFirst();

  public:
    virtual void operator ++();
    virtual bool end();
    virtual void reset();
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos);

	OrderedTransposeArrayIterator(OrderedTransposeArray const& array, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> inputIterator);
};

class OrderedTransposeArray : public UnorderedTransposeArray
{
    friend class OrderedTransposeChunk;
    friend class OrderedTransposeChunkIterator;
    friend class OrderedTransposeArrayIterator;

  public:
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;

    OrderedTransposeArray(ArrayDesc const& desc, boost::shared_ptr<Array> const& array);
};


}

#endif
