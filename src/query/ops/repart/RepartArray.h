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
 * @file RepartArray.cpp
 *
 * @brief Repart array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#ifndef REPART_ARRAY_H
#define REPART_ARRAY_H

#include "array/MemArray.h"
#include "array/DelegateArray.h"

namespace scidb {

using namespace boost;
using namespace std;

class RepartArray;
class RepartArrayIterator;
class RepartChunk;
class RepartChunkIterator;

class RepartChunkIterator : public ConstChunkIterator
{
    RepartChunk& chunk;
    Coordinates pos;
    Coordinates first;
    Coordinates last;
    boost::shared_ptr<ConstChunkIterator> inputIterator;
    boost::shared_ptr<ConstArrayIterator> arrayIterator;
    int  inMode;
    int  outMode;
    bool hasCurrent;

  public:
    virtual int    getMode();
    virtual bool   setPosition(Coordinates const& pos);
    virtual Coordinates const& getPosition();
    virtual void   operator++();
    virtual void   reset();
    virtual Value& getItem();
    virtual bool   isEmpty();
    virtual bool   end();
    virtual ConstChunk const& getChunk();

    RepartChunkIterator(RepartChunk& chunk, int iterationMode);
};

class RepartChunk : public DelegateChunk
{
    friend class RepartChunkIterator;

    MemChunk chunk;
    int  overlapMode;
    bool sparse;

  public:
    virtual bool isSparse() const;
    virtual boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
    void initialize(Coordinates const& pos, bool sparse);

    RepartChunk(RepartArray const& array, DelegateArrayIterator const& iterator, AttributeID attrID);
};

class RepartArrayIterator : public DelegateArrayIterator
{
    Coordinates pos;
    bool hasCurrent;
    bool isSparseArray;
    Coordinates upperBound;
    MemChunk repartChunk; // materialized result chunk (used for sparse arrays)
    vector<size_t> chunkInterval;
    vector<size_t> chunkOverlap;
    boost::shared_ptr<ConstArrayIterator> emptyIterator;
    void fillSparseChunk(Coordinates const& chunkPos);
    bool containsChunk(Coordinates const& chunkPos);
    boost::weak_ptr<Query> _query;
    int overlapMode;
    ConstChunk const* currChunk;
  public:
    virtual ConstChunk const& getChunk();
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos);
    virtual bool end();
    virtual void operator ++();
    virtual void reset();
    virtual boost::shared_ptr<Query> getQuery() { return _query.lock(); }

    RepartArrayIterator(RepartArray const& array, AttributeID attrID,
                        boost::shared_ptr<ConstArrayIterator> inputIterator,
                        const boost::shared_ptr<Query>& query);
};

class RepartArray : public DelegateArray
{
    friend class RepartChunk;
    friend class RepartArrayIterator;

  private:
    int overlapMode;
    boost::weak_ptr<Query> _query;
    
  public:
    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const;
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;
    
    RepartArray(ArrayDesc const& desc,
                boost::shared_ptr<Array> const& array,
                const boost::shared_ptr<Query>& query);
};

}

#endif
