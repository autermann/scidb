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
 * @file MapJoinArray.h
 *
 * @brief The implementation of the array iterator for the map operator
 *
 * The array iterator for the map maps incoming getChunks calls into the
 * appropriate getChunks calls for its input array. Then, if the requested chunk
 * fits in the map range, the entire chunk is returned as-is. Otherwise,
 * the appropriate piece of the chunk is carved out.
 *
 * NOTE: In the current implementation if the map window stretches beyond the
 * limits of the input array, the behavior of the operator is undefined.
 *
 * The top-level array object simply serves as a factory for the iterators.
 */

#ifndef MAP_JOIN_ARRAY_H_
#define MAP_JOIN_ARRAY_H_

#include <string>
#include "array/DelegateArray.h"
#include "array/Metadata.h"

namespace scidb
{

using namespace std;
using namespace boost;

class MapJoinArray;

class MapJoinChunkIterator : public DelegateChunkIterator
{
  public:
    Value& getItem();
    bool isEmpty();
    bool end();
    void operator ++();
    bool setPosition(Coordinates const& pos);
    void reset();

    MapJoinChunkIterator(MapJoinArray const& array, DelegateChunk const* chunk, int iterationMode);

  private:
    bool setRightPosition();

    MapJoinArray const& array;
    AttributeID attr;
    shared_ptr<ConstChunkIterator> rightIterator;
    shared_ptr<ConstArrayIterator> rightArrayIterator;
    bool hasCurrent;
    bool notMatched;
    bool isEmptyIndicator;
    bool isLeftAttribute;
    int mode;
    Value boolValue;
};

class MapJoinArrayIterator : public DelegateArrayIterator
{
    friend class MapJoinChunkIterator;

  public:
        MapJoinArrayIterator(DelegateArray const& delegate, AttributeID attrID,
                         shared_ptr<ConstArrayIterator> leftIterator,
                         shared_ptr<ConstArrayIterator> rightIterator,
                         shared_ptr<ConstArrayIterator> inputIterator);

        virtual void operator ++();
        virtual void reset();

  private:
    shared_ptr<ConstArrayIterator> leftIterator;
    shared_ptr<ConstArrayIterator> rightIterator;
    shared_ptr<ConstArrayIterator> inputIterator;
};

class MapJoinArray : public DelegateArray
{
    friend class MapJoinChunkIterator;

  public:
    MapJoinArray(ArrayDesc& desc, shared_ptr<Array> left, shared_ptr<Array> right,
                 const boost::shared_ptr<Query>& query);

    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;

    bool mapPosition(Coordinates& pos) const;

  private:
        ArrayDesc leftDesc;
        ArrayDesc rightDesc;
        shared_ptr<Array> left;
        shared_ptr<Array> right;
    const size_t nLeftAttrs;
    const size_t nRightAttrs;
    int    leftEmptyTagPosition;
    int    rightEmptyTagPosition;
    boost::weak_ptr<Query> _query;
};


} //namespace

#endif /* MAP_JOIN_ARRAY_H_ */
