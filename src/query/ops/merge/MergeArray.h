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
 * @file MergeArray.h
 *
 * @brief The implementation of the array iterator for the merge operator
 *
 */

#ifndef MERGE_ARRAY_H_
#define MERGE_ARRAY_H_

#include <string>
#include <vector>
#include "array/DelegateArray.h"
#include "array/Metadata.h"

namespace scidb
{

using namespace std;
using namespace boost;

class MergeArray;
class MergeArrayIterator;
class MergeChunkIterator;


class MergeChunkIterator : public DelegateChunkIterator
{    
  public:
    virtual bool end();
    virtual bool isEmpty();
    virtual  Value& getItem();
    virtual void operator ++();
    virtual void reset();
	virtual bool setPosition(Coordinates const& pos);
	virtual Coordinates const& getPosition();

    MergeChunkIterator(vector< ConstChunk const* > const& inputChunks, DelegateChunk const* chunk, int iterationMode);
    
  private:
    int currIterator;
    int mode;
    vector< boost::shared_ptr<ConstChunkIterator> > iterators;
};

class MergeChunk : public DelegateChunk
{
  public:
    vector< ConstChunk const* > inputChunks;

    MergeChunk(DelegateArray const& array, DelegateArrayIterator const& iterator, AttributeID attrID) 
    : DelegateChunk(array, iterator, attrID, false) {}    
};   

   
class MergeArrayIterator : public DelegateArrayIterator
{
  public:
    virtual bool end();
    virtual void operator ++();
    virtual void reset();
	virtual bool setPosition(Coordinates const& pos);
	virtual Coordinates const& getPosition();
  	virtual ConstChunk const& getChunk();
    MergeArrayIterator(MergeArray const& array, AttributeID attrID);

  private:
    MergeChunk chunk;
    vector< boost::shared_ptr<ConstArrayIterator> > iterators;
    int currIterator;
    bool isEmptyable;
    ConstChunk const* currentChunk;
};

class MergeArray : public DelegateArray
{
    friend class MergeArrayIterator;
  public:
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;
    virtual bool supportsRandomAccess() const;

    MergeArray(ArrayDesc const& desc, vector< boost::shared_ptr<Array> > const& inputArrays);

  private:
    vector< boost::shared_ptr<Array> > inputArrays;
};

}

#endif
