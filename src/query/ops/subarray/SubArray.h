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
 * @file SubArray.h
 *
 * @brief The implementation of the array iterator for the subarray operator
 *
 * The array iterator for the subarray maps incoming getChunks calls into the
 * appropriate getChunks calls for its input array. Then, if the requested chunk
 * fits in the subarray range, the entire chunk is returned as-is. Otherwise,
 * the appropriate piece of the chunk is carved out.
 *
 * NOTE: In the current implementation if the subarray window stretches beyond the
 * limits of the input array, the behavior of the operator is undefined.
 *
 * The top-level array object simply serves as a factory for the iterators.
 */

#ifndef SUB_ARRAY_H_
#define SUB_ARRAY_H_

#include <string>
#include "array/DelegateArray.h"
#include "array/Metadata.h"

namespace scidb
{

using namespace std;
using namespace boost;

class SubArray;
class SubArrayIterator;
class SubArrayChunkIterator;

void subarrayMappingArray(string const& dimName, string const& mappingArrayName, string const& tmpMappingArrayName, 
                          Coordinate from, Coordinate till, boost::shared_ptr<Query> const& query);


class SubArrayChunk : public DelegateChunk
{
    friend class SubArrayChunkIterator;
    friend class SubArrayDirectChunkIterator;
  public:
    Coordinates const& getFirstPosition(bool withOverlap) const;
    Coordinates const& getLastPosition(bool withOverlap) const;
    boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;

    void setPosition(Coordinates const& pos);

    SubArrayChunk(SubArray const& array, DelegateArrayIterator const& iterator, AttributeID attrID);

  private:
    SubArray const& array;
    Coordinates firstPos;
    Coordinates firstPosWithOverlap;
    Coordinates lastPos;
    Coordinates lastPosWithOverlap;
    bool        fullyBelongs;
};
    
class SubArrayChunkIterator : public ConstChunkIterator
{
  public:
    int getMode();
    Value& getItem();
    bool isEmpty();
    bool end();
    void operator ++();
    Coordinates const& getPosition();
    bool setPosition(Coordinates const& pos);
    void reset();
    ConstChunk const& getChunk();

    SubArrayChunkIterator(SubArrayChunk const& chunk, int iterationMode);

  private:
    SubArray const& array;
    SubArrayChunk const& chunk;
    ConstChunk const* inputChunk;
    boost::shared_ptr<ConstChunkIterator> inputIterator;
    Coordinates outPos; 
    Coordinates inPos; 
    bool hasCurrent;
    int mode;
};

class SubArrayDirectChunkIterator : public DelegateChunkIterator
{
  public:
    Coordinates const& getPosition();
    bool setPosition(Coordinates const& pos);

    SubArrayDirectChunkIterator(SubArrayChunk const& chunk, int iterationMode);

  private:
    SubArray const& array;
    Coordinates currPos; 
};

/***
 * The iterator of the subarray implements all the calls of the array iterator API though setPosition
 * calls.
 * NOTE: This looks like a candidate for an intermediate abstract class: PositionConstArrayIterator.
 */
class SubArrayIterator : public DelegateArrayIterator
{
    friend class SubArrayChunkIterator;
  public:
	/***
	 * Constructor for the subarray iterator
	 * Here we initialize the current position vector to all zeros, and obtain an iterator for the appropriate
	 * attribute in the input array.
	 */
	SubArrayIterator(SubArray const& subarray, AttributeID attrID);

	/***
	 * The end call checks whether we're operating with the last chunk of the subarray
	 * window.
	 */
	virtual bool end();

	/***
	 * The ++ operator advances the current position to the next chunk of the subarray
	 * window.
	 */
	virtual void operator ++();

	/***
	 * Simply returns the current position
	 * Initial position is a vector of zeros of appropriate dimensionality
	 */
	virtual Coordinates const& getPosition();

	/***
	 * Here we only need to check that we're not moving beyond the bounds of the subarray window
	 */
	virtual bool setPosition(Coordinates const& pos);

	/***
	 * Reset simply changes the current position to all zeros
	 */
	virtual void reset();

    virtual ConstChunk const& getChunk();

  private:
    bool setInputPosition(size_t i);
    void fillSparseChunk(size_t i);
    void checkState();

    SubArray const& array;	
	Coordinates outPos; 
	Coordinates inPos; 
    bool hasCurrent;
    bool positioned;

    Coordinates outChunkPos;
    boost::shared_ptr<ChunkIterator> outIterator;
    MemChunk sparseChunk;
    boost::shared_ptr<ConstArrayIterator> emptyIterator;
};

class SubArray : public DelegateArray
{
    friend class SubArrayChunk;
    friend class SubArrayChunkIterator;
    friend class SubArrayDirectChunkIterator;
    friend class SubArrayIterator;

  public:
	SubArray(ArrayDesc& d, Coordinates lowPos, Coordinates highPos, boost::shared_ptr<Array> input);

    DelegateArrayIterator* createArrayIterator(AttributeID attrID) const;
    DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const;
 
    void out2in(Coordinates const& out, Coordinates& in) const;
    void in2out(Coordinates const& in, Coordinates& out) const;
  private:
	Coordinates subarrayLowPos;
	Coordinates subarrayHighPos;
    Dimensions const& dims;
    Dimensions const& inputDims;
    bool aligned;
};


} //namespace

#endif /* SUB_ARRAY_H_ */
