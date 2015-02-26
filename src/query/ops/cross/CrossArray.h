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
 * @file CrossArray.h
 *
 * @brief The implementation of the array iterator for the cross operator
 *
 * The array iterator for the cross maps incoming getChunks calls into the
 * appropriate getChunks calls for its input array. Then, if the requested chunk
 * fits in the cross range, the entire chunk is returned as-is. Otherwise,
 * the appropriate piece of the chunk is carved out.
 *
 * NOTE: In the current implementation if the cross window stretches beyond the
 * limits of the input array, the behavior of the operator is undefined.
 *
 * The top-level array object simply serves as a factory for the iterators.
 */

#ifndef CROSS_ARRAY_H_
#define CROSS_ARRAY_H_

#include <string>
#include "array/Array.h"
#include "array/Metadata.h"

namespace scidb
{

using namespace std;
using namespace boost;

class CrossArray;
class CrossArrayIterator;
class CrossChunkIterator;

class CrossChunk : public ConstChunk
{
    friend class CrossChunkIterator;
  public:
    const ArrayDesc& getArrayDesc() const;
    const AttributeDesc& getAttributeDesc() const;
    int getCompressionMethod() const;
    Coordinates const& getFirstPosition(bool withOverlap) const;
    Coordinates const& getLastPosition(bool withOverlap) const;
    boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;

    void setInputChunk(ConstChunk const* leftChunk, ConstChunk const* rightChunk);

    bool isMaterialized() const;
    bool isSparse() const;

    Array const& getArray() const;

    CrossChunk(CrossArray const& array, AttributeID attrID);

  private:
    CrossArray const& array;
    AttributeID attr;
    ConstChunk const* leftChunk;
    ConstChunk const* rightChunk;
    Coordinates firstPos;
    Coordinates firstPosWithOverlap;
    Coordinates lastPos;
    Coordinates lastPosWithOverlap;
};
    
class CrossChunkIterator : public ConstChunkIterator
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

    CrossChunkIterator(CrossChunk const& chunk, int iterationMode);

  private:
    CrossArray const& array;
    CrossChunk const& chunk;
    boost::shared_ptr<ConstChunkIterator> leftIterator;
    boost::shared_ptr<ConstChunkIterator> rightIterator;
    boost::shared_ptr<ConstChunkIterator> inputIterator;
    Coordinates currentPos; 
    bool hasCurrent;
    AttributeID attrID;
    Value trueValue;
};

/***
 * The iterator of the cross implements all the calls of the array iterator API though setPosition
 * calls.
 * NOTE: This looks like a candidate for an intermediate abstract class: PositionConstArrayIterator.
 */
class CrossArrayIterator : public ConstArrayIterator
{
  public:
	/***
	 * Constructor for the cross iterator
	 * Here we initialize the current position vector to all zeros, and obtain an iterator for the appropriate
	 * attribute in the input array.
	 */
	CrossArrayIterator(CrossArray const& cross, AttributeID attrID);

	/***
	 * Get chunk method retrieves the chunk at current position from the input iterator and either
	 * passes it up intact (if the chunk is completely within the cross window), or carves out
	 * a piece of the input chunk into a fresh chunk and sends that up.
	 *
	 * It performs a mapping from the cross coordinates to the coordinates of the input array
	 * It does not check any bounds, since the setPosition call will not set an invalid position.
	 *
	 * NOTE: Currently we don't check where the cross window streches beyond the limits of the
	 * input array. What is the behavior then - padd with NULLs?
	 */
	virtual ConstChunk const& getChunk();

	/***
	 * The end call checks whether we're operating with the last chunk of the cross
	 * window.
	 */
	virtual bool end();

	/***
	 * The ++ operator advances the current position to the next chunk of the cross
	 * window.
	 */
	virtual void operator ++();

	/***
	 * Simply returns the current position
	 * Initial position is a vector of zeros of appropriate dimensionality
	 */
	virtual Coordinates const& getPosition();

	/***
	 * Here we only need to check that we're not moving beyond the bounds of the cross window
	 */
	virtual bool setPosition(Coordinates const& pos);

	/***
	 * Reset simply changes the current position to all zeros
	 */
	virtual void reset();

  private:
    CrossArray const& array;	
	AttributeID attr;
    CrossChunk chunk;
	Coordinates currentPos; 
    boost::shared_ptr<ConstArrayIterator> leftIterator;
    boost::shared_ptr<ConstArrayIterator> rightIterator;
    bool hasCurrent;
    bool chunkInitialized;
};

class CrossArray : public Array
{
    friend class CrossChunk;
    friend class CrossChunkIterator;
    friend class CrossArrayIterator;

  public:
	CrossArray(ArrayDesc& desc, boost::shared_ptr<Array> left, boost::shared_ptr<Array> right);

	virtual const ArrayDesc& getArrayDesc() const;
	virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID id) const;

  private:
	ArrayDesc desc;
	boost::shared_ptr<Array> leftArray;
	boost::shared_ptr<Array> rightArray;
    size_t nLeftDims;
    size_t nLeftAttrs;
    size_t nRightAttrs;
};


} //namespace

#endif /* CROSS_ARRAY_H_ */
