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
 * @file BetweenArray.h
 *
 * @brief The implementation of the array iterator for the between operator
 *
 * The array iterator for the between maps incoming getChunks calls into the
 * appropriate getChunks calls for its input array. Then, if the requested chunk
 * fits in the between range, the entire chunk is returned as-is. Otherwise,
 * the appropriate piece of the chunk is carved out.
 *
 * NOTE: In the current implementation if the between window stretches beyond the
 * limits of the input array, the behavior of the operator is undefined.
 *
 * The top-level array object simply serves as a factory for the iterators.
 */

#ifndef BETWEEN_ARRAY_H_
#define BETWEEN_ARRAY_H_

#include <string>
#include "array/DelegateArray.h"
#include "array/Metadata.h"

namespace scidb
{

using namespace std;
using namespace boost;

class BetweenArray;
class BetweenArrayIterator;
class BetweenChunkIterator;

class BetweenChunk : public DelegateChunk
{
    friend class BetweenChunkIterator;
  public:
    boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;

    void setInputChunk(ConstChunk const& inputChunk);

    BetweenChunk(BetweenArray const& array, DelegateArrayIterator const& iterator, AttributeID attrID);

  private:
    BetweenArray const& array;
    Coordinates firstPos;
    Coordinates lastPos;
    bool fullyInside;
    bool fullyOutside;
    boost::shared_ptr<ConstArrayIterator> emptyBitmapIterator;
};
    
    class BetweenChunkIterator : public ConstChunkIterator, CoordinatesMapper
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

    bool between() const;
    
    BetweenChunkIterator(BetweenChunk const& chunk, int iterationMode);

  protected:
    Value& buildBitmap();

    BetweenArray const& array;
    BetweenChunk const& chunk;
    boost::shared_ptr<ConstChunkIterator> inputIterator;
    Coordinates currPos; 
    bool hasCurrent;
    bool isSparse;
    int mode;
    Value tileValue;
    MemChunk shapeChunk;
    boost::shared_ptr<ConstChunkIterator> emptyBitmapIterator;
    TypeId type;
};

class ExistedBitmapBetweenChunkIterator : public BetweenChunkIterator
{
  public:
    virtual  Value& getItem();

    ExistedBitmapBetweenChunkIterator(BetweenChunk const& chunk, int iterationMode);

  private:
     Value _value;
};
     
   
class NewBitmapBetweenChunkIterator : public BetweenChunkIterator
{
  public:
    virtual  Value& getItem();

    NewBitmapBetweenChunkIterator(BetweenChunk const& chunk, int iterationMode);

  protected:
     Value _value;
};

class EmptyBitmapBetweenChunkIterator : public NewBitmapBetweenChunkIterator
{
  public:
    virtual  Value& getItem();
    virtual bool isEmpty();

    EmptyBitmapBetweenChunkIterator(BetweenChunk const& chunk, int iterationMode);
};


class BetweenArrayIterator : public DelegateArrayIterator
{
    friend class BetweenChunkIterator;
  public:
	/***
	 * Constructor for the between iterator
	 * Here we initialize the current position vector to all zeros, and obtain an iterator for the appropriate
	 * attribute in the input array.
	 */
	BetweenArrayIterator(BetweenArray const& between, AttributeID attrID, AttributeID inputAttrID);

	/***
	 * The end call checks whether we're operating with the last chunk of the between
	 * window.
	 */
	virtual bool end();

	/***
	 * The ++ operator advances the current position to the next chunk of the between
	 * window.
	 */
	virtual void operator ++();

	/***
	 * Simply returns the current position
	 * Initial position is a vector of zeros of appropriate dimensionality
	 */
	virtual Coordinates const& getPosition();

	/***
	 * Here we only need to check that we're not moving beyond the bounds of the between window
	 */
	virtual bool setPosition(Coordinates const& pos);

	/***
	 * Reset simply changes the current position to all zeros
	 */
	virtual void reset();

  private:
    BetweenArray const& array;	
    Coordinates lowPos;
    Coordinates highPos;
	Coordinates pos; 
    bool hasCurrent;
};

class BetweenArray : public DelegateArray
{
    friend class BetweenChunk;
    friend class BetweenChunkIterator;
    friend class BetweenArrayIterator;

  public:
	BetweenArray(ArrayDesc& desc, Coordinates const& lowPos, Coordinates const& highPos, boost::shared_ptr<Array> input, bool tileMode);

    DelegateArrayIterator* createArrayIterator(AttributeID attrID) const;
    DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const;

  private:
	Coordinates lowPos;
	Coordinates highPos;
    Dimensions const& dims;
    bool tileMode;
};


} //namespace

#endif /* BETWEEN_ARRAY_H_ */
