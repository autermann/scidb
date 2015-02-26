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

/*
 * InverseArray.h
 *
 */
#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/MemArray.h"

#ifndef INVERSE_ARRAY_H
#define INVERSE_ARRAY_H

namespace scidb
{

class InverseArray;
class InverseArrayIterator;

class InverseArrayIterator : public ConstArrayIterator
{
  public:
	virtual ConstChunk const& getChunk();
	virtual bool end();
	virtual void operator ++();
	virtual Coordinates const& getPosition();
	virtual bool setPosition(Coordinates const& pos);
	virtual void reset();

 	InverseArrayIterator(InverseArray const& array, AttributeID id);
	~InverseArrayIterator();

  private:

	void computeInverse();

	InverseArray const& array;
	Coordinates currPos;
	bool hasCurrent;
    bool chunkInitialized;
	MemChunk chunk;
	double *matrix;

	int iChunkLen, jChunkLen;
	int iLength, jLength;
};

class InverseArray : public Array
{
  public:
	virtual ArrayDesc const& getArrayDesc() const;
	virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attr) const;
	InverseArray(ArrayDesc desc, boost::shared_ptr<Array> const& inputArray);

  private:
	ArrayDesc desc;
	boost::shared_ptr<Array> inputArray;
	
  friend class InverseArrayIterator;
}; 

} //namespace scidb

#endif /* MUTLIPLY_ARRAY_H */
