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
 * @file EmbeddedArray.h
 *
 * @brief Embedded array implementation
 */

#ifndef EMBEDDED_ARRAY_H_
#define EMBEDDED_ARRAY_H_

#include "array/Array.h"

using namespace std;
using namespace boost;

namespace scidb
{

class EmbeddedArray;
class EmbeddedArrayIterator;

/**
 * Chunk of embedded array
 */
class EmbeddedChunk : public Chunk
{
    Coordinates basePos;
    Coordinates basePosWithOverlaps;
    Coordinates firstPos;
    Coordinates firstPosWithOverlaps;
    Coordinates lastPos;
    Coordinates lastPosWithOverlaps;
    Chunk*      chunk;
    EmbeddedArray& array;

  public:
    EmbeddedChunk(EmbeddedArray& arr);
    void setChunk(Chunk& physicalChunk);

    virtual Array const& getArray() const; 

    const ArrayDesc& getArrayDesc() const;
    const AttributeDesc& getAttributeDesc() const;
    int getCompressionMethod() const;
    Coordinates const& getFirstPosition(bool withOverlap) const;
    Coordinates const& getLastPosition(bool withOverlap) const;
    boost::shared_ptr<ChunkIterator> getIterator(boost::shared_ptr<Query> const& query, int iterationMode);
    boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
    bool pin() const;
    void unPin() const;
};

/**
 * Embedded array implementation
 */
class EmbeddedArray : public Array
{
    friend class EmbeddedChunk;
    friend class EmbeddedChunkIterator;
    friend class EmbeddedArrayIterator;

    void physicalToVirtual(Coordinates& pos) const;
    void virtualToPhysical(Coordinates& pos) const;
    bool contains(Coordinates const& physicalPos) const;

  public:
    virtual string const& getName() const;
    virtual ArrayID getHandle() const;

    virtual ArrayDesc const& getArrayDesc() const;

    virtual boost::shared_ptr<ArrayIterator> getIterator(AttributeID attId);
    virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attId) const;

    EmbeddedArray(Array& physicalArrayImpl, ArrayDesc const& embeddedArrayDesc, Coordinates const& positionInParentArray);
    EmbeddedArray(const EmbeddedArray& other);

  private:
    Array& physicalArray;
    ArrayDesc embeddedDesc;
    Coordinates basePos;
};

/**
 * Embedded array iterator
 */
class EmbeddedArrayIterator : public ArrayIterator
{
    EmbeddedArray& array;
    AttributeID attribute;
    EmbeddedChunk chunk;
    Coordinates pos;
    boost::shared_ptr<ArrayIterator> iterator;

  public:
    EmbeddedArrayIterator(EmbeddedArray& arr, AttributeID attId);
    ConstChunk const& getChunk();
    bool end();
    void operator ++();
    Coordinates const& getPosition();
    bool setPosition(Coordinates const& pos);
    void setCurrent();
    void reset();
    Chunk& newChunk(Coordinates const& pos);
    Chunk& newChunk(Coordinates const& pos, int compressionMethod);
};

/**
 * Embedded array chunk iterator
 */
class EmbeddedChunkIterator : public ChunkIterator
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
    void writeItem(const  Value& item);
    void flush();
    ConstChunk const& getChunk();

    EmbeddedChunkIterator(EmbeddedArray& arr, EmbeddedChunk const* chunk, boost::shared_ptr<ChunkIterator> iterator);

  protected:
    EmbeddedArray& array;
    EmbeddedChunk const* chunk;
    boost::shared_ptr<ChunkIterator> iterator;
    Coordinates pos;
};

}

#endif

