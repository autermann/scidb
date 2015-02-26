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
 * @file FileArray.h
 *
 * @brief In-Memory (temporary) array implementation
 */

#ifndef FILE_ARRAY_H_
#define FILE_ARRAY_H_

#include <map>
#include <assert.h>

#include "array/MemArray.h"
#include "query/Statistics.h"

using namespace std;
using namespace boost;

namespace scidb
{

class FileArray;
class FileArrayIterator;

struct ChunkHeader {
    uint64_t offset;
    size_t   size;
    bool     sparse;
    bool     rle;
};

class FileChunk : public MemChunk
{
  public:
    bool isTemporary() const;

    virtual void write(boost::shared_ptr<Query>& query);
};


/**
 * Temporary (in-memory) array implementation
 */
class FileArray : public Array
{
    friend class FileArrayIterator;
  public:
    virtual ArrayDesc const& getArrayDesc() const;

    virtual boost::shared_ptr<ArrayIterator> getIterator(AttributeID attId);
    virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attId) const;

    FileArray(ArrayDesc const& arr, char const* filePath = NULL);
    FileArray(boost::shared_ptr<Array> input, bool verical = true, char const* filePath = NULL);
    ~FileArray();

    void writeChunk(FileChunk* chunk);

  private:
    void init(ArrayDesc const& arr, char const* filePath);

    ArrayDesc desc;
    uint64_t fileSize;
    int fd;
    AttributeID emptyBitmapID;
    vector< map<Coordinates, ChunkHeader, CoordinatesLess> > chunks;
    map<Coordinates, FileChunk, CoordinatesLess> bitmapChunks;
    AttributeDesc const* bitmapAttr;
};

/**
 * Temporary (in-memory) array iterator
 */
class FileArrayIterator : public ArrayIterator
{
    map<Coordinates, ChunkHeader, CoordinatesLess>::iterator curr;
    map<Coordinates, ChunkHeader, CoordinatesLess>::iterator last;
    FileArray& array;
    Address addr;
    FileChunk dataChunk;
    FileChunk* currChunk;
    boost::shared_ptr<FileChunk> bitmapChunk;

    void setBitmapChunk();

  public:
    FileArrayIterator(FileArray& arr, AttributeID attId);
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

boost::shared_ptr<Array> createTmpArray(ArrayDesc const& arr);

}

#endif

