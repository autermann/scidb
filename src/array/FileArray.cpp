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
 * @file FileArray.cpp
 *
 * @brief Temporary on-disk array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include "util/FileIO.h"

#include "array/FileArray.h"
#include <inttypes.h>
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"
#include "network/NetworkManager.h"
#include "query/OperatorLibrary.h"

namespace scidb
{
    using namespace boost;
    using namespace std;

    //
    // File Array Chunk
    //
    bool FileChunk::isTemporary() const
    {
        return false;
    }

    void  FileChunk::write(boost::shared_ptr<Query>& query)
    {
       // the query is ignored because we are writing to a temp file
        ((FileArray*)array)->writeChunk(this);
    }

    //
    // File Array
    //
    FileArray::FileArray(ArrayDesc const& arr, char const* filePath)
    {
        init(arr, filePath);
    }

    FileArray::FileArray(boost::shared_ptr<Array> input, bool vertical, char const* filePath)
    {
        init(input->getArrayDesc(), filePath);
        append(input, vertical);
    }

    void FileArray::init(ArrayDesc const& arr, char const* filePath)
    {
        desc = arr;
        fileSize = 0;
        chunks.resize(arr.getAttributes().size());
        bitmapAttr = desc.getEmptyBitmapAttribute();
        emptyBitmapID = bitmapAttr != NULL ? bitmapAttr->getId() : (AttributeID)-1;
        fd = File::createTemporary(arr.getName(), filePath);
    }

    FileArray::~FileArray()
    {
        ::close(fd);
    }

    void FileArray::writeChunk(FileChunk* chunk)
    {
        Address const& addr = chunk->getAddress();
        ChunkHeader hdr;
        hdr.size = chunk->getSize();
        hdr.offset = fileSize;
        hdr.sparse = chunk->isSparse();
        hdr.rle = chunk->isRLE();
        chunks[addr.attId][addr.coords] = hdr;
        if (emptyBitmapID != addr.attId) {
            File::writeAll(fd, chunk->getData(), hdr.size, hdr.offset);
            fileSize += hdr.size;
        }
    }

    ArrayDesc const& FileArray::getArrayDesc() const
    {
        return desc;
    }

    boost::shared_ptr<ArrayIterator> FileArray::getIterator(AttributeID attId)
    {
        return boost::shared_ptr<ArrayIterator>(new FileArrayIterator(*this, attId));
    }

    boost::shared_ptr<ConstArrayIterator> FileArray::getConstIterator(AttributeID attId) const
    {
        return ((FileArray*)this)->getIterator(attId);
    }

    //
    // File Array Iterator
    //
    FileArrayIterator::FileArrayIterator(FileArray& arr, AttributeID attId) : array(arr)
    {
        addr.attId = attId;
        reset();
    }

    ConstChunk const& FileArrayIterator::getChunk()
    {
        if (!currChunk)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        return *currChunk;
    }

    bool FileArrayIterator::end()
    {
        return currChunk == NULL;
    }

    void FileArrayIterator::operator ++()
    {
        ++curr;
        setCurrent();
    }

    Coordinates const& FileArrayIterator::getPosition()
    {
        if (!currChunk)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        return addr.coords;
    }

    bool FileArrayIterator::setPosition(Coordinates const& pos)
    {
        addr.coords = pos;
        array.desc.getChunkPositionFor(addr.coords);
        curr = array.chunks[addr.attId].find(addr.coords);
        setCurrent();
        return currChunk != NULL;
    }

    void FileArrayIterator::setCurrent()
    {
        if (curr != last) {
            addr.coords = curr->first;
            if (array.emptyBitmapID == addr.attId) {
                currChunk = &array.bitmapChunks[addr.coords];
                assert(currChunk->isInitialized());
            } else {
                currChunk = &dataChunk;
                dataChunk.initialize(&array, &array.desc, addr, 0);
                ChunkHeader const& hdr = curr->second;
                dataChunk.allocate(hdr.size);
                dataChunk.setSparse(hdr.sparse);
                dataChunk.setRLE(hdr.rle);
                File::readAll(array.fd, dataChunk.getData(), hdr.size, hdr.offset);
                setBitmapChunk();
            }
        } else {
            currChunk = NULL;
        }
    }

    void FileArrayIterator::reset()
    {
        curr = array.chunks[addr.attId].begin();
        last = array.chunks[addr.attId].end();
        setCurrent();
    }

    Chunk& FileArrayIterator::newChunk(Coordinates const& pos)
    {
        addr.coords = pos;
        array.desc.getChunkPositionFor(addr.coords);
        if (array.emptyBitmapID == addr.attId) {
            FileChunk& chunk = array.bitmapChunks[addr.coords];
            if (!chunk.isInitialized()) {
                chunk.initialize(&array, &array.desc, addr, 0);
            }
            return chunk;
        } else {
            dataChunk.initialize(&array, &array.desc, addr, 0);
            setBitmapChunk();
            return dataChunk;
        }
    }

    Chunk& FileArrayIterator::newChunk(Coordinates const& pos, int compressionMethod)
    {
        return newChunk(pos);
    }

    void FileArrayIterator::setBitmapChunk()
    {
        if (array.bitmapAttr != NULL) {
            FileChunk& chunk = array.bitmapChunks[addr.coords];
            if (!chunk.isInitialized()) {
                Address bitmapChunkAddr = addr;
                bitmapChunkAddr.attId = array.bitmapAttr->getId();
                chunk.initialize(&array, &array.desc, bitmapChunkAddr, 0);
            }
            dataChunk.setBitmapChunk(&chunk);
        }
    }

shared_ptr<Array> createTmpArray(ArrayDesc const& arr)
{
    if (Config::getInstance()->getOption<bool>(CONFIG_SAVE_RAM))
    {
        return shared_ptr<Array>(new FileArray(arr));
    }
    else
    {
        return shared_ptr<Array>(new MemArray(arr));
    }
}

}

