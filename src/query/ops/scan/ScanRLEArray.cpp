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

#include <fstream>
#include <boost/filesystem.hpp>
#include <iostream>
#include <stdio.h>
#include <sstream>

#include "system/ErrorCodes.h"
#include "array/Compressor.h"

#include "ScanRLEArray.h"

using namespace boost;
using namespace scidb;

namespace scidb
{

ScanRLEArray::ScanRLEArray(ArrayDesc const& arr, std::string path) :
    RLEArray(arr), _dirPath(path), _maxChunkNo(0), logger(log4cxx::Logger::getLogger("scidb.query.ops.ScanRQArray"))
{
    filesystem::path full_path = filesystem::system_complete(filesystem::path(_dirPath));
    if (!filesystem::exists(full_path))
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_OPEN_PATH) << _dirPath;
    }
    if (!filesystem::is_directory(full_path))
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_DIRECTORY_EXPECTED) << _dirPath;
    }

    _maxChunkNo = arr.getNumberOfChunks() / arr.getAttributes().size();
}

boost::shared_ptr<ConstRLEArrayIterator> ScanRLEArray::getConstRqArrayIterator(AttributeID attId) const
{
    return boost::shared_ptr<ConstRLEArrayIterator>(new ScanRLEArrayIterator(this, attId, _dirPath, _maxChunkNo));
}

ScanRLEArrayIterator::ScanRLEArrayIterator(const RLEArray * const source, AttributeID attId, string path, size_t maxChunkNo) :
    currChunk(NULL), logger(log4cxx::Logger::getLogger("scidb.query.ops.ScanRQArrayIterator")), _attId(attId), _dirPath(path), _chunkNo(0),
            _maxChunkNo(maxChunkNo), _desc(&source->getArrayDesc()), _currPos(_desc->getDimensions().size(), 0)
{
    if (_desc->getAttributes()[attId].getFlags() & AttributeDesc::IS_NULLABLE)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_NOT_IMPLEMENTED) << "nullable attributes";
    }
    if (_desc->getAttributes()[attId].getSize() == 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_NOT_IMPLEMENTED) << "variable size attributes";
    }

    // set up the file offsets and chunk sizes
    uint8_t attrBytes, chunkBytes, offsetBytes;
    stringstream chunkFileName, offsetFileName;
    chunkId lhs0, lhs1;
    chunkFileInfo rhs0, rhs1;

    chunkFileName << path << "/chunks";
    offsetFileName << path << "/offsets";

    ifstream offsetFile(offsetFileName.str().c_str(), ios_base::binary | ios_base::in | ios_base::ate);
    _chunkFile.open(chunkFileName.str().c_str(), ios_base::binary | ios_base::in | ios_base::ate);

    size_t offsetFileSize = offsetFile.tellg();
    size_t chunkFileSize = _chunkFile.tellg();

    offsetFile.seekg(0, ios_base::beg);
    offsetFile.read((char *) &attrBytes, 1);
    offsetFile.read((char *) &chunkBytes, 1);
    offsetBytes = sizeof(size_t);

    size_t offsetChunks = offsetFileSize / (attrBytes + chunkBytes + offsetBytes);

    // init them all
    rhs0.first = rhs0.second = rhs1.first = rhs1.second = lhs0.first = lhs0.second = lhs1.first = lhs1.second = 0;

    // read starter chunk
    offsetFile.read((char *) &lhs0.first, attrBytes);
    offsetFile.read((char *) &lhs0.second, chunkBytes);
    offsetFile.read((char *) &rhs0.first, offsetBytes);

    // the chunk size is wrong because it will be off by one
    for (size_t i = 1; i < offsetChunks; ++i)
    {
        // read in tuple 1
        offsetFile.read((char *) &lhs1.first, attrBytes);
        offsetFile.read((char *) &lhs1.second, chunkBytes);
        offsetFile.read((char *) &rhs1.first, offsetBytes);

        // calculate chunk offset of predecessor
        rhs0.second = rhs1.first - rhs0.first;

        // commit it
        _chunkFileData[lhs0] = rhs0;

        // roll over to the next one
        lhs0 = lhs1;
        rhs0 = rhs1;
    }

    // output the last one
    rhs0.second = chunkFileSize - rhs0.first;
    _chunkFileData[lhs0] = rhs0;

    reset();
}

bool ScanRLEArrayIterator::haveChunk() const
{
    chunkId current(_attId, _chunkNo);
    if (_chunkFileData.find(current) != _chunkFileData.end())
    {
        return true;
    }
    return false;

}

ConstRLEChunk const& ScanRLEArrayIterator::getRQChunk()
{
    Address addr(_desc->getId(), _attId, getPosition());

    ifstream in;
    chunkId chunkDesc;
    chunkFileInfo payloadFileInfo;

    chunkDesc.first = _attId;
    chunkDesc.second = _chunkNo;

    payloadFileInfo = _chunkFileData[chunkDesc];
    size_t payloadOffset = payloadFileInfo.first;
    size_t payloadSize = payloadFileInfo.second;

    vector<char> data (payloadSize, 0);
    _chunkFile.seekg(payloadOffset, ios_base::beg);
    _chunkFile.read(&data[0], payloadSize);

    TypeId type = _desc->getAttributes()[_attId].getType();
    uint16_t compressionMethod = _desc->getAttributes()[_attId].getDefaultCompressionMethod();

    _chunk.initialize(_desc, addr, compressionMethod);

    if (_desc->getEmptyBitmapAttribute() && _attId == _desc->getEmptyBitmapAttribute()->getId())
    {
        //bitmask chunk
        ConstRLEEmptyBitmap emptyMask(&data[0]);
        //inefficient double copy
        shared_ptr<RLEEmptyBitmap> em(new RLEEmptyBitmap(emptyMask));
        _chunk.setMask(em);
    }
    else
    {
        //data chunk

        //another inefficient double copy
        ConstRLEPayload payload(&data[0]);
        _chunk.setPayload(payload);

        //now we still need to find bitmask and attach it to chunk
        if (_desc->getEmptyBitmapAttribute())
        {
            ifstream bmin;
            ostringstream bmPath;
            chunkId bmId;
            chunkFileInfo bmFile;

            bmId.first = _desc->getEmptyBitmapAttribute()->getId();
            bmId.second = _chunkNo;
            bmFile = _chunkFileData[bmId];
            size_t bitmaskSize = bmFile.second;
            size_t fileOffset = bmFile.first;

            vector<char> bmData(bitmaskSize,0);
            _chunkFile.seekg(fileOffset, ios_base::beg);
            _chunkFile.read(&bmData[0], bitmaskSize);

            //another inefficient double copy
            ConstRLEEmptyBitmap emptyMask(&bmData[0]);
            shared_ptr<RLEEmptyBitmap> em(new RLEEmptyBitmap(emptyMask));

            _chunk.setMask(em);
        }
        else //no bitmask? create one that looks like "111111111111111"
        {
           size_t nBits = _chunk.getNumberOfElements(true);
           shared_ptr<RLEEmptyBitmap> em(new RLEEmptyBitmap(nBits));
            _chunk.setMask(em);
        }
    }
    return _chunk;
}

bool ScanRLEArrayIterator::end()
{
    return _chunkNo > _maxChunkNo;
}

// do nothing for now
bool ScanRLEArrayIterator::setPosition(Coordinates const& pos)
{
    _chunkNo = _desc->getChunkNumber(pos);
    if (end())
    {
        return false;
    }
    return haveChunk();
}

void ScanRLEArrayIterator::operator ++()
{
    _chunkNo++;
    while (!end() && !haveChunk())
    {
        _chunkNo++;
    }
}

const Coordinates & ScanRLEArrayIterator::getPosition()
{
    size_t cn = _chunkNo;
    for (ssize_t i = _currPos.size() - 1; i >= 0; i--)
    {
        if (cn == 0)
        {
            _currPos[i] = _desc->getDimensions()[i].getStartMin();
            continue;
        }

        size_t chunksPerDim = (_desc->getDimensions()[i].getLength() + _desc->getDimensions()[i].getChunkInterval() - 1)
                / _desc->getDimensions()[i].getChunkInterval();
        _currPos[i] = _desc->getDimensions()[i].getStartMin() + (cn % chunksPerDim) * _desc->getDimensions()[i].getChunkInterval();
        cn = cn / chunksPerDim;
    }
    return _currPos;
}

void ScanRLEArrayIterator::reset()
{
    _chunkNo = 0;
    while (!end() && !haveChunk())
    {
        _chunkNo++;
    }
}

ScanRLEArrayIterator::~ScanRLEArrayIterator()
{
    if (_chunkFile.is_open())
    {
        _chunkFile.close();
    }
}

}

