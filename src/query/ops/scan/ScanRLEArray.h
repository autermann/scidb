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

#ifndef _SCAN_RQ_ARRAY_
#define _SCAN_RQ_ARRAY_

#include "array/RLEArray.h"

#include <log4cxx/logger.h>

// pair attrNo, chunkNo
typedef pair<size_t, size_t> chunkId;
// file offset, chunk length in bytes
typedef pair<size_t, size_t> chunkFileInfo;

namespace scidb
{
class ScanRLEArray: public RLEArray
{

public:
    ScanRLEArray(ArrayDesc const& arr, std::string path);
    // returns a specialized iterator that maintains just one ConstRQChunk
    virtual boost::shared_ptr<ConstRLEArrayIterator> getConstRqArrayIterator(AttributeID attId) const;

private:
    std::string _dirPath;
    size_t _maxChunkNo;
    log4cxx::LoggerPtr logger;
};

class ScanRLEArrayIterator: public ConstRLEArrayIterator
{
public:
    ScanRLEArrayIterator(const RLEArray * const source, AttributeID attId, std::string path, size_t maxChunkNo);
    bool haveChunk() const;
    ConstRLEChunk const& getRQChunk();

    bool end();
    void operator ++();
    bool setPosition(Coordinates const& pos);
    const Coordinates & getPosition();
    void reset();

    ~ScanRLEArrayIterator();

private:
    //RQArray *array;
    // only consts for now -- extend this to RQChunk later
    ConstRLEChunk* currChunk;
    log4cxx::LoggerPtr logger;

    AttributeID _attId;
    ConstRLEChunk _chunk;
    string _dirPath;
    size_t _chunkNo;
    size_t _maxChunkNo;
    ArrayDesc const* _desc;
    Coordinates _currPos;

    map<chunkId, chunkFileInfo> _chunkFileData;
    ifstream _chunkFile;
};

}

#endif
