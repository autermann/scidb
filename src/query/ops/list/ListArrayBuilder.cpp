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
 * ListArrayBuilder.cpp
 *
 *  Created on: May 25, 2012
 *      Author: poliocough@gmail.com
 */

#include "ListArrayBuilder.h"

using namespace boost;

namespace scidb
{

template <typename T>
Dimensions ListArrayBuilder<T>::getDimensions(shared_ptr<Query> const& query) const
{
    shared_ptr<const InstanceLiveness> queryLiveness(query->getCoordinatorLiveness());
    size_t numInstances = queryLiveness->getNumInstances();

    Dimensions dims(LIST_NUM_DIMS);
    dims[0] = DimensionDesc("inst", 0, 0, numInstances-1, numInstances-1, 1, 0);
    dims[1] = DimensionDesc("n", 0, 0, MAX_COORDINATE, MAX_COORDINATE, LIST_CHUNK_SIZE, 0);
    return dims;
}

template <typename T>
ArrayDesc ListArrayBuilder<T>::getSchema(shared_ptr<Query> const& query) const
{
    return ArrayDesc("list", getAttributes(), getDimensions(query));
}

template <typename T>
void ListArrayBuilder<T>::initialize(shared_ptr<Query> const& query)
{
    _query = query;
    ArrayDesc schema = getSchema(_query);
    _nAttrs = schema.getAttributes().size() - 1;
    _array = shared_ptr<MemArray>(new MemArray(schema));
    Coordinate myInstance = query->getInstanceID();
    _currPos = Coordinates(LIST_NUM_DIMS,0);
    _currPos[0] = myInstance;
    _outAIters.reserve(_nAttrs);
    for(size_t i =0; i<_nAttrs; ++i)
    {
        _outAIters.push_back(_array->getIterator(i));
    }
    _outCIters.reserve(_nAttrs);
    for(AttributeID i=0; i<_nAttrs; ++i)
    {
        Chunk& ch = _outAIters[i]->newChunk(_currPos);
        _outCIters.push_back(ch.getIterator(_query, i == 0 ? ChunkIterator::SEQUENTIAL_WRITE :
                                                             ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK));
    }
    _nextChunkPos = _currPos;
    _nextChunkPos[1] += LIST_CHUNK_SIZE;
    _initialized = true;
}

template <typename T>
void ListArrayBuilder<T>::listElement(T const& element)
{
    assert(_initialized);
    if(_currPos[1]==_nextChunkPos[1])
    {
        for(AttributeID i=0; i<_nAttrs; ++i)
        {
            _outCIters[i]->flush();
            Chunk& ch = _outAIters[i]->newChunk(_currPos);
            _outCIters[i] = ch.getIterator(_query,i == 0 ? ChunkIterator::SEQUENTIAL_WRITE :
                                                           ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
        }
        _nextChunkPos[1] += LIST_CHUNK_SIZE;
    }
    for(AttributeID i=0; i<_nAttrs; ++i)
    {
        _outCIters[i]->setPosition(_currPos);
    }
    addToArray(element);
    ++_currPos[1];
}

template <typename T>
shared_ptr<MemArray> ListArrayBuilder<T>::getArray()
{
    assert(_initialized);
    for(AttributeID i=0; i<_nAttrs; ++i)
    {
        _outCIters[i]->flush();
    }
    return _array;
}

Attributes ListChunkDescriptorsArrayBuilder::getAttributes() const
{
    Attributes attrs(15);
    attrs[0] = AttributeDesc(0,  "instn", TID_UINT32,  0, 0);
    attrs[1] = AttributeDesc(1,  "dseg",  TID_UINT8,  0, 0);
    attrs[2] = AttributeDesc(2,  "dhdrp", TID_UINT64, 0, 0);
    attrs[3] = AttributeDesc(3,  "doffs", TID_UINT64, 0, 0);
    attrs[4] = AttributeDesc(4,  "arrid", TID_UINT64, 0, 0);
    attrs[5] = AttributeDesc(5,  "attid", TID_UINT64, 0, 0);
    attrs[6] = AttributeDesc(6,  "coord", TID_STRING, 0, 0);
    attrs[7] = AttributeDesc(7,  "comp",  TID_INT8,   0, 0);
    attrs[8] = AttributeDesc(8,  "flags", TID_UINT8,   0, 0);
    attrs[9] = AttributeDesc(9,  "nelem", TID_UINT32,  0, 0);
    attrs[10] = AttributeDesc(10,"csize", TID_UINT32, 0, 0);
    attrs[11] = AttributeDesc(11,"usize", TID_UINT32, 0, 0);
    attrs[12] = AttributeDesc(12,"asize", TID_UINT32,  0, 0);

    attrs[13] = AttributeDesc(13,"free",  TID_BOOL,   0, 0);

    attrs[14] = AttributeDesc(14,"empty_indicator", TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0);
    return attrs;
}

void ListChunkDescriptorsArrayBuilder::addToArray(pair<ChunkDescriptor, bool> const& value)
{
    Value v;
    ChunkDescriptor const& desc = value.first;

    v.setUint32(desc.hdr.instanceId);
    _outCIters[0]->writeItem(v);

    v.setUint8(desc.hdr.pos.segmentNo);
    _outCIters[1]->writeItem(v);

    v.setUint64(desc.hdr.pos.hdrPos);
    _outCIters[2]->writeItem(v);

    v.setUint64(desc.hdr.pos.offs);
    _outCIters[3]->writeItem(v);

    v.setUint64(desc.hdr.arrId);
    _outCIters[4]->writeItem(v);

    v.setUint64(desc.hdr.attId);
    _outCIters[5]->writeItem(v);

    Coordinates coords(desc.coords,  desc.coords + desc.hdr.nCoordinates);
    std::ostringstream str;
    str<<CoordsToStr(coords);
    v.setString(str.str().c_str());
    _outCIters[6]->writeItem(v);

    v.setInt8(desc.hdr.compressionMethod);
    _outCIters[7]->writeItem(v);

    v.setUint8(desc.hdr.flags);
    _outCIters[8]->writeItem(v);

    v.setUint32(desc.hdr.nElems);
    _outCIters[9]->writeItem(v);

    v.setUint32(desc.hdr.compressedSize);
    _outCIters[10]->writeItem(v);

    v.setUint32(desc.hdr.size);
    _outCIters[11]->writeItem(v);

    v.setUint32(desc.hdr.allocatedSize);
    _outCIters[12]->writeItem(v);

    v.setBool(value.second);
    _outCIters[13]->writeItem(v);
}

Attributes ListChunkMapArrayBuilder::getAttributes() const
{
    Attributes attrs(31);
    attrs[0]  = AttributeDesc(0,  "instn", TID_UINT32, 0, 0);
    attrs[1]  = AttributeDesc(1,  "dseg",  TID_UINT8,  0, 0);
    attrs[2]  = AttributeDesc(2,  "dhdrp", TID_UINT64, 0, 0);
    attrs[3]  = AttributeDesc(3,  "doffs", TID_UINT64, 0, 0);
    attrs[4]  = AttributeDesc(4,  "uaid", TID_UINT64, 0, 0);
    attrs[5]  = AttributeDesc(5,  "arrid", TID_UINT64, 0, 0);
    attrs[6]  = AttributeDesc(6,  "attid", TID_UINT64, 0, 0);
    attrs[7]  = AttributeDesc(7,  "coord", TID_STRING, 0, 0);
    attrs[8]  = AttributeDesc(8,  "comp",  TID_INT8,   0, 0);
    attrs[9]  = AttributeDesc(9,  "flags", TID_UINT8,  0, 0);
    attrs[10] = AttributeDesc(10,  "nelem", TID_UINT32, 0, 0);
    attrs[11] = AttributeDesc(11, "csize", TID_UINT32, 0, 0);
    attrs[12] = AttributeDesc(12, "usize", TID_UINT32, 0, 0);
    attrs[13] = AttributeDesc(13, "asize", TID_UINT32, 0, 0);

    attrs[14] = AttributeDesc(14, "addrs", TID_UINT64, 0, 0);
    attrs[15] = AttributeDesc(15, "clnof", TID_UINT64, 0, 0);
    attrs[16] = AttributeDesc(16, "clons", TID_STRING, 0, 0);
    attrs[17] = AttributeDesc(17, "next",  TID_UINT64, 0, 0);
    attrs[18] = AttributeDesc(18, "prev",  TID_UINT64, 0, 0);
    attrs[19] = AttributeDesc(19, "data",  TID_UINT64, 0, 0);

    attrs[20] = AttributeDesc(20, "accnt", TID_INT32,  0, 0);
    attrs[21] = AttributeDesc(21, "nwrit", TID_INT32,  0, 0);
    attrs[22] = AttributeDesc(22, "tstmp", TID_UINT64, 0, 0);
    attrs[23] = AttributeDesc(23, "raw",   TID_BOOL,   0, 0);
    attrs[24] = AttributeDesc(24, "waitn", TID_BOOL,   0, 0);

    attrs[25] = AttributeDesc(25, "lpos",  TID_STRING, 0, 0);
    attrs[26] = AttributeDesc(26, "fposo", TID_STRING, 0, 0);
    attrs[27] = AttributeDesc(27, "lposo", TID_STRING, 0, 0);

    attrs[28] = AttributeDesc(28, "strge", TID_UINT64, 0, 0);
    attrs[29] = AttributeDesc(29, "loadr", TID_UINT64, 0, 0);

    attrs[30] = AttributeDesc(30, "empty_indicator", TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0);

    return attrs;
}

void ListChunkMapArrayBuilder::addToArray(ChunkMapEntry const& value)
{
    Value v;
    DBChunk const* chunk = value._chunk;
    assert(chunk == NULL || value._uaid == chunk->getArrayDesc().getUAId());

    v.setUint32(chunk == NULL ? -1 : chunk->_hdr.instanceId);
    _outCIters[0]->writeItem(v);

    v.setUint8(chunk == NULL ? -1 : chunk->_hdr.pos.segmentNo);
    _outCIters[1]->writeItem(v);

    v.setUint64(chunk == 0 ? -1 : chunk->_hdr.pos.hdrPos);
    _outCIters[2]->writeItem(v);

    v.setUint64(chunk == 0 ? -1 : chunk->_hdr.pos.offs);
    _outCIters[3]->writeItem(v);

    v.setUint64(value._uaid);
    _outCIters[4]->writeItem(v);

    v.setUint64(value._addr.arrId);
    _outCIters[5]->writeItem(v);

    v.setUint64(value._addr.attId);
    _outCIters[6]->writeItem(v);

    std::ostringstream str;
    str<<CoordsToStr(value._addr.coords);
    v.setString(str.str().c_str());
    _outCIters[7]->writeItem(v);

    v.setInt8(chunk == 0 ? -1 : chunk->_hdr.compressionMethod);
    _outCIters[8]->writeItem(v);

    v.setUint8(chunk == 0 ? -1 : chunk->_hdr.flags);
    _outCIters[9]->writeItem(v);

    v.setUint32(chunk == 0 ? -1 : chunk->_hdr.nElems);
    _outCIters[10]->writeItem(v);

    v.setUint32(chunk == 0 ? -1 : chunk->_hdr.compressedSize);
    _outCIters[11]->writeItem(v);

    v.setUint32(chunk == 0 ? -1 : chunk->_hdr.size);
    _outCIters[12]->writeItem(v);

    v.setUint32(chunk == 0 ? -1 : chunk->_hdr.allocatedSize);
    _outCIters[13]->writeItem(v);

    v.setUint64((uint64_t)(chunk));
    _outCIters[14]->writeItem(v);

    v.setUint64(chunk == 0 ? -1 : (uint64_t)chunk->_cloneOf);
    _outCIters[15]->writeItem(v);

    str.str("");
    str<<"[";
    if ( chunk )
    {
        for(size_t i=0; i<chunk->_clones.size(); i++)
        {
            str<<(uint64_t) chunk->_clones[i]<<" ";
        }
    }
    str<<"]";
    v.setString(str.str().c_str());
    _outCIters[16]->writeItem(v);

    v.setUint64(chunk == 0 ? -1 : (uint64_t)chunk->_next);
    _outCIters[17]->writeItem(v);

    v.setUint64(chunk == 0 ? -1 : (uint64_t)chunk->_prev);
    _outCIters[18]->writeItem(v);

    v.setUint64(chunk == 0 ? -1 : (uint64_t)chunk->_data);
    _outCIters[19]->writeItem(v);

    v.setInt32(chunk == 0 ? -1 : chunk->_accessCount);
    _outCIters[20]->writeItem(v);

    v.setInt32(chunk == 0 ? -1 : chunk->_nWriters);
    _outCIters[21]->writeItem(v);

    v.setUint64(chunk == 0 ? -1 : chunk->_timestamp);
    _outCIters[22]->writeItem(v);

    v.setBool(chunk == 0 ? false : chunk->_raw);
    _outCIters[23]->writeItem(v);

    v.setBool(chunk == 0 ? false : chunk->_waiting);
    _outCIters[24]->writeItem(v);

    str.str("");
    if ( chunk )
    {
        str<<CoordsToStr(chunk->_lastPos);
    }
    v.setString(str.str().c_str());
    _outCIters[25]->writeItem(v);

    str.str("");
    if ( chunk )
    {
        str<< CoordsToStr(chunk->_firstPosWithOverlaps);
    }
    v.setString(str.str().c_str());
    _outCIters[26]->writeItem(v);

    str.str("");
    if (chunk)
    {
        str<< CoordsToStr(chunk->_lastPosWithOverlaps);
    }
    v.setString(str.str().c_str());
    _outCIters[27]->writeItem(v);

    v.setUint64(chunk == 0 ? -1 : (uint64_t)chunk->_storage);
    _outCIters[28]->writeItem(v);

    v.setUint64(chunk == 0 ? -1 : (uint64_t)chunk->_loader);
    _outCIters[29]->writeItem(v);
}

}
