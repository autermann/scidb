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
 * @file RemoteArray.cpp
 *
 * @author roman.simakov@gmail.com
 */

#include "log4cxx/logger.h"
#include "log4cxx/basicconfigurator.h"
#include "log4cxx/helpers/exception.h"

#include "boost/shared_ptr.hpp"
#include "boost/make_shared.hpp"

#include "query/RemoteArray.h"
#include "network/proto/scidb_msg.pb.h"
#include "network/NetworkManager.h"
#include "query/QueryProcessor.h"
#include "query/Statistics.h"

using namespace std;
using namespace boost;

namespace scidb
{

// to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.operator"));


boost::shared_ptr<RemoteArray> RemoteArray::create(const ArrayDesc& arrayDesc, QueryID queryId, InstanceID instanceID)
{
    boost::shared_ptr<Query> query = Query::getQueryByID(queryId);
    boost::shared_ptr<RemoteArray> array = boost::shared_ptr<RemoteArray>(new RemoteArray(arrayDesc, queryId, instanceID));
    query->setRemoteArray(instanceID, array);
    return array;
}

RemoteArray::RemoteArray(const ArrayDesc& arrayDesc, QueryID queryId, InstanceID instanceID)
: StreamArray(arrayDesc), _queryId(queryId), _instanceID(instanceID),
  _received(arrayDesc.getAttributes().size()),
  _messages(arrayDesc.getAttributes().size()),
  _requested(arrayDesc.getAttributes().size())
{
}

void RemoteArray::requestNextChunk(AttributeID attId)
{
    LOG4CXX_TRACE(logger, "RemoteArray fetches next chunk of " << attId << " attribute");
    boost::shared_ptr<MessageDesc> fetchDesc = boost::make_shared<MessageDesc>(mtFetch);
    boost::shared_ptr<scidb_msg::Fetch> fetchRecord = fetchDesc->getRecord<scidb_msg::Fetch>();
    fetchDesc->setQueryID(_queryId);
    fetchRecord->set_attribute_id(attId);
    fetchRecord->set_position_only(false);
    fetchRecord->set_obj_type(0);
    NetworkManager::getInstance()->send(_instanceID, fetchDesc);
}


void RemoteArray::handleChunkMsg(boost::shared_ptr<MessageDesc> chunkDesc)
{
    assert(chunkDesc->getMessageType() == mtRemoteChunk);
    boost::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();
    AttributeID attId = chunkMsg->attribute_id();
    _messages[attId] = chunkDesc;
    _received[attId].release();
}

bool RemoteArray::proceedChunkMsg(AttributeID attId, MemChunk& chunk)
{
    boost::shared_ptr<MessageDesc>  chunkDesc = _messages[attId];
    _messages[attId].reset();

    StatisticsScope sScope(_statistics);
    boost::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();
    currentStatistics->receivedSize += chunkDesc->getMessageSize();
    currentStatistics->receivedMessages++;

    if (!chunkMsg->eof())
    {
        LOG4CXX_TRACE(logger, "RemoteArray received next chunk message");
        assert(chunkDesc->getBinary());

        const int compMethod = chunkMsg->compression_method();
        const size_t decompressedSize = chunkMsg->decompressed_size();

        Address firstElem;
        firstElem.attId = attId;
        for (int i = 0; i < chunkMsg->coordinates_size(); i++) {
            firstElem.coords.push_back(chunkMsg->coordinates(i));
        }

        chunk.initialize(this, &desc, firstElem, compMethod);
        chunk.setSparse(chunkMsg->sparse());
        chunk.setCount(chunkMsg->count());

        boost::shared_ptr<CompressedBuffer> compressedBuffer = dynamic_pointer_cast<CompressedBuffer>(chunkDesc->getBinary());
        compressedBuffer->setCompressionMethod(compMethod);
        compressedBuffer->setDecompressedSize(decompressedSize);
        chunk.decompress(*compressedBuffer);
        LOG4CXX_TRACE(logger, "RemoteArray initializes next chunk");

        requestNextChunk(attId);
        return true;
    }
    else
    {
        return false;
    }
}


ConstChunk const* RemoteArray::nextChunk(AttributeID attId, MemChunk& chunk)
{
    if (!_requested[attId]) {
        requestNextChunk(attId);
    }
    boost::shared_ptr<Query> query = Query::getQueryByID(_queryId);
    Semaphore::ErrorChecker errorChecker = bind(&Query::validateQueryPtr, query);
    _received[attId].enter(errorChecker);
    _requested[attId] = true;
    return proceedChunkMsg(attId, chunk) ? &chunk : NULL;
}

/** R E M O T E   M E R G E D   A R R A Y */

RemoteMergedArray::RemoteMergedArray(const ArrayDesc& arrayDesc, QueryID queryId, Statistics& statistics):
        MultiStreamArray(Query::getQueryByID(queryId)->getInstancesCount(), arrayDesc),
        _queryId(queryId),
        _received(arrayDesc.getAttributes().size(), vector<Semaphore>(getStreamsCount())),
        _messages(arrayDesc.getAttributes().size(), vector< boost::shared_ptr<MessageDesc> >(getStreamsCount())),
        _nextPositions(arrayDesc.getAttributes().size(), vector<Coordinates>(getStreamsCount())),
        _hasPositions(arrayDesc.getAttributes().size(), vector<bool>(getStreamsCount(), false))
{
    boost::shared_ptr<Query> query = Query::getQueryByID(queryId);
    _localArray = query->getCurrentResultArray();
}

boost::shared_ptr<RemoteMergedArray> RemoteMergedArray::create(const ArrayDesc& arrayDesc, QueryID queryId, Statistics& statistics)
{
    boost::shared_ptr<Query> query = Query::getQueryByID(queryId);
    //assert(query->mergedArray == NULL);
    boost::shared_ptr<RemoteMergedArray> array = boost::shared_ptr<RemoteMergedArray>(new RemoteMergedArray(arrayDesc, queryId, statistics));
    query->setMergedArray(array);
    return array;
}

void RemoteMergedArray::handleChunkMsg(boost::shared_ptr< MessageDesc> chunkDesc)
{
    assert(chunkDesc->getMessageType() == mtRemoteChunk);
    boost::shared_ptr<Query> query = Query::getQueryByID(_queryId);
    boost::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();
    AttributeID attId = chunkMsg->attribute_id();
    size_t stream = size_t(query->mapPhysicalToLogical(chunkDesc->getSourceInstanceID()));

    if (chunkMsg->warnings_size())
    {
        for (int i = 0; i < chunkMsg->warnings_size(); ++i)
        {
            const ::scidb_msg::Chunk_Warning& w = chunkMsg->warnings(i);
            query->postWarning(
                        Warning(
                            w.file().c_str(),
                            w.function().c_str(),
                            w.line(),
                            w.strings_namespace().c_str(),
                            w.code(),
                            w.what_str().c_str(),
                            w.stringified_code().c_str())
                        );
        }
    }

    _messages[attId][stream] = chunkDesc;
    _received[attId][stream].release();
}

bool RemoteMergedArray::proceedChunkMsg(size_t stream, AttributeID attId, MemChunk* chunk)
{
    boost::shared_ptr<MessageDesc>  chunkDesc = _messages[attId][stream];
    _messages[attId][stream].reset();
    boost::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();

    StatisticsScope sScope(_statistics);
    currentStatistics->receivedSize += chunkDesc->getMessageSize();
    currentStatistics->receivedMessages++;

    _nextPositions[attId][stream].clear();

    if (!chunkMsg->eof())
    {
        LOG4CXX_TRACE(logger, "RemoteMergedArray received next chunk message");
        if (chunkDesc->getBinary() && chunk != NULL)
        {
            const int compMethod = chunkMsg->compression_method();
            const size_t decompressedSize = chunkMsg->decompressed_size();

            Address firstElem;
            firstElem.attId = attId;
            for (int i = 0; i < chunkMsg->coordinates_size(); i++) {
                firstElem.coords.push_back(chunkMsg->coordinates(i));
            }

            chunk->initialize(this, &desc, firstElem, compMethod);
            chunk->setSparse(chunkMsg->sparse());
            chunk->setRLE(chunkMsg->rle());
            chunk->setCount(chunkMsg->count());

            boost::shared_ptr<CompressedBuffer> compressedBuffer = dynamic_pointer_cast<CompressedBuffer>(chunkDesc->getBinary());
            compressedBuffer->setCompressionMethod(compMethod);
            compressedBuffer->setDecompressedSize(decompressedSize);
            chunk->decompress(*compressedBuffer);
            assert(checkChunkMagic(*chunk));
            LOG4CXX_TRACE(logger, "RemoteMergedArray initializes next chunk");
        }

        LOG4CXX_TRACE(logger, "RemoteMergedArray initializes next position");
        if (chunkMsg->has_next())
        {
            for (int i = 0; i < chunkMsg->next_coordinates_size(); i++) {
                _nextPositions[attId][stream].push_back(chunkMsg->next_coordinates(i));
            }
            requestNextChunk(stream, attId, false);
        }
        return true;
    }
    else
    {
        LOG4CXX_TRACE(logger, "RemoteMergedArray has no new chunks");
        return false;
    }
}

void RemoteMergedArray::requestNextChunk(size_t stream, AttributeID attId, bool positionOnly)
{
    LOG4CXX_TRACE(logger, "RemoteMergedArray fetches next chunk of " << attId << " attribute of the stream #" << stream);
    boost::shared_ptr<MessageDesc> fetchDesc = boost::make_shared<MessageDesc>(mtFetch);
    boost::shared_ptr<scidb_msg::Fetch> fetchRecord = fetchDesc->getRecord<scidb_msg::Fetch>();
    fetchDesc->setQueryID(_queryId);
    fetchRecord->set_attribute_id(attId);
    fetchRecord->set_position_only(positionOnly);
    fetchRecord->set_obj_type(1);
    NetworkManager::getInstance()->send(stream, fetchDesc);
}

bool RemoteMergedArray::fetchChunk(size_t stream, AttributeID attId, MemChunk* chunk)
{
   boost::shared_ptr<Query> query = Query::getQueryByID(_queryId);
   if (query->getInstanceID() != stream) {
       if (!_hasPositions[attId][stream]) {
           requestNextChunk(stream, attId, chunk == NULL);
       }
       Semaphore::ErrorChecker errorChecker = bind(&Query::validateQueryPtr, query);
       _received[attId][stream].enter(errorChecker);
       _hasPositions[attId][stream] = true;
       return proceedChunkMsg(stream, attId, chunk);
   } else {
       // We get chunk body from the current result array on local instance
       bool result = false;
       if (chunk) {
           if (!_localArray->getConstIterator(attId)->end()) {
               {
                   const ConstChunk& srcChunk = _localArray->getConstIterator(attId)->getChunk();
                   PinBuffer buf(srcChunk);
                   Address firstElem;
                   firstElem.attId = attId;
                   firstElem.coords = srcChunk.getFirstPosition(false);
                   chunk->initialize(this, &desc, firstElem, srcChunk.getCompressionMethod());
                   chunk->setSparse(srcChunk.isSparse());
                   chunk->setRLE(srcChunk.isRLE());
                   if (srcChunk.isRLE() && !srcChunk.getAttributeDesc().isEmptyIndicator() && getArrayDesc().getEmptyBitmapAttribute() != NULL && srcChunk.getBitmapSize() == 0) { 
                       srcChunk.makeClosure(*chunk, srcChunk.getEmptyBitmap());
                   } else { 
                       chunk->allocate(srcChunk.getSize());
                       assert(checkChunkMagic(srcChunk));
                       memcpy(chunk->getData(), srcChunk.getData(), srcChunk.getSize());
                   }
                   chunk->write(query);
               }
               ++(*_localArray->getConstIterator(attId));
               result = true;
           }
       }
       _hasPositions[attId][stream] = true;
       if (!_localArray->getConstIterator(attId)->end()) {
           _nextPositions[attId][stream] = _localArray->getConstIterator(attId)->getPosition();
           result = true;
       } else {
           _nextPositions[attId][stream].clear();
       }
       return result;
   }
}


ConstChunk const* RemoteMergedArray::nextChunkBody(size_t stream, AttributeID attId, MemChunk& chunk, Coordinates const& pos)
{
    return fetchChunk(stream, attId, &chunk) ? &chunk : NULL;
}

bool RemoteMergedArray::nextChunkPos(size_t stream, AttributeID attId, Coordinates& pos)
{
    if (!_hasPositions[attId][stream]) {
        // Requesting position data first time
        if (!fetchChunk(stream, attId, NULL)) {
            return false;
        }
    }
    if (_nextPositions[attId][stream].size() > 0) {
        pos = _nextPositions[attId][stream];
        return true;
    }
    return false;
}


} // namespace
