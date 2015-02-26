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
 * MessageHandleJob.cpp
 *
 *  Created on: Jan 12, 2010
 *      Author: roman.simakov@gmail.com
 */

#include "log4cxx/logger.h"
#include <boost/make_shared.hpp>

#include "MessageHandleJob.h"
#include "system/Exceptions.h"
#include "query/QueryProcessor.h"
#include "network/NetworkManager.h"
#include "array/DBArray.h"
#include "network/MessageUtils.h"
#include "util/RWLock.h"
#include "query/Query.h"
#include "smgr/io/Storage.h"
#include "system/Resources.h"

using namespace std;
using namespace boost;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network"));

MessageHandleJob::MessageHandleJob(const boost::shared_ptr<MessageDesc>& messageDesc)
  : Job(boost::shared_ptr<Query>()), _messageDesc(messageDesc),
    networkManager(*NetworkManager::getInstance()), sourceId(INVALID_NODE), _mustValidateQuery(true)
{
   LOG4CXX_TRACE(logger, "Creating a new job for message of type=" << _messageDesc->getMessageType()
                 << " from node=" << _messageDesc->getSourceNodeID()
                 << " for queryID=" << _messageDesc->getQueryID());
   
    const QueryID queryID = _messageDesc->getQueryID();
    if (queryID != 0) {
       bool createIfNotFound = (_messageDesc->getMessageType() == mtPreparePhysicalPlan);
       _query = Query::getQueryByID(queryID,createIfNotFound);                              

    } else {
        LOG4CXX_TRACE(logger, "Creating fake query: type=" << _messageDesc->getMessageType()
                      << ", for message from node=" << _messageDesc->getSourceNodeID());
       // create a fake query for the recovery mode
       boost::shared_ptr<const scidb::NodeLiveness> myLiveness =
       Cluster::getInstance()->getNodeLiveness();
       assert(myLiveness);
       _query = Query::createDetached();
       _query->init(0, COORDINATOR_NODE,
                    Cluster::getInstance()->getLocalNodeId(),
                    myLiveness);
    }
}

MessageHandleJob::~MessageHandleJob()
{
}

void MessageHandleJob::handleInvalidMessage()
{
    const MessageType messageType = static_cast<MessageType>(_messageDesc->getMessageType());
    LOG4CXX_ERROR(logger, "Unknown/unexpected message type " << messageType);
    throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_UNKNOWN_MESSAGE_TYPE) << messageType;
}

MessageHandleJob::MsgHandler MessageHandleJob::_msgHandlers[scidb::mtSystemMax] = {
    // mtNone,
    &MessageHandleJob::handleInvalidMessage,
    // mtExecuteQuery,
    &MessageHandleJob::handleInvalidMessage,
    // mtPreparePhysicalPlan,
    &MessageHandleJob::handlePreparePhysicalPlan,
    // mtExecutePhysicalPlan,
    &MessageHandleJob::handleExecutePhysicalPlan,
    // mtFetch,
    &MessageHandleJob::handleFetchChunk,
    // mtChunk,
    &MessageHandleJob::handleChunk,
    // mtChunkReplica,
    &MessageHandleJob::handleChunkReplica,
    // mtRecoverChunk,
    &MessageHandleJob::handleChunkReplica,
    // mtReplicaSyncRequest,
    &MessageHandleJob::handleReplicaSyncRequest,
    // mtReplicaSyncResponse,
    &MessageHandleJob::handleReplicaSyncResponse,
    // mtAggregateChunk,
    &MessageHandleJob::handleAggregateChunk,
    // mtQueryResult,
    &MessageHandleJob::handleQueryResult,
    // mtError,
    &MessageHandleJob::handleError,
    // mtSyncRequest,
    &MessageHandleJob::handleSyncRequest,
    // mtSyncResponse,
    &MessageHandleJob::handleSyncResponse,
    // mtCancelQuery,
    &MessageHandleJob::handleInvalidMessage,
    // mtRemoteChunk,
    &MessageHandleJob::handleRemoteChunk,
    // mtNotify,
    &MessageHandleJob::handleNotify,
    // mtWait,
    &MessageHandleJob::handleWait,
    // mtBarrier,
    &MessageHandleJob::handleBarrier,
    // mtMPISend,
    &MessageHandleJob::handleMPISend,
    // mtAlive,
    &MessageHandleJob::handleInvalidMessage,
    // mtPrepareQuery,
    &MessageHandleJob::handleInvalidMessage,
    // mtResourcesFileExistsRequest,
    &MessageHandleJob::handleResourcesFileExists,
    // mtResourcesFileExistsResponse,
    &MessageHandleJob::handleResourcesFileExists,
    // mtAbort
    &MessageHandleJob::handleAbortQuery,
    // mtCommit
    &MessageHandleJob::handleCommitQuery,
    // mtSystemMax // must be last
};

void MessageHandleJob::dispatch(boost::shared_ptr<JobQueue>& jobQueue)
{
    assert(jobQueue);
    assert(_messageDesc->getMessageType() < mtSystemMax);

    const MessageType messageType = static_cast<MessageType>(_messageDesc->getMessageType());
    const QueryID queryID = _messageDesc->getQueryID();

    LOG4CXX_TRACE(logger, "Dispatching message of type=" << messageType
                  << ", for queryID=" << queryID
                  << ", from nodeID=" << _messageDesc->getSourceNodeID());

    switch (messageType)
    {
    case mtChunkReplica:
    {
        sourceId = _query->mapPhysicalToLogical(_messageDesc->getSourceNodeID());
        _query->chunkReplicasReqs[sourceId].increment();
    }
    break;
    case mtChunk:
    case mtAggregateChunk:
    {
        sourceId = _query->mapPhysicalToLogical(_messageDesc->getSourceNodeID());
        _query->chunkReqs[sourceId].increment();
    }
    break;
    case mtMPISend:
    {
        boost::shared_ptr<WorkQueue> q = _query->getMpiReceiveQueue();
        enqueue(q);
        return;
    }
    case mtRecoverChunk:
    case mtResourcesFileExistsRequest:
    case mtResourcesFileExistsResponse:
    {
        _mustValidateQuery = false;
    }
    break;
    case mtError:
    case mtAbort:
    case mtCommit:
    {
        _mustValidateQuery = false;
        boost::shared_ptr<WorkQueue> q = _query->getErrorQueue();
        enqueue(q);
        return;
    }
    default:
    break;
    };

    jobQueue->pushJob(shared_from_this());
    LOG4CXX_TRACE(logger, "Dispatched message of type=" << messageType
                  << ", for queryID=" << queryID
                  << ", from nodeID=" << _messageDesc->getSourceNodeID());
    return;
}

void MessageHandleJob::enqueue(boost::shared_ptr<WorkQueue>& q)
{
    if (!q)
    {
        LOG4CXX_TRACE(logger, "Dropping message of type=" <<  _messageDesc->getMessageType()
                      << ", for queryID=" << _messageDesc->getQueryID()
                      << ", from nodeID=" << _messageDesc->getSourceNodeID()
                      << " because the query appears deallocated");
        return;
    }
    WorkQueue::WorkItem item = bind(&MessageHandleJob::run, shared_from_this());
    try {
        q->enqueue(item);
    } catch (const WorkQueue::OverflowException& e) {
        LOG4CXX_ERROR(logger, "Overflow exception from the query error message queue: "<<e.what());
        // XXX TODO: deal with this exception
        assert(false);
    }
}

void MessageHandleJob::run()
{
    assert(_query);
    assert(_messageDesc);
    assert(_messageDesc->getMessageType() < mtSystemMax);
    const MessageType messageType = static_cast<MessageType>(_messageDesc->getMessageType());
    LOG4CXX_TRACE(logger, "Starting message handling: type=" << messageType
                  << ", queryID=" << _messageDesc->getQueryID());
    try
    {
        Query::setCurrentQueryID(_query->getQueryID());
        StatisticsScope sScope(&_query->statistics);

        if (_mustValidateQuery) {
            Query::validateQueryPtr(_query);
        }

        if (messageType < 0 || messageType >= mtSystemMax) {
            handleInvalidMessage();
            return;
        }

        MsgHandler handler = _msgHandlers[messageType];
        assert(handler);

        (this->*handler)();

        LOG4CXX_TRACE(logger, "Finishing message handling: type=" << messageType);
    }
    catch ( const Exception& e)
    {
       assert(_query);
       StatisticsScope sScope(_query ? &_query->statistics : NULL);

       assert(messageType != mtCancelQuery);

       if (messageType != mtError && messageType != mtAbort)
       {
          boost::shared_ptr<MessageDesc> errorMessage =
              makeErrorMessageFromException(e, _messageDesc->getQueryID());
          networkManager.broadcast(errorMessage); //_query may not have the node map, so broadcast to all
          LOG4CXX_DEBUG(logger, "Broadcasted error:" << e.what());
       }
       if (_query)
       {
          LOG4CXX_ERROR(logger, "Error occurred in message handler: "
                        << e.what()
                        << ", messageType = " << messageType
                        << ", queryID="<<_messageDesc->getQueryID());

          if (messageType == mtExecutePhysicalPlan) {
              _query->done(e.copy());
          } else {
              _query->handleError(e.copy());
          }
       }
       if (e.getShortErrorCode() == SCIDB_SE_THREAD)
       {
          throw;
       }
    }
}

void MessageHandleJob::handlePreparePhysicalPlan()
{
    boost::shared_ptr<scidb_msg::PhysicalPlan> ppMsg = _messageDesc->getRecord<scidb_msg::PhysicalPlan>();
    const string physicalPlan = ppMsg->physical_plan();

    LOG4CXX_DEBUG(logger, "Preparing physical plan: queryID="
                  << _messageDesc->getQueryID() << ", physicalPlan='" << physicalPlan << "'");

    boost::shared_ptr<NodeLiveness> coordinatorLiveness;
    bool rc = parseQueryLiveness(coordinatorLiveness, ppMsg);
    if (!rc) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_INVALID_LIVENESS);
    }
    currentStatistics->receivedSize += _messageDesc->getMessageSize();
    currentStatistics->receivedMessages++;

    boost::shared_ptr<const scidb::NodeLiveness> myLiveness =
        Cluster::getInstance()->getNodeLiveness();
    assert(myLiveness);

    if (myLiveness->isEqual(*coordinatorLiveness)) {
       _query->init(_messageDesc->getQueryID(),
                    _messageDesc->getSourceNodeID(),
                    Cluster::getInstance()->getLocalNodeId(),
                    myLiveness);
    } else {
       _query->init(_messageDesc->getQueryID(),
                    _messageDesc->getSourceNodeID(),
                    Cluster::getInstance()->getLocalNodeId(),
                    coordinatorLiveness);
       throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_LIVENESS_MISMATCH);
    }

    boost::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();

    queryProcessor->parsePhysical(physicalPlan, _query);
    LOG4CXX_DEBUG(logger, "Physical plan was parsed")

    boost::shared_ptr<MessageDesc> messageDesc = boost::make_shared<MessageDesc>(mtNotify);
    messageDesc->setQueryID(_messageDesc->getQueryID());
    networkManager.sendMessage(_messageDesc->getSourceNodeID(), messageDesc);
    LOG4CXX_DEBUG(logger, "Coordinator is notified about ready for physical plan running")
}

void MessageHandleJob::handleExecutePhysicalPlan()
{
   try {
      LOG4CXX_DEBUG(logger, "Running physical plan: queryID=" << _messageDesc->getQueryID())
      
      currentStatistics->receivedSize += _messageDesc->getMessageSize();
      currentStatistics->receivedMessages++;
      boost::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();
      _query->start();
      {
         queryProcessor->execute(_query);
         LOG4CXX_DEBUG(logger, "Query was executed");
      }

      _query->done();

      // Creating message with result for sending to client
      boost::shared_ptr<MessageDesc> resultMessage = boost::make_shared<MessageDesc>(mtQueryResult);
      resultMessage->setQueryID(_query->getQueryID());
      networkManager.sendMessage(_messageDesc->getSourceNodeID(), resultMessage);
      LOG4CXX_DEBUG(logger, "Result was sent to node #" << _messageDesc->getSourceNodeID());
   }
   catch (const scidb::Exception& e)
   {
      LOG4CXX_ERROR(logger, "Query ID=" << _query->getQueryID()
                    << " encountered the error: "
                    << e.what());
      e.raise();
   }
}

void MessageHandleJob::handleQueryResult()
{
    const string arrayName = _messageDesc->getRecord<scidb_msg::QueryResult>()->array_name();

    LOG4CXX_DEBUG(logger, "Received query result from node#" << _messageDesc->getSourceNodeID()
                  << ", queryID=" << _messageDesc->getQueryID() << ", arrayName=" << arrayName)

    currentStatistics->receivedSize += _messageDesc->getMessageSize();
    currentStatistics->receivedMessages++;

    // Signaling to query context to defreeze
    _query->results.release();
}

void MessageHandleJob::handleAggregateChunk()
{
    boost::shared_ptr<scidb_msg::Chunk> chunkRecord = _messageDesc->getRecord<scidb_msg::Chunk>();
    assert(!chunkRecord->eof());
    assert(_query);

    RWLock::ErrorChecker noopEc;
    ScopedRWLockRead shared(_query->queryLock, noopEc);

    try
    {
        LOG4CXX_TRACE(logger, "Next chunk message was received")

        const int compMethod = chunkRecord->compression_method();
        const size_t decompressedSize = chunkRecord->decompressed_size();
        const AttributeID attributeID = chunkRecord->attribute_id();
        const size_t count = chunkRecord->count();

        boost::shared_ptr<Array> outputArray = _query->getSGContext()->_resultSG;
        AggregatePtr aggregate = _query->getSGContext()->_aggregateList[attributeID];

        ScopedMutexLock cs(_query->resultCS);
        boost::shared_ptr<ArrayIterator> outputIter = outputArray->getIterator(attributeID);

        Coordinates coordinates;
        for (int i = 0; i < chunkRecord->coordinates_size(); i++)
        {
            coordinates.push_back(chunkRecord->coordinates(i));
        }

        boost::shared_ptr<CompressedBuffer> compressedBuffer = dynamic_pointer_cast<CompressedBuffer>(_messageDesc->getBinary());
        compressedBuffer->setCompressionMethod(compMethod);
        compressedBuffer->setDecompressedSize(decompressedSize);
        Chunk* outChunk;
        if (outputIter->setPosition(coordinates))
        {
            outChunk = &outputIter->updateChunk();
            bool isSparse = outChunk->isSparse() || chunkRecord->sparse();

            MemChunk tmpChunk;
            Address chunkAddr;
            ArrayDesc const& desc = outputArray->getArrayDesc();
            chunkAddr.coords = coordinates;
            chunkAddr.arrId = desc.getId();
            chunkAddr.attId = attributeID;
            tmpChunk.initialize(outputArray.get(), &desc, chunkAddr, compMethod);
            tmpChunk.setSparse(isSparse);
            tmpChunk.setRLE(chunkRecord->rle());
            tmpChunk.decompress(*compressedBuffer);

            outChunk->aggregateMerge(tmpChunk, aggregate, _query);
        }
        else
        {
            outChunk = &outputIter->newChunk(coordinates, compMethod);
            outChunk->setSparse(chunkRecord->sparse());
            outChunk->setRLE(chunkRecord->rle());
            outChunk->decompress(*compressedBuffer); // TODO: it's better avoid decompression. It can be written compressed
            outChunk->setCount(count);
            outChunk->write(_query);
        }
        sgSync();
        LOG4CXX_TRACE(logger, "Chunk was stored")
    }
    catch(const Exception& e)
    {
        sgSync();
        throw;
    }
}

void MessageHandleJob::sgSync()
{
    sourceId = _query->mapPhysicalToLogical(_messageDesc->getSourceNodeID());
    if (_query->chunkReqs[sourceId].decrement()) {
        boost::shared_ptr<MessageDesc> syncMsg = boost::make_shared<MessageDesc>(mtSyncResponse);
        syncMsg->setQueryID(_messageDesc->getQueryID());
        networkManager.sendMessage(_messageDesc->getSourceNodeID(), syncMsg);
        LOG4CXX_TRACE(logger, "Sync confirmation was sent to node #" << _messageDesc->getSourceNodeID());
    }
}

void MessageHandleJob::handleChunk()
{
    boost::shared_ptr<scidb_msg::Chunk> chunkRecord = _messageDesc->getRecord<scidb_msg::Chunk>();
    assert(!chunkRecord->eof());
    assert(_query);
    RWLock::ErrorChecker noopEc;
    ScopedRWLockRead shared(_query->queryLock, noopEc);
    try
    {
        // TODO: Apply it to statistics of current SG
        currentStatistics->receivedSize += _messageDesc->getMessageSize();
        currentStatistics->receivedMessages++;
        LOG4CXX_TRACE(logger, "Next chunk message was received")
        const int compMethod = chunkRecord->compression_method();
        const size_t decompressedSize = chunkRecord->decompressed_size();
        const AttributeID attributeID = chunkRecord->attribute_id();
        const size_t count = chunkRecord->count();

        boost::shared_ptr<Array> outputArray = _query->getSGContext()->_resultSG;
        ScopedMutexLock cs(_query->resultCS);
        boost::shared_ptr<ArrayIterator> outputIter = outputArray->getIterator(attributeID);

        Coordinates coordinates;
        for (int i = 0; i < chunkRecord->coordinates_size(); i++) {
            coordinates.push_back(chunkRecord->coordinates(i));
        }

        boost::shared_ptr<CompressedBuffer> compressedBuffer = dynamic_pointer_cast<CompressedBuffer>(_messageDesc->getBinary());
        compressedBuffer->setCompressionMethod(compMethod);
        compressedBuffer->setDecompressedSize(decompressedSize);
        Chunk* outChunk;
        if (outputIter->setPosition(coordinates)) { // merge with existed chunk
            outChunk = &outputIter->updateChunk();
            if (outChunk->getDiskChunk() != NULL)
                throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CANT_UPDATE_CHUNK);
            outChunk->setCount(0); // unknown
            AttributeDesc const& attr = outChunk->getAttributeDesc();
            bool isSparse = outChunk->isSparse() || chunkRecord->sparse();
            bool isRLE = outChunk->isRLE() || chunkRecord->rle();
            bool isVarying = TypeLibrary::getType(attr.getType()).variableSize();
            char* dst = (char*)outChunk->getData();
            Value const& defaultValue = attr.getDefaultValue();
            if ((dst == NULL || (!isSparse && !isRLE && !isVarying && defaultValue.isZero() && !attr.isNullable())) && compMethod == 0) { // chunk is not compressed
                char const* src = (char const*)compressedBuffer->getData();
                if (dst == NULL) {
                    outChunk->allocate(decompressedSize);
                    outChunk->setSparse(chunkRecord->sparse());
                    outChunk->setRLE(chunkRecord->rle());
                    outChunk->setCount(count);
                    memcpy(outChunk->getData(), src, decompressedSize);
                } else {
                    if (decompressedSize != outChunk->getSize())
                        throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CANT_MERGE_CHUNKS_WITH_VARYING_SIZE);
                    for (size_t j = 0; j < decompressedSize; j++) {
                        dst[j] |= src[j];
                    }
                }
                outChunk->write(_query);
            } else {
                MemChunk tmpChunk;
                Address chunkAddr;
                ArrayDesc const& desc = outputArray->getArrayDesc();
                chunkAddr.coords = coordinates;
                chunkAddr.arrId = desc.getId();
                chunkAddr.attId = attributeID;
                tmpChunk.initialize(outputArray.get(), &desc, chunkAddr, compMethod);
                tmpChunk.setSparse(isSparse);
                tmpChunk.setRLE(chunkRecord->rle());
                tmpChunk.decompress(*compressedBuffer);

                char* dst = (char*)outChunk->getData();
                if (dst != NULL && (isSparse || isRLE || isVarying || !defaultValue.isZero() || attr.isNullable())) {
                    boost::shared_ptr<ChunkIterator> dstIterator = outChunk->getIterator(_query, (outChunk->isSparse() ? ChunkIterator::SPARSE_CHUNK : 0)
                                                                                        | ChunkIterator::APPEND_CHUNK | ChunkIterator::NO_EMPTY_CHECK);
                    boost::shared_ptr<ConstChunkIterator> srcIterator = tmpChunk.getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS
                                                                                                  |ChunkIterator::NO_EMPTY_CHECK
                                                                                                  |ChunkIterator::IGNORE_DEFAULT_VALUES);
                    if (desc.getEmptyBitmapAttribute() != NULL) {
                        while (!srcIterator->end()) {
                            if (!dstIterator->setPosition(srcIterator->getPosition()))
                                throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                            Value const& value = srcIterator->getItem();
                            dstIterator->writeItem(value);
                            ++(*srcIterator);
                        }
                    } else {
                        while (!srcIterator->end()) {
                            Value const& value = srcIterator->getItem();
                            if (value != defaultValue) {
                                if (!dstIterator->setPosition(srcIterator->getPosition()))
                                    throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                                dstIterator->writeItem(value);
                            }
                            ++(*srcIterator);
                        }
                    } 
                    dstIterator->flush();
                } else {
                    char const* src = (char const*)tmpChunk.getData();
                    if (dst == NULL) {
                        outChunk->allocate(decompressedSize);
                        outChunk->setSparse(isSparse);
                        outChunk->setRLE(chunkRecord->rle());
                        outChunk->setCount(count);
                        memcpy(outChunk->getData(), src, decompressedSize);
                    } else {
                        if (decompressedSize != outChunk->getSize())
                            throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CANT_MERGE_CHUNKS_WITH_VARYING_SIZE);
                        for (size_t j = 0; j < decompressedSize; j++) {
                            dst[j] |= src[j];
                        }
                    }
                    outChunk->write(_query);
                }
            }
        } else { // new chunk
            outChunk = &outputIter->newChunk(coordinates, compMethod);
            outChunk->setSparse(chunkRecord->sparse());
            outChunk->setRLE(chunkRecord->rle());
            outChunk->decompress(*compressedBuffer); // TODO: it's better avoid decompression. It can be written compressed
            outChunk->setCount(count);
            outChunk->write(_query);
        }
        Coordinates const& first = outChunk->getFirstPosition(false);
        Coordinates const& last = outChunk->getLastPosition(false);
        for (size_t i = 0, n = _query->lowBoundary.size(); i < n; i++) {
            if (first[i] < _query->lowBoundary[i]) {
                _query->lowBoundary[i] = first[i];
            }
            if (last[i] > _query->highBoundary[i]) {
                _query->highBoundary[i] = last[i];
            }
        }
        sgSync();

        LOG4CXX_TRACE(logger, "Chunk was stored")
    }
    catch(const Exception& e)
    {
        sgSync();
        throw;
    }

}

void MessageHandleJob::handleReplicaSyncRequest()
{
    sourceId = _query->mapPhysicalToLogical(_messageDesc->getSourceNodeID());
    if (_query->chunkReplicasReqs[sourceId].test()) {
        boost::shared_ptr<MessageDesc> resultMessage = boost::make_shared<MessageDesc>(mtReplicaSyncResponse);
        resultMessage->setQueryID(_query->getQueryID());
        networkManager.sendMessage(_messageDesc->getSourceNodeID(), resultMessage);
    }
}

void MessageHandleJob::handleReplicaSyncResponse()
{
    _query->replicaSem.release();
}

void MessageHandleJob::handleChunkReplica()
{
    boost::shared_ptr<scidb_msg::Chunk> chunkRecord = _messageDesc->getRecord<scidb_msg::Chunk>();

    const int compMethod = chunkRecord->compression_method();
    const size_t decompressedSize = chunkRecord->decompressed_size();
    const AttributeID attributeID = chunkRecord->attribute_id();
    const size_t count = chunkRecord->count();

    assert(_query);
    RWLock::ErrorChecker noopEc;
    ScopedRWLockRead shared(_query->queryLock, noopEc);
    DBArray outputArray(chunkRecord->array_id(), _query);
    boost::shared_ptr<ArrayIterator> outputIter = outputArray.getIterator(attributeID);

    Coordinates coordinates;
    for (int i = 0; i < chunkRecord->coordinates_size(); i++) {
        coordinates.push_back(chunkRecord->coordinates(i));
    }
    boost::shared_ptr<CompressedBuffer> compressedBuffer = dynamic_pointer_cast<CompressedBuffer>(_messageDesc->getBinary());
    compressedBuffer->setCompressionMethod(compMethod);
    compressedBuffer->setDecompressedSize(decompressedSize);
    Chunk& outChunk = outputIter->newChunk(coordinates, compMethod);
    outChunk.setSparse(chunkRecord->sparse());
    outChunk.setRLE(chunkRecord->rle());
    outChunk.decompress(*compressedBuffer); // TODO: it's better avoid decompression. It can be written compressed
    outChunk.setCount(count);
    outChunk.write(_query);

    if (static_cast<MessageType>(_messageDesc->getMessageType()) != mtChunkReplica) {
       return;
    }
    assert(sourceId != INVALID_NODE);
    if (_query->chunkReplicasReqs[sourceId].decrement()) {
       boost::shared_ptr<MessageDesc> resultMessage = boost::make_shared<MessageDesc>(mtReplicaSyncResponse);
       resultMessage->setQueryID(_query->getQueryID());
       networkManager.sendMessage(_messageDesc->getSourceNodeID(), resultMessage);
    }
}

void MessageHandleJob::handleRemoteChunk()
{
    boost::shared_ptr<scidb_msg::Chunk> chunkRecord = _messageDesc->getRecord<scidb_msg::Chunk>();
    const uint32_t objType = chunkRecord->obj_type();
    assert(_query);

    sourceId = _query->mapPhysicalToLogical(_messageDesc->getSourceNodeID());

    // Network statistics will be updated in remote or merge arrays
    switch(objType)
    {
    case 0:
    {
        boost::shared_ptr<RemoteArray> ra = _query->getRemoteArray(sourceId);
        assert(ra);
        ra->handleChunkMsg(_messageDesc);
    }
       break;
    case 1:
    {
        boost::shared_ptr<RemoteMergedArray> rma = _query->getMergedArray();
        assert(rma);
        rma->handleChunkMsg(_messageDesc);
    }
       break;
    default:
        assert(false);
    }
}


void MessageHandleJob::handleFetchChunk()
{
    boost::shared_ptr<scidb_msg::Fetch> fetchRecord = _messageDesc->getRecord<scidb_msg::Fetch>();
    const QueryID queryID = _messageDesc->getQueryID();

    currentStatistics->receivedSize += _messageDesc->getMessageSize();
    currentStatistics->receivedMessages++;
    const uint32_t attributeId = fetchRecord->attribute_id();
    const bool positionOnly = fetchRecord->position_only();
    const uint32_t objType = fetchRecord->obj_type();
    LOG4CXX_TRACE(logger, "Fetching remote chunk of " << attributeId << " attribute in context of " << queryID << " query")

    boost::shared_ptr<Array> resultArray = _query->getCurrentResultArray();
    assert(resultArray); //XXX TODO: this needs to be an exception
    boost::shared_ptr<ConstArrayIterator> iter = resultArray->getConstIterator(attributeId);

    boost::shared_ptr<MessageDesc> chunkMsg;

    if (!iter->end())
    {
        boost::shared_ptr<scidb_msg::Chunk> chunkRecord;
        if (!positionOnly) {
            const ConstChunk* chunk = &iter->getChunk();
            boost::shared_ptr<CompressedBuffer> buffer = boost::make_shared<CompressedBuffer>();
            boost::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
            if (chunk->isRLE() && resultArray->getArrayDesc().getEmptyBitmapAttribute() != NULL && !chunk->getAttributeDesc().isEmptyIndicator()) { 
                emptyBitmap = chunk->getEmptyBitmap();
            }
            chunk->compress(*buffer, emptyBitmap);
            chunkMsg = boost::make_shared<MessageDesc>(mtRemoteChunk, buffer);
            chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
            chunkRecord->set_sparse(chunk->isSparse());
            chunkRecord->set_rle(chunk->isRLE());
            chunkRecord->set_compression_method(buffer->getCompressionMethod());
            chunkRecord->set_decompressed_size(buffer->getDecompressedSize());
            chunkRecord->set_count(chunk->isCountKnown() ? chunk->count() : 0);
            const Coordinates& coordinates = chunk->getFirstPosition(false);
            for (size_t i = 0; i < coordinates.size(); i++) {
                chunkRecord->add_coordinates(coordinates[i]);
            }
            ++(*iter);
        }
        else {
            chunkMsg = boost::make_shared<MessageDesc>(mtRemoteChunk);
            chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
        }
        chunkMsg->setQueryID(queryID);
        chunkRecord->set_eof(false);
        chunkRecord->set_obj_type(objType);
        chunkRecord->set_attribute_id(attributeId);
        if (!iter->end() || positionOnly)
        {
            chunkRecord->set_has_next(true);
            const Coordinates& next_coordinates = iter->getPosition();
            for (size_t i = 0; i < next_coordinates.size(); i++) {
                chunkRecord->add_next_coordinates(next_coordinates[i]);
            }
        }
        else
        {
            chunkRecord->set_has_next(false);
        }

        shared_ptr<Query> query = Query::getQueryByID(queryID);
        if (query->getWarnings().size())
        {
            //Propagate warnings gathered on coordinator to client
            vector<Warning> v = query->getWarnings();
            for (vector<Warning>::const_iterator it = v.begin(); it != v.end(); ++it)
            {
                ::scidb_msg::Chunk_Warning* warn = chunkRecord->add_warnings();
                warn->set_code(it->getCode());
                warn->set_file(it->getFile());
                warn->set_function(it->getFunction());
                warn->set_line(it->getLine());
                warn->set_what_str(it->msg());
                warn->set_strings_namespace(it->getStringsNamespace());
                warn->set_stringified_code(it->getStringifiedCode());
            }
            query->clearWarnings();
        }

        LOG4CXX_TRACE(logger, "Prepared message with chunk data")
    }
    else
    {
        chunkMsg = boost::make_shared<MessageDesc>(mtRemoteChunk);
        boost::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
        chunkMsg->setQueryID(queryID);
        chunkRecord->set_eof(true);
        chunkRecord->set_obj_type(objType);
        chunkRecord->set_attribute_id(attributeId);
        LOG4CXX_TRACE(logger, "Prepared message with information that there are no unread chunks")
    }

    networkManager.sendMessage(_messageDesc->getSourceNodeID(), chunkMsg);

    LOG4CXX_TRACE(logger, "Remote chunk was sent to client")
}


void MessageHandleJob::handleSyncRequest()
{
    currentStatistics->receivedSize += _messageDesc->getMessageSize();
    currentStatistics->receivedMessages++;

    sourceId = _query->mapPhysicalToLogical(_messageDesc->getSourceNodeID());
    
    if (_query->chunkReqs[sourceId].test()) {
        boost::shared_ptr<MessageDesc> syncMsg = boost::make_shared<MessageDesc>(mtSyncResponse);
        syncMsg->setQueryID(_messageDesc->getQueryID());
        networkManager.sendMessage(_messageDesc->getSourceNodeID(), syncMsg);
        LOG4CXX_TRACE(logger, "Sync confirmation was sent to node #" << _messageDesc->getSourceNodeID());
    }
}


void MessageHandleJob::handleBarrier()
{
    boost::shared_ptr<scidb_msg::DummyQuery> barrierRecord = _messageDesc->getRecord<scidb_msg::DummyQuery>();

    currentStatistics->receivedSize += _messageDesc->getMessageSize();
    currentStatistics->receivedMessages++;
    LOG4CXX_TRACE(logger, "handling barrier message in query " << _messageDesc->getQueryID())
    _query->semSG[barrierRecord->barrier_id()].release();
}


void MessageHandleJob::handleSyncResponse()
{
    currentStatistics->receivedSize += _messageDesc->getMessageSize();
    currentStatistics->receivedMessages++;
    LOG4CXX_TRACE(logger, "Receiving confirmation for sync message and release syncSG in query" << _messageDesc->getQueryID())

    // Signaling to query to release SG semaphore inside physical operator and continue to work
    _query->syncSG.release();
}


void MessageHandleJob::handleError()
{
    boost::shared_ptr<scidb_msg::Error> errorRecord = _messageDesc->getRecord<scidb_msg::Error>();
    const string errorText = errorRecord->what_str();
    const int32_t errorCode = errorRecord->long_error_code();
    currentStatistics->receivedSize += _messageDesc->getMessageSize();
    currentStatistics->receivedMessages++;

    LOG4CXX_ERROR(logger, "Error on processing query " << _messageDesc->getQueryID()
                  << " on node " << _messageDesc->getSourceNodeID()
                  << ". Message: " << errorText);

    assert(_query->getQueryID() == _messageDesc->getQueryID());

    shared_ptr<Exception> e = makeExceptionFromErrorMessage(_messageDesc);
    bool isAbort = false; 
    if (errorCode == SCIDB_LE_QUERY_NOT_FOUND || errorCode == SCIDB_LE_QUERY_NOT_FOUND2)
    {
        if (_query->getPhysicalCoordinatorID() == _messageDesc->getSourceNodeID()) {
            // The coordinator does not know about this query, we will also abort the query
            isAbort = true;
        }
        else
        {
            // A remote node did not find the query, it must be out of sync (because of restart?).
            e = SYSTEM_EXCEPTION_SPTR(SCIDB_SE_NETWORK, SCIDB_LE_NO_QUORUM);
        }
    }
    _query->handleError(e);
    if (isAbort) {
        handleAbortQuery();
    }
}

void MessageHandleJob::handleAbortQuery()
{
    currentStatistics->receivedSize += _messageDesc->getMessageSize();
    currentStatistics->receivedMessages++;

    if (_query->getPhysicalCoordinatorID() != _messageDesc->getSourceNodeID()
        || _query->getCoordinatorID() == COORDINATOR_NODE) {
        shared_ptr<Exception> e = (SYSTEM_EXCEPTION_SPTR(SCIDB_SE_NETWORK,
                                                         SCIDB_LE_UNKNOWN_MESSAGE_TYPE)
                                   << mtAbort);
        e->raise();
    }
    _query->handleAbort();
}

void MessageHandleJob::handleCommitQuery()
{
    currentStatistics->receivedSize += _messageDesc->getMessageSize();
    currentStatistics->receivedMessages++;

    if (_query->getPhysicalCoordinatorID() != _messageDesc->getSourceNodeID()
        || _query->getCoordinatorID() == COORDINATOR_NODE) {
        shared_ptr<Exception> e = (SYSTEM_EXCEPTION_SPTR(SCIDB_SE_NETWORK,
                                                         SCIDB_LE_UNKNOWN_MESSAGE_TYPE)
                                   << mtCommit);
        e->raise();
    }
    _query->handleCommit();
}

void MessageHandleJob::handleNotify()
{
    currentStatistics->receivedSize += _messageDesc->getMessageSize();
    currentStatistics->receivedMessages++;
    LOG4CXX_DEBUG(logger, "Notify on processing query "
                  << _messageDesc->getQueryID() << " from node "
                  << _messageDesc->getSourceNodeID())

    assert(_query->getCoordinatorID() == COORDINATOR_NODE);

    _query->results.release();
}


void MessageHandleJob::handleWait()
{
    currentStatistics->receivedSize += _messageDesc->getMessageSize();
    currentStatistics->receivedMessages++;
    LOG4CXX_DEBUG(logger, "Wait on processing query " << _messageDesc->getQueryID())

    assert(_query->getCoordinatorID() != COORDINATOR_NODE);

    _query->results.release();
}


void MessageHandleJob::handleMPISend()
{
    boost::shared_ptr<scidb_msg::DummyQuery> msgRecord = _messageDesc->getRecord<scidb_msg::DummyQuery>();
    currentStatistics->receivedSize += _messageDesc->getMessageSize();
    currentStatistics->receivedMessages++;
    assert(_query);
    sourceId = _query->mapPhysicalToLogical(_messageDesc->getSourceNodeID());
    {
        ScopedMutexLock mutexLock(_query->_receiveMutex);
        _query->_receiveMessages[sourceId].push_back(_messageDesc);
    }
    _query->_receiveSemaphores[sourceId].release();
}

void MessageHandleJob::handleResourcesFileExists()
{
    LOG4CXX_TRACE(logger, "MessageHandleJob::handleResourcesFileExists");
    Resources::getInstance()->handleFileExists(_messageDesc);
}

} // namespace
