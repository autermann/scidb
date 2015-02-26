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
#include "boost/make_shared.hpp"
#include <time.h>

#include "ClientMessageHandleJob.h"
#include "system/Exceptions.h"
#include "query/QueryProcessor.h"
#include "network/NetworkManager.h"
#include "network/MessageUtils.h"
#include "query/parser/Serialize.h"
#include "array/Metadata.h"
#include "query/executor/SciDBExecutor.h"


using namespace std;
using namespace boost;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network"));

ClientMessageHandleJob::ClientMessageHandleJob(boost::shared_ptr< Connection > connection,
        const boost::shared_ptr<MessageDesc>& messageDesc)
    : Job(boost::shared_ptr<Query>()), _connection(connection), _messageDesc(messageDesc)
{
}


void ClientMessageHandleJob::run()
{
   assert(_messageDesc->getMessageType() < mtSystemMax);
   MessageType messageType = static_cast<MessageType>(_messageDesc->getMessageType());
   LOG4CXX_TRACE(logger, "Starting client message handling: type=" << messageType)

   CurrentQueryScope queryScope(_messageDesc->getQueryID());

    switch (messageType)
    {
    case mtPrepareQuery:
        prepareClientQuery();
        break;
    case mtExecuteQuery:
        executeClientQuery();
        break;
    case mtFetch:
        fetchChunk();
        break;
    case mtCompleteQuery:
        completeQuery();
        break;
    case mtCancelQuery:
        cancelQuery();
        break;
    default:
        LOG4CXX_ERROR(logger, "Unknown message type " << messageType);
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_UNKNOWN_MESSAGE_TYPE) << messageType;
    }
    LOG4CXX_TRACE(logger, "Finishing client message handling: type=" << messageType)
}

string ClientMessageHandleJob::getProgramOptions(std::string const& programOptions) const
{
    stringstream ip;
    boost::system::error_code ec;
    boost::asio::ip::tcp::endpoint endpoint = _connection->getSocket().remote_endpoint(ec);
    if (!ec) {
        ip << endpoint.address().to_string() << ":" << endpoint.port();
    }
    ip << programOptions;
    return ip.str();
}


void ClientMessageHandleJob::prepareClientQuery()
{
    const scidb::SciDB& scidb = getSciDBExecutor();
    scidb::QueryResult queryResult;
    try
    {
        // Getting needed parameters for execution
        const string queryString = _messageDesc->getRecord<scidb_msg::Query>()->query();
        bool afl = _messageDesc->getRecord<scidb_msg::Query>()->afl();
        string programOptions = _messageDesc->getRecord<scidb_msg::Query>()->program_options();

        queryResult.queryID = Query::generateID();
        assert(queryResult.queryID > 0);
        _connection->attachQuery(queryResult.queryID);

        scidb.prepareQuery(queryString, afl, getProgramOptions(programOptions), queryResult);

        // Creating message with result for sending to client
        boost::shared_ptr<MessageDesc> resultMessage = boost::make_shared<MessageDesc>(mtQueryResult);
        boost::shared_ptr<scidb_msg::QueryResult> queryResultRecord = resultMessage->getRecord<scidb_msg::QueryResult>();
        resultMessage->setQueryID(queryResult.queryID);
        queryResultRecord->set_explain_logical(queryResult.explainLogical);
        queryResultRecord->set_selective(queryResult.selective);
        queryResultRecord->set_exclusive_array_access(queryResult.requiresExclusiveArrayAccess);

        vector<Warning> v = Query::getQueryByID(queryResult.queryID)->getWarnings();
        for (vector<Warning>::const_iterator it = v.begin(); it != v.end(); ++it)
        {
            ::scidb_msg::QueryResult_Warning* warn = queryResultRecord->add_warnings();

            cout << "Propagate warning during prepare" << endl;
            warn->set_code(it->getCode());
            warn->set_file(it->getFile());
            warn->set_function(it->getFunction());
            warn->set_line(it->getLine());
            warn->set_what_str(it->msg());
            warn->set_strings_namespace(it->getStringsNamespace());
            warn->set_stringified_code(it->getStringifiedCode());
        }
        Query::getQueryByID(queryResult.queryID)->clearWarnings();

        for (vector<string>::const_iterator it = queryResult.plugins.begin();
            it != queryResult.plugins.end(); ++it)
        {
            queryResultRecord->add_plugins(*it);
        }

        StatisticsScope sScope;
        _connection->sendMessage(resultMessage);
        LOG4CXX_DEBUG(logger, "The result preparation of query is sent to the client")
    }
    catch (const Exception& e)
    {
        LOG4CXX_ERROR(logger, "prepareClientQuery failed to complete: " << e.what())
        StatisticsScope sScope;
        handleExecuteOrPrepareError(e, queryResult, scidb);
    }
}

void ClientMessageHandleJob::handleExecuteOrPrepareError(const Exception& err,
                                                         const scidb::QueryResult& queryResult,
                                                         const scidb::SciDB& scidb)
{
    _connection->sendMessage(makeErrorMessageFromException(err));
    if (queryResult.queryID==0) {
        return;
    }
    try {
        scidb.cancelQuery(queryResult.queryID);
    } catch (const scidb::SystemException& e) {
        if (e.getLongErrorCode() != SCIDB_LE_QUERY_NOT_FOUND
            && e.getLongErrorCode() != SCIDB_LE_QUERY_NOT_FOUND2) {
            throw;
        }
    }
    assert(_connection);
    _connection->detachQuery(queryResult.queryID);
}

void ClientMessageHandleJob::executeClientQuery()
{
    const scidb::SciDB& scidb = getSciDBExecutor();
    scidb::QueryResult queryResult;
    try
    {
        // Getting needed parameters for execution
        const string queryString = _messageDesc->getRecord<scidb_msg::Query>()->query();
        bool afl = _messageDesc->getRecord<scidb_msg::Query>()->afl();
        const string programOptions = _messageDesc->getRecord<scidb_msg::Query>()->program_options();

        queryResult.queryID = _messageDesc->getQueryID();

        if (queryResult.queryID <= 0) {
            queryResult.queryID = Query::generateID();
            assert(queryResult.queryID > 0);
            _connection->attachQuery(queryResult.queryID);
            scidb.prepareQuery(queryString, afl, getProgramOptions(programOptions), queryResult);
        }

        scidb.executeQuery(queryString, afl, queryResult);

        // Creating message with result for sending to client
        boost::shared_ptr<MessageDesc> resultMessage = boost::make_shared<MessageDesc>(mtQueryResult);
        boost::shared_ptr<scidb_msg::QueryResult> queryResultRecord = resultMessage->getRecord<scidb_msg::QueryResult>();
        resultMessage->setQueryID(queryResult.queryID);
        queryResultRecord->set_execution_time(queryResult.executionTime);
        queryResultRecord->set_explain_logical(queryResult.explainLogical);
        queryResultRecord->set_explain_physical(queryResult.explainPhysical);
        queryResultRecord->set_selective(queryResult.selective); 
        
        boost::shared_ptr<Query> query = Query::getQueryByID(queryResult.queryID);
        if (queryResult.selective)
        {
            const ArrayDesc& arrayDesc = queryResult.array->getArrayDesc();

            queryResultRecord->set_array_name(arrayDesc.getName());

            const Attributes& attributes = arrayDesc.getAttributes();
            for (size_t i = 0; i < attributes.size(); i++)
            {
                ::scidb_msg::QueryResult_AttributeDesc* attribute = queryResultRecord->add_attributes();

                attribute->set_id(attributes[i].getId());
                attribute->set_name(attributes[i].getName());
                attribute->set_type(attributes[i].getType());
                attribute->set_flags(attributes[i].getFlags());
                attribute->set_default_compression_method(attributes[i].getDefaultCompressionMethod());
                attribute->set_default_missing_reason(attributes[i].getDefaultValue().getMissingReason());
                attribute->set_default_value(string((char*)attributes[i].getDefaultValue().data(), attributes[i].getDefaultValue().size()));
            }

            const Dimensions& dimensions = arrayDesc.getDimensions();
            for (size_t i = 0; i < dimensions.size(); i++)
            {
                ::scidb_msg::QueryResult_DimensionDesc* dimension = queryResultRecord->add_dimensions();

                dimension->set_name(dimensions[i].getBaseName());
                dimension->set_start_min(dimensions[i].getStartMin());
                dimension->set_curr_start(dimensions[i].getCurrStart());
                dimension->set_curr_end(dimensions[i].getCurrEnd());
                dimension->set_end_max(dimensions[i].getEndMax());
                dimension->set_chunk_interval(dimensions[i].getChunkInterval());
                dimension->set_chunk_overlap(dimensions[i].getChunkOverlap());
                dimension->set_type_id(dimensions[i].getType());
                dimension->set_flags(dimensions[i].getFlags());
                dimension->set_mapping_array_name("");
                if (dimensions[i].getType() != TID_INT64 && !dimensions[i].getMappingArrayName().empty()) {
                    boost::shared_ptr<Array> mappingArray = query->getArray(dimensions[i].getMappingArrayName());
                    boost::shared_ptr<ConstArrayIterator> mappingArrayIterator = mappingArray->getConstIterator(0);
                    if (!mappingArrayIterator->end()) {
                        dimension->set_mapping_array_name(dimensions[i].getMappingArrayName());
                        dimension->set_coordinates_mapping_size(mappingArray->getArrayDesc().getDimensions()[0].getLength());
                    }
                }
            }
        }

        vector<Warning> v = query->getWarnings();
        for (vector<Warning>::const_iterator it = v.begin(); it != v.end(); ++it)
        {
            ::scidb_msg::QueryResult_Warning* warn = queryResultRecord->add_warnings();

            warn->set_code(it->getCode());
            warn->set_file(it->getFile());
            warn->set_function(it->getFunction());
            warn->set_line(it->getLine());
            warn->set_what_str(it->msg());
            warn->set_strings_namespace(it->getStringsNamespace());
            warn->set_stringified_code(it->getStringifiedCode());
        }
        query->clearWarnings();

        for (vector<string>::const_iterator it = queryResult.plugins.begin();
            it != queryResult.plugins.end(); ++it)
        {
            queryResultRecord->add_plugins(*it);
        }

        queryResult.array.reset();

        StatisticsScope sScope;
        _connection->sendMessage(resultMessage);
        LOG4CXX_DEBUG(logger, "The result of query is sent to the client")
    }
    catch (const Exception& e)
    {
       LOG4CXX_ERROR(logger, "executeClientQuery failed to complete: " << e.what())
       StatisticsScope sScope;
       handleExecuteOrPrepareError(e, queryResult, scidb);
    }
}

void ClientMessageHandleJob::fetchChunk()
{
    const QueryID queryID = _messageDesc->getQueryID();
    boost::shared_ptr<Query> query;
    try
    {
        query = Query::getQueryByID(queryID);
        Query::validateQueryPtr(query);
        RWLock::ErrorChecker noopEc;
        ScopedRWLockRead shared(query->queryLock, noopEc);
        StatisticsScope sScope(&query->statistics);
        uint32_t attributeId = _messageDesc->getRecord<scidb_msg::Fetch>()->attribute_id();
        string arrayName = _messageDesc->getRecord<scidb_msg::Fetch>()->array_name();
        LOG4CXX_TRACE(logger, "Fetching chunk of " << attributeId << " attribute in context of " << queryID << " query")

        boost::shared_ptr<Array> fetchArray;

        map<string, shared_ptr<Array> >::const_iterator i = query->_mappingArrays.find(arrayName);
        if (i != query->_mappingArrays.end() ) {
            fetchArray = i->second;
        } else {
            fetchArray = query->getCurrentResultArray();
        }
        assert(fetchArray); //XXX TODO: this needs to be an exception
        boost::shared_ptr< ConstArrayIterator> iter = fetchArray->getConstIterator(attributeId);

        boost::shared_ptr<MessageDesc> chunkMsg;
        boost::shared_ptr<scidb_msg::Chunk> chunkRecord;
        if (!iter->end())
        {
            const ConstChunk* chunk = &iter->getChunk();
            checkChunkMagic(*chunk);
            boost::shared_ptr<CompressedBuffer> buffer = boost::make_shared<CompressedBuffer>();
            boost::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
            chunk->compress(*buffer, emptyBitmap);
            chunkMsg = boost::make_shared<MessageDesc>(mtChunk, buffer);
            chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
            chunkRecord->set_eof(false);
            chunkRecord->set_sparse(chunk->isSparse());
            chunkRecord->set_rle(chunk->isRLE());
            chunkRecord->set_compression_method(buffer->getCompressionMethod());
            chunkRecord->set_attribute_id(chunk->getAttributeDesc().getId());
            chunkRecord->set_decompressed_size(buffer->getDecompressedSize());
            chunkMsg->setQueryID(queryID);
            chunkRecord->set_count(chunk->isCountKnown() ? chunk->count() : 0);
            const Coordinates& coordinates = chunk->getFirstPosition(false);
            for (size_t i = 0; i < coordinates.size(); i++) {
                chunkRecord->add_coordinates(coordinates[i]);
            }
            ++(*iter);
            LOG4CXX_TRACE(logger, "Prepared message with chunk data")
        }
        else
        {
            chunkMsg = boost::make_shared<MessageDesc>(mtChunk);
            chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
            chunkMsg->setQueryID(queryID);
            chunkRecord->set_eof(true);
            LOG4CXX_TRACE(logger, "Prepared message with information that there are no unread chunks")
        }

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

        query->validate();
        _connection->sendMessage(chunkMsg);

        LOG4CXX_TRACE(logger, "Chunk was sent to client")
    }
    catch (const Exception& e)
    {
        StatisticsScope sScope(query ? &query->statistics : NULL);
        LOG4CXX_ERROR(logger, "Client's fetchChunk failed to complete: " << e.what()) ;
        _connection->sendMessage(makeErrorMessageFromException(e, queryID));
    }
}

void ClientMessageHandleJob::cancelQuery()
{
    const scidb::SciDB& scidb = getSciDBExecutor();

    const QueryID queryID = _messageDesc->getQueryID();

    StatisticsScope sScope;
    try
    {
        scidb.cancelQuery(queryID);
        if (_connection) { 
            _connection->detachQuery(queryID);
            _connection->sendMessage(makeOkMessage(queryID));
        }
        LOG4CXX_TRACE(logger, "The query " << queryID << " execution was canceled")
    }
    catch (const Exception& e)
    {
        LOG4CXX_DEBUG(logger, e.what()) ;
        if (_connection) { 
            _connection->sendMessage(makeErrorMessageFromException(e, queryID));
        }
    }
}

void ClientMessageHandleJob::completeQuery()
{
    const scidb::SciDB& scidb = getSciDBExecutor();

    const QueryID queryID = _messageDesc->getQueryID();

    StatisticsScope sScope;
    try
    {
        scidb.completeQuery(queryID);
        if (_connection) {
            _connection->detachQuery(queryID);
            _connection->sendMessage(makeOkMessage(queryID));
        }
        LOG4CXX_TRACE(logger, "The query " << queryID << " execution was completed")
    }
    catch (const Exception& e)
    {
        LOG4CXX_DEBUG(logger, e.what()) ;
        if (_connection) {
            _connection->sendMessage(makeErrorMessageFromException(e, queryID));
        }
    }
}

} // namespace
