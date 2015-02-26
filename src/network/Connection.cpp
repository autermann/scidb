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
 * Connection.cpp
 *
 *  Created on: Jan 15, 2010
 *      Author: roman.simakov@gmail.com
 */

#include "log4cxx/logger.h"
#include "boost/bind.hpp"
#include "boost/make_shared.hpp"

#include "network/NetworkManager.h"
#include "Connection.h"
#include "system/Exceptions.h"
#include "query/executor/SciDBExecutor.h"

using namespace std;
using namespace boost;

namespace scidb
{


// Logger for network subsystem. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network"));


/***
 * C o n n e c t i o n
 */
Connection::Connection(NetworkManager& networkManager, NodeID sourceNodeID, NodeID nodeID):
    BaseConnection(networkManager.getIOService()),
    _networkManager(networkManager),
    _nodeID(nodeID),
    _sourceNodeID(sourceNodeID),
    _connectionState(NOT_CONNECTED),
    _isSending(false),
    _logConnectErrors(true)
{
   assert(sourceNodeID != INVALID_NODE);
}


void Connection::start()
{
   assert(_connectionState == NOT_CONNECTED);
   assert(!_error);
   _connectionState = CONNECTED;
   getRemoteIp();

   LOG4CXX_DEBUG(logger, "Connection started from " << getPeerId());

   // The first work we should do is reading initial message from client
   _networkManager.getIOService().post(bind(&Connection::readMessage,
                                            shared_from_this()));
}

// Reading messages
void Connection::readMessage()
{
   LOG4CXX_TRACE(logger, "Reading next message");

   assert(_messageDesc == boost::shared_ptr<MessageDesc>());
   _messageDesc = boost::shared_ptr<MessageDesc>(new ServerMessageDesc());
   // XXX TODO: add a timeout after we get the first byte
   async_read(_socket, asio::buffer(&_messageDesc->_messageHeader, sizeof(_messageDesc->_messageHeader)),
              bind(&Connection::handleReadMessage, shared_from_this(), asio::placeholders::error,
                   asio::placeholders::bytes_transferred));
}

void Connection::handleReadMessage(const boost::system::error_code& error, size_t bytes_transferr)
{
   if (error)
   {
      handleReadError(error);
      return;
   }

   if(!_messageDesc->validate()) {
      LOG4CXX_ERROR(logger, "Network error in handleReadMessage: unknown message, closing connection");
      if (_connectionState == CONNECTED) {
         abortMessages();
         disconnectInternal();
      }
      return;
   }
   assert(bytes_transferr == sizeof(_messageDesc->_messageHeader));
   assert(_messageDesc->_messageHeader.sourceNodeID != _sourceNodeID);
   // TODO: This must not be an assert but exception of correct handled backward compatibility
   assert(_messageDesc->_messageHeader.netProtocolVersion == NET_PROTOCOL_CURRENT_VER);

   async_read(_socket, _messageDesc->_recordStream.prepare(_messageDesc->_messageHeader.recordSize),
              bind(&Connection::handleReadRecordPart,
                   shared_from_this(),  asio::placeholders::error,
                   asio::placeholders::bytes_transferred));

   LOG4CXX_TRACE(logger, "handleReadMessage: messageType=" << _messageDesc->_messageHeader.messageType
                 << "; nodeID="<< _messageDesc->_messageHeader.sourceNodeID <<
                 " ; recordSize=" << _messageDesc->_messageHeader.recordSize <<
                 " ; messageDesc.binarySize=" << _messageDesc->_messageHeader.binarySize);
}


void Connection::handleReadRecordPart(const boost::system::error_code& error, size_t bytes_transferr)
{
   if (error)
   {
      handleReadError(error);
      return;
   }

   // It's better to know an operator for which this message came
   // TODO: it we need to move queryID or maybe some other fields into MessageHeader
   StatisticsScope sScope;

   assert(_messageDesc->validate());
   assert(_messageDesc->_messageHeader.recordSize == bytes_transferr);
   assert(_messageDesc->_messageHeader.sourceNodeID != _sourceNodeID);

   if (!_messageDesc->parseRecord(bytes_transferr)) {
      LOG4CXX_ERROR(logger, "Network error in handleReadRecordPart: cannot parse record for msgID="
                    << _messageDesc->_messageHeader.messageType <<", closing connection");
      if (_connectionState == CONNECTED) {
         abortMessages();
         disconnectInternal();
      }
      return;
   }
   _messageDesc->prepareBinaryBuffer();

   LOG4CXX_TRACE(logger, "handleReadRecordPart: messageType=" << _messageDesc->_messageHeader.messageType <<
                 " ; messageDesc.binarySize=" << _messageDesc->_messageHeader.binarySize);

   if (_messageDesc->_messageHeader.binarySize)
   {
      async_read(_socket, asio::buffer(_messageDesc->_binary->getData(), _messageDesc->_binary->getSize()),
                 bind(&Connection::handleReadBinaryPart, shared_from_this(), asio::placeholders::error,
                      asio::placeholders::bytes_transferred));
      return;
   }

   handleReadBinaryPart(error, 0);
}

void Connection::handleReadBinaryPart(const boost::system::error_code& error, size_t bytes_transferr)
{
   if (error)
   {
      handleReadError(error);
      return;
   }
   assert(_messageDesc);
   assert(_messageDesc->_messageHeader.binarySize == bytes_transferr);

   boost::shared_ptr<MessageDesc> msgPtr;
   _messageDesc.swap(msgPtr);

   _networkManager.handleMessage(shared_from_this(), msgPtr);

   // Preparing to read new message
   assert(_messageDesc.get() == NULL);
   readMessage();
}

void Connection::sendMessage(boost::shared_ptr<MessageDesc> messageDesc)
{
   {
      ScopedMutexLock mutexLock(_mutex);
      _messagesQueue.push_back(messageDesc);
      LOG4CXX_TRACE(logger, "sendMessage: new message queue size = "
                    << _messagesQueue.size());
   }
   _networkManager.getIOService().post(bind(&Connection::pushNextMessage,
                                            shared_from_this()));
}

void Connection::handleSendMessage(const boost::system::error_code& error,
                size_t bytes_transferred, boost::shared_ptr<MessageDesc> messageDesc)
{
   _isSending = false;
   if (!error) { // normal case

      LOG4CXX_TRACE(logger, "handleSendMessage: bytes_transferred="
                    << bytes_transferred
                    << ", "<< getPeerId() << ", msgID ="
                    << messageDesc->getMessageType());
      assert(bytes_transferred == messageDesc->getMessageSize());
      assert(messageDesc->_messageHeader.sourceNodeID == _sourceNodeID);

      messageDesc->_sent.release();
      {
         ScopedMutexLock mutexLock(_mutex);
         if ((!_messagesQueue.empty()) &&
             _messagesQueue.front() == messageDesc) {
            _messagesQueue.pop_front();
         } else {
            LOG4CXX_TRACE(logger, "handleSendMessage: sent message must have been cancelled"
                          << ", "<< getPeerId() << ", msgID ="
                          << messageDesc->getMessageType());
         }
      }
      pushNextMessage();
      return;
   }

   // errror case

   LOG4CXX_ERROR(logger, "Network error in handleSendMessage #"
                  << error.value() << "('" << error.message() << "')"
                  << ", "<< getPeerId() << ", msgID =" << messageDesc->getMessageType());
    assert(error != asio::error::interrupted);
    assert(error != asio::error::would_block);
    assert(error != asio::error::try_again);

    if (_connectionState == CONNECTED) {
       abortMessages();
       disconnectInternal();
    }
    if (_nodeID == INVALID_NODE) {
       LOG4CXX_TRACE(logger, "Not recovering connection from "<<getPeerId());
       return;
    }

    LOG4CXX_DEBUG(logger, "Recovering connection to " << getPeerId());
    _networkManager.reconnect(_nodeID);
}

void Connection::pushNextMessage()
{
   if (_connectionState != CONNECTED) {
      assert(!_isSending);
      LOG4CXX_TRACE(logger, "Not yet connected to " << getPeerId());
      return;
   }
   if (_isSending) {
      LOG4CXX_TRACE(logger, "Already sending to " << getPeerId());
      return;
   }
   shared_ptr<MessageDesc> messageDesc;
   {
      ScopedMutexLock mutexLock(_mutex);
      if (!_messagesQueue.empty()) {
         messageDesc = _messagesQueue.front();
         assert(messageDesc);
      }
   }
   if(!messageDesc) {
      LOG4CXX_TRACE(logger, "Nothing to send to " << getPeerId());
      return;
   }
   messageDesc->_messageHeader.sourceNodeID = _sourceNodeID;
   vector<asio::const_buffer> constBuffers;
   messageDesc->writeConstBuffers(constBuffers);

   asio::async_write(_socket, constBuffers,
                     bind(&Connection::handleSendMessage,
                          shared_from_this(), asio::placeholders::error,
                          asio::placeholders::bytes_transferred, messageDesc));
   _isSending = true;
   currentStatistics->sentSize += messageDesc->getMessageSize();
   currentStatistics->sentMessages++;
}

MessagePtr
Connection::ServerMessageDesc::createRecord(MessageID messageType)
{
   if (messageType < mtSystemMax) {
      return MessageDesc::createRecord(messageType);
   }

   boost::shared_ptr<NetworkMessageFactory> msgFactory;
   msgFactory = getNetworkMessageFactory();
   assert(msgFactory);
   MessagePtr recordPtr = msgFactory->createMessage(messageType);

   if (!recordPtr) {
      LOG4CXX_ERROR(logger, "Unknown message type " << messageType);
      throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_UNKNOWN_MESSAGE_TYPE) << messageType;
   }
   return recordPtr;
}

bool
Connection::ServerMessageDesc::validate()
{
   if (MessageDesc::validate()) {
      return true;
   }
   boost::shared_ptr<NetworkMessageFactory> msgFactory;
   msgFactory = getNetworkMessageFactory();
   assert(msgFactory);
   return msgFactory->isRegistered(getMessageType());
}

void Connection::onResolve(shared_ptr<asio::ip::tcp::resolver>& resolver,
                           shared_ptr<asio::ip::tcp::resolver::query>& query,
                           const boost::system::error_code& err,
                           asio::ip::tcp::resolver::iterator endpoint_iterator)
 {
    assert(query);
    assert(resolver);

    if (_connectionState != CONNECT_IN_PROGRESS ||
        _query != query) {
       LOG4CXX_DEBUG(logger, "Dropping resolve query "
                     << query->host_name() << ":" << query->service_name());
       return;
    }

    asio::ip::tcp::resolver::iterator end;
    if (err || endpoint_iterator == end) {
       _error = err ? err : boost::asio::error::host_not_found;
       if (_logConnectErrors) {
          _logConnectErrors = false;
          LOG4CXX_ERROR(logger, "Network error #"
                        << _error.value() << "('" << _error.message() << "')"
                        << " while resolving name of "
                        << getPeerId() << ", "
                        << _query->host_name() << ":" << _query->service_name());

       }
       abortMessages();
       disconnectInternal();
       _networkManager.reconnect(_nodeID);
       return;
    }

    LOG4CXX_TRACE(logger, "Connecting to the first candidate for: "
                  << _query->host_name() << ":" << _query->service_name());
    asio::ip::tcp::endpoint ep = *endpoint_iterator;
    _socket.async_connect(ep,
                          boost::bind(&Connection::onConnect, shared_from_this(),
                                      resolver, query,
                                      ++endpoint_iterator,
                                      boost::asio::placeholders::error));
 }

void Connection::onConnect(shared_ptr<asio::ip::tcp::resolver>& resolver,
                           shared_ptr<asio::ip::tcp::resolver::query>& query,
                           asio::ip::tcp::resolver::iterator endpoint_iterator,
                           const boost::system::error_code& err)

{
   assert(query);
   assert(resolver);
   asio::ip::tcp::resolver::iterator end;

   if (_connectionState != CONNECT_IN_PROGRESS ||
       _query != query) {
      LOG4CXX_TRACE(logger, "Dropping resolve query "
                    << query->host_name() << ":" << query->service_name());
      return;
   }

   if (err && endpoint_iterator == end) {
      if (_logConnectErrors) {
         _logConnectErrors = false;
         LOG4CXX_ERROR(logger, "Network error #"
                       << err.value() << "('" << err.message() << "')"
                       << " while connecting to "
                       << getPeerId() << ", "
                       << _query->host_name() << ":" << _query->service_name());
      }
      abortMessages();
      disconnectInternal();
      _error = err;
      _networkManager.reconnect(_nodeID);
      return;
   }

   if (err) {
      LOG4CXX_TRACE(logger, "Connecting to the next candidate,"
                    << getPeerId() << ", "
                    << _query->host_name() << ":" << _query->service_name()
                    << "Last error #"
                    << err.value() << "('" << err.message() << "')");
      _error = err;
      _socket.close();
      asio::ip::tcp::endpoint ep = *endpoint_iterator;
      _socket.async_connect(ep,
                            boost::bind(&Connection::onConnect, shared_from_this(),
                                        resolver, query,
                                        ++endpoint_iterator,
                                        boost::asio::placeholders::error));
      return;
   }

   configConnectedSocket();
   getRemoteIp();

   LOG4CXX_DEBUG(logger, "Connected to "
                 << getPeerId() << ", "
                 << _query->host_name() << ":"
                 << _query->service_name());

   _connectionState = CONNECTED;
   _error.clear();
   _query.reset();
   _logConnectErrors = true;

   assert(!_isSending);
   pushNextMessage();
}

void Connection::connectAsync(const string& address, uint16_t port)
{
   _networkManager.getIOService().post(bind(&Connection::connectAsyncInternal,
                                            shared_from_this(), address, port));
}

void Connection::connectAsyncInternal(const string& address, uint16_t port)
{
   if (_connectionState == CONNECTED ||
       _connectionState == CONNECT_IN_PROGRESS) {
      LOG4CXX_WARN(logger, "Already connected/ing! Not Connecting to " << address << ":" << port);
      return;
   }

   disconnectInternal();
   LOG4CXX_TRACE(logger, "Connecting (async) to " << address << ":" << port);

   shared_ptr<asio::ip::tcp::resolver>  resolver(new asio::ip::tcp::resolver(_socket.get_io_service()));
   stringstream serviceName;
   serviceName << port;
   _query.reset(new asio::ip::tcp::resolver::query(address, serviceName.str()));
   _error.clear();
   _connectionState = CONNECT_IN_PROGRESS;
   resolver->async_resolve(*_query,
                           boost::bind(&Connection::onResolve, shared_from_this(),
                                       resolver, _query,
                                       boost::asio::placeholders::error,
                                       boost::asio::placeholders::iterator));
}

void Connection::startQuery(QueryID queryID)
{
    ScopedMutexLock mutexLock(_mutex);
    _activeClientQueries.insert(queryID);
}

void Connection::stopQuery(QueryID queryID)
{
    ScopedMutexLock mutexLock(_mutex);
    _activeClientQueries.erase(queryID);
}

void Connection::disconnectInternal()
{
   LOG4CXX_TRACE(logger, "Disconnecting from " << getPeerId());
   _socket.close();
   _connectionState = NOT_CONNECTED;
   _query.reset();
   _remoteIp = boost::asio::ip::address();
   set<QueryID> clientQueries;
   {
       ScopedMutexLock mutexLock(_mutex);
       clientQueries.swap(_activeClientQueries);
   }
   LOG4CXX_TRACE(logger, str(format("Number of active client queries %lld") % clientQueries.size()));
   for (std::set<QueryID>::const_iterator i = clientQueries.begin(); i != clientQueries.end(); ++i)
   {
       assert(_nodeID == CLIENT_NODE);
       QueryID queryID = *i;
       _networkManager.cancelClientQuery(queryID);
   }
}

void Connection::disconnect()
{
   _networkManager.getIOService().post(bind(&Connection::disconnectInternal,
                                            shared_from_this()));
}

void Connection::handleReadError(const boost::system::error_code& error)
{
   assert(error);

   if (error != boost::asio::error::eof) {
      LOG4CXX_ERROR(logger, "Network error while reading, #"
                    << error.value() << "('" << error.message() << "')");

   } else {
      LOG4CXX_TRACE(logger, "Sender disconnected");
   }
   if (_connectionState == CONNECTED) {
      abortMessages();
      disconnectInternal();
   }
}

Connection::~Connection()
{
   LOG4CXX_TRACE(logger, "Destroying connection to " << getPeerId());

   abortMessages();
   disconnectInternal();
}

void Connection::abortMessages()
{
   MessageQueue mQ;
   {
      ScopedMutexLock mutexLock(_mutex);
      mQ.swap(_messagesQueue);
   }

   LOG4CXX_TRACE(logger, "Aborting "<< mQ.size()
                 << " buffered connection messages to "
                 << getPeerId());

   std::set<QueryID> queries;
   for (MessageQueue::iterator iter = mQ.begin();
        iter != mQ.end(); ++iter) {
      shared_ptr<MessageDesc>& messageDesc = *iter;
      queries.insert(messageDesc->getQueryID());
      messageDesc->_sent.release();
   }
   for (std::set<QueryID>::const_iterator iter = queries.begin();
        iter != queries.end(); ++iter) {
      _networkManager.abortMessageQuery(*iter);
   }
}

string Connection::getPeerId()
{
   string res((_nodeID == CLIENT_NODE)
              ? std::string("CLIENT")
              : boost::str(boost::format("node %lld") % _nodeID));
   if (boost::asio::ip::address() != _remoteIp) {
      boost::system::error_code ec;
      string ip(_remoteIp.to_string(ec));
      assert(!ec);
      return (res + boost::str(boost::format(" (%s)") % ip));
   }
   return res;
}

void Connection::getRemoteIp()
{
   boost::system::error_code ec;
   boost::asio::ip::tcp::endpoint endpoint = _socket.remote_endpoint(ec);
   if (!ec)
   {
      _remoteIp = endpoint.address();
   }
   else
   {
      LOG4CXX_ERROR(logger,
                    "Could not get the remote IP from connected socket to/from"
                    << getPeerId()
                    << ". Error:" << ec.value() << "('" << ec.message() << "')");
      assert(false);
   }
}

} // namespace
