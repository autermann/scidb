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
 * BaseConnection.cpp
 *
 *  Created on: Jan 15, 2010
 *      Author: roman.simakov@gmail.com
 */

#include <log4cxx/logger.h>
#include <boost/bind.hpp>
#include <boost/make_shared.hpp>

#include "BaseConnection.h"
#include "system/Exceptions.h"


using namespace std;
using namespace boost;

namespace scidb
{


// Logger for network subsystem. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network"));


/**
 * Message descriptor
 * @param messageType provides related google protocol buffer message
 * @param binary a pointer to buffer that will be used for reading or writing
 * binary data. Can be ommited when the message has no binary data
 */
MessageDesc::MessageDesc(MessageType messageType, boost::shared_ptr<SharedBuffer> binary)
: _binary(binary)
{
   init(messageType);
}
MessageDesc::MessageDesc()
{
  init(mtNone);
}
MessageDesc::MessageDesc(MessageType messageType)
{
   init(messageType);
}
MessageDesc::MessageDesc(boost::shared_ptr<SharedBuffer> binary)
: _binary(binary)
{
   init(mtNone);
}
void
MessageDesc::init(MessageType messageType)
{
    _messageHeader.netProtocolVersion = NET_PROTOCOL_CURRENT_VER;
    _messageHeader.sourceNodeID = CLIENT_NODE;
    _messageHeader.recordSize = 0;
    _messageHeader.binarySize = 0;
    _messageHeader.messageType = static_cast<uint16_t>(messageType);
    _messageHeader.queryID = 0;

    if (messageType != mtNone)
        _record = createRecordByType(messageType);
}
void MessageDesc::writeConstBuffers(std::vector<asio::const_buffer>& constBuffers)
{
    if (_messageHeader.recordSize == 0) {
        ostream out(&_recordStream);
        _record->SerializeToOstream(&out);
        _messageHeader.recordSize = _recordStream.size();
    }
    const bool haveBinary = _binary && _binary->getSize();
    if (haveBinary) {
        _messageHeader.binarySize = _binary->getSize();
    }

    constBuffers.push_back(asio::buffer(&_messageHeader, sizeof(_messageHeader)));
    constBuffers.push_back(asio::buffer(_recordStream.data()));
    if (haveBinary) {
        constBuffers.push_back(asio::buffer(_binary->getData(), _binary->getSize()));
    }

    LOG4CXX_TRACE(logger, "writeConstBuffers: messageType=" << _messageHeader.messageType <<
            " ; headerSize=" << sizeof(_messageHeader) <<
            " ; recordSize=" << _messageHeader.recordSize <<
            " ; binarySize=" << _messageHeader.binarySize);
}


bool MessageDesc::parseRecord(size_t bufferSize)
{
    _recordStream.commit(bufferSize);

    _record = createRecord(static_cast<MessageID>(_messageHeader.messageType));

    istream inStream(&_recordStream);
    bool rc = _record->ParseFromIstream(&inStream);
    return (rc && _record->IsInitialized());
}


void MessageDesc::prepareBinaryBuffer()
{
    if (_messageHeader.binarySize) {
        if (_binary) {
            _binary->reallocate(_messageHeader.binarySize);
        }
        else {
            // For chunks it's correct but for other data it can required other buffers
            _binary = boost::shared_ptr<SharedBuffer>(new CompressedBuffer());
            _binary->allocate(_messageHeader.binarySize);
        }

    }
}


MessagePtr MessageDesc::createRecordByType(MessageType messageType)
{
    switch (messageType)
    {
    case mtPrepareQuery:
    case mtExecuteQuery:
        return MessagePtr(new scidb_msg::Query());
    case mtPreparePhysicalPlan:
        return MessagePtr(new scidb_msg::PhysicalPlan());
    case mtFetch:
        return MessagePtr(new scidb_msg::Fetch());
    case mtChunk:
    case mtChunkReplica:
    case mtRecoverChunk:
    case mtAggregateChunk:
    case mtRemoteChunk:
        return MessagePtr(new scidb_msg::Chunk());
    case mtQueryResult:
        return MessagePtr(new scidb_msg::QueryResult());
    case mtError:
        return MessagePtr(new scidb_msg::Error());
    case mtSyncRequest:
    case mtSyncResponse:
    case mtCancelQuery:
    case mtNotify:
    case mtWait:
    case mtBarrier:
    case mtMPISend:
    case mtAlive:
    case mtReplicaSyncRequest:
    case mtReplicaSyncResponse:
    case mtExecutePhysicalPlan:
    case mtAbort:
    case mtCommit:
        return MessagePtr(new scidb_msg::DummyQuery());

    //Resources chat messages
    case mtResourcesFileExistsRequest:
        return MessagePtr(new scidb_msg::ResourcesFileExistsRequest());
    case mtResourcesFileExistsResponse:
        return MessagePtr(new scidb_msg::ResourcesFileExistsResponse());

    default:
        LOG4CXX_ERROR(logger, "Unknown message type " << messageType);
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_UNKNOWN_MESSAGE_TYPE) << messageType;
    }
}

bool MessageDesc::validate()
{
    if (_messageHeader.netProtocolVersion != NET_PROTOCOL_CURRENT_VER) {
        LOG4CXX_ERROR(logger, "Invalid protocol version: " << _messageHeader.netProtocolVersion);
        return false;
    }
    switch (_messageHeader.messageType)
    {
    case mtPrepareQuery:
    case mtExecuteQuery:
    case mtPreparePhysicalPlan:
    case mtExecutePhysicalPlan:
    case mtFetch:
    case mtChunk:
    case mtRecoverChunk:
    case mtChunkReplica:
    case mtReplicaSyncRequest:
    case mtReplicaSyncResponse:
    case mtAggregateChunk:
    case mtQueryResult:
    case mtError:
    case mtSyncRequest:
    case mtSyncResponse:
    case mtCancelQuery:
    case mtRemoteChunk:
    case mtNotify:
    case mtWait:
    case mtBarrier:
    case mtMPISend:
    case mtAlive:
    case mtResourcesFileExistsRequest:
    case mtResourcesFileExistsResponse:
    case mtAbort:
    case mtCommit:
        break;
    default:
        return false;
    }

    return true;
}

/***
 * B a s e C o n n e c t i o n
 */
BaseConnection::BaseConnection(boost::asio::io_service& ioService): _socket(ioService)
{
}

BaseConnection::~BaseConnection()
{
    disconnect();
}

void BaseConnection::connect(string address, uint16_t port)
{
   LOG4CXX_DEBUG(logger, "Connecting to " << address << ":" << port)

   asio::ip::tcp::resolver resolver(_socket.get_io_service());

   stringstream serviceName;
   serviceName << port;
   asio::ip::tcp::resolver::query query(address, serviceName.str());
   asio::ip::tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
   asio::ip::tcp::resolver::iterator end;

   boost::system::error_code error = boost::asio::error::host_not_found;
   while (error && endpoint_iterator != end)
   {
      _socket.close();
      _socket.connect(*endpoint_iterator++, error);
   }
   if (error)
   {
      LOG4CXX_FATAL(logger, "Error #" << error << " when connecting to " << address << ":" << port);
      throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CONNECTION_ERROR) << error << address << port;
   }

   configConnectedSocket();
   LOG4CXX_DEBUG(logger, "Connected to " << address << ":" << port);
}

void BaseConnection::configConnectedSocket()
{
   boost::asio::ip::tcp::no_delay no_delay(true);
   _socket.set_option(no_delay);
   boost::asio::socket_base::keep_alive keep_alive(true);
   _socket.set_option(keep_alive);

#ifndef __APPLE__
   int s = _socket.native();
   int optval;
   socklen_t optlen = sizeof(optval);

   /* Set the option active */
   optval = 1;
   if(setsockopt(s, SOL_TCP, TCP_KEEPCNT, &optval, optlen) < 0) {
      perror("setsockopt()");
   }
   optval = 30;
   if(setsockopt(s, SOL_TCP, TCP_KEEPIDLE, &optval, optlen) < 0) {
      perror("setsockopt()");
   }
   optval = 30;
   if(setsockopt(s, SOL_TCP, TCP_KEEPINTVL, &optval, optlen) < 0) {
      perror("setsockopt()");
   }
#endif

   if (logger->isTraceEnabled()) {
      boost::asio::socket_base::receive_buffer_size optionRecv;
      _socket.get_option(optionRecv);
      int size = optionRecv.value();
      LOG4CXX_TRACE(logger, "Socket receive buffer size = " << size);
      boost::asio::socket_base::send_buffer_size optionSend;
      _socket.get_option(optionSend);
      size = optionSend.value();
      LOG4CXX_TRACE(logger, "Socket send buffer size = " << size);
   }
}

void BaseConnection::disconnect()
{
    _socket.close();
    LOG4CXX_DEBUG(logger, "Disconnected")
}

boost::shared_ptr<MessageDesc> BaseConnection::sendAndReadMessage(boost::shared_ptr<MessageDesc>& messageDesc)
{
   LOG4CXX_TRACE(logger, "The sendAndReadMessage begin");
   try
   {
       { // scope for sending
          vector<asio::const_buffer> constBuffers;
          messageDesc->_messageHeader.sourceNodeID = CLIENT_NODE;
          messageDesc->writeConstBuffers(constBuffers);

          asio::write(_socket, constBuffers);
       }

       LOG4CXX_TRACE(logger, "Message was sent. Reading...");

       boost::shared_ptr<MessageDesc> _resultDesc = boost::shared_ptr<MessageDesc>(new MessageDesc());

       // Reading message description
       size_t readBytes = read(_socket, asio::buffer(&_resultDesc->_messageHeader, sizeof(_resultDesc->_messageHeader)));
       assert(readBytes == sizeof(_resultDesc->_messageHeader));
       assert(_resultDesc->validate());
       // TODO: This must not be an assert but exception of correct handled backward compatibility
       assert(_resultDesc->_messageHeader.netProtocolVersion == NET_PROTOCOL_CURRENT_VER);

       // Reading serialized structured part
       readBytes = read(_socket, _resultDesc->_recordStream.prepare(_resultDesc->_messageHeader.recordSize));
       assert(readBytes == _resultDesc->_messageHeader.recordSize);
       LOG4CXX_TRACE(logger, "ReadMessage: recordSize=" << _resultDesc->_messageHeader.recordSize);

       bool rc = _resultDesc->parseRecord(readBytes);
       assert(rc);
       _resultDesc->prepareBinaryBuffer();

       if (_resultDesc->_messageHeader.binarySize > 0)
       {
          readBytes = read(_socket, asio::buffer(_resultDesc->_binary->getData(), _resultDesc->_binary->getSize()));
          assert(readBytes == _resultDesc->_binary->getSize());
       }

       LOG4CXX_TRACE(logger, "read message: messageType=" << _resultDesc->_messageHeader.messageType <<
                     " ; binarySize=" << _resultDesc->_messageHeader.binarySize);

       LOG4CXX_TRACE(logger, "The sendAndReadMessage end");

       return _resultDesc;
   }
   catch (const boost::exception &e)
   {
       throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CANT_SEND_RECEIVE);
   }
}


} // namespace
