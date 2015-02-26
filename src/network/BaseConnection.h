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
 * @file BaseConnection.h
 *
 * @author: roman.simakov@gmail.com
 *
 * @brief The BaseConnection class
 *
 * The file includes the main data structures and interfaces used in message exchanging.
 * Also the file contains BaseConnection class for synchronous connection and message exchanging.
 * This class is used in client code. The scidb engine will use a class which is derived from BaseConnection.
 */

#ifndef BASECONNECTION_H_
#define BASECONNECTION_H_

#include "stdint.h"
#include "boost/asio.hpp"

#include "array/Metadata.h"
#include "network/proto/scidb_msg.pb.h"
#include "array/Array.h"
#include "util/Semaphore.h"
#include "util/NetworkMessage.h"

namespace scidb
{

const uint32_t NET_PROTOCOL_VER1 = 1;
const uint32_t NET_PROTOCOL_VER2 = 2;
const uint32_t NET_PROTOCOL_VER3 = 3;
const uint32_t NET_PROTOCOL_CURRENT_VER = NET_PROTOCOL_VER3;

/**
 * Messageg types
 */
enum MessageType
{
    mtNone,
    mtExecuteQuery,
    mtPreparePhysicalPlan,
    mtExecutePhysicalPlan,
    mtFetch,
    mtChunk,
    mtChunkReplica,
    mtRecoverChunk,
    mtReplicaSyncRequest,
    mtReplicaSyncResponse,
    mtAggregateChunk,
    mtQueryResult,
    mtError,
    mtSyncRequest,
    mtSyncResponse,
    mtCancelQuery,
    mtRemoteChunk,
    mtNotify,
    mtWait,
    mtBarrier,
    mtMPISend,
    mtAlive,
    mtPrepareQuery,
    mtResourcesFileExistsRequest,
    mtResourcesFileExistsResponse,
    mtAbort,
    mtCommit,
    mtSystemMax // must be last
};


struct MessageHeader
{
   uint16_t netProtocolVersion;         /** < Version of network protocol */
   uint16_t messageType;                        /** < Type of message */
   uint32_t recordSize;                 /** < The size of structured part of message to know what buffer size we must allocate */
   uint32_t binarySize;                 /** < The size of unstructured part of message to know what buffer size we must allocate */
   NodeID sourceNodeID;            /** < The source node number */
   uint64_t queryID;               /** < Query ID */
};


/**
 * Message descriptor with all necessary parts
 */
class MessageDesc
{
public:
   MessageDesc();
   MessageDesc(MessageType messageType);
   MessageDesc(boost::shared_ptr< SharedBuffer > binary);
   MessageDesc(MessageType messageType, boost::shared_ptr< SharedBuffer > binary);
   virtual ~MessageDesc() {}
   void writeConstBuffers(std::vector<boost::asio::const_buffer>& constBuffers);
   bool parseRecord(size_t bufferSize);
   void prepareBinaryBuffer();

   NodeID getSourceNodeID() {
      return _messageHeader.sourceNodeID;
   }

   /**
    * This method is not part of the public API
    */
   void setSourceNodeID(const NodeID& nodeId) {
      _messageHeader.sourceNodeID = nodeId;
   }

template <class Derived>
    boost::shared_ptr<Derived> getRecord() {
        return boost::static_pointer_cast<Derived>(_record);
    }

    MessageID getMessageType() {
       return static_cast<MessageID>(_messageHeader.messageType);
    }

    boost::shared_ptr< SharedBuffer > getBinary()
    {
        return _binary;
    }

    virtual bool validate();

    size_t getMessageSize() const
    {
        return _messageHeader.recordSize + _messageHeader.binarySize + sizeof(MessageHeader);
    }

    QueryID getQueryID() const
    {
        return _messageHeader.queryID;
    }

    void setQueryID(QueryID queryID)
    {
        _messageHeader.queryID = queryID;
    }

    void waitSent(size_t n = 1) {
       _sent.enter(n);
    }

    void releaseSent()
    {
       _sent.release();
    }

    void initRecord(MessageID messageType)
    {
       assert( _messageHeader.messageType == mtNone);
       _record = createRecord(messageType);
       _messageHeader.messageType = static_cast<uint16_t>(messageType);
    }

 protected:

    virtual MessagePtr createRecord(MessageID messageType)
    {
       return createRecordByType(static_cast<MessageType>(messageType));
    }

private:

    void init(MessageType messageType);
    MessageHeader _messageHeader;   /** < Message header */
    MessagePtr _record;             /** < Structured part of message */
    boost::shared_ptr< SharedBuffer > _binary;     /** < Buffer for binary data to be transfered */
    boost::asio::streambuf _recordStream; /** < Buffer for serializing Google Protocol Buffers objects */
    Semaphore _sent; /**< Semaphore for waitong notification about message sent */

    static MessagePtr createRecordByType(MessageType messageType);

    friend class BaseConnection;
    friend class Connection;
};


/**
 * Base class for connection to a network manager and send message to it.
 * Class uses sync mode and knows nothing about NetworkManager.
 */
class BaseConnection
{
protected:

        boost::asio::ip::tcp::socket _socket;
        boost::shared_ptr<MessageDesc> _messageDesc;
        /**
         * Set socket options such as TCP_KEEP_ALIVE
         */
        void configConnectedSocket();

public:
        BaseConnection(boost::asio::io_service& ioService);
        ~BaseConnection();

        /// Connect to remote site
        void connect(std::string address, uint16_t port);

        void disconnect();

        boost::asio::ip::tcp::socket& getSocket() {
                return _socket;
        }

        /**
         * Send message to peer and read message from it.
         * @param inMessageDesc a message descriptor for sending message.
         * @param binary a buffer for writing binary data to. If it has
         * default value, buffer will be allocated.
         * @return message descriptor of received message.
         */
        boost::shared_ptr<MessageDesc> sendAndReadMessage(boost::shared_ptr<MessageDesc>& inMessageDesc);
};


}

#endif /* SYNCCONNECTION_H_ */
