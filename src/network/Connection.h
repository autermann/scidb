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
 * Connection.h
 *
 *  Created on: Jan 15, 2010
 *      Author: roman.simakov@gmail.com
 */

#ifndef CONNECTION_H_
#define CONNECTION_H_

#include <deque>
#include <set>
#include "stdint.h"
#include "boost/asio.hpp"
#include "boost/enable_shared_from_this.hpp"

#include "util/Mutex.h"
#include "array/Metadata.h"
#include "network/proto/scidb_msg.pb.h"
#include "network/BaseConnection.h"

namespace scidb
{

/**
 * Class for connect to asynchronous message exchanging between
 * network managers. It is used by network manager itself for sending message
 * to another node and by client to connect to scidb node.
 * @note
 * All operations are executed on the io_service::run() thread.
 * If/when multiple threads ever execute io_service::run(), io_service::strand
 * should/can be used to serialize this class execution.
 */
    class Connection: private BaseConnection, public boost::enable_shared_from_this<Connection>
    {
      private:

        typedef std::deque<boost::shared_ptr<MessageDesc> > MessageQueue;
        MessageQueue _messagesQueue;
        class NetworkManager& _networkManager;
        NodeID _nodeID;

        NodeID _sourceNodeID;
        typedef enum
        {
            NOT_CONNECTED,
            CONNECT_IN_PROGRESS,
            CONNECTED
        } ConnectionState;

        ConnectionState _connectionState;
        boost::asio::ip::address _remoteIp;
        boost::system::error_code _error;
        boost::shared_ptr<boost::asio::ip::tcp::resolver::query> _query;
        std::set<QueryID> _activeClientQueries;
        Mutex _mutex;
        bool _isSending;
        bool _logConnectErrors;

        void handleReadError(const boost::system::error_code& error);
        void onResolve(boost::shared_ptr<boost::asio::ip::tcp::resolver>& resolver,
                       boost::shared_ptr<boost::asio::ip::tcp::resolver::query>& query,
                       const boost::system::error_code& err,
                       boost::asio::ip::tcp::resolver::iterator endpoint_iterator);

        void onConnect(boost::shared_ptr<boost::asio::ip::tcp::resolver>& resolver,
                       boost::shared_ptr<boost::asio::ip::tcp::resolver::query>& query,
                       boost::asio::ip::tcp::resolver::iterator endpoint_iterator,
                       const boost::system::error_code& err);
        void disconnectInternal();
        void connectAsyncInternal(const std::string& address, uint16_t port);
        void abortMessages();

        void readMessage();
        void handleReadMessage(const boost::system::error_code&, size_t);
        void handleReadRecordPart(const boost::system::error_code&, size_t);
        void handleReadBinaryPart(const boost::system::error_code&, size_t);
        void handleSendMessage(const boost::system::error_code&, size_t, boost::shared_ptr<MessageDesc>);
        void pushNextMessage();
        std::string getPeerId();
        void getRemoteIp();

      public:
        Connection(NetworkManager& networkManager, NodeID sourceNodeID, NodeID nodeID = INVALID_NODE);
        virtual ~Connection();

        void startQuery(QueryID queryID);
        void stopQuery(QueryID queryID);

        bool isConnected() const { 
            return  _connectionState == CONNECTED;
        }

        /// The first method executed for connected socket
        void start();
        void sendMessage(boost::shared_ptr<MessageDesc> messageDesc);

        /**
         * Asynchronously connect to the remote site, address:port.
         * It does not wait for the connect to complete.
         * If the connect operation fails, it is scheduled for
         * a reconnection (with the currently available address:port from SystemCatalog).
         * Connection operations can be invoked immediately after the call to connectAsync.
         * @param address[in] target DNS name or IP(v4)
         * @param port target port
         */
        void connectAsync(const std::string& address, uint16_t port);

        /**
         * Disconnect the socket and abort all in-flight async operations
         */
        void disconnect();
        boost::asio::ip::tcp::socket& getSocket()
        {
            return _socket;
        }

        class ServerMessageDesc : public MessageDesc
        {
          public:
            ServerMessageDesc() {}
            ServerMessageDesc(boost::shared_ptr<SharedBuffer> binary) : MessageDesc(binary) {}
            virtual ~ServerMessageDesc() {}
            virtual bool validate();
          protected:
            virtual MessagePtr createRecord(MessageID messageType);
          private:
            ServerMessageDesc(const ServerMessageDesc&);
            ServerMessageDesc& operator=(const ServerMessageDesc&);
        };
    };
}

#endif /* CONNECTION_H_ */
