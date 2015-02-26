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
 * NetworkManager.h
 *      Author: roman.simakov@gmail.com
 *      Description: NetworkManager class provides high-level API
 *      for message exchanging and also register node in system catalog.
 */

#ifndef NETWORKMANAGER_H_
#define NETWORKMANAGER_H_

#include "boost/asio.hpp"
#include "log4cxx/logger.h"

#include "util/Singleton.h"
#include "util/JobQueue.h"
#include "util/WorkQueue.h"
#include "util/Network.h"
#include "array/Metadata.h"
#include "network/Connection.h"
#include "query/QueryProcessor.h"
#include <system/Cluster.h>
#include "ThrottledScheduler.h"

namespace scidb
{
 class Statistics;
 class Connection;

/***
 * The network manager implementation depends on system catalog API
 * and storage manager API. It will register itself online in system catalog and read node number from
 * storage manager. It's necessary since storage content will be described in system
 * catalog with node number and its content must be related to node number.
 * So the best place to store node number is a storage manager. By reading
 * this value scidb instance can find itself in system catalog and register itself
 * as online. In other words node number is a local data part number. Registering
 * node in system catalog online means pointing where data part with given number is.
 */
class NetworkManager: public Singleton<NetworkManager>
{
    friend class Cluster;
    friend class Connection;
private:
    // Service of boost asio
    static boost::asio::io_service _ioService;

    // Acceptor of incoming connections
    boost::asio::ip::tcp::acceptor _acceptor;

    boost::asio::posix::stream_descriptor _input;
    char one_byte_buffer;

    // A timer to check connections for alive
    boost::asio::deadline_timer _aliveTimer;

    // NodeID of this instance of manager
    NodeID _selfNodeID;

    /**
     *  A pool of connection to other nodes.
     */
    typedef std::map<NodeID, boost::shared_ptr<Connection> > ConnectionMap;
    ConnectionMap _outConnections;

    // Job queue for processing received messages
    boost::shared_ptr< JobQueue> _jobQueue;

    Nodes _nodes;

    boost::shared_ptr<const NodeLiveness> _nodeLiveness;

    Mutex _mutex;

    static volatile bool _shutdown;

    // If > 0 handler of reconnect is added
    std::set<NodeID> _brokenNodes;

    // A timer to handle reconnects
    boost::shared_ptr<ThrottledScheduler> _reconnectScheduler;
    static const time_t DEFAULT_RECONNECT_TIMEOUT = 3; //sec

    // A timer to handle reconnects
    boost::shared_ptr<ThrottledScheduler> _livenessHandleScheduler;
    static const time_t DEFAULT_LIVENESS_HANDLE_TIMEOUT = 60; //sec

    class DefaultMessageDescription : public MessageDescription
    {
    public:
    DefaultMessageDescription( NodeID nodeID,
                               MessageID msgID,
                               MessagePtr msgRec,
                               boost::shared_ptr<SharedBuffer> bin)
       : _nodeID(nodeID), _msgID(msgID), _msgRecord(msgRec), _binary(bin)
       {
          assert(msgRec.get());
       }
       virtual NodeID getSourceNodeID() const   {return _nodeID; }
       virtual MessagePtr getRecord()     {return _msgRecord; }
       virtual MessageID getMessageType()const  {return _msgID; }
       virtual boost::asio::const_buffer getBinary()
       {
          if (_binary) {
             return asio::const_buffer(_binary->getData(), _binary->getSize());
          }
          return asio::const_buffer(NULL, 0);
       }
       virtual ~DefaultMessageDescription() {}
    private:
       DefaultMessageDescription();
       DefaultMessageDescription(const DefaultMessageDescription&);
       DefaultMessageDescription& operator=(const DefaultMessageDescription&);
       NodeID _nodeID;
       MessageID _msgID;
       MessagePtr _msgRecord;
       boost::shared_ptr<SharedBuffer> _binary;
    };

    class DefaultNetworkMessageFactory : public NetworkMessageFactory
    {
    public:
       DefaultNetworkMessageFactory() {}
       virtual ~DefaultNetworkMessageFactory() {}
       bool isRegistered(const MessageID& msgID);
       bool addMessageType(const MessageID& msgID,
                           const NetworkMessageFactory::MessageCreator& msgCreator,
                           const NetworkMessageFactory::MessageHandler& msgHandler);
       MessagePtr createMessage(const MessageID& msgID);
       NetworkMessageFactory::MessageHandler getMessageHandler(const MessageID& msgType);
    private:
       DefaultNetworkMessageFactory(const DefaultNetworkMessageFactory&);
       DefaultNetworkMessageFactory& operator=(const DefaultNetworkMessageFactory&);

       typedef std::map<MessageID,
                        std::pair<NetworkMessageFactory::MessageCreator,
                                  NetworkMessageFactory::MessageHandler> >  MessageHandlerMap;
       MessageHandlerMap _msgHandlers;
       Mutex _mutex;
    };

    boost::shared_ptr<DefaultNetworkMessageFactory>  _msgHandlerFactory;
    boost::shared_ptr<WorkQueue> _workQueue;

    void startAccept();
    void handleAccept(boost::shared_ptr<Connection> newConnection, const boost::system::error_code& error);

    void startInputWatcher();
    void handleInput(const boost::system::error_code& error, size_t bytes_transferr);

    void _handleReconnect();
    static void handleReconnect() {
        getInstance()->_handleReconnect();
    }

    void _handleAlive(const boost::system::error_code& error);
    static void handleAlive(const boost::system::error_code& error) {
        getInstance()->_handleAlive(error);
    }

    void _sendMessage(NodeID targetNodeID,
                      boost::shared_ptr<MessageDesc>& messageDesc);

    static void* doRecover(void*);

    static void handleLivenessNotification(boost::shared_ptr<const NodeLiveness> liveInfo) {
       getInstance()->_handleLivenessNotification(liveInfo);
    }
    void _handleLivenessNotification(boost::shared_ptr<const NodeLiveness>& liveInfo);
    static void handleLiveness() {
       getInstance()->_handleLiveness();
    }
    void _handleLiveness();

    static void publishMessage(const boost::shared_ptr<MessageDescription>& msgDesc);

    void dispatchMessageToListener(const boost::shared_ptr<MessageDesc>& messageDesc,
                                   NetworkMessageFactory::MessageHandler& handler);
    void abortMessageQuery(const QueryID& queryId);
    void cancelClientQuery(const QueryID& queryId);

    /**
     * Handle the messages which are not generated by the SciDB engine proper
     * but registered with NetworkMessageFactory
     * @param messageDesc message description
     * @param handler gets assigned if the return value is true
     * @return true if the message is handled, false if it is a system message
     */
    bool handleNonSystemMessage(const boost::shared_ptr<MessageDesc>& messageDesc,
                                NetworkMessageFactory::MessageHandler& handler);

    /**
     * Request information about nodes from system catalog.
     * @param force a flag to force usage system catalog. If false and _nodes.size() > 0
     * function just return existing version.
     * @return a number of nodes registered in the system catalog.
     */
    void getNodes(bool force = false);

    /**
     * Get currently known node liveness
     * @return liveness or NULL if not yet known
     * @see scidb::Cluster::getNodeLiveness()
     */
    boost::shared_ptr<const NodeLiveness> getNodeLiveness()
    {
       ScopedMutexLock mutexLock(_mutex);
       getNodes(false);
       return _nodeLiveness;
    }

    size_t getPhysicalNodes(std::vector<NodeID>& nodes);

    NodeID getPhysicalNodeID() {
       ScopedMutexLock mutexLock(_mutex);
       return _selfNodeID;
    }

    void reconnect(NodeID nodeID);
    void handleShutdown();

public:
    NetworkManager();
    ~NetworkManager();

    /**
     *  This method send asynchronous message to node with given target node number.
     *  @param targetNodeID is a node number for sending message to.
     *  @param MessageDesc contains Google Protocol Buffer message and octet data.
     *  @package waitSent waits when until message is sent then return
     */
    void sendMessage(NodeID targetNodeID, boost::shared_ptr<MessageDesc>& messageDesc, bool waitSent = false);

    /**
     *  This method sends out asynchronous message to every node except this node (using per-query node ID maps)
     *  @param MessageDesc contains Google Protocol Buffer message and octet data.
     *  @param waitSent waits when until message is sent then return
     */
    void sendOutMessage(boost::shared_ptr<MessageDesc>& messageDesc, bool waitSent = false);

    /**
     *  This method sends out asynchronous message to every physical node except this node
     *  @param MessageDesc contains Google Protocol Buffer message and octet data.
     */
    void broadcast(boost::shared_ptr<MessageDesc>& messageDesc);

    /// This method handle messages received by connections. Called by Connection class
    void handleMessage(boost::shared_ptr< Connection > connection, const boost::shared_ptr<MessageDesc>& messageDesc);

    /// This method block own thread
    void run(boost::shared_ptr<JobQueue> jobQueue);

    /**
     * @return a queue suitable for running arbitrary tasks
     * @note No new threads are created as a result of adding work to the queue
     */
    boost::shared_ptr<WorkQueue> getWorkQueue() {
       return _workQueue;
    }

    /**
     * @return a new queue suitable for running arbitrary tasks
     * @note No new threads are created as a result of adding work to the queue
     *       The items on this queue will not out-live the queue itself, i.e.
     *       once the queue is destroyed an unspecified number of elements may not get to run.
     */
    boost::shared_ptr<WorkQueue> createWorkQueue() {
        return boost::make_shared<WorkQueue>(_jobQueue);
    }

    static boost::asio::io_service& getIOService() {
        return _ioService;
    }

    // Network Interface for operators
    void send(NodeID logicalTargetID, boost::shared_ptr<MessageDesc>& msg);

    // MPI functions
    void send(NodeID logicalTargetID, boost::shared_ptr< SharedBuffer> data, boost::shared_ptr<Query> query);

    boost::shared_ptr< SharedBuffer> receive(NodeID source, boost::shared_ptr<Query> query);

    static void shutdown() {
       _shutdown = true;
    }
    static bool isShutdown() {
       return _shutdown;
    }

    /**
     * Get a factory to register new network messages and
     * to retrieve already registered message handlers.
     * @return the message factory
     */
    boost::shared_ptr<NetworkMessageFactory> getNetworkMessageFactory()
    {
       boost::shared_ptr<NetworkMessageFactory> ptr(_msgHandlerFactory);
       return ptr;
    }
};


} //namespace


#endif /* NETWORKMANAGER_H_ */
