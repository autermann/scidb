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
 * @file NetworkManager.cpp
 * @author roman.somakov@gmail.com
 *
 * @brief NetworkManager class implementation.
 */
#include <sys/types.h>
#include <signal.h>
#include <boost/bind.hpp>
#include <boost/format.hpp>
#include <boost/make_shared.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>

#include "network/NetworkManager.h"
#include "system/SystemCatalog.h"
#include "system/Exceptions.h"
#include "network/MessageHandleJob.h"
#include "network/ClientMessageHandleJob.h"
#include "network/MessageUtils.h"
#include "array/Metadata.h"
#include "system/Config.h"
#include "smgr/io/Storage.h"
#include "util/PluginManager.h"
#include "util/Notification.h"
#include "query/Statistics.h"
#include "system/Constants.h"

using namespace std;
using namespace boost;

namespace scidb
{

// Logger for network subsystem. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network"));

/***
 * N e t w o r k M a n a g e r
 */

const time_t RECOVER_TIMEOUT = 2;

asio::io_service NetworkManager::_ioService;
volatile bool NetworkManager::_shutdown=false;

NetworkManager::NetworkManager():
        _acceptor(_ioService, asio::ip::tcp::endpoint(
                asio::ip::tcp::v4(),
                Config::getInstance()->getOption<int>(CONFIG_PORT))),
        _input(_ioService, STDIN_FILENO),
        _aliveTimer(_ioService),
        _selfNodeID(INVALID_NODE),
        _msgHandlerFactory(new DefaultNetworkMessageFactory)
{
   int64_t reconnTimeout = Config::getInstance()->getOption<int>(CONFIG_RECONNECT_TIMEOUT);
   Scheduler::Work func = boost::bind(&NetworkManager::handleReconnect);
   _reconnectScheduler =
   shared_ptr<ThrottledScheduler>(new ThrottledScheduler(reconnTimeout,
                                                         func, _ioService));
   func = boost::bind(&NetworkManager::handleLiveness);
   _livenessHandleScheduler =
   shared_ptr<ThrottledScheduler>(new ThrottledScheduler(DEFAULT_LIVENESS_HANDLE_TIMEOUT,
                                                         func, _ioService));

   LOG4CXX_DEBUG(logger, "Network manager is intialized");
}

NetworkManager::~NetworkManager()
{
    LOG4CXX_DEBUG(logger, "Network manager is shutting down");
}

void NetworkManager::run(boost::shared_ptr<JobQueue> jobQueue)
{
    LOG4CXX_DEBUG(logger, "NetworkManager::run()");

    asio::ip::tcp::endpoint endPoint = _acceptor.local_endpoint();
    const string address = Config::getInstance()->getOption<string>(CONFIG_ADDRESS);
    const unsigned short port = endPoint.port();

    const bool registerNode = Config::getInstance()->getOption<bool>(CONFIG_REGISTER);

    SystemCatalog* catalog = SystemCatalog::getInstance();

    StorageManager::getInstance().open(
            Config::getInstance()->getOption<string>(CONFIG_STORAGE_URL),
            Config::getInstance()->getOption<int>(CONFIG_CACHE_SIZE)*MB);
    _selfNodeID = StorageManager::getInstance().getNodeId();
    if (registerNode) {
        if (_selfNodeID != INVALID_NODE) {
            throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_ALREADY_REGISTERED) << _selfNodeID;
        }
        _selfNodeID = catalog->addNode(NodeDesc(address, port));
        StatisticsScope sScope;
        StorageManager::getInstance().setNodeId(_selfNodeID);
        LOG4CXX_DEBUG(logger, "Registered node # " << _selfNodeID);
        return;
    }
    else {
        if (_selfNodeID == INVALID_NODE) {
            if (Config::getInstance()->optionActivated(CONFIG_RECOVER)) {
                _selfNodeID = Config::getInstance()->getOption<int>(CONFIG_RECOVER);
                StorageManager::getInstance().setNodeId(_selfNodeID);
                LOG4CXX_DEBUG(logger, "Re-register node # " << _selfNodeID);
            } else {
                throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_NOT_REGISTERED);
            }
        }
        if (Config::getInstance()->getOption<int>(CONFIG_REDUNDANCY) >= (int)SystemCatalog::getInstance()->getNumberOfNodes())
            throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_INVALID_REDUNDANCY);
        catalog->markNodeOnline(_selfNodeID, address, port);
    }
    _jobQueue = jobQueue;
    _workQueue = boost::make_shared<WorkQueue>(jobQueue);

    LOG4CXX_INFO(logger, "Network manager is started on " << address << ":" << port << " node #" << _selfNodeID);

    if (!Config::getInstance()->getOption<bool>(CONFIG_NO_WATCHDOG)) {
       startInputWatcher();
    }

    NodeLivenessNotification::PublishListener listener = bind(&handleLivenessNotification, _1);
    NodeLivenessNotification::ListenerID lsnrID =
        NodeLivenessNotification::addPublishListener(listener);

    startAccept();
    _aliveTimer.expires_from_now(posix_time::seconds(5));
    _aliveTimer.async_wait(NetworkManager::handleAlive);

    if (Config::getInstance()->optionActivated(CONFIG_RECOVER) &&
        (int)_selfNodeID != Config::getInstance()->getOption<int>(CONFIG_RECOVER)) {
        pthread_t t;
        pthread_create(&t, 0, doRecover, NULL);
    }

    LOG4CXX_DEBUG(logger, "Start connection accepting and async message exchanging");

    // main loop
    _ioService.run();

    try
    {
        SystemCatalog::getInstance()->markNodeOffline(_selfNodeID);
    }
    catch(const Exception &e)
    {
        LOG4CXX_ERROR(logger, "Marking node offline failed:\n" << e.what());
    }
}

void* NetworkManager::doRecover(void*)
{
    sleep(RECOVER_TIMEOUT);
    int recoveredNodeID = Config::getInstance()->getOption<int>(CONFIG_RECOVER);
    LOG4CXX_DEBUG(logger, "Start recovery of node # " << recoveredNodeID);

    // create a fake query
    boost::shared_ptr<const scidb::NodeLiveness> myLiveness =
        Cluster::getInstance()->getNodeLiveness();
    assert(myLiveness);
    boost::shared_ptr<Query> query = Query::createDetached();
    query->init(0, COORDINATOR_NODE,
                Cluster::getInstance()->getLocalNodeId(),
                myLiveness);

    StorageManager::getInstance().recover(recoveredNodeID, query);
    LOG4CXX_DEBUG(logger, "Complete recovery of node # " << recoveredNodeID);
    return NULL;
}

void NetworkManager::handleShutdown()
{
   LOG4CXX_INFO(logger, "SciDB is going down ...");
   ScopedMutexLock scope(_mutex);
   assert(_shutdown);

   _acceptor.close();
   _input.close();
   ConnectionMap().swap(_outConnections);
   getIOService().stop();
}

void NetworkManager::startInputWatcher()
{
   _input.async_read_some(boost::asio::buffer((void*)&one_byte_buffer,sizeof(one_byte_buffer)),
                          bind(&NetworkManager::handleInput, this,
                               asio::placeholders::error,
                               asio::placeholders::bytes_transferred));
}

void NetworkManager::handleInput(const boost::system::error_code& error, size_t bytes_transferr)
{
   _input.close();
   if (error == boost::system::errc::operation_canceled) {
      return;
   }
   if (!error) {
      LOG4CXX_INFO(logger, "Got std input event. Terminating myself.");
      // Send SIGTERM to ourselves
      // to initiate the normal shutdown process
      assert(one_byte_buffer == 1);
      kill(getpid(), SIGTERM);
   } else {
      LOG4CXX_INFO(logger, "Got std input error: "
                   << error.value() << " : " << error.message()
                   << ". Killing myself.");
      // let us die
      kill(getpid(), SIGKILL);
   }
}

void NetworkManager::startAccept()
{
   assert(_selfNodeID != INVALID_NODE);
   boost::shared_ptr<Connection> newConnection(new Connection(*this, _selfNodeID));
   _acceptor.async_accept(newConnection->getSocket(),
                          bind(&NetworkManager::handleAccept, this,
                               newConnection, asio::placeholders::error));
}

void NetworkManager::handleAccept(boost::shared_ptr<Connection> newConnection, const boost::system::error_code& error)
{
    if (error == boost::system::errc::operation_canceled)
        return;

    if (!error)
    {
        LOG4CXX_DEBUG(logger, "Waiting for the first message");
        newConnection->start();
        startAccept();
    }
    else
    {
        stringstream s;
        s << "Error # " << error.value() << " : " << error.message() << " when accepting connection";
        LOG4CXX_ERROR(logger, s.str());
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CANT_ACCEPT_CONNECTION) << error.value() << error.message();
    }
}

void NetworkManager::handleMessage(boost::shared_ptr< Connection > connection, const boost::shared_ptr<MessageDesc>& messageDesc)
{
   if (_shutdown) {
      handleShutdown();
      return;
   }
   if (messageDesc->getMessageType() == mtAlive) {
      return;
   }
   try
   {
      NetworkMessageFactory::MessageHandler handler;

      if (!handleNonSystemMessage(messageDesc, handler)) {
         assert(!handler);

         if (messageDesc->getSourceNodeID() == CLIENT_NODE)
         {
             shared_ptr<Job> job = shared_ptr<Job>(new ClientMessageHandleJob(connection, messageDesc));
             _jobQueue->pushJob(job);
         }
         else
         {
             shared_ptr<MessageHandleJob> job = shared_ptr<MessageHandleJob>(new MessageHandleJob(messageDesc));
             job->dispatch(_jobQueue);
         }
         handler = bind(&NetworkManager::publishMessage, _1);
      }
      if (handler) {
         dispatchMessageToListener(messageDesc, handler);
      }
   }
   catch (const Exception& e)
   {
      // It's possible to continue of message handling for other queries so we just logging error message.
      NodeID nodeId = messageDesc->getSourceNodeID();
      MessageType messageType = static_cast<MessageType>(messageDesc->getMessageType());
      QueryID queryId = messageDesc->getQueryID();

      LOG4CXX_ERROR(logger, "Exception in message handler: messageType = "<< messageType);
      LOG4CXX_ERROR(logger, "Exception in message handler: source node ID = "
                    << string((nodeId == CLIENT_NODE)
                              ? std::string("CLIENT")
                              : boost::str(boost::format("node %lld") % nodeId)));
      LOG4CXX_ERROR(logger, "Exception in message handler: " << e.what());

      assert(nodeId != CLIENT_NODE);

      if (messageType != mtError && messageType != mtCancelQuery && messageType != mtAbort
          && queryId != 0 && queryId != INVALID_QUERY_ID
          && nodeId != INVALID_NODE && nodeId != _selfNodeID && nodeId < _nodes.size())
       {
          boost::shared_ptr<MessageDesc> errorMessage = makeErrorMessageFromException(e, queryId);
          _sendMessage(nodeId, errorMessage);
          LOG4CXX_DEBUG(logger, "Error returned to sender")
       }
   }
}

bool
NetworkManager::handleNonSystemMessage(const boost::shared_ptr<MessageDesc>& messageDesc,
                                       NetworkMessageFactory::MessageHandler& handler)
{
   assert(messageDesc);
   MessageID msgID = messageDesc->getMessageType();
   if (msgID < mtSystemMax) {
      return false;
   }
   handler = _msgHandlerFactory->getMessageHandler(msgID);
   if (handler.empty()) {
      stringstream s;
      s <<  "Registered message handler (MsgID="<< msgID <<") is empty!";
      LOG4CXX_WARN(logger, s.str());
      return true;
   }
   return true;
}

void NetworkManager::publishMessage(const boost::shared_ptr<MessageDescription>& msgDesc)
{
   boost::shared_ptr<const MessageDescription> msg(msgDesc);
   Notification<MessageDescription> event(msg);
   event.publish();
}

void NetworkManager::dispatchMessageToListener(const boost::shared_ptr<MessageDesc>& messageDesc,
                                               NetworkMessageFactory::MessageHandler& handler)
{
    // no locks must be held
    boost::shared_ptr<MessageDescription> msgDesc(new DefaultMessageDescription(
                                                      messageDesc->getSourceNodeID(),
                                                      messageDesc->getMessageType(),
                                                      messageDesc->getRecord<Message>(),
                                                      messageDesc->getBinary()));
    // invoke in-line, the handler is not expected to block
    handler(msgDesc);
}

void
NetworkManager::_sendMessage(NodeID targetNodeID,
                             boost::shared_ptr<MessageDesc>& messageDesc)
{
    if (_shutdown) {
        handleShutdown();
        abortMessageQuery(messageDesc->getQueryID());
        return;
    }
    ScopedMutexLock mutexLock(_mutex);

    assert(_selfNodeID != INVALID_NODE);
    assert(targetNodeID != _selfNodeID);
    assert(targetNodeID < _nodes.size());

    // Opening connection if it's not opened yet
    boost::shared_ptr<Connection> connection = _outConnections[targetNodeID];
    if (!connection)
    {
        getNodes(false);
        connection = shared_ptr<Connection>(new Connection(*this, _selfNodeID, targetNodeID));
        assert(_nodes[targetNodeID].getNodeId() == targetNodeID);
        _outConnections[targetNodeID] = connection;
        connection->connectAsync(_nodes[targetNodeID].getHost(), _nodes[targetNodeID].getPort());
    }
    // Sending message through connection
    connection->sendMessage(messageDesc);
}

void
NetworkManager::sendMessage(NodeID targetNodeID,
                            boost::shared_ptr<MessageDesc>& messageDesc,
                            bool waitSent)
{
    getNodes(false);
   _sendMessage(targetNodeID, messageDesc);

    if (waitSent) {
       messageDesc->waitSent();
    }
}

void NetworkManager::broadcast(boost::shared_ptr<MessageDesc>& messageDesc)
{
   ScopedMutexLock mutexLock(_mutex);
   getNodes(false);

   for (Nodes::const_iterator i = _nodes.begin();
        i != _nodes.end(); ++i) {
      NodeID targetNodeID = i->getNodeId();
      if (targetNodeID != _selfNodeID) {
        _sendMessage(targetNodeID, messageDesc);
      }
   }
}

void NetworkManager::sendOutMessage(boost::shared_ptr<MessageDesc>& messageDesc, bool waitSent)
{
    if (!messageDesc->getQueryID())
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MESSAGE_MISSED_QUERY_ID);
   boost::shared_ptr<Query> query = Query::getQueryByID(messageDesc->getQueryID());
   size_t nodesCount = query->getNodesCount();
   NodeID myNodeID   = query->getNodeID();
   assert(nodesCount>0);
   {
      ScopedMutexLock mutexLock(_mutex);
      for (size_t targetNodeID = 0; targetNodeID < nodesCount; ++targetNodeID)
        {
           if (targetNodeID != myNodeID) {
              send(targetNodeID, messageDesc);
           }
        }
    }

   if (waitSent) {
      messageDesc->waitSent(nodesCount-1);
   }
}

void NetworkManager::getNodes(bool force)
{
    ScopedMutexLock mutexLock(_mutex);

    // The node membership does not change in RQ
    // when it does we may need to change this logic
    if (force || _nodes.size() == 0)
    {
        _nodes.clear();
        SystemCatalog::getInstance()->getNodes(_nodes);
    }
}

size_t NetworkManager::getPhysicalNodes(std::vector<NodeID>& nodes)
{
   nodes.clear();
   ScopedMutexLock mutexLock(_mutex);
   getNodes(false);
   nodes.reserve(_nodes.size());
   for (Nodes::const_iterator i = _nodes.begin();
        i != _nodes.end(); ++i) {
      nodes.push_back(i->getNodeId());
   }
   return nodes.size();
}

void
NetworkManager::send(NodeID targetNodeID,
                     boost::shared_ptr<MessageDesc>& msg)
{
   assert(msg);
   if (!msg->getQueryID())
       throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MESSAGE_MISSED_QUERY_ID);
   boost::shared_ptr<Query> query = Query::getQueryByID(msg->getQueryID());
   NodeID target = query->mapLogicalToPhysical(targetNodeID);
   sendMessage(target, msg);
}

void NetworkManager::send(NodeID targetNodeID, boost::shared_ptr<SharedBuffer> data, boost::shared_ptr< Query> query)
{
    boost::shared_ptr<MessageDesc> msg = boost::make_shared<MessageDesc>(mtMPISend, data);
    msg->setQueryID(query->getQueryID());
    NodeID target = query->mapLogicalToPhysical(targetNodeID);
    sendMessage(target, msg);
}

boost::shared_ptr<SharedBuffer> NetworkManager::receive(NodeID sourceNodeID, boost::shared_ptr< Query> query)
{
   Semaphore::ErrorChecker ec = bind(&Query::validate, query);
   query->_receiveSemaphores[sourceNodeID].enter(ec);
    ScopedMutexLock mutexLock(query->_receiveMutex);
    if (query->_receiveMessages[sourceNodeID].size() <= 0) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_NODE_OFFLINE) << sourceNodeID;
    }
    boost::shared_ptr<SharedBuffer> res = query->_receiveMessages[sourceNodeID].front()->getBinary();
    query->_receiveMessages[sourceNodeID].pop_front();

    return res;
}

void NetworkManager::_handleReconnect()
{
   StatisticsScope scope;
   set<NodeID> brokenNodes;
   ScopedMutexLock mutexLock(_mutex);
   if (_shutdown) {
      handleShutdown();
      return;
   }
   if(_brokenNodes.size() <= 0 ) {
      return;
   }

   getNodes(true); // force to refresh nodes description

   brokenNodes.swap(_brokenNodes);

   for (set<NodeID>::const_iterator iter = brokenNodes.begin();
        iter != brokenNodes.end(); ++iter) {
      const NodeID& i = *iter;
      assert(i < _nodes.size());
      assert(_nodes[i].getNodeId() == i);
      ConnectionMap::iterator connIter = _outConnections.find(i);

      if (connIter == _outConnections.end()) {
         continue;
      }
      shared_ptr<Connection>& connection = (*connIter).second;

      if (!connection) {
         _outConnections.erase(connIter);
         continue;
      }
      connection->connectAsync(_nodes[i].getHost(), _nodes[i].getPort());
   }
}

void NetworkManager::_handleLivenessNotification(boost::shared_ptr<const NodeLiveness>& liveInfo)
{
    if (logger->isDebugEnabled()) {
        ViewID viewId = liveInfo->getViewId();
        const NodeLiveness::LiveNodes& liveNodes = liveInfo->getLiveNodes();
        const NodeLiveness::DeadNodes& deadNodes = liveInfo->getDeadNodes();
        uint64_t ver = liveInfo->getVersion();

        LOG4CXX_DEBUG(logger, "New liveness information, viewID=" << viewId<<", ver="<<ver);
        for ( NodeLiveness::DeadNodes::const_iterator i = deadNodes.begin();
             i != deadNodes.end(); ++i) {
           LOG4CXX_DEBUG(logger, "Dead nodeID=" << (*i)->getNodeId());
           LOG4CXX_DEBUG(logger, "Dead genID=" << (*i)->getGenerationId());
        }
        for ( NodeLiveness::LiveNodes::const_iterator i = liveNodes.begin();
             i != liveNodes.end(); ++i) {
           LOG4CXX_DEBUG(logger, "Live nodeID=" << (*i)->getNodeId());
           LOG4CXX_DEBUG(logger, "Live genID=" << (*i)->getGenerationId());
        }
    }
    ScopedMutexLock mutexLock(_mutex);
    if (_shutdown) {
       handleShutdown();
       return;
    }
    if (_nodeLiveness &&
        _nodeLiveness->getVersion() == liveInfo->getVersion()) {
       assert(_nodeLiveness->isEqual(*liveInfo));
       return;
    }

    assert(!_nodeLiveness ||
           _nodeLiveness->getVersion() < liveInfo->getVersion());
    _nodeLiveness = liveInfo;

    _livenessHandleScheduler->schedule();
}

void NetworkManager::_handleLiveness()
{
   ScopedMutexLock mutexLock(_mutex);
   assert(_nodeLiveness);
   assert(_nodeLiveness->getNumNodes() == _nodes.size());
   const NodeLiveness::DeadNodes& deadNodes = _nodeLiveness->getDeadNodes();

   for (NodeLiveness::DeadNodes::const_iterator iter = deadNodes.begin();
        iter != deadNodes.end(); ++iter) {
      NodeID nodeID = (*iter)->getNodeId();
      ConnectionMap::iterator connIter = _outConnections.find(nodeID);

      if (connIter == _outConnections.end()) {
         continue;
      }

      shared_ptr<Connection>& connection = (*connIter).second;
      if (connection) {
         connection->disconnect();
         connection.reset();
      }
      _outConnections.erase(connIter);
   }
   if (deadNodes.size() > 0) {
      _livenessHandleScheduler->schedule();
   }
}

void NetworkManager::_handleAlive(const boost::system::error_code& error)
{
    if (error == boost::asio::error::operation_aborted) {
       return;
    }
    StatisticsScope scope;

    shared_ptr<MessageDesc> messageDesc = make_shared<MessageDesc>(mtAlive);

    ScopedMutexLock mutexLock(_mutex);
    if (_shutdown) {
       handleShutdown();
       return;
    }

    for (Nodes::const_iterator i = _nodes.begin();
        i != _nodes.end(); ++i) {
       if (i->getNodeId() != _selfNodeID) {
          _sendMessage(i->getNodeId(), messageDesc);
       }
    }
    _aliveTimer.expires_from_now(posix_time::seconds(5));
    _aliveTimer.async_wait(NetworkManager::handleAlive);
}

void NetworkManager::reconnect(NodeID nodeID)
{
   {
      ScopedMutexLock mutexLock(_mutex);
      _brokenNodes.insert(nodeID);
      if (_brokenNodes.size() > 1) {
         return;
      }
      _reconnectScheduler->schedule();
   }
}

void NetworkManager::cancelClientQuery(const QueryID& queryId)
{
   if (!queryId) {
      return;
   }

   LOG4CXX_WARN(logger, str(format("Cancel client query %lld on disconnect") % queryId));

   try {
      shared_ptr<Query> query = Query::getQueryByID(queryId, false);
      
      WorkQueue::WorkItem item = bind(&Query::handleCancel, query);

      _workQueue->enqueue(item);
   } catch (const WorkQueue::OverflowException& e) {
      LOG4CXX_ERROR(logger, "Overflow exception from the work queue: "<<e.what());
      // XXX TODO: deal with this exception
      assert(false);
   } catch (const scidb::SystemException& e) {
      if (e.getLongErrorCode() != SCIDB_LE_QUERY_NOT_FOUND
              && e.getLongErrorCode() != SCIDB_LE_QUERY_NOT_FOUND2) {
         throw;
      }
   }
}

void NetworkManager::abortMessageQuery(const QueryID& queryID)
{
   if (!queryID) {
      return;
   }
   LOG4CXX_ERROR(logger, "Query " << queryID << " is aborted on connection error");

   try {
      shared_ptr<Query> query = Query::getQueryByID(queryID, false);
      shared_ptr<scidb::WorkQueue> errorQ = query->getErrorQueue();
      if (errorQ) {
          WorkQueue::WorkItem item = bind(&Query::handleError, query,
                                          SYSTEM_EXCEPTION_SPTR(SCIDB_SE_NETWORK, SCIDB_LE_CONNECTION_ERROR2));
          errorQ->enqueue(item);
      } else {
          LOG4CXX_TRACE(logger, "Query " << queryID << " no longer has the queue for error reporting,"
                        " it must be no longer active");
      }
   } catch (const WorkQueue::OverflowException& e) {
      LOG4CXX_ERROR(logger, "Overflow exception from the work queue: "<<e.what());
      // XXX TODO: deal with this exception
      assert(false);
   } catch (const scidb::SystemException& e) {
      if (e.getLongErrorCode() != SCIDB_LE_QUERY_NOT_FOUND
              && e.getLongErrorCode() != SCIDB_LE_QUERY_NOT_FOUND2) {
         throw;
      }
   }
}

void Send(void* ctx, int node, void const* data, size_t size)
{
    NetworkManager::getInstance()->send(node, boost::shared_ptr< SharedBuffer>(new MemoryBuffer(data, size)), *(boost::shared_ptr< Query>*)ctx);
}


void Receive(void* ctx, int node, void* data, size_t size)
{
    boost::shared_ptr< SharedBuffer> buf =  NetworkManager::getInstance()->receive(node, *(boost::shared_ptr< Query>*)ctx);
    assert(buf->getSize() == size);
    memcpy(data, buf->getData(), buf->getSize());
}

void BufSend(NodeID target, boost::shared_ptr<SharedBuffer> data, boost::shared_ptr< Query> query)
{
    NetworkManager::getInstance()->send(target,data,query);
}

boost::shared_ptr<SharedBuffer> BufReceive(NodeID source, boost::shared_ptr< Query> query)
{
    return NetworkManager::getInstance()->receive(source,query);
}

bool
NetworkManager::DefaultNetworkMessageFactory::isRegistered(const MessageID& msgID)
{
   ScopedMutexLock mutexLock(_mutex);
   return (_msgHandlers.find(msgID) != _msgHandlers.end());
}

bool
NetworkManager::DefaultNetworkMessageFactory::addMessageType(const MessageID& msgID,
                                                             const MessageCreator& msgCreator,
                                                             const MessageHandler& msgHandler)
{
   if (msgID < mtSystemMax) {
      return false;
   }
   ScopedMutexLock mutexLock(_mutex);
   return  _msgHandlers.insert(
              std::make_pair(msgID,
                 std::make_pair(msgCreator, msgHandler))).second;
}

MessagePtr
NetworkManager::DefaultNetworkMessageFactory::createMessage(const MessageID& msgID)
{
   MessagePtr msgPtr;
   NetworkMessageFactory::MessageCreator creator;
   {
      ScopedMutexLock mutexLock(_mutex);
      MessageHandlerMap::const_iterator iter = _msgHandlers.find(msgID);
      if (iter != _msgHandlers.end()) {
         creator = iter->second.first;
      }
   }
   if (!creator.empty()) {
      msgPtr = creator(msgID);
   }
   return msgPtr;
}

NetworkMessageFactory::MessageHandler
NetworkManager::DefaultNetworkMessageFactory::getMessageHandler(const MessageID& msgType)
{
   ScopedMutexLock mutexLock(_mutex);

   MessageHandlerMap::const_iterator iter = _msgHandlers.find(msgType);
   if (iter != _msgHandlers.end()) {
      NetworkMessageFactory::MessageHandler handler = iter->second.second;
      return handler;
   }
   NetworkMessageFactory::MessageHandler emptyHandler;
   return emptyHandler;
}

/**
 * @see Network.h
 */
boost::shared_ptr<NetworkMessageFactory> getNetworkMessageFactory()
{
   return NetworkManager::getInstance()->getNetworkMessageFactory();
}

/**
 * @see Network.h
 */
boost::asio::io_service& getIOService()
{
   return NetworkManager::getInstance()->getIOService();
}

/**
 * @see Network.h
 */
void sendAsync(NodeID targetNodeID,
               MessageID msgID,
               MessagePtr record,
               boost::asio::const_buffer& binary)
{
   shared_ptr<SharedBuffer> payload;
   if (asio::buffer_size(binary) > 0) {
      assert(asio::buffer_cast<const void*>(binary));
      payload = shared_ptr<SharedBuffer>(new MemoryBuffer(asio::buffer_cast<const void*>(binary),
                                                          asio::buffer_size(binary)));
   }
   shared_ptr<MessageDesc> msgDesc =
      shared_ptr<Connection::ServerMessageDesc>(new Connection::ServerMessageDesc(payload));

   msgDesc->initRecord(msgID);
   MessagePtr msgRecord = msgDesc->getRecord<Message>();
   const google::protobuf::Descriptor* d1 = msgRecord->GetDescriptor();
   assert(d1);
   const google::protobuf::Descriptor* d2 = record->GetDescriptor();
   assert(d2);
   if (d1->full_name().compare(d2->full_name()) != 0) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_UNKNOWN_MESSAGE_TYPE);
   }
   msgRecord->CopyFrom(*record.get());

   NetworkManager::getInstance()->sendMessage(targetNodeID, msgDesc);
}

uint32_t getLivenessTimeout()
{
   return Config::getInstance()->getOption<int>(CONFIG_LIVENESS_TIMEOUT);
}

boost::shared_ptr<Scheduler> getScheduler(Scheduler::Work& workItem, time_t period)
{
   if (!workItem) {
      throw USER_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_INVALID_SHEDULER_WORK_ITEM);
   }
   if (period < 1) {
      throw USER_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_INVALID_SHEDULER_PERIOD);
   }
   shared_ptr<scidb::Scheduler> scheduler(new ThrottledScheduler(period, workItem,
                                          NetworkManager::getInstance()->getIOService()));
   return scheduler;
}

} // namespace scidb
