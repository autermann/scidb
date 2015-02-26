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
 * @file Query.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Implementation of query context methods
 */
#ifdef __APPLE__
#include <malloc/malloc.h>
#else
#include <malloc.h>
#endif

#include <time.h>
#include <boost/make_shared.hpp>
#include <boost/serialization/string.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <log4cxx/logger.h>

#include "query/QueryProcessor.h"
#include "query/parser/QueryParser.h"
#include "query/parser/AST.h"
#include "query/RemoteArray.h"
#include "smgr/io/Storage.h"
#include "network/NetworkManager.h"
#include "system/SciDBConfigOptions.h"
#include "system/SystemCatalog.h"
#include "system/Cluster.h"
#include "util/iqsort.h"
#include <system/Exceptions.h>
#include <system/System.h>
#include <network/MessageUtils.h>

using namespace std;
using namespace boost;

namespace boost
{
   template<>
   bool operator< (shared_ptr<scidb::SystemCatalog::LockDesc> const& l,
                   shared_ptr<scidb::SystemCatalog::LockDesc> const& r) {
      if (!l || !r) {
         assert(false);
         return false;
      }
      return (l->getArrayName() < r->getArrayName());
   }

   template<>
   bool operator== (shared_ptr<scidb::SystemCatalog::LockDesc> const& l,
                    shared_ptr<scidb::SystemCatalog::LockDesc> const& r) {
         assert(false);
         throw SYSTEM_EXCEPTION(scidb::SCIDB_SE_INTERNAL, scidb::SCIDB_LE_NOT_IMPLEMENTED);
         return false;
   }

   template<>
   bool operator!= (shared_ptr<scidb::SystemCatalog::LockDesc> const& l,
                    shared_ptr<scidb::SystemCatalog::LockDesc> const& r) {
       return !operator==(l,r);
   }
} // namespace boost

namespace std
{
   bool less<shared_ptr<scidb::SystemCatalog::LockDesc> >::operator() (
                               const shared_ptr<scidb::SystemCatalog::LockDesc>& l,
                               const shared_ptr<scidb::SystemCatalog::LockDesc>& r) const
   {
      return l < r;
   }
}

namespace scidb
{

// Logger for query processor. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.processor"));


// Query class implementation
Mutex Query::queriesMutex;
map<QueryID, boost::shared_ptr<Query> > Query::_queries;
uint32_t Query::nextID = 0;

void Query::PendingRequests::increment()
{
    ScopedMutexLock cs(mutex);
    nReqs += 1;
}

bool Query::PendingRequests::decrement()
{
    ScopedMutexLock cs(mutex);
    if (--nReqs == 0 && sync) {
        sync = false;
        return true;
    }
    return false;
}

bool Query::PendingRequests::test()
{
    ScopedMutexLock cs(mutex);
    if (nReqs != 0) {
        sync = true;
        return false;
    }
    return true;
}


boost::shared_ptr<Query> Query::createDetached()
{
    boost::shared_ptr<Query> query = boost::shared_ptr<Query>(new Query());

    const size_t smType = Config::getInstance()->getOption<int>(CONFIG_STATISTICS_MONITOR);
    if (smType) {
        const string& smParams = Config::getInstance()->getOption<string>(CONFIG_STATISTICS_MONITOR_PARAMS);
        query->statisticsMonitor = StatisticsMonitor::create(smType, smParams);
    }

    return query;
}

Query::Query():
    _queryID(INVALID_QUERY_ID), _nodeID(INVALID_NODE), _coordinatorID(INVALID_NODE), _error(SYSTEM_EXCEPTION_SPTR(SCIDB_E_NO_ERROR, SCIDB_E_NO_ERROR)),
    completionStatus(INIT), _commitState(UNKNOWN), _creationTime(time(NULL)), _useCounter(0), _doesExclusiveArrayAccess(false), isDDL(false)
{
}

void Query::init(QueryID queryID, NodeID coordID, NodeID localNodeID,
                 shared_ptr<const NodeLiveness> liveness)
{
   assert(liveness);
   assert(localNodeID != INVALID_NODE);
   {
      ScopedMutexLock cs(errorMutex);

      validate();

      assert( _queryID == INVALID_QUERY_ID || _queryID == queryID);
      _queryID = queryID;
      assert( _queryID != INVALID_QUERY_ID);

      assert(!_coordinatorLiveness);
      _coordinatorLiveness = liveness;
      assert(_coordinatorLiveness);

      size_t nNodes = _coordinatorLiveness->getNumLive();
      if (nNodes <= 0)
          throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_LIVENESS_EMPTY);

      assert(_liveNodes.size() == 0);
      _liveNodes.clear();
      _liveNodes.reserve(nNodes);

      const NodeLiveness::LiveNodes& liveNodes = _coordinatorLiveness->getLiveNodes();
      assert(liveNodes.size() == nNodes);
      for ( NodeLiveness::LiveNodes::const_iterator iter = liveNodes.begin();
        iter != liveNodes.end(); ++iter) {
         _liveNodes.push_back((*iter)->getNodeId());
      }
      _nodeID = mapPhysicalToLogical(localNodeID);
      assert(_nodeID != INVALID_NODE);
      assert(_nodeID < nNodes);

      if (coordID == COORDINATOR_NODE) {
         _coordinatorID = COORDINATOR_NODE;
      } else {
         _coordinatorID = mapPhysicalToLogical(coordID);
         assert(_coordinatorID < nNodes);
      }

      _remoteArrays.resize(nNodes);
      _receiveSemaphores.resize(nNodes);
      _receiveMessages.resize(nNodes);
      chunkReqs.resize(nNodes);
      chunkReplicasReqs.resize(nNodes);
      Finalizer f = bind(&Query::destroyFinalizer, _1);
      pushFinalizer(f);
      _errorQueue = NetworkManager::getInstance()->createWorkQueue();
      assert(_errorQueue);
      _mpiReceiveQueue = NetworkManager::getInstance()->createWorkQueue();
      assert(_mpiReceiveQueue);
   }
   // register for notifications
   NodeLivenessNotification::PublishListener listener =
      boost::bind(&Query::handleLivenessNotification, shared_from_this(), _1);
   _livenessListenerID = NodeLivenessNotification::addPublishListener(listener);

   LOG4CXX_DEBUG(logger, "Initialized query (" << queryID << ")");
}

bool Query::insert(QueryID queryID, shared_ptr<Query>& query)
{
   // queriesMutex must be locked
   pair<map<QueryID, shared_ptr<Query> >::iterator, bool > res =
   _queries.insert(make_pair(queryID, query));

   if (res.second) {
      query = createDetached();
      assert(query);
      query->_queryID = queryID;
      res.first->second = query;
      LOG4CXX_DEBUG(logger, "Allocating query (" << queryID << ")");
      LOG4CXX_DEBUG(logger, "Number of allocated queries = " << _queries.size());
   } else {
      query = res.first->second;
   }
   return res.second;
}

QueryID Query::generateID()
{
   const uint64_t nodeID = StorageManager::getInstance().getNodeId();
   const uint32_t timeVal = time(NULL);
   const uint32_t clockVal = clock();

   ScopedMutexLock mutexLock(queriesMutex);

   // It allows us to have about 16 000 000 nodes while about 10 000 years
   QueryID queryID = ((nodeID+1) << 40) | (timeVal + clockVal + nextID++);
   LOG4CXX_DEBUG(logger, "Generated queryID: nodeID=" << nodeID << ", time=" << timeVal
                 << ", clock=" << clockVal<< ", nextID=" << nextID - 1
                 << ", queryID=" << queryID);
   return queryID;
}

boost::shared_ptr<Query> Query::create(QueryID queryID)
{
    assert(queryID > 0 && queryID != INVALID_QUERY_ID);
    boost::shared_ptr<Query> query;
    {
       ScopedMutexLock mutexLock(queriesMutex);

       bool rc = insert(queryID, query);
       if (!rc)
           throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_DUPLICATE_QUERY_ID);
    }
    assert(query);
    assert(query->_queryID == queryID);

    boost::shared_ptr<const scidb::NodeLiveness> myLiveness =
       Cluster::getInstance()->getNodeLiveness();
    assert(myLiveness);

    query->init(query->_queryID, COORDINATOR_NODE,
                Cluster::getInstance()->getLocalNodeId(), myLiveness);
    return query;
}

void Query::start()
{
    ScopedMutexLock cs(errorMutex);
    assert(completionStatus == INIT);
    completionStatus = START;
    if (SCIDB_E_NO_ERROR != _error->getLongErrorCode())
    {
        _error->raise();
    }
}

void Query::done()
{
    ScopedMutexLock cs(errorMutex);

    if (SCIDB_E_NO_ERROR != _error->getLongErrorCode())
    {
        _error->raise();
    }
    completionStatus = OK;
}

void Query::done(const shared_ptr<Exception> unwindException)
{
   {
      ScopedMutexLock cs(errorMutex);
      if (SCIDB_E_NO_ERROR == _error->getLongErrorCode())
      {
          _error = unwindException;
      }
      completionStatus = ERROR;
   }
   handleError(unwindException);
}

bool Query::doesExclusiveArrayAccess()
{
    return _doesExclusiveArrayAccess;
}

boost::shared_ptr<SystemCatalog::LockDesc>
Query::requestLock(boost::shared_ptr<SystemCatalog::LockDesc>& requestedLock)
{
    assert(requestedLock);
    ScopedMutexLock cs(errorMutex);

    if (requestedLock->getLockMode() > SystemCatalog::LockDesc::RD) {
        _doesExclusiveArrayAccess = true;
    }

    pair<QueryLocks::const_iterator, bool> res = _requestedLocks.insert(requestedLock);
    if (res.second) {
        LOG4CXX_TRACE(logger, "Requested lock: " << requestedLock->toString() << " inserted");
        return requestedLock;
    }

    if ((*(res.first))->getLockMode() < requestedLock->getLockMode()) {
        _requestedLocks.erase(res.first);
        res = _requestedLocks.insert(requestedLock);
        assert(res.second);
        LOG4CXX_TRACE(logger, "New requested lock: " << requestedLock->toString() << " inserted");
    }
    return (*(res.first));
}

void Query::handleError(shared_ptr<Exception> unwindException)
{
    assert(unwindException);
    assert(unwindException->getLongErrorCode() != SCIDB_E_NO_ERROR);

    QueryID queryId = INVALID_QUERY_ID;
    deque<shared_ptr<ErrorHandler> > errorHandlersOnStack;
    deque<Finalizer> finalizersOnStack;
    {
        ScopedMutexLock cs(errorMutex);

        if (_error->getLongErrorCode() == SCIDB_E_NO_ERROR)
        {
            _error = unwindException;
            //XXX TODO: exceptions should be fixed to contain the right query ID
            //XXX assert(_error->getQueryId() == _queryID);
        }
        if (completionStatus == START)
        {
            LOG4CXX_DEBUG(logger, "Query (" << _queryID << ") is still in progress");
            return;
        }
        assert(_error);
        assert(_error->getLongErrorCode() != SCIDB_E_NO_ERROR);

        if (completionStatus == ERROR || // error before reporting success to the coordinator/client
            _commitState == ABORTED)  // coordinator requested abort
        {
            assert(_commitState != COMMITTED);
            LOG4CXX_INFO(logger, "Query (" << _queryID << ") error handlers ("
                         << _errorHandlers.size() << ") are being executed");
            errorHandlersOnStack.swap(_errorHandlers);

            finalizersOnStack.swap(_finalizers);

            if (_coordinatorID != COORDINATOR_NODE) {
                queryId = _queryID;
                unwindException = _error;
            } else {
                assert(completionStatus != OK);
            }
        }
    }

    if (queryId != INVALID_QUERY_ID) {
        // Make sure everybody knows what caused this node to error out
        boost::shared_ptr<MessageDesc> msg =
        makeErrorMessageFromException((*unwindException), queryId);
        NetworkManager::getInstance()->broadcast(msg);
    }

    invokeErrorHandlers(errorHandlersOnStack);
    errorHandlersOnStack.clear();

    if (queryId != INVALID_QUERY_ID) {
        // A worker node is allowed to free the query on error
        // because the query will not commit and the worker has
        // no user to report the query status (unlike the coordinator)
        freeQuery(queryId);
    }
    invokeFinalizers(finalizersOnStack);
}

bool Query::checkFinalState()
{
   ScopedMutexLock cs(errorMutex);
   return ( _finalizers.empty() &&
            ((completionStatus == INIT &&
              _error->getLongErrorCode() != SCIDB_E_NO_ERROR) ||
             completionStatus == OK ||
             completionStatus == ERROR) );
}
void Query::invokeFinalizers(deque<Finalizer>& finalizers)
{
   assert(finalizers.empty() || checkFinalState());
   for (deque<Finalizer>::reverse_iterator riter = finalizers.rbegin();
        riter != finalizers.rend(); riter++) {
      Finalizer& fin = *riter;
      if (!fin) {
         continue;
      }
      try {
         fin(shared_from_this());
      } catch (const scidb::Exception& e) {
         LOG4CXX_ERROR(logger, "Query (" << _queryID
                       << ") finalizer failed:"
                       << e.what());
      }
   }
}

void Query::invokeErrorHandlers(deque<shared_ptr<ErrorHandler> >& errorHandlers)
{
    RWLock::ErrorChecker noopEc;
    ScopedRWLockWrite exclusive(queryLock, noopEc);
    for (deque<shared_ptr<ErrorHandler> >::reverse_iterator riter = errorHandlers.rbegin();
         riter != errorHandlers.rend(); riter++) {
        shared_ptr<ErrorHandler>& eh = *riter;
        try {
            eh->handleError(shared_from_this());
        } catch (const scidb::Exception& e) {
            LOG4CXX_ERROR(logger, "Query (" << _queryID
                          << ") error handler failed:"
                          << e.what());
        }
    }
}

void Query::handleCancel()
{
   QueryID queryId = INVALID_QUERY_ID;
   boost::shared_ptr<MessageDesc> msg;
   deque<Finalizer> finalizers;
   shared_ptr<Exception> abortError;
   {
      ScopedMutexLock cs(errorMutex);
      queryId = _queryID;
      handleCancelOnCoordinator(msg, finalizers, abortError);
   }
   assert(queryId != INVALID_QUERY_ID);

   if (abortError) {
       handleError(abortError);
   }
   if (msg && !isDDL) {
       // broadcast abort/commit
       NetworkManager::getInstance()->broadcast(msg);
   }

   freeQuery(queryId);

   invokeFinalizers(finalizers);
}

void Query::handleCancelOnCoordinator(boost::shared_ptr<MessageDesc>& msg,
                                      deque<Finalizer>& finalizers,
                                      shared_ptr<Exception>& abortError)
{
   // errorMutex must be locked
   LOG4CXX_DEBUG(logger, "Query (" << _queryID << ") is cancelled by the client");
   assert(_coordinatorID == COORDINATOR_NODE);

   if (completionStatus == OK)
   {
       assert(_commitState != ABORTED);
       if (logger->isWarnEnabled() && _commitState == COMMITTED) {
           LOG4CXX_WARN(logger, "Query (" << _queryID << ") is already commited");
       }
       _commitState = COMMITTED;
       // query completed successfully, let the workers know
       msg = makeCommitMessage(_queryID);

       // discard error handlers
       deque<shared_ptr<ErrorHandler> > errorHandlersOnStack;
       errorHandlersOnStack.swap(_errorHandlers);

       finalizers.swap(_finalizers);
   } else {
       assert(_commitState != COMMITTED);

       if (logger->isWarnEnabled() && _commitState == ABORTED) {
           LOG4CXX_WARN(logger, "Query (" << _queryID << ") is already aborted");
       }
       _commitState = ABORTED;
       abortError = (SYSTEM_EXCEPTION_SPTR(SCIDB_SE_QPROC, SCIDB_LE_QUERY_CANCELLED) << _queryID);
       msg = makeAbortMessage(_queryID);
   }
}

void Query::handleAbort()
{
    QueryID queryId = INVALID_QUERY_ID;
    {
        ScopedMutexLock cs(errorMutex);

        queryId = _queryID;
        LOG4CXX_DEBUG(logger, "Query (" << _queryID << ") is aborted on worker");

        assert (getNodeID() != getCoordinatorID());
        if (_commitState == COMMITTED) {
            LOG4CXX_ERROR(logger, "Query (" << _queryID
                          << ") cannot be aborted after commit."
                          << " completion status=" << completionStatus
                          << " commit status=" << _commitState
                          << " error=" << _error->getLongErrorCode());
            throw (SYSTEM_EXCEPTION_SPTR(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_COMMIT_STATE) << _queryID << "abort");
            assert(false);
        }
        _commitState = ABORTED;
    }
    assert(queryId != INVALID_QUERY_ID);
    shared_ptr<Exception> e = (SYSTEM_EXCEPTION_SPTR(SCIDB_SE_QPROC, SCIDB_LE_QUERY_CANCELLED) << queryId);
    handleError(e);
}

void Query::handleCommit()
{
    QueryID queryId = INVALID_QUERY_ID;
    deque<Finalizer> finalizers;
    {
        ScopedMutexLock cs(errorMutex);

        queryId = _queryID;
        LOG4CXX_DEBUG(logger, "Query (" << _queryID << ") is committed on worker");

        assert (getNodeID() != getCoordinatorID());

        if (completionStatus != OK ||
            _commitState == ABORTED) {

            LOG4CXX_ERROR(logger, "Query (" << _queryID
                          << ") cannot be committed after abort."
                          << " completion status=" << completionStatus
                          << " commit status=" << _commitState
                          << " error=" << _error->getLongErrorCode());
            throw (SYSTEM_EXCEPTION_SPTR(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_COMMIT_STATE) << _queryID << "commit");
            assert(false);
        }

        _commitState = COMMITTED;

        // The coordinator told us everything is OK,
        // no other errors can reverse that decision
        deque<shared_ptr<ErrorHandler> > errorHandlersOnStack;
        errorHandlersOnStack.swap(_errorHandlers);
        finalizers.swap(_finalizers);
    }
    assert(queryId != INVALID_QUERY_ID);
    freeQuery(queryId);
    invokeFinalizers(finalizers);
}

void Query::handleLivenessNotification(boost::shared_ptr<const NodeLiveness>& newLiveness)
{
   ScopedMutexLock cs(errorMutex);

   assert(newLiveness->getVersion() >= _coordinatorLiveness->getVersion());

   if (newLiveness->getVersion() == _coordinatorLiveness->getVersion()) {
      assert(newLiveness->isEqual(*_coordinatorLiveness));
      return;
   }

   LOG4CXX_ERROR(logger, "Query " << _queryID << " is aborted on changed liveness");

   if (_error->getLongErrorCode() == SCIDB_E_NO_ERROR)
   {
       _error = SYSTEM_EXCEPTION_SPTR(SCIDB_SE_QPROC, SCIDB_LE_NO_QUORUM);
   }

   bool isAbort = false;
   boost::shared_ptr<Exception> err = _error;
   if (_coordinatorID != COORDINATOR_NODE) {

       NodeID coordId = getPhysicalCoordinatorID();
       NodeLiveness::NodePtr newCoordState = newLiveness->find(coordId);
       isAbort = newCoordState->isDead();
       if (!isAbort) {
           NodeLiveness::NodePtr oldCoordState = _coordinatorLiveness->find(coordId);
           isAbort = (newCoordState != oldCoordState);
       }
   }

   if (!_errorQueue) {
       LOG4CXX_TRACE(logger, "Liveness change will not be handled for a deallocated query (" << _queryID << ")");
       return;
   }
   WorkQueue::WorkItem item = bind(&Query::handleError, shared_from_this(), err);
   try {
      _errorQueue->enqueue(item);

       if (isAbort) {
           // if the coordinator is dead, we abort the query
           LOG4CXX_TRACE(logger, "Query " << _queryID << " is aborted due to a change in coordinator liveness");
           item = bind(&Query::handleAbort, shared_from_this());
           _errorQueue->enqueue(item);
       }
   } catch (const WorkQueue::OverflowException& ) {
      LOG4CXX_ERROR(logger, "Overflow exception from the work queue");
      // XXX TODO: deal with this exception
      assert(false);
   }
}

NodeID Query::getPhysicalCoordinatorID()
{
   ScopedMutexLock cs(errorMutex);
   if (_coordinatorID == COORDINATOR_NODE) {
      return COORDINATOR_NODE;
   }
   if (_coordinatorID == INVALID_NODE) {
      return INVALID_NODE;
   }   
   assert(_liveNodes.size() > 0);
   assert(_liveNodes.size() > _coordinatorID);
   return  _liveNodes[_coordinatorID];
}

NodeID Query::mapLogicalToPhysical(NodeID node)
{
   if (node == INVALID_NODE) {
      return node;
   }
   ScopedMutexLock cs(errorMutex);
   assert(_liveNodes.size() > 0);
   if (node >= _liveNodes.size())
       throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_NODE_OFFLINE) << node;
   if (SCIDB_E_NO_ERROR != _error->getLongErrorCode()) {
      _error->raise();
   }
   node = _liveNodes[node];
   return node;
}

NodeID Query::mapPhysicalToLogical(NodeID nodeID)
{
   ScopedMutexLock cs(errorMutex);
   assert(_liveNodes.size() > 0);
   size_t index=0;
   bool found = bsearch(_liveNodes, nodeID, index);
   if (!found)
       throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_NODE_OFFLINE) << nodeID;

   return index;
}

bool Query::isPhysicalNodeDead(NodeID node)
{
   ScopedMutexLock cs(errorMutex);
   if (SCIDB_E_NO_ERROR != _error->getLongErrorCode()) {
      _error->raise();
   }
   bool isDead = _coordinatorLiveness->isDead(node);
   assert(isDead ||
          _coordinatorLiveness->find(node));
   return isDead;
}

#ifndef __APPLE__
static __thread QueryID currentQueryID = 0;
QueryID Query::getCurrentQueryID()
{
    return currentQueryID;
}

void Query::setCurrentQueryID(QueryID queryID)
{
    currentQueryID = queryID;
}
#else
static ThreadContext<QueryID> currentQueryID;

QueryID Query::getCurrentQueryID()
{
    QueryID* ptr = currentQueryID;
    return ptr == NULL ? 0 : *ptr;
}

void Query::setCurrentQueryID(QueryID queryID)
{
    QueryID* ptr = currentQueryID;
    if (ptr == NULL) {
        ptr = new QueryID();
        currentQueryID = ptr;
    }
    *ptr = queryID;
}
#endif

Query::~Query()
{
    if (statisticsMonitor) {
        statisticsMonitor->pushStatistics(*this);
    }
    runMultinodePostOddJob();
}

boost::shared_ptr<Query> Query::getQueryByID(QueryID queryID, bool create, bool raise)
{
    shared_ptr<Query> query;
    ScopedMutexLock mutexLock(queriesMutex);

    if (create) {
       insert(queryID, query);
       assert(query);
       return query;
    }
    map<QueryID, shared_ptr<Query> >::const_iterator q = _queries.find(queryID);
    if (q != _queries.end()) {
        return q->second;
    }
    LOG4CXX_DEBUG(logger, "Query " << queryID << " is not found");
    if (raise) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_QUERY_NOT_FOUND) << queryID;
    }
    assert(!query);
    return query;
}



void Query::freeQueries()
{
    map<QueryID, shared_ptr<Query> > queries;
    {
        ScopedMutexLock mutexLock(queriesMutex);
        queries.swap(_queries);
    }
    for (map<QueryID, shared_ptr<Query> >::iterator q = queries.begin();
         q != queries.end(); ++q) {
        LOG4CXX_DEBUG(logger, "Deallocating query (" << q->second->getQueryID() << ")");
        q->second->handleAbort();
    }
}

#ifndef SCIDB_CLIENT
extern size_t totalDBChunkAllocatedSize;
extern size_t totalMemChunkAllocatedSize;
#endif

void dumpMemoryUsage(const QueryID queryId)
{
#ifdef DEBUG
#ifdef HAVE_MALLOC_STATS
    if (Config::getInstance()->getOption<bool>(CONFIG_OUTPUT_PROC_STATS)) {
        cerr << "Stats after query ID ("<<queryId<<") :" << endl;
        malloc_stats();
#ifndef SCIDB_CLIENT
        cerr << "Allocated size for DBChunks: " << totalDBChunkAllocatedSize << ", allocated size for MemChunks: " << totalMemChunkAllocatedSize << endl;
#endif
    }
#endif
#endif
}

void Query::destroy()
{
    shared_ptr<Array> resultArray;
    shared_ptr<RemoteMergedArray> mergedArray;
    vector<shared_ptr<RemoteArray> > remoteArrays;
    shared_ptr<WorkQueue> mpiQueue;
    shared_ptr<WorkQueue> errQueue;
    {
        ScopedMutexLock cs(errorMutex);

        LOG4CXX_TRACE(logger, "Cleaning up query (" << getQueryID() << ")");

        // Drop all unprocessed messages and cut any circular references
        // (from MessageHandleJob to Query).
        // This should be OK because we broadcast either
        // the error or abort before dropping the messages

        _mpiReceiveQueue.swap(mpiQueue);
        _errorQueue.swap(errQueue);

        // Unregister this query from liveness notifications
        NodeLivenessNotification::removePublishListener(_livenessListenerID);

        // The result array may also have references to this query
        _currentResultArray.swap(resultArray);

        _mergedArray.swap(mergedArray);

        _remoteArrays.swap(remoteArrays);
    }
    dumpMemoryUsage(getQueryID());
}

void Query::freeQuery(QueryID queryID)
{
    ScopedMutexLock mutexLock(queriesMutex);
    map<QueryID, shared_ptr<Query> >::iterator q = _queries.find(queryID);
    if (q != _queries.end()) {
       LOG4CXX_DEBUG(logger, "Deallocating query (" << q->second->getQueryID() << ")");
       _queries.erase(q);
    }
}

bool Query::validateQueryPtr(const boost::shared_ptr<Query>& query)
{
   if (!query) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_QUERY_NOT_FOUND2);
   }
   return query->validate();
}

bool Query::validate()
{
   bool isShutdown = NetworkManager::isShutdown();
   if (isShutdown) {
       handleAbort();
   }

   ScopedMutexLock cs(errorMutex);

   if (SCIDB_E_NO_ERROR != _error->getLongErrorCode()) {
      _error->raise();
   }
   return true;
}

void Query::setSGContext(shared_ptr<SGContext> const& sgContext)
{
    {
        ScopedMutexLock lock(mutexSG);
        _sgContext = sgContext;
    }
    eventSG.signal();
}

void Query::replicationBarrier()
{
    if (Config::getInstance()->getOption<int>(CONFIG_REDUNDANCY) != 0) {
        boost::shared_ptr<MessageDesc> msg = boost::make_shared<MessageDesc>(mtReplicaSyncRequest);
        msg->setQueryID(_queryID);

        // we may relax this assert once we support update on a subset of nodes
        assert(Cluster::getInstance()->getNodeMembership()->getNodes().size() == getNodesCount());

        NetworkManager::getInstance()->sendOutMessage(msg);
        Semaphore::ErrorChecker ec = bind(&Query::validate, this);
        replicaSem.enter(getNodesCount()-1, ec);
    }
}

boost::shared_ptr<SGContext> Query::getSGContext()
{
    ScopedMutexLock lock(mutexSG);
    while (_sgContext.get()==NULL) {
       Semaphore::ErrorChecker ec = bind(&Query::validate, this);
       eventSG.wait(mutexSG, ec);
    }
    return _sgContext;
}


ostream& writeStatistics(ostream& os, shared_ptr<PhysicalQueryPlanNode> node, size_t tab)
{
    string tabStr(tab*4, ' ');
    shared_ptr<PhysicalOperator> op = node->getPhysicalOperator();
    os << tabStr << "*" << op->getPhysicalName() << "*: " << endl;
    writeStatistics(os, op->getStatistics(), tab + 1);
    for (size_t i = 0; i < node->getChildren().size(); i++) {
        writeStatistics(os, node->getChildren()[i], tab + 1);
    }
    return os;
}


std::ostream& Query::writeStatistics(std::ostream& os) const
{
    os << endl << "=== Query statistics: ===" << endl;
    scidb::writeStatistics(os, statistics, 0);
    for (size_t i = 0; (i < _physicalPlans.size()) && _physicalPlans[i]->getRoot(); i++)
    {
        os << "=== Statistics of plan #" << i << ": ===" << endl;
        scidb::writeStatistics(os, _physicalPlans[i]->getRoot(), 0);
    }
    os << endl << "=== Current state of system statistics: ===" << endl;
    scidb::writeStatistics(os, StatisticsScope::systemStatistics, 0);
    return os;
}

void Query::runMultinodePostOddJob()
{
    while (!_multinodePostOddJob.empty())
    {
        _multinodePostOddJob.front()->run(*this);
        _multinodePostOddJob.pop();
    }
}

void Query::postWarning(const Warning& warn)
{
    ScopedMutexLock lock(_warningsMutex);
    _warnings.push_back(warn);
}

std::vector<Warning> Query::getWarnings()
{
    ScopedMutexLock lock(_warningsMutex);
    return _warnings;
}

void Query::clearWarnings()
{
    ScopedMutexLock lock(_warningsMutex);
    _warnings.clear();
}

void RemoveErrorHandler::handleError(const shared_ptr<Query>& query)
{
   handleRemoveLock(_lock, true);
}

bool RemoveErrorHandler::handleRemoveLock(const shared_ptr<SystemCatalog::LockDesc>& lock,
                                          bool forceLockCheck)
{
   assert(lock);
   assert(lock->getLockMode() == SystemCatalog::LockDesc::RM);

   shared_ptr<SystemCatalog::LockDesc> coordLock;
   if (!forceLockCheck) {
      coordLock = lock;
   } else {
      coordLock = SystemCatalog::getInstance()->checkForCoordinatorLock(lock->getArrayName(),
                                                                        lock->getQueryId());
   }
   if (!coordLock) {
      LOG4CXX_DEBUG(logger, "RemoveErrorHandler::handleRemoveLock"
                    " lock does not exist. No abort action for query "
                    << lock->getQueryId());
      return false;
   }
   // we need to always delete the array metadata because the data may have been deleted
   return SystemCatalog::getInstance()->deleteArray(coordLock->getArrayName());
}

void UpdateErrorHandler::handleError(const shared_ptr<Query>& query)
{
   assert(query);
   if (!_lock) {
      assert(false);
      LOG4CXX_TRACE(logger,
                    "Update error handler has nothing to do for query ("
                    << query->getQueryID() << ")");
      return;
   }
   assert(_lock->getNodeId() == Cluster::getInstance()->getLocalNodeId());
   assert( (_lock->getLockMode() == SystemCatalog::LockDesc::CRT)
           || (_lock->getLockMode() == SystemCatalog::LockDesc::WR) );
   assert(query->getQueryID() == _lock->getQueryId());

   LOG4CXX_DEBUG(logger,
                 "Update error handler is invoked for query ("
                 << query->getQueryID() << ")");

   RollbackWork rw = bind(&UpdateErrorHandler::doRollback, _1, _2, _3);

   if (_lock->getNodeRole() == SystemCatalog::LockDesc::COORD) {
      handleErrorOnCoordinator(_lock, rw);
   } else {
      assert(_lock->getNodeRole() == SystemCatalog::LockDesc::WORKER);
      handleErrorOnWorker(_lock, query->isForceCancelled(), rw);
   }
}

void UpdateErrorHandler::releaseLock(const shared_ptr<SystemCatalog::LockDesc>& lock,
                                     const shared_ptr<Query>& query)
{
   assert(lock);
   assert(query);
   bool rc = SystemCatalog::getInstance()->unlockArray(lock);
   if (!rc) {
      LOG4CXX_WARN(logger, "Failed to release the lock for query ("
                   << query->getQueryID() << ")");
   }
}

void UpdateErrorHandler::handleErrorOnCoordinator(const shared_ptr<SystemCatalog::LockDesc> & lock,
                                                  RollbackWork& rollback)
{
   assert(lock);
   assert(lock->getNodeRole() == SystemCatalog::LockDesc::COORD);

   string const& arrayName = lock->getArrayName();

   shared_ptr<SystemCatalog::LockDesc> coordLock =
   SystemCatalog::getInstance()->checkForCoordinatorLock(arrayName,
                                                         lock->getQueryId());
   if (!coordLock) {
      LOG4CXX_DEBUG(logger, "UpdateErrorHandler::handleErrorOnCoordinator:"
                    " coordinator lock does not exist. No abort action for query "
                    << lock->getQueryId());
      return;
   }

   VersionID newVersion      = coordLock->getArrayVersion();
   ArrayID newArrayVersionId = coordLock->getArrayVersionId();

   if (newVersion != 0) {
      ArrayID arrayId = coordLock->getArrayId();
      assert(arrayId != 0);
      VersionID lastVersion = SystemCatalog::getInstance()->getLastVersion(arrayId);

      assert(lastVersion <= newVersion);
      if (lastVersion == newVersion) {
         // query is committed
         LOG4CXX_DEBUG(logger, "UpdateErrorHandler::handleErrorOnCoordinator:"
                       " the new version ("<< newVersion <<") already committed. No abort action for query "
                       << lock->getQueryId());
         return;
      }

      if (newArrayVersionId > 0 && rollback) {
         rollback(lastVersion, arrayId, newArrayVersionId);
      }
   }

   if (coordLock->getLockMode() == SystemCatalog::LockDesc::CRT) {
      SystemCatalog::getInstance()->deleteArray(arrayName);
   } else if (newVersion != 0) {
      assert(coordLock->getLockMode() == SystemCatalog::LockDesc::WR);
      string newArrayVersionName = formArrayNameVersion(arrayName,newVersion);
      SystemCatalog::getInstance()->deleteArray(newArrayVersionName);
   }
}

void UpdateErrorHandler::handleErrorOnWorker(const shared_ptr<SystemCatalog::LockDesc>& lock,
                                             bool forceCoordLockCheck,
                                             RollbackWork& rollback)
{
   assert(lock);
   assert(lock->getNodeRole() == SystemCatalog::LockDesc::WORKER);

   string const& arrayName   = lock->getArrayName();
   VersionID newVersion      = lock->getArrayVersion();
   ArrayID newArrayVersionId = lock->getArrayVersionId();

   if (newVersion != 0) {

      if (forceCoordLockCheck) {
         shared_ptr<SystemCatalog::LockDesc> coordLock;
         do {  //XXX TODO: fix the wait, possibly with batching the checks
            coordLock = SystemCatalog::getInstance()->checkForCoordinatorLock(arrayName,
                                                                              lock->getQueryId());
            sleep(2);
         } while (coordLock);
      }
      ArrayID arrayId = lock->getArrayId();
      if(arrayId == 0) {
         LOG4CXX_WARN(logger, "Invalid update lock for query ("
                      << lock->getQueryId()
                      << ") Lock:" << lock->toString()
                      << " No rollback is possible.");
      }
      VersionID lastVersion = SystemCatalog::getInstance()->getLastVersion(arrayId);

      assert(lastVersion <= newVersion);
      if (lastVersion < newVersion &&
          newArrayVersionId > 0 && rollback) {
         rollback(lastVersion, arrayId, newArrayVersionId);
      }
   }
}

void UpdateErrorHandler::doRollback(const VersionID& lastVersion,
                                    const ArrayID& baseArrayId,
                                    const ArrayID& newArrayId)
{
   // if a query stopped before the coordinator recorded the new array version id
   // there is no rollback to do 
   assert(newArrayId>0);
   assert(baseArrayId>0);

   std::map<ArrayID,VersionID> undoArray;
   undoArray[baseArrayId] = lastVersion;
   try {
      StorageManager::getInstance().rollback(undoArray);
      StorageManager::getInstance().remove(newArrayId); 
      SystemCatalog::getInstance()->deleteArrayCache(newArrayId);
   } catch (const scidb::Exception& e) {
      LOG4CXX_ERROR(logger, "UpdateErrorHandler::doRollback:"
                    << " lastVersion = "<< lastVersion
                    << " baseArrayId = "<< baseArrayId
                    << " newArrayId = "<< newArrayId
                    << ". Error: "<< e.what());
      assert(false);
      throw; //XXX TODO: anything to do ???
   }
}

void Query::releaseLocks(const shared_ptr<Query>& q)
{
    assert(q);
    LOG4CXX_DEBUG(logger, "Releasing locks for query " << q->getQueryID());

    SystemCatalog::getInstance()->deleteArrayLocks(Cluster::getInstance()->getLocalNodeId(),
                                                   q->getQueryID());
}

void Query::acquireLocks()
{
    Query::QueryLocks locks;
    {
        ScopedMutexLock cs(errorMutex);
        validate();
        locks = _requestedLocks; // TODO: swap instead of copy ?
    }
    LOG4CXX_DEBUG(logger, "Acquiring "<< locks.size()
                  << " array locks for query " << _queryID);

    bool foundDeadNodes = (_coordinatorLiveness->getNumDead() > 0);

    try {
        SystemCatalog::ErrorChecker errorChecker = bind(&Query::validate, this);
        BOOST_FOREACH(const shared_ptr<SystemCatalog::LockDesc>& lock, locks)
        {
            assert(lock);
            assert(lock->getQueryId() == _queryID);
            LOG4CXX_TRACE(logger, "Acquiring lock: " << lock->toString());

            if (foundDeadNodes && (lock->getLockMode() > SystemCatalog::LockDesc::RD)) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_NO_QUORUM);
            }

            bool rc = SystemCatalog::getInstance()->lockArray(lock, errorChecker);
            assert(rc);
        }
        validate();
    } catch (std::exception&) {
        releaseLocks(shared_from_this());
        throw;
    }
}

} // namespace







