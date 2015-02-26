/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2013 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
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

#include "query/QueryProcessor.h"
#include "query/parser/QueryParser.h"
#include "query/parser/AST.h"
#include "query/RemoteArray.h"
#include "array/DBArray.h"
#include "smgr/io/Storage.h"
#include "network/NetworkManager.h"
#include "system/SciDBConfigOptions.h"
#include "system/SystemCatalog.h"
#include "system/Cluster.h"
#include "util/iqsort.h"
#include "util/LockManager.h"
#include <system/BlockCyclic.h>
#include <system/Exceptions.h>
#include <system/System.h>
#include <network/MessageUtils.h>
#include <smgr/io/ReplicationManager.h>

using namespace std;
using namespace boost;

namespace boost
{
   bool operator< (shared_ptr<scidb::SystemCatalog::LockDesc> const& l,
                   shared_ptr<scidb::SystemCatalog::LockDesc> const& r) {
      if (!l || !r) {
         assert(false);
         return false;
      }
      return (l->getArrayName() < r->getArrayName());
   }

   bool operator== (shared_ptr<scidb::SystemCatalog::LockDesc> const& l,
                    shared_ptr<scidb::SystemCatalog::LockDesc> const& r) {
         assert(false);
         throw SYSTEM_EXCEPTION(scidb::SCIDB_SE_INTERNAL, scidb::SCIDB_LE_NOT_IMPLEMENTED);
         return false;
   }

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
} // namespace

namespace scidb
{

// Query class implementation
Mutex Query::queriesMutex;
map<QueryID, boost::shared_ptr<Query> > Query::_queries;
uint32_t Query::nextID = 0;
log4cxx::LoggerPtr Query::_logger = log4cxx::Logger::getLogger("scidb.qproc.processor");
log4cxx::LoggerPtr UpdateErrorHandler::_logger = log4cxx::Logger::getLogger("scidb.qproc.processor");
log4cxx::LoggerPtr RemoveErrorHandler::_logger = log4cxx::Logger::getLogger("scidb.qproc.processor");
log4cxx::LoggerPtr BroadcastAbortErrorHandler::_logger = log4cxx::Logger::getLogger("scidb.qproc.processor");
boost::mt19937 Query::_rng;

size_t Query::PendingRequests::increment()
{
    ScopedMutexLock cs(_mutex);
    _nReqs += 1;
    return _nReqs;
}

bool Query::PendingRequests::decrement()
{
    ScopedMutexLock cs(_mutex);
    if (--_nReqs == 0 && _sync) {
        _sync = false;
        return true;
    }
    return false;
}

bool Query::PendingRequests::test()
{
    ScopedMutexLock cs(_mutex);
    if (_nReqs != 0) {
        _sync = true;
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

boost::shared_ptr<Query> Query::createFakeQuery(InstanceID coordID,
                                                InstanceID localInstanceID,
                                                shared_ptr<const InstanceLiveness> liveness)
{
  boost::shared_ptr<Query> query = createDetached();
  try {
      query->init(0,coordID, localInstanceID, liveness);
  } catch (const std::exception& e) {
      destroyFakeQuery(query.get());
      throw;
  }
  return query;
}

void Query::destroyFakeQuery(Query* q)
 {
     if (q!=NULL && q->getQueryID() == 0) {
         try {
             q->handleAbort();
         } catch (scidb::Exception&) { }
     }
 }


Query::Query():
    _queryID(INVALID_QUERY_ID), _instanceID(INVALID_INSTANCE), _coordinatorID(INVALID_INSTANCE), _error(SYSTEM_EXCEPTION_SPTR(SCIDB_E_NO_ERROR, SCIDB_E_NO_ERROR)),
    completionStatus(INIT), _commitState(UNKNOWN), _creationTime(time(NULL)), _useCounter(0), _doesExclusiveArrayAccess(false), _procGrid(NULL), isDDL(false)
{
}

Query::~Query()
{
    if (statisticsMonitor) {
        statisticsMonitor->pushStatistics(*this);
    }
    for (map< string, shared_ptr<RWLock> >::const_iterator i = locks.begin(); i != locks.end(); i++) {
        LOG4CXX_DEBUG(_logger, "Release lock of array " << i->first << " for query " << _queryID);
        i->second->unLock();
    }

    delete _procGrid ; _procGrid = NULL ;
}

void Query::init(QueryID queryID, InstanceID coordID, InstanceID localInstanceID,
                 shared_ptr<const InstanceLiveness> liveness)
{
   assert(liveness);
   assert(localInstanceID != INVALID_INSTANCE);
   {
      ScopedMutexLock cs(errorMutex);

      validate();

      assert( _queryID == INVALID_QUERY_ID || _queryID == queryID);
      _queryID = queryID;
      assert( _queryID != INVALID_QUERY_ID);

      assert(!_coordinatorLiveness);
      _coordinatorLiveness = liveness;
      assert(_coordinatorLiveness);

      size_t nInstances = _coordinatorLiveness->getNumLive();
      if (nInstances <= 0) {
          throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_LIVENESS_EMPTY);
      }
      assert(_liveInstances.size() == 0);
      _liveInstances.clear();
      _liveInstances.reserve(nInstances);

      const InstanceLiveness::LiveInstances& liveInstances = _coordinatorLiveness->getLiveInstances();
      assert(liveInstances.size() == nInstances);
      for ( InstanceLiveness::LiveInstances::const_iterator iter = liveInstances.begin();
        iter != liveInstances.end(); ++iter) {
         _liveInstances.push_back((*iter)->getInstanceId());
      }
      _instanceID = mapPhysicalToLogical(localInstanceID);
      assert(_instanceID != INVALID_INSTANCE);
      assert(_instanceID < nInstances);

      if (coordID == COORDINATOR_INSTANCE) {
         _coordinatorID = COORDINATOR_INSTANCE;
         shared_ptr<Query::ErrorHandler> ptr(new BroadcastAbortErrorHandler());
         pushErrorHandler(ptr);
      } else {
         _coordinatorID = mapPhysicalToLogical(coordID);
         assert(_coordinatorID < nInstances);
      }

      _remoteArrays.resize(nInstances);
      _receiveSemaphores.resize(nInstances);
      _receiveMessages.resize(nInstances);
      chunkReqs.resize(nInstances);
      Finalizer f = bind(&Query::destroyFinalizer, _1);
      pushFinalizer(f);
      _errorQueue = NetworkManager::getInstance()->createWorkQueue();
      assert(_errorQueue);
      _mpiReceiveQueue = NetworkManager::getInstance()->createWorkQueue();
      assert(_mpiReceiveQueue);
      _operatorQueue = NetworkManager::getInstance()->createWorkQueue();
      _operatorQueue->stop();
      assert(_operatorQueue);
      _replicationCtx = boost::make_shared<ReplicationContext>(shared_from_this(), nInstances);
      assert(_replicationCtx);
   }

   // register for notifications
   InstanceLivenessNotification::PublishListener listener =
      boost::bind(&Query::handleLivenessNotification, shared_from_this(), _1);
   _livenessListenerID = InstanceLivenessNotification::addPublishListener(listener);

   LOG4CXX_DEBUG(_logger, "Initialized query (" << queryID << ")");
}

bool Query::insert(QueryID queryID, shared_ptr<Query>& query)
{
   // queriesMutex must be locked
   pair<map<QueryID, shared_ptr<Query> >::iterator, bool > res =
   _queries.insert(make_pair(queryID, query));
   setCurrentQueryID(queryID);

   if (res.second) {
      query = createDetached();
      assert(query);
      query->_queryID = queryID;
      res.first->second = query;
      LOG4CXX_DEBUG(_logger, "Allocating query (" << queryID << ")");
      LOG4CXX_DEBUG(_logger, "Number of allocated queries = " << _queries.size());
   } else {
      query = res.first->second;
   }
   return res.second;
}

QueryID Query::generateID()
{
   const uint64_t instanceID = StorageManager::getInstance().getInstanceId();

   ScopedMutexLock mutexLock(queriesMutex);

   const uint32_t timeVal = time(NULL);
   const uint32_t clockVal = clock();

   // It allows us to have about 16 000 000 instances while about 10 000 years
   QueryID queryID = ((instanceID+1) << 40) | (timeVal + clockVal + nextID++);
   LOG4CXX_DEBUG(_logger, "Generated queryID: instanceID=" << instanceID << ", time=" << timeVal
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

    boost::shared_ptr<const scidb::InstanceLiveness> myLiveness =
       Cluster::getInstance()->getInstanceLiveness();
    assert(myLiveness);

    query->init(query->_queryID, COORDINATOR_INSTANCE,
                Cluster::getInstance()->getLocalInstanceId(), myLiveness);
    return query;
}

void Query::start()
{
    ScopedMutexLock cs(errorMutex);
    checkNoError();
    if (completionStatus == INIT) {
        completionStatus = START;
    }
}

void Query::stop()
{
    ScopedMutexLock cs(errorMutex);
    checkNoError();
    if (completionStatus == START) {
        completionStatus = INIT;
    }
}

void Query::pushErrorHandler(const boost::shared_ptr<ErrorHandler>& eh)
{
    assert(eh);
    ScopedMutexLock cs(errorMutex);
    checkNoError();
    _errorHandlers.push_back(eh);
}

void Query::pushFinalizer(const Finalizer& f)
{
    assert(f);
    ScopedMutexLock cs(errorMutex);
    checkNoError();
    _finalizers.push_back(f);
}

void Query::done()
{
    ScopedMutexLock cs(errorMutex);
    if (SCIDB_E_NO_ERROR != _error->getLongErrorCode())
    {
        completionStatus = ERROR;
        _error->raise();
    }
    completionStatus = OK;
}

void Query::done(const shared_ptr<Exception> unwindException)
{
    bool isAbort = false;
    {
        ScopedMutexLock cs(errorMutex);
        if (SCIDB_E_NO_ERROR == _error->getLongErrorCode())
        {
            _error = unwindException;
        }
        completionStatus = ERROR;
        isAbort = (_commitState != UNKNOWN);

        LOG4CXX_DEBUG(_logger, "Query::done: queryID=" << _queryID
                      << ", _commitState=" << _commitState
                      << ", erorCode=" << _error->getLongErrorCode());
    }
    if (isAbort) {
        handleAbort();
    }
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
        LOG4CXX_TRACE(_logger, "Requested lock: " << requestedLock->toString() << " inserted");
        return requestedLock;
    }

    if ((*(res.first))->getLockMode() < requestedLock->getLockMode()) {
        _requestedLocks.erase(res.first);
        res = _requestedLocks.insert(requestedLock);
        assert(res.second);
        LOG4CXX_TRACE(_logger, "New requested lock: " << requestedLock->toString() << " inserted");
    }
    return (*(res.first));
}

void Query::handleError(const shared_ptr<Exception>& unwindException)
{
    assert(unwindException);
    assert(unwindException->getLongErrorCode() != SCIDB_E_NO_ERROR);
    {
        ScopedMutexLock cs(errorMutex);

        if (_error->getLongErrorCode() == SCIDB_E_NO_ERROR)
        {
            _error = unwindException;
            //XXX TODO: exceptions should be fixed to contain the right query ID
            //XXX assert(_error->getQueryId() == _queryID);
        }
    }
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
      } catch (const std::exception& e) {
         LOG4CXX_FATAL(_logger, "Query (" << _queryID
                       << ") finalizer failed:"
                       << e.what()
                       << "Aborting!");
         abort();
      }
   }
}

void Query::sharedLock(string const& arrayName)
{
    shared_ptr<RWLock> lock;
    {
        ScopedMutexLock cs(lockMutex);
        lock = locks[arrayName];
        if (lock) {
            return;
        }
        locks[arrayName] = lock = LockManager::getInstance()->getLock(arrayName);
    }
    LOG4CXX_DEBUG(_logger, "Request shared lock of array " << arrayName << " for query " << _queryID);
    RWLock::ErrorChecker noopEc;
    lock->lockRead(noopEc);
    LOG4CXX_DEBUG(_logger, "Granted shared lock of array " << arrayName << " for query " << _queryID);
}

void Query::exclusiveLock(string const& arrayName)
{
    shared_ptr<RWLock> lock;
    {
        ScopedMutexLock cs(lockMutex);
        lock = locks[arrayName];
        if (lock) {
            if (lock->getNumberOfReaders() == 0) {
                return;
            }
            lock->unLockRead();
        } else {
            locks[arrayName] = lock = LockManager::getInstance()->getLock(arrayName);
        }
    }
    LOG4CXX_DEBUG(_logger, "Request exclusive lock of array " << arrayName << " for query " << _queryID);
    RWLock::ErrorChecker noopEc;
    lock->lockWrite(noopEc);
    LOG4CXX_DEBUG(_logger, "Granted exclusive lock of array " << arrayName << " for query " << _queryID);
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
        } catch (const std::exception& e) {
            LOG4CXX_FATAL(_logger, "Query (" << _queryID
                          << ") error handler failed:"
                          << e.what()
                          << "Aborting!");
            abort();
        }
    }
}

void Query::handleAbort()
{
    QueryID queryId = INVALID_QUERY_ID;
    deque<Finalizer> finalizersOnStack;
    deque<shared_ptr<ErrorHandler> > errorHandlersOnStack;
    {
        ScopedMutexLock cs(errorMutex);

        queryId = _queryID;
        LOG4CXX_DEBUG(_logger, "Query (" << queryId << ") is being aborted");

        if(_commitState == COMMITTED) {
            LOG4CXX_ERROR(_logger, "Query (" << queryId
                          << ") cannot be aborted after commit."
                          << " completion status=" << completionStatus
                          << " commit status=" << _commitState
                          << " error=" << _error->getLongErrorCode());
            assert(false);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_COMMIT_STATE) << _queryID << "abort");
        }

        _commitState = ABORTED;

        if (_error->getLongErrorCode() == SCIDB_E_NO_ERROR)
        {
            _error = (SYSTEM_EXCEPTION_SPTR(SCIDB_SE_QPROC, SCIDB_LE_QUERY_CANCELLED) << queryId);
            //XXX TODO: exceptions should be fixed to contain the right query ID
            //XXX assert(_error->getQueryId() == _queryID);
        }
        if (completionStatus == START)
        {
            LOG4CXX_DEBUG(_logger, "Query (" << queryId << ") is still in progress");
            return;
        }
        errorHandlersOnStack.swap(_errorHandlers);
        finalizersOnStack.swap(_finalizers);
    }
    if (!errorHandlersOnStack.empty()) {
        LOG4CXX_ERROR(_logger, "Query (" << queryId << ") error handlers ("
                     << errorHandlersOnStack.size() << ") are being executed");
        invokeErrorHandlers(errorHandlersOnStack);
        errorHandlersOnStack.clear();
    }
    freeQuery(queryId);
    invokeFinalizers(finalizersOnStack);
}

void Query::handleCommit()
{
    QueryID queryId = INVALID_QUERY_ID;
    deque<Finalizer> finalizersOnStack;
    {
        ScopedMutexLock cs(errorMutex);

        queryId = _queryID;

        LOG4CXX_DEBUG(_logger, "Query (" << _queryID << ") is being committed");

        if (completionStatus != OK || _commitState == ABORTED) {
            LOG4CXX_ERROR(_logger, "Query (" << _queryID
                          << ") cannot be committed after abort."
                          << " completion status=" << completionStatus
                          << " commit status=" << _commitState
                          << " error=" << _error->getLongErrorCode());
            assert(false);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_COMMIT_STATE) << _queryID << "commit");
        }

        _errorHandlers.clear();

        _commitState = COMMITTED;

        if (_error->getLongErrorCode() == SCIDB_E_NO_ERROR)
        {
            _error = SYSTEM_EXCEPTION_SPTR(SCIDB_SE_QPROC, SCIDB_LE_QUERY_ALREADY_COMMITED);
            (*static_cast<scidb::SystemException*>(_error.get())) << queryId;
            //XXX TODO: exceptions should be fixed to contain the right query ID
            //XXX assert(_error->getQueryId() == _queryID);
        }
        finalizersOnStack.swap(_finalizers);
    }
    assert(queryId != INVALID_QUERY_ID);
    freeQuery(queryId);
    invokeFinalizers(finalizersOnStack);
}

void Query::handleComplete()
{
    handleCommit();
    boost::shared_ptr<MessageDesc>  msg(makeCommitMessage(_queryID));
    NetworkManager::getInstance()->broadcast(msg);
}

void Query::handleCancel()
{
    handleAbort();
}

void Query::handleLivenessNotification(boost::shared_ptr<const InstanceLiveness>& newLiveness)
{
   ScopedMutexLock cs(errorMutex);

   assert(newLiveness->getVersion() >= _coordinatorLiveness->getVersion());

   if (newLiveness->getVersion() == _coordinatorLiveness->getVersion()) {
      assert(newLiveness->isEqual(*_coordinatorLiveness));
      return;
   }

   LOG4CXX_ERROR(_logger, "Query " << _queryID << " is aborted on changed liveness");

   if (_error->getLongErrorCode() == SCIDB_E_NO_ERROR)
   {
       _error = SYSTEM_EXCEPTION_SPTR(SCIDB_SE_QPROC, SCIDB_LE_NO_QUORUM);
   }

   bool isAbort = false;
   boost::shared_ptr<Exception> err = _error;
   if (_coordinatorID != COORDINATOR_INSTANCE) {

       InstanceID coordId = getPhysicalCoordinatorID();
       InstanceLiveness::InstancePtr newCoordState = newLiveness->find(coordId);
       isAbort = newCoordState->isDead();
       if (!isAbort) {
           InstanceLiveness::InstancePtr oldCoordState = _coordinatorLiveness->find(coordId);
           isAbort = (newCoordState != oldCoordState);
       }
   }

   if (!_errorQueue) {
       LOG4CXX_TRACE(_logger, "Liveness change will not be handled for a deallocated query (" << _queryID << ")");
       return;
   }
   WorkQueue::WorkItem item = bind(&Query::handleError, shared_from_this(), err);
   try {
      _errorQueue->enqueue(item);

       if (isAbort) {
           // if the coordinator is dead, we abort the query
           LOG4CXX_TRACE(_logger, "Query " << _queryID << " is aborted due to a change in coordinator liveness");
           item = bind(&Query::handleAbort, shared_from_this());
           _errorQueue->enqueue(item);
       }
   } catch (const WorkQueue::OverflowException& ) {
       LOG4CXX_ERROR(_logger, "Overflow exception from the error queue, queryID="<<_queryID);
      // XXX TODO: deal with this exception maybe by retrying ...
      assert(false);
      throw;
   }
}

InstanceID Query::getPhysicalCoordinatorID()
{
   ScopedMutexLock cs(errorMutex);
   if (_coordinatorID == COORDINATOR_INSTANCE) {
      return COORDINATOR_INSTANCE;
   }
   if (_coordinatorID == INVALID_INSTANCE) {
      return INVALID_INSTANCE;
   }
   assert(_liveInstances.size() > 0);
   assert(_liveInstances.size() > _coordinatorID);
   return  _liveInstances[_coordinatorID];
}

InstanceID Query::mapLogicalToPhysical(InstanceID instance)
{
   if (instance == INVALID_INSTANCE) {
      return instance;
   }
   ScopedMutexLock cs(errorMutex);
   assert(_liveInstances.size() > 0);
   if (instance >= _liveInstances.size())
       throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_INSTANCE_OFFLINE) << instance;
   checkNoError();
   instance = _liveInstances[instance];
   return instance;
}

InstanceID Query::mapPhysicalToLogical(InstanceID instanceID)
{
   ScopedMutexLock cs(errorMutex);
   assert(_liveInstances.size() > 0);
   size_t index=0;
   bool found = bsearch(_liveInstances, instanceID, index);
   if (!found) {
       throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_INSTANCE_OFFLINE) << instanceID;
   }
   return index;
}

bool Query::isPhysicalInstanceDead(InstanceID instance)
{
   ScopedMutexLock cs(errorMutex);
   checkNoError();
   bool isDead = _coordinatorLiveness->isDead(instance);
   assert(isDead ||
          _coordinatorLiveness->find(instance));
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
        setCurrentQueryID(queryID);
        return q->second;
    }
    LOG4CXX_DEBUG(_logger, "Query " << queryID << " is not found");
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
        LOG4CXX_DEBUG(_logger, "Deallocating query (" << q->second->getQueryID() << ")");
        try {
            q->second->handleAbort();
        } catch (Exception&) { }
    }
}

#ifndef SCIDB_CLIENT
extern size_t totalPersistentChunkAllocatedSize;
extern size_t totalMemChunkAllocatedSize;
#endif

void dumpMemoryUsage(const QueryID queryId)
{
#ifndef NDEBUG
#ifdef HAVE_MALLOC_STATS
    if (Config::getInstance()->getOption<bool>(CONFIG_OUTPUT_PROC_STATS)) {
        cerr << "Stats after query ID ("<<queryId<<") :" << endl;
        malloc_stats();
#ifndef SCIDB_CLIENT
        cerr <<
                "Allocated size for PersistentChunks: " << totalPersistentChunkAllocatedSize <<
                ", allocated size for MemChunks: " << SharedMemCache::getInstance().getUsedMemSize() <<
                ", MemChunks were swapped out: " << SharedMemCache::getInstance().getSwapNum() <<
                ", MemChunks were loaded: " << SharedMemCache::getInstance().getLoadsNum() <<
                endl;
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
    shared_ptr<WorkQueue> sgQueue;
    boost::shared_ptr<ReplicationContext> replicationCtx;
    {
        ScopedMutexLock cs(errorMutex);

        LOG4CXX_TRACE(_logger, "Cleaning up query (" << getQueryID() << ")");

        // Drop all unprocessed messages and cut any circular references
        // (from MessageHandleJob to Query).
        // This should be OK because we broadcast either
        // the error or abort before dropping the messages

        _mpiReceiveQueue.swap(mpiQueue);
        _errorQueue.swap(errQueue);
        _operatorQueue.swap(sgQueue);
        _replicationCtx.swap(replicationCtx);

        // Unregister this query from liveness notifications
        InstanceLivenessNotification::removePublishListener(_livenessListenerID);

        // The result array may also have references to this query
        _currentResultArray.swap(resultArray);

        _mergedArray.swap(mergedArray);

        _remoteArrays.swap(remoteArrays);
    }
    dumpMemoryUsage(getQueryID());
}

void
BroadcastAbortErrorHandler::handleError(const boost::shared_ptr<Query>& query)
{
    if (query->getQueryID() == 0) {
        return;
    }
    if (query->getQueryID() == INVALID_QUERY_ID) {
        assert(false);
        return;
    }
    if (query->getCoordinatorID() != COORDINATOR_INSTANCE) {
        assert(false);
        return;
    }
    LOG4CXX_DEBUG(_logger, "Broadcast ABORT message to all instances for query " << query->getQueryID());
    shared_ptr<MessageDesc> abortMessage = makeAbortMessage(query->getQueryID());
    // query may not have the instance map, so broadcast to all
    NetworkManager::getInstance()->broadcast(abortMessage);
}

void Query::freeQuery(QueryID queryID)
{
    ScopedMutexLock mutexLock(queriesMutex);
    map<QueryID, shared_ptr<Query> >::iterator i = _queries.find(queryID);
    if (i != _queries.end()) {
        shared_ptr<Query> q = i->second;
        LOG4CXX_DEBUG(_logger, "Deallocating query (" << q->getQueryID() << ")");
        _queries.erase(i);
    }
}

bool Query::validate()
{
   bool isShutdown = NetworkManager::isShutdown();
   if (isShutdown) {
       handleAbort();
   }

   ScopedMutexLock cs(errorMutex);
   checkNoError();
   return true;
}

Query::OperatorContext::~OperatorContext()
{
}

void Query::setOperatorContext(shared_ptr<OperatorContext> const& opContext)
{
    assert(opContext);
    ScopedMutexLock lock(errorMutex);
    _operatorContext = opContext;
    _operatorQueue->start();
}

void Query::unsetOperatorContext()
{
    assert(_operatorContext);
    ScopedMutexLock lock(errorMutex);
    _operatorContext.reset();
    _operatorQueue->stop();
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
    boost::function<bool()> work = boost::bind(&RemoveErrorHandler::handleRemoveLock, _lock, true);
    Query::runRestartableWork<bool, Exception>(work);
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
      LOG4CXX_DEBUG(_logger, "RemoveErrorHandler::handleRemoveLock"
                    " lock does not exist. No abort action for query "
                    << lock->getQueryId());
      return false;
   }
   return SystemCatalog::getInstance()->deleteArray(coordLock->getArrayName());
}

void UpdateErrorHandler::handleError(const shared_ptr<Query>& query)
{
    boost::function<void()> work = boost::bind(&UpdateErrorHandler::_handleError, this, query);
    Query::runRestartableWork<void, Exception>(work);
}

void UpdateErrorHandler::_handleError(const shared_ptr<Query>& query)
{
   assert(query);
   if (!_lock) {
      assert(false);
      LOG4CXX_TRACE(_logger,
                    "Update error handler has nothing to do for query ("
                    << query->getQueryID() << ")");
      return;
   }
   assert(_lock->getInstanceId() == Cluster::getInstance()->getLocalInstanceId());
   assert( (_lock->getLockMode() == SystemCatalog::LockDesc::CRT)
           || (_lock->getLockMode() == SystemCatalog::LockDesc::WR) );
   assert(query->getQueryID() == _lock->getQueryId());

   LOG4CXX_DEBUG(_logger,
                 "Update error handler is invoked for query ("
                 << query->getQueryID() << ")");

   RollbackWork rw = bind(&UpdateErrorHandler::doRollback, _1, _2, _3, _4);

   if (_lock->getInstanceRole() == SystemCatalog::LockDesc::COORD) {
      handleErrorOnCoordinator(_lock, rw);
   } else {
      assert(_lock->getInstanceRole() == SystemCatalog::LockDesc::WORKER);
      handleErrorOnWorker(_lock, query->isForceCancelled(), rw);
   }
   return;
}

void UpdateErrorHandler::releaseLock(const shared_ptr<SystemCatalog::LockDesc>& lock,
                                     const shared_ptr<Query>& query)
{
   assert(lock);
   assert(query);
   boost::function<bool()> work = boost::bind(&SystemCatalog::unlockArray,
                                             SystemCatalog::getInstance(),
                                             lock);
   bool rc = Query::runRestartableWork<bool, Exception>(work);
   if (!rc) {
      LOG4CXX_WARN(_logger, "Failed to release the lock for query ("
                   << query->getQueryID() << ")");
   }
}

void UpdateErrorHandler::handleErrorOnCoordinator(const shared_ptr<SystemCatalog::LockDesc> & lock,
                                                  RollbackWork& rollback)
{
   assert(lock);
   assert(lock->getInstanceRole() == SystemCatalog::LockDesc::COORD);

   string const& arrayName = lock->getArrayName();

   shared_ptr<SystemCatalog::LockDesc> coordLock =
   SystemCatalog::getInstance()->checkForCoordinatorLock(arrayName,
                                                         lock->getQueryId());
   if (!coordLock) {
      LOG4CXX_DEBUG(_logger, "UpdateErrorHandler::handleErrorOnCoordinator:"
                    " coordinator lock does not exist. No abort action for query "
                    << lock->getQueryId());
      return;
   }

   VersionID newVersion      = coordLock->getArrayVersion();
   ArrayID newArrayVersionId = coordLock->getArrayVersionId();
   uint64_t timestamp = lock->getTimestamp();

   if (newVersion != 0) {
      ArrayID arrayId = coordLock->getArrayId();
      assert(arrayId != 0);
      VersionID lastVersion = SystemCatalog::getInstance()->getLastVersion(arrayId);

      assert(lastVersion <= newVersion);
      LOG4CXX_DEBUG(_logger, "UpdateErrorHandler::handleErrorOnCoordinator:"
                    " the new version "<< newVersion <<" of array "<<arrayName<<" is being rolled back for query ("
                    << lock->getQueryId() << ")");

      if (newArrayVersionId > 0 && rollback) {
         rollback(lastVersion, arrayId, newArrayVersionId, timestamp);
      }
   } else if (rollback && lock->getImmutableArrayId() > 0) {
       rollback(-1, lock->getImmutableArrayId(), newArrayVersionId, timestamp);
   }

   if (coordLock->getLockMode() == SystemCatalog::LockDesc::CRT) {
      SystemCatalog::getInstance()->deleteArray(arrayName);
   } else if (newVersion != 0) {
      assert(coordLock->getLockMode() == SystemCatalog::LockDesc::WR);
      string newArrayVersionName = ArrayDesc::makeVersionedName(arrayName,newVersion);
      SystemCatalog::getInstance()->deleteArray(newArrayVersionName);
   }
}

void UpdateErrorHandler::handleErrorOnWorker(const shared_ptr<SystemCatalog::LockDesc>& lock,
                                             bool forceCoordLockCheck,
                                             RollbackWork& rollback)
{
   assert(lock);
   assert(lock->getInstanceRole() == SystemCatalog::LockDesc::WORKER);

   string const& arrayName   = lock->getArrayName();
   VersionID newVersion      = lock->getArrayVersion();
   ArrayID newArrayVersionId = lock->getArrayVersionId();
   uint64_t timestamp = lock->getTimestamp();

   LOG4CXX_TRACE(_logger, "UpdateErrorHandler::handleErrorOnWorker:"
                 << " forceLockCheck = "<< forceCoordLockCheck
                 << " arrayName = "<< arrayName
                 << " newVersion = "<< newVersion
                 << " newArrayVersionId = "<< newArrayVersionId
                 << " timestamp = "<< timestamp);

   if (newVersion != 0) {

       if (forceCoordLockCheck) {
           shared_ptr<SystemCatalog::LockDesc> coordLock;
           do {  //XXX TODO: fix the wait, possibly with batching the checks
               coordLock = SystemCatalog::getInstance()->checkForCoordinatorLock(arrayName,
                                                                                 lock->getQueryId());
               sleep((Query::_rng()%2)+1);
           } while (coordLock);
       }
       ArrayID arrayId = lock->getArrayId();
       if(arrayId == 0) {
           LOG4CXX_WARN(_logger, "Invalid update lock for query ("
                        << lock->getQueryId()
                        << ") Lock:" << lock->toString()
                        << " No rollback is possible.");
       }
       VersionID lastVersion = SystemCatalog::getInstance()->getLastVersion(arrayId);

       LOG4CXX_TRACE(_logger, "UpdateErrorHandler::handleErrorOnWorker:"
                     << " lastVersion = "<< lastVersion);

       assert(lastVersion <= newVersion);
       // if we are not checking the coordinator lock, we'd better rollback
       assert(forceCoordLockCheck || lastVersion < newVersion);

       if (lastVersion < newVersion && newArrayVersionId > 0 && rollback) {
           rollback(lastVersion, arrayId, newArrayVersionId, timestamp);
       }
   } else if (rollback && lock->getImmutableArrayId() > 0) {
       rollback(-1, lock->getImmutableArrayId(), newArrayVersionId, timestamp);
   }
   LOG4CXX_TRACE(_logger, "UpdateErrorHandler::handleErrorOnWorker:"
                     << " exit");
}

void UpdateErrorHandler::doRollback(VersionID lastVersion,
                                    ArrayID baseArrayId,
                                    ArrayID newArrayId,
                                    uint64_t timestamp)
{

    LOG4CXX_TRACE(_logger, "UpdateErrorHandler::doRollback:"
                  << " lastVersion = "<< lastVersion
                  << " baseArrayId = "<< baseArrayId
                  << " newArrayId = "<< newArrayId
                  << " timestamp = "<< timestamp);
   // if a query stopped before the coordinator recorded the new array version id
   // there is no rollback to do
   assert(baseArrayId>0);
   if (newArrayId>0) {
       std::map<ArrayID,VersionID> undoArray;
       undoArray[baseArrayId] = lastVersion;
       try {
           StorageManager::getInstance().rollback(undoArray);
           StorageManager::getInstance().remove(baseArrayId, newArrayId);
           SystemCatalog::getInstance()->deleteArrayCache(newArrayId);
       } catch (const scidb::Exception& e) {
           LOG4CXX_ERROR(_logger, "UpdateErrorHandler::doRollback:"
                         << " lastVersion = "<< lastVersion
                         << " baseArrayId = "<< baseArrayId
                         << " newArrayId = "<< newArrayId
                         << ". Error: "<< e.what());
           throw; //XXX TODO: anything to do ???
       }
   } else {
       try {
           StorageManager::getInstance().remove(baseArrayId, baseArrayId, timestamp);
       } catch (const scidb::Exception& e) {
           LOG4CXX_ERROR(_logger, "UpdateErrorHandler::doRollback:"
                         << " baseArrayId = "<< baseArrayId
                         << " timestamp = "<< timestamp
                         << ". Error: "<< e.what());
           throw; //XXX TODO: anything to do ???
       }
   }
}

void Query::releaseLocks(const shared_ptr<Query>& q)
{
    assert(q);
    LOG4CXX_DEBUG(_logger, "Releasing locks for query " << q->getQueryID());

    boost::function<uint32_t()> work = boost::bind(&SystemCatalog::deleteArrayLocks,
                                                   SystemCatalog::getInstance(),
                                                   Cluster::getInstance()->getLocalInstanceId(),
                                                   q->getQueryID());
    runRestartableWork<uint32_t, Exception>(work);
}

boost::shared_ptr<Array> Query::getTemporaryArray(std::string const& arrayName)
{
    boost::shared_ptr<Array> tmpArray;
    std::map< std::string, boost::shared_ptr<Array> >::const_iterator i = _temporaryArrays.find(arrayName);
    if (i != _temporaryArrays.end()) {
        tmpArray = i->second;
    }
    return tmpArray;
}

boost::shared_ptr<Array> Query::getArray(std::string const& arrayName)
{
    boost::shared_ptr<Array> arr = getTemporaryArray(arrayName);
    return arr ? arr : boost::shared_ptr<Array>(DBArray::newDBArray(arrayName, shared_from_this()));
}

void Query::setTemporaryArray(boost::shared_ptr<Array> const& tmpArray)
{
    _temporaryArrays[tmpArray->getArrayDesc().getName()] = tmpArray;
}

void Query::acquireLocks()
{
    Query::QueryLocks locks;
    {
        ScopedMutexLock cs(errorMutex);
        validate();
        locks = _requestedLocks; // TODO: swap instead of copy ?
    }
    LOG4CXX_DEBUG(_logger, "Acquiring "<< locks.size()
                  << " array locks for query " << _queryID);

    bool foundDeadInstances = (_coordinatorLiveness->getNumDead() > 0);
    try {
        SystemCatalog::ErrorChecker errorChecker = bind(&Query::validate, this);
        BOOST_FOREACH(const shared_ptr<SystemCatalog::LockDesc>& lock, locks)
        {
            assert(lock);
            assert(lock->getQueryId() == _queryID);
            LOG4CXX_DEBUG(_logger, "Acquiring lock: " << lock->toString());

            if (foundDeadInstances && (lock->getLockMode() > SystemCatalog::LockDesc::RD)) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_NO_QUORUM);
            }

            bool rc = SystemCatalog::getInstance()->lockArray(lock, errorChecker);
            if (!rc) assert(false);
        }
        validate();
    } catch (std::exception&) {
        releaseLocks(shared_from_this());
        throw;
    }
}

const ProcGrid* Query::getProcGrid() const
{
    // locking to ensure a single allocation
    // XXX TODO: consider always calling Query::getProcGrid() in MpiManager::checkAndSetCtx
    //           that should guarantee an atomic creation of _procGrid
    ScopedMutexLock lock(const_cast<Mutex&>(errorMutex));
    // logically const, but we made _procGrid mutable to allow the caching
    // NOTE: Tigor may wish to push this down into the MPI context when
    //       that code is further along.  But for now, Query is a fine object
    //       on which to cache the generated procGrid
    if (!_procGrid) {
        _procGrid = new ProcGrid(getInstancesCount());
    }
    return _procGrid;
}

void Query::listLiveInstances(InstanceVisitor& func)
{
    assert(func);
    ScopedMutexLock lock(errorMutex);

    // we may relax this assert once we support update on a subset of instances
    assert(Cluster::getInstance()->getInstanceMembership()->getInstances().size() == getInstancesCount());

    for (vector<InstanceID>::const_iterator iter = _liveInstances.begin(); iter != _liveInstances.end(); ++iter) {
        boost::shared_ptr<Query> thisQuery(shared_from_this());
        func(thisQuery, (*iter));
    }
}

ReplicationContext::ReplicationContext(const boost::shared_ptr<Query>& query, size_t nInstances)
: _query(query)
#ifndef NDEBUG // for debugging
,_chunkReplicasReqs(nInstances)
#endif
{
    // ReplicatonManager singleton is initialized at startup time
    if (_replicationMngr == NULL) {
        _replicationMngr = ReplicationManager::getInstance();
    }
}

ReplicationContext::QueueInfoPtr ReplicationContext::getQueueInfo(QueueID id)
{   // mutex must be locked
    QueueInfoPtr& qInfo = _inboundQueues[id];
    if (!qInfo) {
        uint64_t size = Config::getInstance()->getOption<int>(CONFIG_REPLICATION_RECEIVE_QUEUE_SIZE);
        assert(size>0);
        qInfo = boost::make_shared<QueueInfo>(NetworkManager::getInstance()->createWorkQueue(1,size));
        assert(!qInfo->getArray());
        assert(qInfo->getQueue());
        qInfo->getQueue()->stop();
    }
    assert(qInfo->getQueue());
    return qInfo;
}

void ReplicationContext::enableInboundQueue(ArrayID aId, const boost::shared_ptr<Array>& array)
{
    assert(array);
    assert(aId > 0);
    ScopedMutexLock cs(_mutex);
    QueueID qId(aId);
    QueueInfoPtr qInfo = getQueueInfo(qId);
    assert(qInfo);
    boost::shared_ptr<scidb::WorkQueue> wq = qInfo->getQueue();
    assert(wq);
    qInfo->setArray(array);
    wq->start();
}

boost::shared_ptr<scidb::WorkQueue> ReplicationContext::getInboundQueue(ArrayID aId)
{
    assert(aId > 0);
    ScopedMutexLock cs(_mutex);
    QueueID qId(aId);
    QueueInfoPtr qInfo = getQueueInfo(qId);
    assert(qInfo);
    boost::shared_ptr<scidb::WorkQueue> wq = qInfo->getQueue();
    assert(wq);
    return wq;
}

boost::shared_ptr<scidb::Array> ReplicationContext::getPersistentArray(ArrayID aId)
{
    assert(aId > 0);
    ScopedMutexLock cs(_mutex);
    QueueID qId(aId);
    QueueInfoPtr qInfo = getQueueInfo(qId);
    assert(qInfo);
    boost::shared_ptr<scidb::Array> array = qInfo->getArray();
    assert(array);
    assert(qInfo->getQueue());
    return array;
}

void ReplicationContext::removeInboundQueue(ArrayID aId)
{
    // tigor:
    // Currently, we dont remove the queue until the query is destroyed
    // To implement removal we would need a sync point (in addition to replicationSync)
    // because each instance is not waiting for the INCOMING replication to finish
    // (only the coordinator does by waiting for everyone to complete).
    // The scheme could be similar to SG with two additional barriers,
    // or waiting for all remote instances to send in their eof replicas.
    return;
}

namespace {
void generateReplicationItems(boost::shared_ptr<MessageDesc>& msg,
                              std::vector<boost::shared_ptr<ReplicationManager::Item> >* replicaVec,
                              const boost::shared_ptr<Query>& query,
                              InstanceID iId)
{
    if (iId == query->getInstanceID()) {
        return;
    }
    boost::shared_ptr<ReplicationManager::Item> item(new ReplicationManager::Item(iId, msg, query));
    replicaVec->push_back(item);
}
}
void ReplicationContext::replicationSync(ArrayID arrId)
{
    assert(arrId > 0);
    if (Config::getInstance()->getOption<int>(CONFIG_REDUNDANCY) <= 0) {
        return;
    }

    boost::shared_ptr<MessageDesc> msg = boost::make_shared<MessageDesc>(mtChunkReplica);
    boost::shared_ptr<scidb_msg::Chunk> chunkRecord = msg->getRecord<scidb_msg::Chunk> ();
    chunkRecord->set_array_id(arrId);
    // tell remote instances that we are done replicating
    chunkRecord->set_eof(true);

    assert(_replicationMngr);
    boost::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
    msg->setQueryID(query->getQueryID());

    vector<boost::shared_ptr<ReplicationManager::Item> > replicasVec;
    Query::InstanceVisitor f =
        boost::bind(&generateReplicationItems, msg, &replicasVec, _1, _2);
    query->listLiveInstances(f);
    // we may relax this assert once we support update on a subset of instances
    assert(Cluster::getInstance()->getInstanceMembership()->getInstances().size() == query->getInstancesCount());

    assert(replicasVec.size() == (query->getInstancesCount()-1));
    for (size_t i=0; i<replicasVec.size(); ++i) {
        const boost::shared_ptr<ReplicationManager::Item>& item = replicasVec[i];
        assert(_replicationMngr);
        _replicationMngr->send(item);
    }
    for (size_t i=0; i<replicasVec.size(); ++i) {
        const boost::shared_ptr<ReplicationManager::Item>& item = replicasVec[i];
        assert(_replicationMngr);
        _replicationMngr->wait(item);
        assert(item->isDone());
        assert(item->validate(false));
    }

    QueueInfoPtr qInfo;
    {
        ScopedMutexLock cs(_mutex);
        QueueID qId(arrId);
        qInfo = getQueueInfo(qId);
        assert(qInfo);
        assert(qInfo->getArray());
        assert(qInfo->getQueue());
    }
    // wait for all to ack our eof
    Semaphore::ErrorChecker ec = bind(&Query::validate, query);
    qInfo->getSemaphore().enter(replicasVec.size(), ec);
}

void ReplicationContext::replicationAck(InstanceID sourceId, ArrayID arrId)
{
    assert(arrId > 0);
    QueueInfoPtr qInfo;
    {
        ScopedMutexLock cs(_mutex);
        QueueID qId(arrId);
        qInfo = getQueueInfo(qId);
        assert(qInfo);
        assert(qInfo->getArray());
        assert(qInfo->getQueue());
    }
    // sourceId acked our eof
    qInfo->getSemaphore().release();
}

ReplicationManager*  ReplicationContext::_replicationMngr;

boost::shared_ptr<WorkQueue> ReplicationContext::getReplicationQueue()
{
    assert(_replicationMngr);
    return _replicationMngr->getInboundReplicationQueue();
}

void ReplicationContext::scheduleInbound(const shared_ptr<Job>& job)
{
    assert(job);
    LOG4CXX_TRACE(Query::_logger, "ReplicationContext::scheduleInbound to replication queue="
                  << getReplicationQueue().get());
    WorkQueue::WorkItem item = boost::bind(&Job::execute, job);
    try {
        getReplicationQueue()->enqueue(item);

    } catch (const WorkQueue::OverflowException& e) {

        shared_ptr<Query> query(job->getQuery());
        assert(query);
        query->handleError(e.copy());
        LOG4CXX_ERROR(Query::_logger, "Overflow exception from the replication message"
                      <<" job="<<job.get()
                      <<", queue=" << getReplicationQueue().get()
                      <<", queryID="<<query->getQueryID()
                      <<" : "<<e.what());
        // XXX TODO: deal with this exception
        assert(false);
    }
}

void ReplicationContext::enqueueInbound(ArrayID arrId, const shared_ptr<Job>& job)
{
    assert(job);
    assert(arrId>0);
    ScopedMutexLock cs(_mutex);

    boost::shared_ptr<WorkQueue> queryQ(getInboundQueue(arrId));

    if (Query::_logger->isTraceEnabled()) {
        shared_ptr<Query> query(job->getQuery());
        assert(query);
        LOG4CXX_TRACE(Query::_logger, "ReplicationContext::enqueueInbound"
                      <<" job="<<job.get()
                      <<", queue="<<queryQ.get()
                      <<", arrId="<<arrId
                      << ", queryID="<<query->getQueryID());
    }
    WorkQueue::WorkItem item = boost::bind(&ReplicationContext::scheduleInbound,job);
    queryQ->enqueue(item);
}

} // namespace

