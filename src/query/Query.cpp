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
#include <boost/random.hpp>
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

// Logger for query processor. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.processor"));

// Query class implementation
Mutex Query::queriesMutex;
map<QueryID, boost::shared_ptr<Query> > Query::_queries;
uint32_t Query::nextID = 0;
// RNG
static boost::mt19937 _rng;

template<typename T>
T Query::runRestartableWork(boost::function<T()>& work)
{
    assert(work);
    while (true)
    {
        try {
            return work();
        } catch (const scidb::Exception& e) {
            LOG4CXX_ERROR(logger, "Query::runRestartableWork:"
                          << " Exception: "<< e.what()
                          << " will attempt to restart the operation");
            sleep(_rng()%5);
        }
    }
    assert(false);
    return T();
}

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
        LOG4CXX_DEBUG(logger, "Release lock of array " << i->first << " for query " << _queryID);
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
      if (nInstances <= 0)
          throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_LIVENESS_EMPTY);

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
      } else {
         _coordinatorID = mapPhysicalToLogical(coordID);
         assert(_coordinatorID < nInstances);
      }

      _remoteArrays.resize(nInstances);
      _receiveSemaphores.resize(nInstances);
      _receiveMessages.resize(nInstances);
      chunkReqs.resize(nInstances);
      chunkReplicasReqs.resize(nInstances);
      Finalizer f = bind(&Query::destroyFinalizer, _1);
      pushFinalizer(f);
      _errorQueue = NetworkManager::getInstance()->createWorkQueue();
      assert(_errorQueue);
      _mpiReceiveQueue = NetworkManager::getInstance()->createWorkQueue();
      assert(_mpiReceiveQueue);
      _operatorQueue = NetworkManager::getInstance()->createWorkQueue();
      _operatorQueue->stop();
      assert(_operatorQueue);
   }
   // register for notifications
   InstanceLivenessNotification::PublishListener listener =
      boost::bind(&Query::handleLivenessNotification, shared_from_this(), _1);
   _livenessListenerID = InstanceLivenessNotification::addPublishListener(listener);

   LOG4CXX_DEBUG(logger, "Initialized query (" << queryID << ")");
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
      LOG4CXX_DEBUG(logger, "Allocating query (" << queryID << ")");
      LOG4CXX_DEBUG(logger, "Number of allocated queries = " << _queries.size());
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
   LOG4CXX_DEBUG(logger, "Generated queryID: instanceID=" << instanceID << ", time=" << timeVal
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

        LOG4CXX_DEBUG(logger, "Query::done: queryID=" << _queryID
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
         LOG4CXX_FATAL(logger, "Query (" << _queryID
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
    LOG4CXX_DEBUG(logger, "Request shared lock of array " << arrayName << " for query " << _queryID);
    RWLock::ErrorChecker noopEc;
    lock->lockRead(noopEc);
    LOG4CXX_DEBUG(logger, "Granted shared lock of array " << arrayName << " for query " << _queryID);
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
    LOG4CXX_DEBUG(logger, "Request exclusive lock of array " << arrayName << " for query " << _queryID);
    RWLock::ErrorChecker noopEc;
    lock->lockWrite(noopEc);
    LOG4CXX_DEBUG(logger, "Granted exclusive lock of array " << arrayName << " for query " << _queryID);
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
            LOG4CXX_FATAL(logger, "Query (" << _queryID
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
        LOG4CXX_DEBUG(logger, "Query (" << queryId << ") is being aborted");

        if(_commitState == COMMITTED) {
            LOG4CXX_ERROR(logger, "Query (" << queryId
                          << ") cannot be aborted after commit."
                          << " completion status=" << completionStatus
                          << " commit status=" << _commitState
                          << " error=" << _error->getLongErrorCode());
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
            LOG4CXX_DEBUG(logger, "Query (" << queryId << ") is still in progress");
            return;
        }
        errorHandlersOnStack.swap(_errorHandlers);
        finalizersOnStack.swap(_finalizers);
    }
    if (!errorHandlersOnStack.empty()) {
        LOG4CXX_ERROR(logger, "Query (" << queryId << ") error handlers ("
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

        LOG4CXX_DEBUG(logger, "Query (" << _queryID << ") is committed");

        if (completionStatus != OK || _commitState == ABORTED) {
            LOG4CXX_ERROR(logger, "Query (" << _queryID
                          << ") cannot be committed after abort."
                          << " completion status=" << completionStatus
                          << " commit status=" << _commitState
                          << " error=" << _error->getLongErrorCode());
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_COMMIT_STATE) << _queryID << "commit");
            assert(false);
        }

        _errorHandlers.clear();

        _commitState = COMMITTED;

        if (_error->getLongErrorCode() == SCIDB_E_NO_ERROR)
        {
            _error = (SYSTEM_EXCEPTION_SPTR(SCIDB_SE_QPROC, SCIDB_LE_QUERY_ALREADY_COMMITED) << queryId);
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
    boost::shared_ptr<MessageDesc>  msg(makeAbortMessage(_queryID));
    NetworkManager::getInstance()->broadcast(msg);
}

void Query::handleLivenessNotification(boost::shared_ptr<const InstanceLiveness>& newLiveness)
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
       LOG4CXX_ERROR(logger, "Overflow exception from the error queue, queryID="<<_queryID);
      // XXX TODO: deal with this exception
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
   if (!found)
       throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_INSTANCE_OFFLINE) << instanceID;

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
        try {
            q->second->handleAbort();
        } catch (Exception&) { }
    }
}

#ifndef SCIDB_CLIENT
extern size_t totalDBChunkAllocatedSize;
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
                "Allocated size for DBChunks: " << totalDBChunkAllocatedSize <<
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
    {
        ScopedMutexLock cs(errorMutex);

        LOG4CXX_TRACE(logger, "Cleaning up query (" << getQueryID() << ")");

        // Drop all unprocessed messages and cut any circular references
        // (from MessageHandleJob to Query).
        // This should be OK because we broadcast either
        // the error or abort before dropping the messages

        _mpiReceiveQueue.swap(mpiQueue);
        _errorQueue.swap(errQueue);
        _operatorQueue.swap(sgQueue);

        // Unregister this query from liveness notifications
        InstanceLivenessNotification::removePublishListener(_livenessListenerID);

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
    map<QueryID, shared_ptr<Query> >::iterator i = _queries.find(queryID);
    if (i != _queries.end()) {
        shared_ptr<Query> q = i->second;
        LOG4CXX_DEBUG(logger, "Deallocating query (" << q->getQueryID() << ")");
        _queries.erase(i);
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

void Query::replicationBarrier()
{
    if (Config::getInstance()->getOption<int>(CONFIG_REDUNDANCY) <= 0) {
            return;
    }

    vector<boost::shared_ptr<ReplicationManager::Item> > replicasVec;
    boost::shared_ptr<MessageDesc> msg = boost::make_shared<MessageDesc>(mtReplicaSyncRequest);
    ReplicationManager* rm =  ReplicationManager::getInstance();
    assert(rm);
    {
        ScopedMutexLock lock(errorMutex);
        msg->setQueryID(_queryID);

        // we may relax this assert once we support update on a subset of instances
        assert(Cluster::getInstance()->getInstanceMembership()->getInstances().size() == getInstancesCount());

        for (vector<InstanceID>::const_iterator iter = _liveInstances.begin(); iter != _liveInstances.end(); ++iter) {
            if ((*iter) == _instanceID) {
                continue;
            }
            boost::shared_ptr<ReplicationManager::Item> item(new ReplicationManager::Item((*iter), msg, shared_from_this()));
            assert(rm);
            replicasVec.push_back(item);
        }
    }
    assert(replicasVec.size() == (getInstancesCount()-1));
    for (size_t i=0; i<replicasVec.size(); ++i) {
        const boost::shared_ptr<ReplicationManager::Item>& item = replicasVec[i];
        assert(rm);
        rm->send(item);
    }
    for (size_t i=0; i<replicasVec.size(); ++i) {
        const boost::shared_ptr<ReplicationManager::Item>& item = replicasVec[i];
        assert(rm);
        rm->wait(item);
        assert(item->isDone());
        assert(item->validate(false));
    }

    Semaphore::ErrorChecker ec = bind(&Query::validate, this);
    replicaSem.enter(replicasVec.size(), ec);
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
    Query::runRestartableWork(work);
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
   return SystemCatalog::getInstance()->deleteArray(coordLock->getArrayName());
}

void UpdateErrorHandler::handleError(const shared_ptr<Query>& query)
{
    boost::function<void()> work = boost::bind(&UpdateErrorHandler::_handleError, this, query);
    Query::runRestartableWork(work);
}

void UpdateErrorHandler::_handleError(const shared_ptr<Query>& query)
{
   assert(query);
   if (!_lock) {
      assert(false);
      LOG4CXX_TRACE(logger,
                    "Update error handler has nothing to do for query ("
                    << query->getQueryID() << ")");
      return;
   }
   assert(_lock->getInstanceId() == Cluster::getInstance()->getLocalInstanceId());
   assert( (_lock->getLockMode() == SystemCatalog::LockDesc::CRT)
           || (_lock->getLockMode() == SystemCatalog::LockDesc::WR) );
   assert(query->getQueryID() == _lock->getQueryId());

   LOG4CXX_DEBUG(logger,
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
   bool rc = Query::runRestartableWork(work);
   if (!rc) {
      LOG4CXX_WARN(logger, "Failed to release the lock for query ("
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
      LOG4CXX_DEBUG(logger, "UpdateErrorHandler::handleErrorOnCoordinator:"
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
      LOG4CXX_DEBUG(logger, "UpdateErrorHandler::handleErrorOnCoordinator:"
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

   if (newVersion != 0) {

       if (forceCoordLockCheck) {
           shared_ptr<SystemCatalog::LockDesc> coordLock;
           do {  //XXX TODO: fix the wait, possibly with batching the checks
               coordLock = SystemCatalog::getInstance()->checkForCoordinatorLock(arrayName,
                                                                                 lock->getQueryId());
               sleep((_rng()%2)+1);
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
       if (lastVersion < newVersion && newArrayVersionId > 0 && rollback) {
           rollback(lastVersion, arrayId, newArrayVersionId, timestamp);
       }
   } else if (rollback && lock->getImmutableArrayId() > 0) {
       rollback(-1, lock->getImmutableArrayId(), newArrayVersionId, timestamp);
   }
}

void UpdateErrorHandler::doRollback(VersionID lastVersion,
                                    ArrayID baseArrayId,
                                    ArrayID newArrayId,
                                    uint64_t timestamp)
{
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
           LOG4CXX_ERROR(logger, "UpdateErrorHandler::doRollback:"
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
           LOG4CXX_ERROR(logger, "UpdateErrorHandler::doRollback:"
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
    LOG4CXX_DEBUG(logger, "Releasing locks for query " << q->getQueryID());

    boost::function<uint32_t()> work = boost::bind(&SystemCatalog::deleteArrayLocks,
                                                   SystemCatalog::getInstance(),
                                                   Cluster::getInstance()->getLocalInstanceId(),
                                                   q->getQueryID());
    runRestartableWork(work);
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
    return arr ? arr : boost::shared_ptr<Array>(new DBArray(arrayName, shared_from_this()));
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
    LOG4CXX_DEBUG(logger, "Acquiring "<< locks.size()
                  << " array locks for query " << _queryID);

    bool foundDeadInstances = (_coordinatorLiveness->getNumDead() > 0);
    try {
        SystemCatalog::ErrorChecker errorChecker = bind(&Query::validate, this);
        BOOST_FOREACH(const shared_ptr<SystemCatalog::LockDesc>& lock, locks)
        {
            assert(lock);
            assert(lock->getQueryId() == _queryID);
            LOG4CXX_DEBUG(logger, "Acquiring lock: " << lock->toString());

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
    // logically const, but we made _procGrid mutable to allow the caching
    // NOTE: Tigor may wish to push this down into the MPI context when
    //       that code is further along.  But for now, Query is a fine object
    //       on which to cache the generated procGrid
    if (!_procGrid) {
        _procGrid = new ProcGrid(getInstancesCount());
    }
    return _procGrid;
}

} // namespace

