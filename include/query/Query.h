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
 * @file Query.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Query context
 */

#ifndef QUERY_H_
#define QUERY_H_

#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_array.hpp>
#include <string>
#include <queue>
#include <deque>
#include <stdio.h>
#include <list>

#include "array/Metadata.h"
#include "array/MemArray.h"
#include "util/Semaphore.h"
#include "util/Event.h"
#include "util/RWLock.h"
#include "SciDBAPI.h"
#include "util/Atomic.h"
#include "query/Aggregate.h"
#include "query/Statistics.h"
#include <system/BlockCyclic.h>
#include "system/Cluster.h"
#include "system/Warnings.h"
#include "system/SystemCatalog.h"
#include <util/WorkQueue.h>

namespace boost
{
   bool operator< (boost::shared_ptr<scidb::SystemCatalog::LockDesc> const& l,
                   boost::shared_ptr<scidb::SystemCatalog::LockDesc> const& r);
   bool operator== (boost::shared_ptr<scidb::SystemCatalog::LockDesc> const& l,
                    boost::shared_ptr<scidb::SystemCatalog::LockDesc> const& r);
   bool operator!= (boost::shared_ptr<scidb::SystemCatalog::LockDesc> const& l,
                    boost::shared_ptr<scidb::SystemCatalog::LockDesc> const& r);
} // namespace boost

namespace std
{
   template<>
   struct less<boost::shared_ptr<scidb::SystemCatalog::LockDesc> > :
   binary_function <const boost::shared_ptr<scidb::SystemCatalog::LockDesc>,
                    const boost::shared_ptr<scidb::SystemCatalog::LockDesc>,bool>
   {
      bool operator() (const boost::shared_ptr<scidb::SystemCatalog::LockDesc>& l,
                       const boost::shared_ptr<scidb::SystemCatalog::LockDesc>& r) const ;
   };
} // namespace std

namespace scidb
{

class LogicalPlan;
class PhysicalPlan;
class RemoteArray;
class RemoteMergedArray;
class MessageDesc;

const size_t MAX_BARRIERS = 2;

/**
 * The query structure keeps track of query execution and manages the resources used by SciDB
 * in order to execute the query. The Query is a state of query processor to make
 * query processor stateless. The object lives while query is used including receiving results.
 */
class Query : public boost::enable_shared_from_this<Query>
{
 public:
    class OperatorContext
    {
      public:
        OperatorContext() {}
        virtual ~OperatorContext() = 0;
    };
    class ErrorHandler
    {
      public:
        virtual void handleError(const boost::shared_ptr<Query>& query) = 0;
        virtual ~ErrorHandler() {}
    };

    typedef boost::function<void(const boost::shared_ptr<Query>&)> Finalizer;

    typedef std::set<boost::shared_ptr<SystemCatalog::LockDesc> >  QueryLocks;
  private:
    /**
     * Hold next value for generation query ID
     */
     static uint32_t nextID;

    /**
     * Query identifier to find the query during asynchronous message exchanging.
     */
    QueryID _queryID;

    /**
     * The map of queries to keep every query running in a system.
     */
    static std::map<QueryID, boost::shared_ptr<Query> > _queries;

    /**
     * Get/insert a query object from/to the list of queries
     * @param queryID query ID
     * @param query the query object
     * @return true if inserted, false if already present
     */
    static bool insert(QueryID queryID, boost::shared_ptr<Query>& query);

    boost::shared_ptr<OperatorContext> _operatorContext;

    std::map< std::string, boost::shared_ptr<Array> > _temporaryArrays;

    /**
     * The physical plan of query. Optimizer generates it for current step of incremental execution
     * from current logical plan. This plan is generated on coordinator and sent out to every instance for execution.
     */
    std::vector< boost::shared_ptr<PhysicalPlan> > _physicalPlans;

    /**
     * Snapshot of the liveness information on the coordiantor
     * The worker instances must fail the query if their liveness view
     * is/becomes different any time during the query execution.
     */
    boost::shared_ptr<const InstanceLiveness> _coordinatorLiveness;

    /// Registration ID for liveness notifications
    InstanceLivenessNotification::ListenerID _livenessListenerID;

    /**
     * The list of instances considered alive for the purposes
     * of this query. It is initialized to the liveness of
     * the coordinator when it starts the query. If any instance
     * detects a discrepancy in its current liveness and
     * this query liveness, it causes the query to abort.
     */
    std::vector<InstanceID> _liveInstances;

    /**
     * A "logical" instance ID of the local instance
     * for the purposes of this query.
     * It is obtained from the "physical" instance ID using
     * the coordinator liveness as the map.
     * Currently, it is the index of the local instance into
     * the sorted list of live instance IDs.
     */
    InstanceID _instanceID;

    /**
     * The "logical" instance ID of the instance responsible for coordination of query.
     * COORDINATOR_INSTANCE if instance execute this query itself.
     */
    InstanceID _coordinatorID;

    std::vector<Warning> _warnings;

    /**
     * Error state
     */
    Mutex errorMutex;

    boost::shared_ptr<Exception> _error;

    friend class UpdateErrorHandler;
    /**
     * @return true if query is in error state due to a cancel request
     */
    bool isForceCancelled()
    {
       ScopedMutexLock cs(errorMutex);
       bool result = (_error->getLongErrorCode() == SCIDB_LE_QUERY_CANCELLED);
       assert (!result || _commitState==ABORTED);
       return result;
    }
    bool checkFinalState();
    Mutex _warningsMutex;

    /// Query array locks requested by the operators
    std::set<boost::shared_ptr<SystemCatalog::LockDesc> >  _requestedLocks;
    std::deque< boost::shared_ptr<ErrorHandler> > _errorHandlers;
    std::deque<Finalizer> _finalizers; // last minute actions

    /** execution of query completion status */
    typedef enum {
        INIT, // query execute() has not started
        START, // query execute() has not completed
        OK,   // query execute() completed with no errors
        ERROR // query execute() completed with errors
    } CompletionStatus;
    CompletionStatus completionStatus;

    /** Query commit state */
    typedef enum {
        UNKNOWN,
        COMMITTED,
        ABORTED
    } CommitState;
    CommitState _commitState;

    /**
     * Queue for MPI-style messages
     */
    boost::shared_ptr<scidb::WorkQueue> _mpiReceiveQueue;

    /**
     * FIFO queue for error messages
     */
    boost::shared_ptr<scidb::WorkQueue> _errorQueue;

    /**
     * FIFO queue for SG messages
     */
    boost::shared_ptr<scidb::WorkQueue> _operatorQueue;

    /**
     *  Helper to invoke the finalizers with exception handling
     */
    void invokeFinalizers(std::deque<Finalizer>& finalizers);

    /**
     *  Helper to invoke the finalizers with exception handling
     */
    void invokeErrorHandlers(std::deque< boost::shared_ptr<ErrorHandler> >& errorHandlers);

    void destroy();
    static void destroyFinalizer(const boost::shared_ptr<Query>& q)
    {
        assert(q);
        q->destroy();
    }

    /**
     * The result of query execution. It lives while the client connection is established.
     * In future we can develop more useful policy of result keeping with multiple
     * re-connections to query.
     */
    boost::shared_ptr<Array> _currentResultArray;

    /**
     * TODO: XXX
     */
    boost::shared_ptr<RemoteMergedArray> _mergedArray;

    /**
     * TODO: XXX
     */
    std::vector<boost::shared_ptr<RemoteArray> > _remoteArrays;

    /**
     * Time of query creation;
     */
    time_t _creationTime;

    /**
     * Used counter - increased for every handler which process Query and decreased after.
     * 0 means that client did not fetching data and query was executed.
     */
    int _useCounter;

    std::map< std::string, boost::shared_ptr<RWLock> > locks;
    Mutex lockMutex;

    void checkNoError() const
    {
        if (SCIDB_E_NO_ERROR != _error->getLongErrorCode())
        {
            _error->raise();
        }
    }

    /**
     * true if the query acquires exclusive locks
     */
    bool _doesExclusiveArrayAccess;

    /**
    * cache for the ProGrid, which depends only on numInstances
    */
    mutable ProcGrid* _procGrid; // only access via getProcGrid()
 public:
    Query();
    ~Query();

    void sharedLock(std::string const& arrayName);
    void exclusiveLock(std::string const& arrayName);

    /**
     * The mutex to serialize access to _queries map.
     */
    static Mutex queriesMutex;
    /**
     * Generate unique(?) query ID
     */
    static QueryID generateID();

    /**
     * @return the number of queries currently in the system
     * @param class Observer_tt
     *{
     *  bool operator!();
     *
     *  void operator()(const shared_ptr<scidb::Query>&)
     *}
     */
    template<class Observer_tt>
    static size_t listQueries(Observer_tt& observer);

    /**
     * Add an error handler to run after a query's "main" routine has completed
     * and the query needs to be aborted/rolled back
     * @param eh - the error handler
     */
    void pushErrorHandler(const boost::shared_ptr<ErrorHandler>& eh);

    /**
     * Add a finalizer to run after a query's "main" routine has completed (with any status)
     * and the query is about to be removed from the system
     * @param f - the finalizer
     */
    void pushFinalizer(const Finalizer& f);

    Mutex resultCS; /** < Critical section for SG result */

    bool isDDL;

    RWLock queryLock;

    std::map<std::string, boost::shared_ptr<const ArrayDesc> > arrayDescByNameCache;

    /**
     * @brief _mappingArrays
     * This is a map of mapping arrays for sending to the client.
     * They must be ready for sending and AccumulatorArray.
     */
    std::map< std::string, boost::shared_ptr<Array> > _mappingArrays;

    /**
     * Program options which is used to run query
     */
    std::string programOptions;

    /**
     * Initialize a query
     * @param queryID
     * @param coordID the "physical" coordinator ID (or COORDINATOR_INSTANCE if on coordinator)
     * @param localInstanceID  "physical" local instance ID
     * @param coordinatorLiveness coordinator liveness at the time of query creation
     */
    void init(QueryID queryID, InstanceID coordID, InstanceID localInstanceID,
              boost::shared_ptr<const InstanceLiveness> coordinatorLiveness);
    /**
     * Handle a change in the local instance liveness. If the new livenes is different
     * from this query's coordinator liveness, the query is marked to be aborted.
     */
    void handleLivenessNotification(boost::shared_ptr<const InstanceLiveness>& newLiveness);
    /**
     * Map a "logical" instance ID to a "physical" one using the coordinator liveness
     */
    InstanceID mapLogicalToPhysical(InstanceID instance);
    /**
     * Map a "physical" instance ID to a "logical" one using the coordinator liveness
     */
    InstanceID mapPhysicalToLogical(InstanceID instance);

    /**
     * @return true if a given instance is considered dead
     * @param instance physical ID of a instance
     * @throw scidb::SystemException if this.errorCode is not 0
     */
    bool isPhysicalInstanceDead(InstanceID instance);

    /**
     * Get the "physical" instance ID of the coordinator
     * @return COORDINATOR_INSTANCE if this instance is the coordinator, else the coordinato instance ID
     */
    InstanceID getPhysicalCoordinatorID();

    /**
     * Get logical instance count
     */
    size_t getInstancesCount() const
    {
        return _liveInstances.size();
    }

    /**
     * Info needed for ScaLAPACK-compatible chunk distributions
     * Redistribution code and ScaLAPACK-based plugins need this,
     * most operators do not.
     */
    const ProcGrid* getProcGrid() const;

    /**
     * Get logical instance ID
     */
    InstanceID getInstanceID() const
    {
        return _instanceID;
    }
    /**
     * Get coordinator's logical instance ID
     */
    InstanceID getCoordinatorID()
    {
        return _coordinatorID;
    }

    InstanceID getCoordinatorInstanceID()
    {
        return _coordinatorID == COORDINATOR_INSTANCE ? _instanceID : _coordinatorID;
    }

    boost::shared_ptr<const InstanceLiveness> getCoordinatorLiveness()
    {
       return _coordinatorLiveness;
    }

    /**
     * The string with query that user want to execute.
     */
    std::string queryString;

    boost::shared_ptr<Array> getCurrentResultArray()
    {
        ScopedMutexLock cs(errorMutex);
        validate();
        return _currentResultArray;
    }

    void setCurrentResultArray(const boost::shared_ptr<Array>& array)
    {
        ScopedMutexLock cs(errorMutex);
        validate();
        _currentResultArray = array;
    }

    boost::shared_ptr<RemoteMergedArray> getMergedArray()
    {
        ScopedMutexLock cs(errorMutex);
        validate();
        return _mergedArray;
    }

    void setMergedArray(const boost::shared_ptr<RemoteMergedArray>& array)
    {
        ScopedMutexLock cs(errorMutex);
        validate();
        _mergedArray = array;
    }


    boost::shared_ptr<RemoteArray> getRemoteArray(const InstanceID& instanceID)
    {
        ScopedMutexLock cs(errorMutex);
        validate();
        assert(!_remoteArrays.empty());
        assert(instanceID < _remoteArrays.size());
        return _remoteArrays[instanceID];
    }

    void setRemoteArray(const InstanceID& instanceID, const boost::shared_ptr<RemoteArray>& array)
    {
        ScopedMutexLock cs(errorMutex);
        validate();
        assert(!_remoteArrays.empty());
        assert(instanceID < _remoteArrays.size());
        _remoteArrays[instanceID] = array;
    }

    std::vector<boost::shared_ptr<Array> > tempArrays;

    Statistics statistics;

    /**
     * The logical plan of query. QueryProcessor generates it by parser only at coordinator instance.
     * Since we use incremental optimization this is the rest of logical plan to be executed.
     */
    boost::shared_ptr<LogicalPlan> logicalPlan;

    /**
     * Request that a given array lock be acquired before the query execution starts
     * @param lock - the lock description
     * @return either the requested lock or the lock that has already been requested for the same array
     *         with a more exclusive mode (RD < WR,CRT,RM,RNF,RNT)
     * @see scidb::SystemCatalog::LockDesc
     */
    boost::shared_ptr<SystemCatalog::LockDesc>
    requestLock(boost::shared_ptr<SystemCatalog::LockDesc>& lock);

    void addPhysicalPlan(boost::shared_ptr<PhysicalPlan> physicalPlan)
    {
        _physicalPlans.push_back(physicalPlan);
    }

    boost::shared_ptr<PhysicalPlan> getCurrentPhysicalPlan()
    {
        return _physicalPlans.back();
    }

    /**
     * Get the queue for delivering MPI-Send (mtMPISend) messages
     * @return empty pointer if the query is no longer active
     */
    boost::shared_ptr<scidb::WorkQueue> getMpiReceiveQueue()
    {
        ScopedMutexLock cs(errorMutex);
        return _mpiReceiveQueue;
    }

    boost::shared_ptr<scidb::WorkQueue> getErrorQueue()
    {
        ScopedMutexLock cs(errorMutex);
        return _errorQueue;
    }

    boost::shared_ptr<scidb::WorkQueue> getOperatorQueue()
    {
        ScopedMutexLock cs(errorMutex);
        return _operatorQueue;
    }

    /**
     *  Context variables to control thread
     */
    Semaphore results;

    /**
     * Semaphores for synchronization SG operations on remote instances
     */
    Semaphore semSG[MAX_BARRIERS];
    Semaphore syncSG;

    Semaphore replicaSem;

    struct PendingRequests {
        Mutex  mutex;
        size_t nReqs;
        bool   sync;

        void increment();
        bool decrement();
        bool test();

        PendingRequests() : nReqs(0), sync(false) {}
    };
    std::vector<PendingRequests> chunkReqs;
    std::vector<PendingRequests> chunkReplicasReqs;

    /**
     * Creates new query object detached from global store. No queryID.
     */
    static boost::shared_ptr<Query> createDetached();

    /**
     * Creates new query object and generate new queryID
     */
    static boost::shared_ptr<Query> create(QueryID queryId);

    /**
     * Finds query with given queryID and creates it if can't find
     */
    static boost::shared_ptr<Query> getQueryByID(QueryID queryID, bool create = false, bool raise = true);

    /**
     * Validates the pointer and the query it points for errors
     * @throws scidb::SystemException if the pointer is empty or if the query is in error state
     * @return true if no exception is thrown
     */
    static bool validateQueryPtr(const boost::shared_ptr<Query>& query);

    /**
     * Destroys query contexts for every still existing query
     */
    static void freeQueries();

    /**
     * Destroy specified query context
     */
    static void freeQuery(QueryID queryID);

    /**
     * Release all the locks previously acquired by acquireLocks()
     * @param query whose locks to release
     * @throws exceptions while releasing the lock
     */
    static void releaseLocks(const boost::shared_ptr<Query>& query);

    /**
     * Get temporary array
     * @param arrayName temporary array name
     * @return reference to the temporayr array or null referenec is not exists
     */
    boost::shared_ptr<Array> getTemporaryArray(std::string const& arrayName);

    /**
     * Get temporary or persistent array
     * @param arrayName array name
     * @return reference to the array
     * @exception SCIDB_LE_ARRAY_DOESNT_EXIST
     */
    boost::shared_ptr<Array> getArray(std::string const& arrayName);

    /**
     * Associate temporay array with this query
     * @param tmpArray temporary array
     */
    void setTemporaryArray(boost::shared_ptr<Array> const& tmpArray);

    /**
     * Repeatedly execute given work until it either succeeds
     * or throws an unrecoverable exception. Any scidb::Exeption
     * is considered recoverable.
     * @param work to execute
     * @return result of running work
     */
    template<typename T>
    static T runRestartableWork(boost::function<T()>& work);

    /**
     * Acquire all locks requested via requestLock(),
     * it blocks until all of the locks are acquired
     * @throws exceptions while acquiring the locks and the same exceptions as validate()
     */
    void acquireLocks();

    /**
     * @return  true if the query acquires exclusive locks
     */
    bool doesExclusiveArrayAccess();

    /**
     * Handle a query error.
     * May attempt to invoke error handlers
     */
    void handleError(boost::shared_ptr<Exception> unwindException);

    /**
     * Handle a client complete request
     */
    void handleComplete();

    /**
     * Handle a client cancellation request
     */
    void handleCancel();

    /**
     * Handle a coordinator commit request
     */
    void handleCommit();

    /**
     * Handle a coordinator abort request
     */
    void handleAbort();

    /**
     * Retursns query ID
     */
    QueryID getQueryID() const
    {
        return _queryID;
    }

    /**
     * Return current queryID for thread.
     */
    static QueryID getCurrentQueryID();

    /**
     * Set current queryID for thread.
     */
    static void setCurrentQueryID(QueryID queryID);

    /**
     * Set result SG context. Thread safe.
     */
    void setOperatorContext(boost::shared_ptr<OperatorContext> const& opContext);

    /**
     * Remove result SG context.
     */
    void unsetOperatorContext();

    /**
     * Synchronize states of replics
     */
    void replicationBarrier();

    /**
     * @return SG Context. Thread safe.
     * wait until context is not NULL,
     */
    boost::shared_ptr<OperatorContext> getOperatorContext(){
        ScopedMutexLock lock(errorMutex);
        return _operatorContext;
    }

    /**
     * Mark query as started
     */
    void start();

    /**
     * Suspend query processsing: state will be INIT
     */
    void stop();

    /**
     * Mark query as completed
     */
    void done();

    /**
     * Mark query as completed with an error
     */
    void done(const boost::shared_ptr<Exception> unwindException);

    /**
     * This section describe member fields needed for implementing send/receive MPI functions.
     */
    Mutex _receiveMutex; //< Mutex for serialization access to _receiveXXX fields.

    /**
     * This vector holds send/receive messages for current query at this instance.
     * index in vector is source instance number.
     */
    std::vector< std::list<boost::shared_ptr< MessageDesc> > > _receiveMessages;

    /**
     * This vector holds semaphores for working with messages queue. One semaphore for every source instance.
     */
    std::vector< Semaphore> _receiveSemaphores;

    void* userDefinedContext;

    /**
     * Write statistics of query into output stream
     */
    std::ostream& writeStatistics(std::ostream& os) const;

    /**
     * Statistics monitor for query
     */
    boost::shared_ptr<StatisticsMonitor> statisticsMonitor;

    /**
     * Validates the query for errors
     * @throws scidb::SystemException if the query is in error state
     * @return true if no exception is thrown
     */
    bool validate();

    void postWarning(const class Warning& warn);

    std::vector<Warning> getWarnings();

    void clearWarnings();

    time_t getCreationTime() const {
        return _creationTime;
    }

    int32_t getErrorCode() const {
        return _error ? _error->getLongErrorCode() : 0;
    }

    std::string getErrorDescription() const {
        return _error ? _error->getErrorMessage() : "";
    }

    bool idle() const {
        return _useCounter == 0;
    }

    void incUseCounter() {
        _useCounter++;
    }

    void decUseCounter() {
        _useCounter--;
        assert(_useCounter >= 0);
    }
};

class CurrentQueryScope
{
public:
    CurrentQueryScope(QueryID queryID): _prevQueryID(Query::getCurrentQueryID())
    {
        Query::setCurrentQueryID(queryID);

        ScopedMutexLock scope(Query::queriesMutex);
        _query = Query::getQueryByID(queryID, false, false);
        if (_query) {
            _query->incUseCounter();
        }
    }

    ~CurrentQueryScope()
    {
        Query::setCurrentQueryID(_prevQueryID);
        if (_query) {
            ScopedMutexLock scope(Query::queriesMutex);
            _query->decUseCounter();
        }
    }

private:
    QueryID _prevQueryID;
    boost::shared_ptr<Query> _query;
};

class UpdateErrorHandler : public Query::ErrorHandler
{
 public:
    typedef boost::function< void(VersionID,ArrayID,ArrayID,uint64_t) > RollbackWork;

    UpdateErrorHandler(const boost::shared_ptr<SystemCatalog::LockDesc> & lock)
    : _lock(lock) { assert(_lock); }
    virtual ~UpdateErrorHandler() {}
    virtual void handleError(const boost::shared_ptr<Query>& query);

    static void releaseLock(const boost::shared_ptr<SystemCatalog::LockDesc>& lock,
                            const boost::shared_ptr<Query>& query);

    static void handleErrorOnCoordinator(const boost::shared_ptr<SystemCatalog::LockDesc>& lock,
                                         RollbackWork& rw);
    static void handleErrorOnWorker(const boost::shared_ptr<SystemCatalog::LockDesc>& lock,
                                    bool forceCoordLockCheck,
                                    RollbackWork& rw);
 private:
    static void doRollback(VersionID lastVersion,
                           ArrayID   baseArrayId,
                           ArrayID   newArrayId,
                           uint64_t  timestamp);
    void _handleError(const boost::shared_ptr<Query>& query);

    UpdateErrorHandler(const UpdateErrorHandler&);
    UpdateErrorHandler& operator=(const UpdateErrorHandler&);

 private:
    const boost::shared_ptr<SystemCatalog::LockDesc> _lock;
};


class RemoveErrorHandler : public Query::ErrorHandler
{
 public:
    RemoveErrorHandler(const boost::shared_ptr<SystemCatalog::LockDesc> & lock)
    : _lock(lock) { assert(_lock); }
    virtual ~RemoveErrorHandler() {}
    virtual void handleError(const boost::shared_ptr<Query>& query);

    static bool handleRemoveLock(const boost::shared_ptr<SystemCatalog::LockDesc>& lock,
                                 bool forceLockCheck);
 private:
    RemoveErrorHandler(const RemoveErrorHandler&);
    RemoveErrorHandler& operator=(const RemoveErrorHandler&);
 private:
    const boost::shared_ptr<SystemCatalog::LockDesc> _lock;
};

template<class Observer_tt>
size_t Query::listQueries(Observer_tt& observer)
{
    ScopedMutexLock mutexLock(queriesMutex);

    if (!observer) {
        return _queries.size();
    }

    for (std::map<QueryID, boost::shared_ptr<Query> >::const_iterator q = _queries.begin();
         q != _queries.end(); ++q) {
        observer(q->second);
    }
    return _queries.size();

}

} // namespace

#endif /* QUERY_H_ */
