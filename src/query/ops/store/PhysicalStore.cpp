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
 * PhysicalStore.cpp
 *
 *  Created on: Apr 16, 2010
 *      Author: Knizhnik
 */


#include "query/Operator.h"
#include "query/QueryProcessor.h"
#include "query/TypeSystem.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "array/DBArray.h"
#include "system/SystemCatalog.h"
#include "network/NetworkManager.h"
#include "smgr/io/Storage.h"
#include "query/Statistics.h"

#include "array/ParallelAccumulatorArray.h"
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"

using namespace std;
using namespace boost;

namespace scidb {

class PhysicalStore: public PhysicalOperator
{
    class StoreJob : public Job, protected SelfStatistics
    {
      private:
        size_t shift;
        size_t step;
        shared_ptr<Array> dstArray;
        shared_ptr<Array> srcArray;
        vector< shared_ptr<ArrayIterator> > dstArrayIterators;
        vector< shared_ptr<ConstArrayIterator> > srcArrayIterators;

      public:
        Coordinates lowBoundary;
        Coordinates highBoundary;

        StoreJob(size_t id, size_t nJobs, shared_ptr<Array> dst,
                 shared_ptr<Array> src, size_t nDims, size_t nAttrs,
                 shared_ptr<Query> query)
        : Job(query), shift(id), step(nJobs), dstArray(dst), srcArray(src),
          dstArrayIterators(nAttrs), srcArrayIterators(nAttrs), lowBoundary(nDims, MAX_COORDINATE), highBoundary(nDims, MIN_COORDINATE)
        {
            for (size_t i = 0; i < nAttrs; i++) {
               dstArrayIterators[i] = dstArray->getIterator(i);
               srcArrayIterators[i] = srcArray->getConstIterator(i);
            }
        }

        virtual void run()
        {
            ArrayDesc const& dstArrayDesc = dstArray->getArrayDesc();
            size_t nAttrs = dstArrayDesc.getAttributes().size();
            size_t nDims = dstArrayDesc.getDimensions().size();
            Query::setCurrentQueryID(_query->getQueryID());

            for (size_t i = shift; i != 0 && !srcArrayIterators[0]->end(); --i) {
                for (size_t j = 0; j < nAttrs; j++) {
                    ++(*srcArrayIterators[j]);
                }
            }

            while(!srcArrayIterators[0]->end()) {
                for (size_t i = 0; i < nAttrs; i++) {
                    ConstChunk const& srcChunk = srcArrayIterators[i]->getChunk();
                    Coordinates const& first = srcChunk.getFirstPosition(false);
                    Coordinates const& last = srcChunk.getLastPosition(false);
                    for (size_t j = 0; j < nDims; j++) {
                        if (last[j] > highBoundary[j]) {
                            highBoundary[j] = last[j];
                        }
                        if (first[j] < lowBoundary[j]) {
                            lowBoundary[j] = first[j];
                        }
                    }
                    dstArrayIterators[i]->copyChunk(srcChunk);

                    Query::validateQueryPtr(_query);

                    for (size_t j = step; j != 0 && !srcArrayIterators[i]->end(); ++(*srcArrayIterators[i]), --j);
                }
            }
        }
    };


   private:
   ArrayID   _arrayID;   /**< ID of new array */
   ArrayID   _updateableArrayID;   /**< ID of new array */
   VersionID _lastVersion;
   shared_ptr<SystemCatalog::LockDesc> _lock;

  public:
        PhysicalStore(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
                PhysicalOperator(logicalName, physicalName, parameters, schema), _arrayID((ArrayID)~0), _updateableArrayID((ArrayID)~0)
        {
        }

   void preSingleExecute(shared_ptr<Query> query)
   {
        ArrayDesc desc;
        shared_ptr<const NodeMembership> membership(Cluster::getInstance()->getNodeMembership());
        assert(membership);
        if ((membership->getViewId() != query->getCoordinatorLiveness()->getViewId()) ||
            (membership->getNodes().size() != query->getNodesCount())) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM2);
        }
        _lock = shared_ptr<SystemCatalog::LockDesc>(new SystemCatalog::LockDesc(_schema.getName(),
                                                         query->getQueryID(),
                                                         Cluster::getInstance()->getLocalNodeId(),
                                                         SystemCatalog::LockDesc::COORD,
                                                         SystemCatalog::LockDesc::WR));
        shared_ptr<Query::ErrorHandler> ptr(new UpdateErrorHandler(_lock));
        query->pushErrorHandler(ptr);

        bool rc = false;
        if (!SystemCatalog::getInstance()->getArrayDesc(_schema.getName(), desc, false)) {

            if (_schema.getId() != 0) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST) << _schema.getName();
            }
           _lock->setLockMode(SystemCatalog::LockDesc::CRT);
           rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
           assert(rc);

           ArrayID newArrayID = SystemCatalog::getInstance()->addArray(_schema, psRoundRobin);
           desc = _schema;
           desc.setId(newArrayID);
        } else if (_schema.getId() != desc.getId()) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST) << _schema.getName();
        }

        if (desc.isImmutable()) {
           return;
        }
        Dimensions const& dims = desc.getDimensions();
        std::stringstream ss;
        _updateableArrayID = desc.getId();

        _lastVersion = SystemCatalog::getInstance()->getLastVersion(_updateableArrayID);
        if (_lastVersion != 0) {
           for (size_t i = 0; i < dims.size(); i++) {
              if (dims[i].getType() != TID_INT64) {
                 throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_STORE_ERROR1);
              }
           }
        }

        _lock->setArrayId(_updateableArrayID);
        _lock->setArrayVersion(_lastVersion+1);
        rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
        assert(rc);

        string const& arrayName = desc.getName();
        ss << arrayName << "@" << (_lastVersion+1);
        string versionName = ss.str();
        _schema = ArrayDesc(versionName, desc.getAttributes(), desc.grabDimensions(versionName));
        _arrayID = SystemCatalog::getInstance()->addArray(_schema, psRoundRobin);

        _lock->setArrayVersionId(_arrayID);
        rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
        assert(rc);
        rc = rc; // Eliminate warnings
   }

    virtual void postSingleExecute(shared_ptr<Query> query)
    {
        assert(_lock);
        if (_updateableArrayID != (ArrayID)~0) {
            SystemCatalog::getInstance()->createNewVersion(_updateableArrayID, _arrayID);
        }
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    virtual DistributionRequirement getDistributionRequirement (const std::vector< ArrayDesc> & inputSchemas) const
    {
        vector<ArrayDistribution> requiredDistribution;
        requiredDistribution.push_back(ArrayDistribution(psRoundRobin));
        return DistributionRequirement(DistributionRequirement::SpecificAnyOrder, requiredDistribution);
    }

    shared_ptr<Array> execute(vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        if (!_lock) {
           VersionID version(0);
           string baseArrayName = splitArrayNameVersion(_schema.getName(), version);
           _lock = shared_ptr<SystemCatalog::LockDesc>(new SystemCatalog::LockDesc(baseArrayName,
                                                                                          query->getQueryID(),
                                                                                          Cluster::getInstance()->getLocalNodeId(),
                                                                                          SystemCatalog::LockDesc::WORKER,
                                                                                          SystemCatalog::LockDesc::WR));
           _lock->setArrayVersion(version);
           shared_ptr<Query::ErrorHandler> ptr(new UpdateErrorHandler(_lock));
           query->pushErrorHandler(ptr);

           Query::Finalizer f = bind(&UpdateErrorHandler::releaseLock,
                                     _lock, _1);
           query->pushFinalizer(f);
           SystemCatalog::ErrorChecker errorChecker = bind(&Query::validate, query);
           bool rc = SystemCatalog::getInstance()->lockArray(_lock, errorChecker);
           if (!rc) {
              throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_INCREMENT_LOCK)
              << baseArrayName;
           }
        }
        shared_ptr<Array> srcArray = inputArrays[0];
        shared_ptr<Array> dstArray = shared_ptr<Array>(new DBArray(_schema.getName(), query)); // We can't use _arrayID because it's not initialized on remote nodes
        ArrayDesc const& dstArrayDesc = dstArray->getArrayDesc();
        size_t nAttrs = dstArrayDesc.getAttributes().size();
        ArrayID arrId = dstArray->getHandle();

        if (nAttrs == 0) { 
            return dstArray;
        }

        // Perform parallel evaluation of aggregate
        shared_ptr<JobQueue> queue = ParallelAccumulatorArray::getQueue();
        size_t nJobs = srcArray->supportsRandomAccess() ? Config::getInstance()->getOption<int>(CONFIG_PREFETCHED_CHUNKS) : 1;
        vector< shared_ptr<StoreJob> > jobs(nJobs);
        size_t nDims = dstArrayDesc.getDimensions().size();
        for (size_t i = 0; i < nJobs; i++) {
            jobs[i] = shared_ptr<StoreJob>(new StoreJob(i, nJobs, dstArray, srcArray, nDims, nAttrs, query));
        }
        for (size_t i = 0; i < nJobs; i++) {
            queue->pushJob(jobs[i]);
        }

        // Combine results
        Coordinates lowBoundary(nDims, MAX_COORDINATE);
        Coordinates highBoundary(nDims, MIN_COORDINATE);
        int errorJob = -1;
        for (size_t i = 0; i < nJobs; i++) {
            if (!jobs[i]->wait()) {
                errorJob = i;
            } else {
                for (size_t j = 0; j < nDims; j++) {
                    if (jobs[i]->highBoundary[j] > highBoundary[j]) {
                        highBoundary[j] = jobs[i]->highBoundary[j];
                    }
                    if (jobs[i]->lowBoundary[j] < lowBoundary[j]) {
                        lowBoundary[j] = jobs[i]->lowBoundary[j];
                    }
                }
            }
        }
        if (errorJob >= 0) {
            jobs[errorJob]->rethrow();
        }
        SystemCatalog::getInstance()->updateArrayBoundaries(arrId, lowBoundary, highBoundary);
        StorageManager::getInstance().flush();
        query->replicationBarrier();

        return dstArray;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalStore, "store", "physicalStore")

}  // namespace ops
