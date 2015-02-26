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
#include "array/DelegateArray.h"
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
            Dimensions const& dims = dstArrayDesc.getDimensions();
            size_t nDims = dims.size();
                
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
                    bool newHighBoundary = false;
                    for (size_t j = 0; j < nDims; j++) {
                        if (last[j] > highBoundary[j]) {
                            if (dims[j].getLength() == INFINITE_LENGTH) { 
                                newHighBoundary = true;
                            } else { 
                                highBoundary[j] = last[j];
                            }
                        }
                        if (first[j] < lowBoundary[j]) {
                            lowBoundary[j] = first[j];
                        }
                    }
                    if (newHighBoundary) { 
                        Coordinates high = srcChunk.getHighBoundary(false);
                        for (size_t j = 0; j < nDims; j++) {
                            if (high[j] > highBoundary[j]) {
                                highBoundary[j] = high[j];
                            }
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
        shared_ptr<const InstanceMembership> membership(Cluster::getInstance()->getInstanceMembership());
        assert(membership);
        if ((membership->getViewId() != query->getCoordinatorLiveness()->getViewId()) ||
            (membership->getInstances().size() != query->getInstancesCount())) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM2);
        }
        string const& arrayName = _schema.getName();
        _lock = shared_ptr<SystemCatalog::LockDesc>(new SystemCatalog::LockDesc(arrayName,
                                                         query->getQueryID(),
                                                         Cluster::getInstance()->getLocalInstanceId(),
                                                         SystemCatalog::LockDesc::COORD,
                                                         SystemCatalog::LockDesc::WR));
        shared_ptr<Query::ErrorHandler> ptr(new UpdateErrorHandler(_lock));
        query->pushErrorHandler(ptr);

        Dimensions const& dims =  _schema.getDimensions();        
        size_t nDims = dims.size();
        Dimensions newVersionDims(nDims);
        bool arrayExists = SystemCatalog::getInstance()->getArrayDesc(arrayName, desc, false);
        _lastVersion = 0;
        bool rc = false;
        if (!arrayExists) { 
            if (_schema.getId() != 0) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST) << arrayName        ;
            }
            _lock->setLockMode(SystemCatalog::LockDesc::CRT);
            rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
            assert(rc);
            desc = _schema;
            desc.setId(SystemCatalog::getInstance()->addArray(desc, psRoundRobin));
        } else { 
            if (_schema.getId() != desc.getId()) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST) << arrayName;
            }
            if (!desc.isImmutable()) { 
                _lastVersion = SystemCatalog::getInstance()->getLastVersion(desc.getId());
            } 
        }

        Dimensions const& dstDims = desc.getDimensions();        

        for (size_t i = 0; i < nDims; i++) {
            DimensionDesc const& dim = dims[i];
            string const& mappingArrayName = dim.getMappingArrayName();
            newVersionDims[i] = dim;
            if (dim.getType() != TID_INT64 && !mappingArrayName.empty()) { 
                if (arrayExists && desc.isImmutable() && mappingArrayName != dstDims[i].getMappingArrayName()) { 
                    throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CAN_NOT_CHANGE_MAPPING) << arrayName;
                }
                boost::shared_ptr<Array> tmpMappingArray = query->getTemporaryArray(mappingArrayName);
                if (tmpMappingArray) { 
                    ArrayDesc const& tmpMappingArrayDesc = tmpMappingArray->getArrayDesc();
                    string newMappingArrayName = desc.createMappingArrayName(i, _lastVersion+1);
                    if (SystemCatalog::getInstance()->containsArray(newMappingArrayName)) { 
                        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_ALREADY_EXIST) << newMappingArrayName;
                    }
                    ArrayDesc mappingArrayDesc(newMappingArrayName, tmpMappingArrayDesc.getAttributes(), tmpMappingArrayDesc.getDimensions(), ArrayDesc::LOCAL);
                    ArrayID mappingArrayID = SystemCatalog::getInstance()->addArray(mappingArrayDesc, psReplication);
                    assert(mappingArrayID > 0);
                    newVersionDims[i] = DimensionDesc(dim.getBaseName(),
                                                      dim.getNamesAndAliases(),
                                                      dim.getStartMin(), dim.getCurrStart(),
                                                      dim.getCurrEnd(), dim.getEndMax(), dim.getChunkInterval(),
                                                      dim.getChunkOverlap(), dim.getType(), dim.getFlags(),
                                                      newMappingArrayName,
                                                      dim.getComment(),
                                                      dim.getFuncMapOffset(),
                                                      dim.getFuncMapScale());
                 }
            }
        }
        if (desc.isImmutable()) {
           return;
        }

        _updateableArrayID = desc.getId();
        _lock->setArrayId(_updateableArrayID);
        _lock->setArrayVersion(_lastVersion+1);
        rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
        assert(rc);

        std::stringstream ss;
        ss << arrayName << "@" << (_lastVersion+1);
        string versionName = ss.str();
        _schema = ArrayDesc(versionName, desc.getAttributes(), newVersionDims);
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
        VersionID version(0);
        string baseArrayName = splitArrayNameVersion(_schema.getName(), version);
        query->exclusiveLock(baseArrayName);

        if (!_lock) {
           _lock = shared_ptr<SystemCatalog::LockDesc>(new SystemCatalog::LockDesc(baseArrayName,
                                                                                   query->getQueryID(),
                                                                                   Cluster::getInstance()->getLocalInstanceId(),
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
        ArrayDesc const& srcArrayDesc = srcArray->getArrayDesc();
        shared_ptr<Array> dstArray = shared_ptr<Array>(new DBArray(_schema.getName(), query)); // We can't use _arrayID because it's not initialized on remote instances
        ArrayDesc const& dstArrayDesc = dstArray->getArrayDesc();
        size_t nAttrs = dstArrayDesc.getAttributes().size();
        ArrayID arrId = dstArray->getHandle();

        if (nAttrs == 0) { 
            return dstArray;
        }
        if (nAttrs != srcArrayDesc.getAttributes().size()) { 
            srcArray = boost::shared_ptr<Array>(new NonEmptyableArray(srcArray));
        } 

        // Perform parallel evaluation of aggregate
        shared_ptr<JobQueue> queue = ParallelAccumulatorArray::getQueue();
        size_t nJobs = srcArray->supportsRandomAccess() ? Config::getInstance()->getOption<int>(CONFIG_PREFETCHED_CHUNKS) : 1;
        vector< shared_ptr<StoreJob> > jobs(nJobs);
        Dimensions const& dims = dstArrayDesc.getDimensions();
        Dimensions const& srcDims = srcArrayDesc.getDimensions();
        size_t nDims = dims.size();
        for (size_t i = 0; i < nJobs; i++) {
            jobs[i] = shared_ptr<StoreJob>(new StoreJob(i, nJobs, dstArray, srcArray, nDims, nAttrs, query));
        }
        for (size_t i = 0; i < nJobs; i++) {
            queue->pushJob(jobs[i]);
        }

        // Combine results
        Coordinates lowBoundary(nDims);
        Coordinates highBoundary(nDims);
        for (size_t i = 0; i < nDims; i++) {
            lowBoundary[i] = srcDims[i].getCurrStart();
            highBoundary[i] = srcDims[i].getCurrEnd();
        }
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
        for (size_t i = 0; i < nDims; i++) {
            if (lowBoundary[i] < dims[i].getStartMin()) {  
                lowBoundary[i] = dims[i].getStartMin();
            }
            if (highBoundary[i] > dims[i].getEndMax()) {  
                highBoundary[i] = dims[i].getEndMax();
            }
        }
        SystemCatalog::getInstance()->updateArrayBoundaries(arrId, lowBoundary, highBoundary);
        StorageManager::getInstance().flush();
        query->replicationBarrier();

        for (size_t i = 0; i < nDims; i++) {
            string const& dstMappingArrayName = dims[i].getMappingArrayName();
            string const& srcMappingArrayName = srcDims[i].getMappingArrayName();
            if (dims[i].getType() != TID_INT64 && srcMappingArrayName != dstMappingArrayName) { 
                boost::shared_ptr<Array> tmpMappingArray = query->getTemporaryArray(srcMappingArrayName);
                if (tmpMappingArray) { 
                    DBArray dbMappingArray(dstMappingArrayName, query);
                    shared_ptr<ArrayIterator> srcArrayIterator = tmpMappingArray->getIterator(0);
                    shared_ptr<ArrayIterator> dstArrayIterator = dbMappingArray.getIterator(0);
                    Chunk& dstChunk = dstArrayIterator->newChunk(srcArrayIterator->getPosition(), 0);
                    ConstChunk const& srcChunk = srcArrayIterator->getChunk();
                    PinBuffer scope(srcChunk);
                    dstChunk.setRLE(false);
                    dstChunk.allocate(srcChunk.getSize());
                    memcpy(dstChunk.getData(), srcChunk.getData(), srcChunk.getSize());
                    dstChunk.write(query);
                }
            }
        }
        getInjectedErrorListener().check();
        return dstArray;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalStore, "store", "physicalStore")

}  // namespace ops
