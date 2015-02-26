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
  private:
   ArrayUAID _arrayUAID;   /**< UAID of new array */
   ArrayID _arrayID;   /**< ID of new array */
   VersionID _lastVersion;
   shared_ptr<SystemCatalog::LockDesc> _lock;

  public:
   PhysicalStore(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema),
        _arrayUAID(0),
        _arrayID(0)
   {}

   void preSingleExecute(shared_ptr<Query> query)
   {
        ArrayDesc parentArrayDesc;
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
        bool arrayExists = SystemCatalog::getInstance()->getArrayDesc(arrayName, parentArrayDesc, false);
        _lastVersion = 0;
        bool rc = false;
        if (!arrayExists) { 
            if (_schema.getId() != 0) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST) << arrayName;
            }
            _lock->setLockMode(SystemCatalog::LockDesc::CRT);
            rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
            assert(rc);
            parentArrayDesc = _schema;
            SystemCatalog::getInstance()->addArray(parentArrayDesc, psRoundRobin);
        } else { 
            if (_schema.getId() != parentArrayDesc.getId()) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST) << arrayName;
            }
            if (!parentArrayDesc.isImmutable()) { 
                _lastVersion = SystemCatalog::getInstance()->getLastVersion(parentArrayDesc.getId());
            } 
        }

        Dimensions const& dstDims = parentArrayDesc.getDimensions();        
        bool changeMapping = false;

        for (size_t i = 0; i < nDims; i++) {
            DimensionDesc const& dim = dims[i];
            string const& mappingArrayName = dim.getMappingArrayName();
            newVersionDims[i] = dim;
            newVersionDims[i].setCurrStart(MAX_COORDINATE);
            newVersionDims[i].setCurrEnd(MIN_COORDINATE);
            if (dim.getType() != TID_INT64 && !mappingArrayName.empty()) { 
                if (arrayExists && parentArrayDesc.isImmutable() && mappingArrayName != dstDims[i].getMappingArrayName() && !dstDims[i].getMappingArrayName().empty()) { 
                    throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CAN_NOT_CHANGE_MAPPING) << arrayName;
                }
                changeMapping = true;
                boost::shared_ptr<Array> tmpMappingArray = query->getTemporaryArray(mappingArrayName);
                if (tmpMappingArray) { 
                    ArrayDesc const& tmpMappingArrayDesc = tmpMappingArray->getArrayDesc();
                    string newMappingArrayName = parentArrayDesc.createMappingArrayName(i, _lastVersion+1);
                    if (SystemCatalog::getInstance()->containsArray(newMappingArrayName)) { 
                        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_ALREADY_EXIST) << newMappingArrayName;
                    }
                    ArrayDesc mappingArrayDesc(newMappingArrayName, tmpMappingArrayDesc.getAttributes(), tmpMappingArrayDesc.getDimensions(), ArrayDesc::LOCAL);
                    SystemCatalog::getInstance()->addArray(mappingArrayDesc, psReplication);
                    assert(mappingArrayDesc.getId()>0);
                    newVersionDims[i] = DimensionDesc(dim.getBaseName(),
                                                      dim.getNamesAndAliases(),
                                                      dim.getStartMin(), MAX_COORDINATE,
                                                      MIN_COORDINATE, dim.getEndMax(), dim.getChunkInterval(),
                                                      dim.getChunkOverlap(), dim.getType(), dim.getFlags(),
                                                      newMappingArrayName,
                                                      dim.getComment(),
                                                      dim.getFuncMapOffset(),
                                                      dim.getFuncMapScale());
                 }
            }
        }
        if (parentArrayDesc.isImmutable()) {
            if (changeMapping) { 
                SystemCatalog::getInstance()->updateArray(ArrayDesc(parentArrayDesc.getId(), parentArrayDesc.getUAId(), parentArrayDesc.getVersionId(), parentArrayDesc.getName(), parentArrayDesc.getAttributes(), newVersionDims));
            };
            return;
        }

        _arrayUAID = parentArrayDesc.getUAId();
        _lock->setArrayId(_arrayUAID);
        _lock->setArrayVersion(_lastVersion+1);
        rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
        assert(rc);

        _schema = ArrayDesc(ArrayDesc::makeVersionedName(arrayName, _lastVersion+1), parentArrayDesc.getAttributes(), newVersionDims);
        SystemCatalog::getInstance()->addArray(_schema, psRoundRobin);
        _arrayID = _schema.getId();
        _lock->setArrayVersionId(_arrayID);
        rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
        assert(rc);
        rc = rc; // Eliminate warnings
   }

    virtual void postSingleExecute(shared_ptr<Query> query)
    {
        assert(_lock);
        if (_arrayID != 0) {
            SystemCatalog::getInstance()->createNewVersion(_arrayUAID, _arrayID);
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
        VersionID version = ArrayDesc::getVersionFromName(_schema.getName());
        string baseArrayName = ArrayDesc::makeUnversionedName(_schema.getName());
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

        if (nAttrs == 0) { 
            return dstArray;
        }
        if (nAttrs > srcArrayDesc.getAttributes().size()) {
            assert(nAttrs == srcArrayDesc.getAttributes().size()+1);
            srcArray = boost::shared_ptr<Array>(new NonEmptyableArray(srcArray));
        } 

        // Perform parallel evaluation of aggregate
        shared_ptr<JobQueue> queue = PhysicalOperator::getGlobalQueueForOperators();
        size_t nJobs = srcArray->getSupportedAccess() == Array::RANDOM ? Config::getInstance()->getOption<int>(CONFIG_PREFETCHED_CHUNKS) : 1;
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

        PhysicalBoundaries bounds = PhysicalBoundaries::createEmpty(nDims);
        int errorJob = -1;
        for (size_t i = 0; i < nJobs; i++) {
            if (!jobs[i]->wait()) {
                errorJob = i;
            }
            else {
                bounds = bounds.unionWith(jobs[i]->bounds);
            }
        }
        if (errorJob >= 0) {
            jobs[errorJob]->rethrow();
        }

        if(!dstArrayDesc.isImmutable())
        {   //Destination array is mutable: collect the coordinates of all chunks created by all jobs
            set<Coordinates, CoordinatesLess> createdChunks;
            for(size_t i =0; i < nJobs; i++)
            {
                createdChunks.insert(jobs[i]->getCreatedChunks().begin(), jobs[i]->getCreatedChunks().end());
            }
            //Insert tombstone entries
            StorageManager::getInstance().removeDeadChunks(dstArrayDesc, createdChunks, query);
        }

        SystemCatalog::getInstance()->updateArrayBoundaries(_schema, bounds);
        StorageManager::getInstance().flush();
        query->replicationBarrier();

        for (size_t i = 0; i < nDims; i++) {
            string const& dstMappingArrayName = dims[i].getMappingArrayName();
            string const& srcMappingArrayName = srcDims[i].getMappingArrayName();
            if (dims[i].getType() != TID_INT64 && srcMappingArrayName != dstMappingArrayName) { 
                boost::shared_ptr<Array> tmpMappingArray = query->getTemporaryArray(srcMappingArrayName);
                if (tmpMappingArray) { 
                    DBArray dbMappingArray(dstMappingArrayName, query);
                    ArrayDesc const& tmpMappingArrayDesc = tmpMappingArray->getArrayDesc();
                    ArrayDesc const& dbMappingArrayDesc = dbMappingArray.getArrayDesc();
                    if (query->getCoordinatorID() == COORDINATOR_INSTANCE 
                        && tmpMappingArrayDesc.getDimensions()[0].getChunkInterval() != dbMappingArrayDesc.getDimensions()[0].getChunkInterval()) { 
                        SystemCatalog::getInstance()->updateArray(ArrayDesc(dbMappingArrayDesc.getId(),
                                                                            dbMappingArrayDesc.getUAId(),
                                                                            dbMappingArrayDesc.getVersionId(),
                                                                            dbMappingArrayDesc.getName(),
                                                                            dbMappingArrayDesc.getAttributes(),
                                                                            tmpMappingArrayDesc.getDimensions(),
                                                                            dbMappingArrayDesc.getFlags()));
                    }
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
