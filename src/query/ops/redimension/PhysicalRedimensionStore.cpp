/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2014 SciDB, Inc.
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

/*
 * PhysicalRedimensionStore.cpp
 *
 *  Created on: Apr 16, 2010
 *      Author: Knizhnik
 *      Author: dzhang
 */

#include "RedimensionCommon.h"

namespace scidb {

using namespace std;
using namespace boost;

void storeToDBArray(shared_ptr<Array> srcArray, shared_ptr<Array>& dstArray, shared_ptr<Query> query)
{
    ArrayDesc const& srcArrayDesc = srcArray->getArrayDesc();
    ArrayDesc const& dstArrayDesc = dstArray->getArrayDesc();
    size_t nAttrs = dstArrayDesc.getAttributes().size();

    if (nAttrs == 0) {
        return;
    }
    if (nAttrs != srcArrayDesc.getAttributes().size()) {
        srcArray = boost::shared_ptr<Array>(new NonEmptyableArray(srcArray));
    }

    // Perform parallel evaluation of aggregate
    shared_ptr<JobQueue> queue = PhysicalOperator::getGlobalQueueForOperators();
    size_t nJobs = srcArray->getSupportedAccess() == Array::RANDOM ? Config::getInstance()->getOption<int>(CONFIG_PREFETCHED_CHUNKS) : 1;
    vector< shared_ptr<StoreJob> > jobs(nJobs);
    Dimensions const& dims = dstArrayDesc.getDimensions();
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
        } else {
            bounds = bounds.unionWith(jobs[i]->bounds);
        }
    }
    if (errorJob >= 0) {
        jobs[errorJob]->rethrow();
    }

    //Destination array is mutable: collect the coordinates of all chunks created by all jobs
    set<Coordinates, CoordinatesLess> createdChunks;
    for(size_t i =0; i < nJobs; i++)
    {
        createdChunks.insert(jobs[i]->getCreatedChunks().begin(), jobs[i]->getCreatedChunks().end());
    }
    //Insert tombstone entries
    StorageManager::getInstance().removeDeadChunks(dstArrayDesc, createdChunks, query);

    SystemCatalog::getInstance()->updateArrayBoundaries(dstArrayDesc, bounds);
}

class PhysicalRedimensionStore: public RedimensionCommon
{
private:
    ArrayUAID _arrayUAID;   /**< UAID of new array */
    ArrayID _arrayID;   /**< ID of new array */
    Dimensions _updateableDims;
    shared_ptr<SystemCatalog::LockDesc> _lock;

public:
    PhysicalRedimensionStore(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        RedimensionCommon(logicalName, physicalName, parameters, schema), _arrayUAID(0), _arrayID(0)
    {}

    void preSingleExecute(shared_ptr<Query> query)
    {
        ArrayDesc parentArrayDesc;
        shared_ptr<const InstanceMembership> membership(Cluster::getInstance()->getInstanceMembership());
        assert(membership);

        if (((membership->getViewId() != query->getCoordinatorLiveness()->getViewId()) ||
                (membership->getInstances().size() != query->getInstancesCount()))) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM2);
        }

        _lock = shared_ptr<SystemCatalog::LockDesc>(new SystemCatalog::LockDesc(_schema.getName(),
                query->getQueryID(),
                Cluster::getInstance()->getLocalInstanceId(),
                SystemCatalog::LockDesc::COORD,
                SystemCatalog::LockDesc::WR));
        shared_ptr<Query::ErrorHandler> ptr(new UpdateErrorHandler(_lock));
        query->pushErrorHandler(ptr);

        bool rc = false;
        if (!SystemCatalog::getInstance()->getArrayDesc(_schema.getName(), parentArrayDesc, false)) {

            if (_schema.getId() != 0) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST) << _schema.getName();
            }
            _lock->setLockMode(SystemCatalog::LockDesc::CRT);
            rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
            assert(rc);

            SystemCatalog::getInstance()->addArray(_schema, psHashPartitioned);
            parentArrayDesc = _schema;

        } else if (_schema.getId() != parentArrayDesc.getId()) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST) << _schema.getName();
        }

        _arrayUAID = parentArrayDesc.getUAId();
        VersionID lastVersion = SystemCatalog::getInstance()->getLastVersion(_arrayUAID);

        _lock->setArrayId(_arrayUAID);
        _lock->setArrayVersion(lastVersion+1);
        rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
        assert(rc);

        string const& arrayName = parentArrayDesc.getName();
        _updateableDims = _schema.getDimensions();
        string versionedName = ArrayDesc::makeVersionedName(arrayName, lastVersion+1);
        _schema = ArrayDesc(versionedName, _schema.getAttributes(), _schema.grabDimensions(lastVersion+1));
        SystemCatalog::getInstance()->addArray(_schema, psHashPartitioned);
        _arrayID = _schema.getId();
        _lock->setArrayVersionId(_arrayID);
        rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
        assert(rc);
        rc = rc; // Eliminate warnings
    }

    virtual void postSingleExecute(shared_ptr<Query> query)
    {
        if (_arrayID != 0) {
            SystemCatalog::getInstance()->createNewVersion(_arrayUAID, _arrayID);
        }
        assert(_lock);
    }

    /**
     * Redimensions the srcArray and stores into destArray.
     *
     * @param destArrayDesc The destArray's description.
     * @param destArray     A DBArray to receive the transformed data.
     * @param srcArrayDesc  The srcArray's description.
     * @param srcArray      The source array.
     *
     * @param attrMapping   A vector with size = #dest attributes (not including empty tag). The i'th element is
     *                      (a) src attribute number that maps to this dest attribute, or
     *                      (b) src attribute number that generates this dest aggregate attribute, or
     *                      (c) src dimension number that maps to this dest attribute (with FLIP).
     *
     * @param dimMapping    A vector with size = #dest dimensions. The i'th element is
     *                      (a) src dim number that maps to this dest dim, or
     *                      (b) src attribute number that maps to this dest dim (with FLIP), or
     *                      (c) SYNTHETIC.
     *
     * @param aggregates    A vector of AggregatePtr with size = #dest attributes (not including empty tag). The i'th element, if not NULL, is
     *                      the aggregate function that is used to generate the i'th attribute in the destArray.
     *
     * @param query         The query context.
     */
    void transformArray(shared_ptr<Array> destArray, ArrayDesc const& srcArrayDesc, shared_ptr<Array> srcArray,
                        vector<size_t> const& attrMapping, vector<size_t> const& dimMapping,
                        vector<AggregatePtr> const& aggregates, shared_ptr<Query> query)
    {
        // timing
        LOG4CXX_DEBUG(logger, "[RedimStore] Begins.");
        ElapsedMilliSeconds timing;

        // Does the dest array have a synthetic dimension?
        bool hasSynthetic = false;
        for (size_t i=0; i<dimMapping.size(); ++i) {
            if (dimMapping[i] == SYNTHETIC) {
                hasSynthetic = true;
                break;
            }
        }

        // Does the dest array have any aggregate?
        bool hasAggregate = false;
        for (size_t i=0; i<aggregates.size(); ++i) {
            if (aggregates[i]) {
                hasAggregate = true;
                break;
            }
        }

        // Only one of hasSynthetic or hasAggregate may be true.
        // This test already was performed in LogicalRedimensionStore.cpp. The sanity check here is redundant.
        if (hasSynthetic && hasAggregate) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "PhysicalRedimensionStore.cpp::transform";
        }

        shared_ptr<Array> withAggregates = redimensionArray(srcArray,
                                                            attrMapping,
                                                            dimMapping,
                                                            aggregates,
                                                            query,
                                                            timing);

        // Store to the DBArray.
        storeToDBArray(withAggregates, destArray, query);

        // timing
        timing.logTiming(logger, "[RedimStore] Store", false); // false = no need to restart timing
    }


    shared_ptr<Array> execute(vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);

        // Lock the output array.

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

        // Get the meta data.

        shared_ptr<Array> srcArray = inputArrays[0];

        ArrayDesc  const& srcArrayDesc = srcArray->getArrayDesc();
        Attributes const& destAttrs = _schema.getAttributes(true); // true = exclude empty tag.
        Dimensions const& destDims = _schema.getDimensions();

        vector<AggregatePtr> aggregates (destAttrs.size());
        vector<size_t> attrMapping(destAttrs.size());
        vector<size_t> dimMapping(destDims.size());

        setupMappings(srcArrayDesc, aggregates, attrMapping, dimMapping, destAttrs, destDims);

        ArrayID arrayId;
        VersionID vid=0;
        if (_arrayID != 0) {
            arrayId =_arrayUAID; //update the unversioned parent descriptor

            // If the parent did not have mapping arrays, don't add it.
            shared_ptr<ArrayDesc> fromCatalog = SystemCatalog::getInstance()->getArrayDesc(arrayId);
            Dimensions const *dimsFromCatalog = &fromCatalog->getDimensions();
            assert(_updateableDims.size()==dimsFromCatalog->size()); dimsFromCatalog = dimsFromCatalog;

            SystemCatalog::getInstance()->updateArray(ArrayDesc(arrayId,
                                                                _arrayUAID,
                                                                vid,
                                                                baseArrayName,
                                                                _schema.getAttributes(),
                                                                _updateableDims,
                                                                _schema.getFlags()));
        }

        shared_ptr<Array> destArray(DBArray::newDBArray(_schema.getName(), query)); // We can't use _arrayID because it's not initialized on remote instances
        arrayId = destArray->getHandle();

        query->getReplicationContext()->enableInboundQueue(arrayId, destArray);

        _schema.setIds(arrayId, _schema.getUAId(), _schema.getVersionId());
        SystemCatalog::getInstance()->updateArray(_schema); //XXX why this ?

        transformArray(destArray, srcArrayDesc, srcArray, attrMapping, dimMapping, aggregates, query);

        query->getReplicationContext()->replicationSync(arrayId);
        query->getReplicationContext()->removeInboundQueue(arrayId);

        StorageManager::getInstance().flush();
        getInjectedErrorListener().check();
        return destArray;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalRedimensionStore, "redimension_store", "PhysicalRedimensionStore")

}  // namespace ops
