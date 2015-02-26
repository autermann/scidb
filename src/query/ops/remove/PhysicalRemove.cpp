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
 * PhysicalRemove.cpp
 *
 *  Created on: Apr 16, 2010
 *      Author: Knizhnik
 */

#include <boost/foreach.hpp>
#include <deque>

#include "query/Operator.h"
#include "query/QueryProcessor.h"
#include "array/DBArray.h"
#include "smgr/io/Storage.h"
#include "system/SystemCatalog.h"


using namespace std;
using namespace boost;

namespace scidb {

class PhysicalRemove: public PhysicalOperator
{
public:
   PhysicalRemove(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
   PhysicalOperator(logicalName, physicalName, parameters, schema)
   {
   }

   void preSingleExecute(shared_ptr<Query> query)
   {
       shared_ptr<const InstanceMembership> membership(Cluster::getInstance()->getInstanceMembership());
       assert(membership);
       if (((membership->getViewId() != query->getCoordinatorLiveness()->getViewId()) ||
            (membership->getInstances().size() != query->getInstancesCount()))) {
           throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM2);
       }
       const string &arrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
       _lock = boost::shared_ptr<SystemCatalog::LockDesc>(new SystemCatalog::LockDesc(arrayName,
                                                                                      query->getQueryID(),
                                                                                      Cluster::getInstance()->getLocalInstanceId(),
                                                                                      SystemCatalog::LockDesc::COORD,
                                                                                      SystemCatalog::LockDesc::RM));
       shared_ptr<Query::ErrorHandler> ptr(new RemoveErrorHandler(_lock));
       query->pushErrorHandler(ptr);
   }

   boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        getInjectedErrorListener().check();

        //First remove array and its versions data from storage on each instance. Also on coordinator
        //we collecting all arrays IDs for deleting this arrays later from catalog.
        ArrayDesc arrayDesc;
        vector<VersionDesc> versions;

        const string &arrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        query->exclusiveLock(arrayName);

        if (SystemCatalog::getInstance()->getArrayDesc(arrayName, arrayDesc, true))
        {
            if (!arrayDesc.isImmutable())
            {
                BOOST_FOREACH(const VersionDesc &ver, SystemCatalog::getInstance()->getArrayVersions(arrayDesc.getId()))
                {
                    std::stringstream versionName;
                    versionName << arrayName << "@" << ver.getVersionID();

                    ArrayDesc versionArrayDesc;
                    if (SystemCatalog::getInstance()->getArrayDesc(versionName.str(), versionArrayDesc, false))
                    {
                        StorageManager::getInstance().remove(versionArrayDesc.getUAId(), versionArrayDesc.getId());
                        SystemCatalog::getInstance()->deleteArrayCache(versionArrayDesc.getId());
                        removeCoordinateIndices(versionArrayDesc, query);
                    }
                }
            }
            StorageManager::getInstance().remove(arrayDesc.getUAId(), arrayDesc.getId());
            removeCoordinateIndices(arrayDesc, query);
            SystemCatalog::getInstance()->deleteArrayCache(arrayDesc.getId());
        }
        return boost::shared_ptr<Array>();
    }

    void postSingleExecute(shared_ptr<Query> query)
    {
        bool rc = RemoveErrorHandler::handleRemoveLock(_lock, true);
        if (!rc) assert(false);
    }

private:
    void removeCoordinateIndices(ArrayDesc const& desc, shared_ptr<Query> query)
    {        
        Dimensions const& dims = desc.getDimensions();
        for (size_t i = 0, n = dims.size(); i < n; i++) { 
            if (dims[i].getType() != TID_INT64) {
                string indexName = dims[i].getMappingArrayName();
                ArrayDesc indexDesc;
                if (SystemCatalog::getInstance()->getArrayDesc(indexName, indexDesc, false)
                    && SystemCatalog::getInstance()->countReferences(indexName) <= 1)
                {
                    StorageManager::getInstance().removeCoordinateMap(indexName);
                    StorageManager::getInstance().remove(indexDesc.getUAId(), indexDesc.getId());
                    SystemCatalog::getInstance()->deleteArray(indexDesc.getId());
                }
            }
        }
    }

   boost::shared_ptr<SystemCatalog::LockDesc> _lock;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalRemove, "remove", "physicalRemove")

}  // namespace ops
