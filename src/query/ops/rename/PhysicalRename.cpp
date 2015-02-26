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
 * PhysicalRename.cpp
 *
 *  Created on: Apr 16, 2010
 *      Author: Knizhnik
 */

#include <boost/foreach.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/bind.hpp>
#include "query/Operator.h"
#include "array/DBArray.h"
#include "system/SystemCatalog.h"
#include "smgr/io/Storage.h"


namespace scidb {

using namespace std;
using namespace boost;

#define RENAME_DELAY 2

class PhysicalRename: public PhysicalOperator
{
public:
	PhysicalRename(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
	    PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                 const std::vector< ArrayDesc> & inputSchemas) const
    {
        SystemCatalog* systemCatalog = SystemCatalog::getInstance();
        ArrayDesc desc;
        const string &oldArrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        systemCatalog->getArrayDesc(oldArrayName, desc);
        ArrayID id = desc.getId();
        PartitioningSchema ps = systemCatalog->getPartitioningSchema(id);
        return ArrayDistribution(ps);
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        SystemCatalog* systemCatalog = SystemCatalog::getInstance();
        ArrayDesc desc;
        const string &oldArrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        systemCatalog->getArrayDesc(oldArrayName, desc);
        ArrayID id = desc.getId();
        Coordinates lowBoundary = systemCatalog->getLowBoundary(id);
        Coordinates highBoundary = systemCatalog->getHighBoundary(id);

        return PhysicalBoundaries(lowBoundary, highBoundary);
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        const string& oldArrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        if (_oldArrayName.empty()) {
            boost::shared_ptr<SystemCatalog::LockDesc>
            lock = boost::shared_ptr<SystemCatalog::LockDesc>(new SystemCatalog::LockDesc(oldArrayName,
                                                                                           query->getQueryID(),
                                                                                           Cluster::getInstance()->getLocalInstanceId(),
                                                                                           SystemCatalog::LockDesc::WORKER,
                                                                                           SystemCatalog::LockDesc::RNF));
            Query::Finalizer f = bind(&UpdateErrorHandler::releaseLock, lock, _1);
            query->pushFinalizer(f);
            SystemCatalog::ErrorChecker errorChecker = bind(&Query::validate, query);
            bool rc = SystemCatalog::getInstance()->lockArray(lock, errorChecker);
            if (!rc) {
                throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_INCREMENT_LOCK)
                << oldArrayName;
            }
        }
        SystemCatalog::getInstance()->invalidateArrayCache(oldArrayName);
        getInjectedErrorListener().check();
        return boost::shared_ptr<Array>();
    }

   void preSingleExecute(shared_ptr<Query> query)
   {
       shared_ptr<const InstanceMembership> membership(Cluster::getInstance()->getInstanceMembership());
       assert(membership);
       if (((membership->getViewId() != query->getCoordinatorLiveness()->getViewId()) ||
            (membership->getInstances().size() != query->getInstancesCount()))) {
           throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM2);
       }
      const string& oldArrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
      _oldArrayName = oldArrayName;
      assert(!_oldArrayName.empty());
   }

    void postSingleExecute(shared_ptr<Query> query)
    {
        assert(!_oldArrayName.empty());
        SystemCatalog::getInstance()->renameArray(_oldArrayName, _schema.getName());
    }

private:
   std::string _oldArrayName;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalRename, "rename", "physicalRename")

}  // namespace scidb
