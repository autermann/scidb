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
 * @file PhysicalResult.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief This file implements physical SCATTER/GATHER operator
 */

#include "boost/make_shared.hpp"
#include <log4cxx/logger.h>

#include "query/Operator.h"
#include "network/NetworkManager.h"
#include "network/BaseConnection.h"
#include "network/MessageUtils.h"
#include "system/SystemCatalog.h"
#include "array/DBArray.h"
#include "query/QueryProcessor.h"
#include "query/parser/ParsingContext.h"

using namespace boost;
using namespace std;


namespace scidb
{

// Logger for network subsystem. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.sg"));

/**
 * Physical implementation of SCATTER/GATHER operator.
 * This physical operator must be inserted into physical plan by optimizer
 * without any logical node in logical plan.
 */
class PhysicalSG: public PhysicalOperator
{
private:
    ArrayID _arrayID;   /**< ID of new array */
    ArrayID _updateableArrayID;   /**< ID of new array */
    boost::shared_ptr<SystemCatalog::LockDesc> _lock;

  public:
    PhysicalSG(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema), _arrayID((ArrayID)~0), _updateableArrayID((ArrayID)~0)
    {
    }

    void preSingleExecute(boost::shared_ptr<Query> query)
    {
        if (_parameters.size() < 3)
        {
            return;
        }
        bool storeResult = true;
        if (_parameters.size() >= 4)
        {
            storeResult = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[3])->getExpression()->evaluate().getBool();
        }

        if (storeResult)
        {
            preSingleExecuteForStore(query);
        }
    }

    void preSingleExecuteForStore(boost::shared_ptr<Query>& query)
    {
        ArrayDesc desc;
        shared_ptr<const NodeMembership> membership(Cluster::getInstance()->getNodeMembership());
        assert(membership);
        if ((membership->getViewId() != query->getCoordinatorLiveness()->getViewId()) ||
            (membership->getNodes().size() != query->getNodesCount())) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM2);
        }
        _lock = boost::shared_ptr<SystemCatalog::LockDesc>(new SystemCatalog::LockDesc(_schema.getName(),
                                                                                       query->getQueryID(),
                                                                                       Cluster::getInstance()->getLocalNodeId(),
                                                                                       SystemCatalog::LockDesc::COORD,
                                                                                       SystemCatalog::LockDesc::WR));
        shared_ptr<Query::ErrorHandler> ptr(new UpdateErrorHandler(_lock));
        query->pushErrorHandler(ptr);

        bool rc = false;
        PartitioningSchema ps = (PartitioningSchema)((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getInt32();

        if (!SystemCatalog::getInstance()->getArrayDesc(_schema.getName(), desc, false)) {

            _lock->setLockMode(SystemCatalog::LockDesc::CRT);
            rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
            assert(rc);

            ArrayID newArrayID = SystemCatalog::getInstance()->addArray(_schema, ps);
            desc = _schema;
            desc.setId(newArrayID); 
        }
        if (desc.isImmutable()) {
            return;
        }
        _updateableArrayID = desc.getId();

        VersionID lastVersion = SystemCatalog::getInstance()->getLastVersion(_updateableArrayID);

        _lock->setArrayId(_updateableArrayID);
        _lock->setArrayVersion(lastVersion+1);
        rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
        assert(rc);

        _schema = ArrayDesc(formArrayNameVersion(desc.getName(),(lastVersion+1)),
                            desc.getAttributes(), desc.getDimensions());
        
        _arrayID = SystemCatalog::getInstance()->addArray(_schema, ps);

        _lock->setArrayVersionId(_arrayID);
        rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
        assert(rc);
        rc = rc; // Eliminate warnings
    }

    virtual void postSingleExecute(shared_ptr<Query>)
    {
        if (_updateableArrayID != (ArrayID)~0) {
            SystemCatalog::getInstance()->createNewVersion(_updateableArrayID, _arrayID);
        }
    }

    virtual bool isDistributionPreserving(const std::vector< ArrayDesc> & inputSchemas) const
    {
        return false;
    }

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                 const std::vector< ArrayDesc> & inputSchemas) const
    {
        PartitioningSchema ps = (PartitioningSchema)((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getInt32();
        DimensionVector offset = getOffsetVector(inputSchemas);
        DimensionVector shape = getShapeVector(inputSchemas);

        boost::shared_ptr<DistributionMapper> distMapper;

        if ( !offset.isEmpty() )
        {
            distMapper = DistributionMapper::createOffsetMapper(offset,shape);
        }

        return ArrayDistribution(ps,distMapper);
    }

    DimensionVector getOffsetVector(const vector<ArrayDesc> & inputSchemas) const
    {
        if (_parameters.size() <= 4)
        {
            return DimensionVector();
        }
        else
        {
            DimensionVector result(_schema.getDimensions().size());
            assert (_parameters.size() == _schema.getDimensions().size()*2 + 4);
            for (size_t i = 0; i < result.numDimensions(); i++)
            {
                result[i] = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i+4])->getExpression()->evaluate().getInt64();
            }
            return result;
        }
    }

    DimensionVector getShapeVector(const vector<ArrayDesc> & inputSchemas) const
    {
        if (_parameters.size() <= 4)
        {
            return DimensionVector();
        }
        else
        {
            DimensionVector result(_schema.getDimensions().size());
            assert (_parameters.size() == _schema.getDimensions().size()*2 + 4);
            for (size_t i = 0; i < result.numDimensions(); i++)
            {
                result[i] = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i + _schema.getDimensions().size() +4])->getExpression()->evaluate().getInt64();
            }
            return result;
        }
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
            PartitioningSchema ps = (PartitioningSchema)((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getInt32();
            int64_t nodeID = -1;
            std::string arrayName = "";
            DimensionVector offsetVector = getOffsetVector(vector<ArrayDesc>());
            DimensionVector shapeVector = getShapeVector(vector<ArrayDesc>());

            boost::shared_ptr <DistributionMapper> distMapper;

            if (!offsetVector.isEmpty())
            {
                assert(offsetVector.numDimensions()==shapeVector.numDimensions());
                distMapper = DistributionMapper::createOffsetMapper(offsetVector, shapeVector);
            }

            bool storeResult=false;

            if (_parameters.size() >=2 )
            {
            	nodeID = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1])->getExpression()->evaluate().getInt64();
            }

            if (_parameters.size() >= 3)
            {
                storeResult=true;
                arrayName = _schema.getName();
            }

            if (_parameters.size() >= 4)
            {
                storeResult = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[3])->getExpression()->evaluate().getBool();
                if (! storeResult)
                {
                    arrayName = "";
                }
            }

            if ((!_lock) && storeResult) {
               assert(!arrayName.empty());
               VersionID version(0);
               string baseArrayName = splitArrayNameVersion(arrayName, version);

               _lock = boost::shared_ptr<SystemCatalog::LockDesc>(new SystemCatalog::LockDesc(baseArrayName,
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
            return redistribute(inputArrays[0], query, ps, arrayName, nodeID, distMapper);
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSG, "sg", "impl_sg")

} //namespace
