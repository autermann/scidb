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
 * @file PhysicalSave.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * Physical implementation of SAVE operator for saveing data from text file
 * which is located on coordinator
 */

#include <string.h>

#include "query/Operator.h"
#include "array/Array.h"
#include "smgr/io/DBLoader.h"
#include "array/DBArray.h"
#include "query/QueryProcessor.h"
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"

using namespace std;
using namespace boost;
using namespace scidb;

namespace scidb
{

class PhysicalSave: public PhysicalOperator
{
public:
    PhysicalSave(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }
    
    int64_t getSourceInstanceID() const
    {
        if (_parameters.size() >= 2)
        {
            assert(_parameters[1]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
            boost::shared_ptr<OperatorParamPhysicalExpression> paramExpr = (boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1];
            assert(paramExpr->isConstant());
            return paramExpr->getExpression()->evaluate().getInt64();
        }
        // return ALL_INSTANCES_MASK; -- old behaviour
        return COORDINATOR_INSTANCE_MASK; // new behaviour compatible with LOAD/INPUT
    }

    virtual DistributionRequirement getDistributionRequirement (const std::vector< ArrayDesc> & inputSchemas) const
    {
        InstanceID sourceInstanceID = getSourceInstanceID();
        if (sourceInstanceID == ALL_INSTANCES_MASK)
        {
            return DistributionRequirement(DistributionRequirement::Any);
        }
        else
        {
            vector<ArrayDistribution> requiredDistribution(1);
            requiredDistribution[0] = ArrayDistribution(psLocalInstance, boost::shared_ptr<DistributionMapper>(), sourceInstanceID);
            return DistributionRequirement(DistributionRequirement::SpecificAnyOrder, requiredDistribution);
        }
    }


    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays,
                                     boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        assert(_parameters.size() >= 1);

        assert(_parameters[0]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
        const string& fileName = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getString();
        string format = "store";
        if (_parameters.size() >= 3) {
            format = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[2])->getExpression()->evaluate().getString();
        }
        InstanceID sourceInstanceID = getSourceInstanceID();
        if (sourceInstanceID == COORDINATOR_INSTANCE_MASK) { 
            sourceInstanceID = query->getCoordinatorInstanceID();
        }
        InstanceID myInstanceID = query->getInstanceID();
        if (sourceInstanceID == ALL_INSTANCES_MASK || sourceInstanceID == myInstanceID) { 
            DBLoader::defaultPrecision = Config::getInstance()->getOption<int>(CONFIG_PRECISION);
            DBLoader::save(*inputArrays[0], fileName, query, format);
        }
        return inputArrays[0];
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSave, "save", "impl_save")

} //namespace
