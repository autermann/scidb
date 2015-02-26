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
 * @file PhysicalExample.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * Physical implementation of INPUT operator for inputing data from text file
 * which is located on coordinator
 */

#include <string.h>
#include <log4cxx/logger.h>

#include "query/Operator.h"
#include "query/QueryProcessor.h"
#include "network/NetworkManager.h"
#include "InputArray.h"
#include "system/Cluster.h"

using namespace std;
using namespace boost;

namespace scidb
{

// Logger for network subsystem. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr oplogger(log4cxx::Logger::getLogger("scidb.ops.impl_input"));

class PhysicalInput: public PhysicalOperator
{
public:
    PhysicalInput(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

        virtual bool isDistributionPreserving(const std::vector< ArrayDesc> & inputSchemas) const
        {
                return false;
        }

        int64_t getSourceNodeID() const
        {
        if (_parameters.size() == 3)
        {
            assert(_parameters[2]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
            boost::shared_ptr<OperatorParamPhysicalExpression> paramExpr = (boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[2];
            assert(paramExpr->isConstant());
            return paramExpr->getExpression()->evaluate().getInt64();
        }
        return -2;
        }

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                    const std::vector< ArrayDesc> & inputSchemas) const
    {
        int64_t sourceNodeID = getSourceNodeID();
        if (sourceNodeID == -1 )
        {
            //The file is loaded from multiple nodes - the distribution could be possibly violated - assume the worst
            return ArrayDistribution(psUndefined);
        }
        else
        {
            return ArrayDistribution(psLocalNode);
        }
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays,
                              boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);
        assert(_parameters.size() >= 2);

        assert(_parameters[1]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
        boost::shared_ptr<OperatorParamPhysicalExpression> paramExpr = (boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1];
        assert(paramExpr->isConstant());
        const string fileName = paramExpr->getExpression()->evaluate().getString();

        int64_t sourceNodeID = getSourceNodeID() == -2 ? query->getCoordinatorID() : getSourceNodeID();

        if (sourceNodeID < -1 || sourceNodeID >= (int64_t) query->getNodesCount())
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_INVALID_NODE_ID) << sourceNodeID;

        int64_t myNodeID = query->getNodeID();

        boost::shared_ptr<Array> result;
        try
        {
            if (sourceNodeID == -1 || sourceNodeID == myNodeID)
            {
               result = boost::shared_ptr<Array>(new InputArray(_schema, fileName, query));
            }
            else
            {
                result = boost::shared_ptr<Array>(new MemArray(_schema));
            }
        }
        catch(const Exception& e)
        {
            if (e.getLongErrorCode() != SCIDB_LE_CANT_OPEN_FILE || sourceNodeID == myNodeID )
            {
                throw;
            }

            LOG4CXX_WARN(oplogger, "Failed to open file " << fileName << " for input");
            result = boost::shared_ptr<Array>(new MemArray(_schema));
        }

        return result;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalInput, "input", "impl_input")

} //namespace
