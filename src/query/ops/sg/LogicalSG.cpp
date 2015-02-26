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
 * @file LogicalSG.cpp
 *
 * @author roman.simakov@gmail.com
 * @brief This file implement logical operator SCATTER/GATHER
 */

#include "query/Operator.h"
#include <smgr/io/Storage.h>


namespace scidb
{

/**
 * @brief The operator: sg().
 *
 * @par Synopsis:
 *   sg( srcArray, instanceId [, outputArray, isStore = True] )
 *
 * @par Summary:
 *   SCATTER/GATHER distributes array chunks over every instance of cluster.
 *   It has one input and outputs a local part of array after redistribution.
 *   It's only operator uses the network manager.
 *   Logical operator for SG must not be present in logical plan, but will be
 *   inserted by the optimizer directly into the physical plan.
 *
 * @par Input:
 *   - srcArray: the souce array, with srcAttrs and srcDims.
 *   - instanceId:
 *   - outputArray: if present, the result will be stored
 *   - isStore: ??
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   srcDims
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - The author should explain what it means to have outputArray but isStore=False.
 *
 */
class LogicalSG: public LogicalOperator
{
public:
    LogicalSG(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT();
        ADD_PARAM_CONSTANT("int32");
        ADD_PARAM_VARIES();
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        switch (_parameters.size()) { 
          case 0:
            assert(false);
            break;
          case 1:
            res.push_back(PARAM_CONSTANT("int64"));
            break;
          case 2:
            res.push_back(PARAM_OUT_ARRAY_NAME());
            break;
          case 3:
            res.push_back(PARAM_CONSTANT("bool"));
            break;
          default:
        	if (_parameters.size() < (schemas[0].getDimensions().size()*2) + 4)
        	{
    			res.push_back(PARAM_CONSTANT("int64"));
        	}
        }
        return res;
    }

    /**
     * The schema of output array is the same as input
     */
    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, boost::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 1);
        ArrayDesc const& desc = inputSchemas[0];
        std::string resultArrayName = desc.getName();

        if (_parameters.size() >= 3)
        {
            std::string suppliedResultName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[2])->getObjectName();
            if (!suppliedResultName.empty())
            {
                resultArrayName = suppliedResultName;
            }
        }
        return ArrayDesc(resultArrayName, desc.getAttributes(), desc.getDimensions());
    }

    void inferArrayAccess(boost::shared_ptr<Query>& query)
    {
        LogicalOperator::inferArrayAccess(query);

        if (_parameters.size() < 3)
        {
            return;
        }
        std::string resultArrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[2])->getObjectName();
        if (resultArrayName.empty())
        {
            return;
        }

        bool storeResult = true;
        if (_parameters.size() >= 4)
        {
            Expression expr;
            expr.compile(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[3])->getExpression(),
                         query, false, TID_BOOL);
            Value const& value = expr.evaluate();
            assert (expr.getType() == TID_BOOL);
            storeResult = value.getBool();
        }
        if (storeResult)
        {
            assert(resultArrayName.find('@') == std::string::npos);
            boost::shared_ptr<SystemCatalog::LockDesc>  lock(new SystemCatalog::LockDesc(resultArrayName,
                                                                                         query->getQueryID(),
                                                                                         Cluster::getInstance()->getLocalInstanceId(),
                                                                                         SystemCatalog::LockDesc::COORD,
                                                                                         SystemCatalog::LockDesc::WR));
            boost::shared_ptr<SystemCatalog::LockDesc> resLock = query->requestLock(lock);
            assert(resLock);
            assert(resLock->getLockMode() >= SystemCatalog::LockDesc::WR);
        }
    }
};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSG, "sg")


} //namespace
