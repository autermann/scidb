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
 * @file LogicalGetTemp.cpp
 *
 * @author roman.simakov@gmail.com
 * @brief This file implement logical operator get temp for
 * getting result of previous iteration.
 */

#include "query/Operator.h"
#include "query/QueryProcessor.h"

using namespace boost;

namespace scidb
{

/**
 *
 */
class LogicalGetTemp: public LogicalOperator
{
public:
    LogicalGetTemp(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT();
        ADD_PARAM_CONSTANT("uint32");
    }

    /**
     * The schema of output array is the same as input
     */
    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, boost::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 0);
        const uint32_t i = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[0])->getExpression(),
                                    query, TID_UINT32).getUint32();
        assert(query->tempArrays.size() > i);
        return query->tempArrays[i]->getArrayDesc();
    }

};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalGetTemp, "get_temp")


} //namespace
