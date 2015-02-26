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
 * @file PhysicalGetTemp.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief This file implements physical get_temp operator
 * to get pointers saved on the prevoius iteration.
 */

#include "query/Operator.h"
#include "query/QueryProcessor.h"

using namespace std;
using namespace boost;

namespace scidb
{

class PhysicalGetTemp: public PhysicalOperator
{
public:
    PhysicalGetTemp(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);
        const uint32_t i = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getUint32();
        assert(query->tempArrays.size() > i);
        return query->tempArrays[i];
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalGetTemp, "get_temp", "impl_get_temp")

} //namespace
