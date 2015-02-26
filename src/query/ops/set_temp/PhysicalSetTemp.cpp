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
 * @file PhysicalSetTemp.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief This file implements physical set_temp operator
 * to save pointers to array and get in on the next iteration.
 */

#include "query/Operator.h"
#include "query/QueryProcessor.h"
#include "system/Config.h"
#include "array/FileArray.h"
#include "array/MemArray.h"

using namespace std;
using namespace boost;

namespace scidb
{

class PhysicalSetTemp: public PhysicalOperator
{
public:
    PhysicalSetTemp(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        const uint32_t i = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getUint32();
        assert(query->tempArrays.size() > i);
        boost::shared_ptr<Array> tempArray;
        if (Config::getInstance()->getOption<int>(CONFIG_MEM_ARRAY_THRESHOLD)*MB < _schema.getUsedSpace()
            || Config::getInstance()->getOption<bool>(CONFIG_SAVE_RAM)) 
        { 
            tempArray = boost::shared_ptr<Array>(new FileArray(inputArrays[0]));
        } else { 
            tempArray = boost::shared_ptr<Array>(new MemArray(inputArrays[0]));
        }
        return query->tempArrays[i] = tempArray;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSetTemp, "set_temp", "impl_set_temp")

} //namespace
