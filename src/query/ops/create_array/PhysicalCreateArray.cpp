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

/**
 * @file
 *
 * @brief Physical DDL operator which create new array
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#include "query/Operator.h"
#include "system/SystemCatalog.h"

namespace scidb
{

class PhysicalCreateArray: public PhysicalOperator
{
public:
    PhysicalCreateArray(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return PhysicalBoundaries::createEmpty(
            ((boost::shared_ptr<OperatorParamSchema>&)_parameters[1])->getSchema().getDimensions().size());
    }

    void preSingleExecute(boost::shared_ptr<Query> query)
    {
        assert(_parameters.size() == 2);
        const string &name = ((boost::shared_ptr<OperatorParamArrayReference>&)_parameters[0])->getObjectName();
        ArrayDesc schema = ((boost::shared_ptr<OperatorParamSchema>&)_parameters[1])->getSchema();
        schema.setName(name);
        SystemCatalog::getInstance()->addArray(schema, psRoundRobin);
    }

    boost::shared_ptr< Array> execute(std::vector< boost::shared_ptr< Array> >& inputArrays,
            boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);
        // It's DDL command and should not return a value
        return boost::shared_ptr<Array>();
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalCreateArray, "create_array", "impl_create_array")

} //namespace
