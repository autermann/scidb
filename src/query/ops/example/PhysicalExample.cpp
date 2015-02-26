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
 * PhysicalExample.cpp
 *
 *  Created on: Feb 15, 2010
 *      Author: roman.simakov@gmail.com
 *      Description: This file shows example of adding new physical operator
 *      according to a logical one.
 *      You must implement new class derived from PhysicalOperator and declare
 *      global static variable for new physical operator factory
 */

#include "query/Operator.h"

namespace scidb
{

/**
 * Example of new physical operator declaration and implementation
 */
class PhysicalExample: public PhysicalOperator
{
public:
    PhysicalExample(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    boost::shared_ptr< Array> execute(std::vector< boost::shared_ptr< Array> >& inputArrays,
            boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() > 1);

        return inputArrays[0];
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalExample, "example", "impl_example")

} //namespace
