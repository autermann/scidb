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
 * PhysicalAggregate.cpp
 *
 *  Created on: Jul 25, 2011
 *      Author: poliocough@gmail.com
 */

#include "Aggregator.h"

namespace scidb
{

class PhysicalAggregate: public AggregatePartitioningOperator
{
  private:
     DimensionGrouping     _grouping;

  public:
	PhysicalAggregate(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
	    AggregatePartitioningOperator(logicalName, physicalName, parameters, schema)
	{
	}

    void initializeOperator(ArrayDesc const& inputSchema)
    {
        AggregatePartitioningOperator::initializeOperator(inputSchema);
        _grouping = DimensionGrouping(inputSchema.getDimensions(), _schema.getDimensions());
    }

    virtual void transformCoordinates(Coordinates const & inPos, Coordinates & outPos)
    {
        _grouping.reduceToGroup(inPos, outPos);
    }
};
    
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalAggregate, "aggregate", "physical_aggregate")

}  // namespace scidb
