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
 * PhysicalApply.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "CrossArray.h"
#include "network/NetworkManager.h"
#include "query/QueryProcessor.h"


namespace scidb {

using namespace boost;
using namespace std;

class PhysicalCross : public  PhysicalOperator
{
  public:
    PhysicalCross(std::string const& logicalName,
                  std::string const& physicalName,
                  Parameters const& parameters,
                  ArrayDesc const& schema)
    :  PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

    virtual PhysicalBoundaries getOutputBoundaries(
            std::vector<PhysicalBoundaries> const& inputBoundaries,
            std::vector< ArrayDesc> const& inputSchemas) const
    {
        return inputBoundaries[0].crossWith(inputBoundaries[1]);
    }

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const&,
            std::vector< ArrayDesc> const&) const
    {
        return ArrayDistribution(psUndefined);
    }

    boost::shared_ptr<Array> execute(
            std::vector< boost::shared_ptr<Array> >& inputArrays,
            boost::shared_ptr<Query> query)
    {
		assert(inputArrays.size() == 2);

        boost::shared_ptr<Array> left = inputArrays[0];
        boost::shared_ptr<Array> right = inputArrays[1];
        if ( query->getInstancesCount() > 1) { 
#ifdef CENTRALIZED_CROSS
            uint64_t coordinatorID = (int64_t)query->getCoordinatorID() == -1 ?  query->getInstanceID() : query->getCoordinatorID();
            left = redistribute(left, query, psLocalInstance, "", coordinatorID);
            right = redistribute(right, query, psLocalInstance, "", coordinatorID);
            if (query->getInstanceID() != coordinatorID) { 
                return boost::shared_ptr<Array>(new MemArray(_schema));
            }
#else
            right = redistribute(right, query, psReplication);
#endif
        }
		return boost::shared_ptr<Array>(new CrossArray(_schema, left, right));
	 }
};
    
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalCross, "cross", "physicalCross")

}  // namespace scidb
