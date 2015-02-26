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

class PhysicalCross: public  PhysicalOperator{
  public:
	PhysicalCross(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema)
    :  PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0].crossWith(inputBoundaries[1]);
    }

	boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
		assert(inputArrays.size() == 2);
        boost::shared_ptr<Array> left = inputArrays[0];
        boost::shared_ptr<Array> right = inputArrays[1];
        if ( query->getNodesCount() > 1) { 
#ifdef CENTRALIZED_CROSS
            uint64_t coordinatorID = (int64_t)query->getCoordinatorID() == -1 ?  query->getNodeID() : query->getCoordinatorID();
            left = redistribute(left, query, psLocalNode, "", coordinatorID);
            right = redistribute(right, query, psLocalNode, "", coordinatorID);
            if (query->getNodeID() != coordinatorID) { 
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
