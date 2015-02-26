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
 * PhysicalInverse.cpp
 *
 *  Created on: Mar 9, 2010
 */
#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/MemArray.h"
#include "query/ops/inverse/InverseArray.h"
#include "network/NetworkManager.h"

namespace scidb
{

class PhysicalInverse: public PhysicalOperator
{
public:
	PhysicalInverse(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
	    PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
		_schema = schema;
	}

	//This will be needed when we support inverse on multiple nodes
    virtual bool isDistributionPreserving(const std::vector<ArrayDesc> & inputSchemas) const
    {
        return false;
    }

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                 const std::vector< ArrayDesc> & inputSchemas) const
    {
        return ArrayDistribution(psLocalNode);
    }

	/***
	 */
	boost::shared_ptr< Array> execute(std::vector< boost::shared_ptr< Array> >& inputArrays,
            boost::shared_ptr<Query> query)
	{
        boost::shared_ptr<Array> input = inputArrays[0];
        if ( query->getNodesCount() > 1) {
           uint64_t coordinatorID = (int64_t)query->getCoordinatorID() == -1 ? query->getNodeID() : query->getCoordinatorID();
            input = redistribute(input, query, psLocalNode, "", coordinatorID);
            if ( query->getNodeID() != coordinatorID) { 
                return boost::shared_ptr<Array>(new MemArray(_schema));
            }
        }
		return boost::shared_ptr<Array>(new InverseArray(_schema, input));
	}

private:
	ArrayDesc _schema;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalInverse, "inverse", "PhysicalInverse")

} //namespace scidb
