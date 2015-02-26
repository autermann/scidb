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
 * PhysicalNormalize.cpp
 *  Created on: Mar 18, 2010
 *      Author: hkimura
 */
#include <math.h>
#include "query/Operator.h"
#include "network/NetworkManager.h"
#include "NormalizeArray.h"

namespace scidb
{

using namespace boost;

class PhysicalNormalize: public  PhysicalOperator
{
  public:
	PhysicalNormalize(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
	    PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

    virtual bool isDistributionPreserving(const std::vector<ArrayDesc> & inputSchemas) const
    {
        return false;
    }

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                 const std::vector< ArrayDesc> & inputSchemas) const
    {
        return ArrayDistribution(psLocalNode);
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }


	boost::shared_ptr<Array> execute(std::vector< boost::shared_ptr<Array> >& inputArrays,
                                      boost::shared_ptr<Query> query)
	{
        boost::shared_ptr<Array> inputArray = inputArrays[0];
        if (query->getNodesCount() > 1) { 
            uint64_t coordinatorID = (int64_t)query->getCoordinatorID() == -1 ?  query->getNodeID() : query->getCoordinatorID();
            inputArray = redistribute(inputArray, query, psLocalNode, "", coordinatorID);
            if (query->getNodeID() != coordinatorID) { 
                return boost::shared_ptr<Array>(new MemArray(_schema));
            }
        }
        double len = 0.0;
        for (boost::shared_ptr<ConstArrayIterator> arrayIterator = inputArray->getConstIterator(0);
             !arrayIterator->end();
             ++(*arrayIterator)) 
        { 
            for (boost::shared_ptr<ConstChunkIterator> chunkIterator = arrayIterator->getChunk().getConstIterator();
                 !chunkIterator->end();
                 ++(*chunkIterator))
            { 
                // TODO: insert converter here
                const double v = chunkIterator->getItem().getDouble();
                len += v*v;
            }
        }
        return boost::shared_ptr<Array>(new NormalizeArray(_schema, inputArray, sqrt(len)));
	}
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalNormalize, "normalize", "physicalNormalize")

} //namespace scidb
