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
#include "ConcatArray.h"
#include "network/NetworkManager.h"

using namespace std;
using namespace boost;

namespace scidb {


class PhysicalConcat: public PhysicalOperator
{
public:
	PhysicalConcat(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
	    PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

	virtual bool isDistributionPreserving(const std::vector< ArrayDesc> & inputSchemas) const
	{
	    return false;
	}

	virtual bool isChunkPreserving(const std::vector< ArrayDesc> & inputSchemas) const
	{
	    DimensionDesc const& dim = inputSchemas[0].getDimensions()[0];
	    return dim.getLength() % dim.getChunkInterval() == 0;
	}

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        if ( inputBoundaries[0].isEmpty() && inputBoundaries[1].isEmpty())
        {
            return PhysicalBoundaries::createEmpty(_schema.getDimensions().size());
        }
        else if (inputBoundaries[1].isEmpty())
        {
            return inputBoundaries[0];
        }

        Coordinates lhsStart = inputBoundaries[0].getStartCoords();
        Coordinates lhsEnd = inputBoundaries[0].getEndCoords();

        Coordinates rhsStart = inputBoundaries[1].getStartCoords();
        Coordinates rhsEnd = inputBoundaries[1].getEndCoords();

        Coordinates resultStart, resultEnd;

        resultStart.push_back(inputBoundaries[0].isEmpty() ? inputSchemas[0].getDimensions()[0].getLength() + rhsStart[0] :
                                                             lhsStart[0]);

        resultEnd.push_back( inputSchemas[0].getDimensions()[0].getLength() + rhsEnd[0] );

        for (size_t i = 1; i < lhsStart.size(); i++ )
        {
            Coordinate start = lhsStart[i] < rhsStart[i] ? lhsStart[i] : rhsStart[i];
            Coordinate end = lhsEnd[i] > rhsEnd[i] ? lhsEnd[i] : rhsEnd[i];
            resultStart.push_back(start);
            resultEnd.push_back(end);
        }

        double lhsCells = inputBoundaries[0].getNumCells() * inputBoundaries[0].getDensity();
        double rhsCells = inputBoundaries[1].getNumCells() * inputBoundaries[0].getDensity();
        double resultCells = PhysicalBoundaries::getNumCells(resultStart, resultEnd);
        double resultDensity = (lhsCells + rhsCells) / resultCells;

        return PhysicalBoundaries(resultStart, resultEnd, resultDensity);
    }

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                 const std::vector< ArrayDesc> & inputSchemas) const
    {
        //TODO: concat mapper
        if (inputDistributions[0] == inputDistributions[1])
        {
            switch (inputDistributions[0].getPartitioningSchema())
            {
                case psLocalNode:
                case psReplication:
                case psByRow:
                    return inputDistributions[0];

                case psRoundRobin:
                {
                    uint64_t numChunks = inputSchemas[0].getNumberOfChunks();
                    if (numChunks != INFINITE_LENGTH)
                    {
                        numChunks = numChunks/inputSchemas[0].getAttributes().size();
                        boost::shared_ptr<Query> query(_query);
                        size_t numNodes = query->getNodesCount();
                        if (numChunks % numNodes == 0)
                        {
                            return inputDistributions[0];
                        }
                    }
                    return ArrayDistribution(psUndefined);
                }

                case psByCol:
                case psUndefined:
                    return ArrayDistribution(psUndefined);
            }
        }

        return ArrayDistribution(psUndefined);
    }

	/***
	 * Concat is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
	 * that overrides the chunkiterator method.
	 */
	boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
		assert(inputArrays.size() == 2);
		boost::shared_ptr<Array> result = boost::shared_ptr<Array>(new ConcatArray(_schema, inputArrays[0], inputArrays[1]));
		return result;
	 }
};
    
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalConcat, "concat", "physicalConcat")

}  // namespace scidb
