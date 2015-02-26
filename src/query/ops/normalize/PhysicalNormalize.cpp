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
    PhysicalNormalize(
            std::string const& logicalName,
            std::string const& physicalName,
            Parameters const& parameters,
            ArrayDesc const& schema):
	    PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const&,
            std::vector< ArrayDesc> const&) const
    {
        return ArrayDistribution(psLocalInstance);
    }

    virtual PhysicalBoundaries getOutputBoundaries(
            std::vector<PhysicalBoundaries> const& inputBoundaries,
            std::vector< ArrayDesc> const& inputSchemas) const
    {
        return inputBoundaries[0];
    }


    boost::shared_ptr<Array> execute(
            std::vector< boost::shared_ptr<Array> >& inputArrays,
            boost::shared_ptr<Query> query)
	{
        if (inputArrays[0]->getSupportedAccess() == Array::SINGLE_PASS)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_UNSUPPORTED_INPUT_ARRAY) << getLogicalName();
        }

        boost::shared_ptr<Array> inputArray = inputArrays[0];
        if (query->getInstancesCount() > 1) { 
            uint64_t coordinatorID = (int64_t)query->getCoordinatorID() == -1 ?  query->getInstanceID() : query->getCoordinatorID();
            inputArray = redistribute(inputArray, query, psLocalInstance, "", coordinatorID);
            if (query->getInstanceID() != coordinatorID) { 
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
                Value const& v = chunkIterator->getItem();
                if (!v.isNull()) { 
                    const double d = v.getDouble();
                    len += d*d;
                }
            }
        }
        return boost::shared_ptr<Array>(new NormalizeArray(_schema, inputArray, sqrt(len)));
	}
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalNormalize, "normalize", "physicalNormalize")

} //namespace scidb
