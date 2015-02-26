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
#include "UnpackArray.h"

using namespace std;
using namespace boost;

namespace scidb {


class PhysicalUnpack: public PhysicalOperator
{
public:
        PhysicalUnpack(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
            PhysicalOperator(logicalName, physicalName, parameters, schema)
        {
        }

        virtual bool isDistributionPreserving(const std::vector< ArrayDesc> & inputSchemas) const
    {
        return false;
    }

    virtual bool isChunkPreserving(const std::vector< ArrayDesc> & inputSchemas) const
    {
        return false;
    }

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                    const std::vector< ArrayDesc> & inputSchemas) const
    {
        return ArrayDistribution(psUndefined);
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                       const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0].reshape(inputSchemas[0].getDimensions(), _schema.getDimensions());
    }

        /***
         * Unpack is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
         * that overrides the chunkiterator method.
         */
        boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
                assert(inputArrays.size() == 1);
                return boost::shared_ptr<Array>(new UnpackArray(_schema, inputArrays[0], query));
         }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalUnpack, "unpack", "physicalUnpack")

}  // namespace scidb
