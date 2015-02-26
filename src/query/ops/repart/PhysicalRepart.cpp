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
#include "RepartArray.h"
#include "network/NetworkManager.h"


using namespace std;
using namespace boost;

namespace scidb {

class PhysicalRepart: public PhysicalOperator
{
public:
        PhysicalRepart(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
            PhysicalOperator(logicalName, physicalName, parameters, schema)
        {
        }

    virtual bool isChunkPreserving(const std::vector< ArrayDesc> & inputSchemas) const
    {
        Dimensions dims = inputSchemas[0].getDimensions();

        for (size_t i = 0; i < dims.size(); i++)
        {
            DimensionDesc dim = dims[i];
            DimensionDesc newDim = _schema.getDimensions()[i];

            if (dim.getChunkInterval() != newDim.getChunkInterval() ||
                dim.getChunkOverlap() != newDim.getChunkOverlap())
            {
                return false;
            }
        }
        return true;
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

   /***
    * Repart is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
    * that overrides the chunkiterator method.
    */
   boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
   {
      assert(inputArrays.size() == 1);
      std::vector<ArrayDesc> inputSchemas(1);
      inputSchemas[0] = inputArrays[0]->getArrayDesc();
      return boost::shared_ptr<Array>(isChunkPreserving(inputSchemas) 
                                      ? (Array*)new DelegateArray(_schema, inputArrays[0], true)
                                      : (Array*)new RepartArray(_schema, inputArrays[0], query));
   }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalRepart, "repart", "physicalRepart")

}  // namespace scidb
