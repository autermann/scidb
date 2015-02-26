/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2014 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

/*
 * PhysicalConsume.cpp
 *
 *  Created on: Aug 6, 2013
 *      Author: sfridella
 */

#include <query/Operator.h>
#include <array/Array.h>
#include <util/MultiConstIterators.h>

using namespace std;
using namespace boost;

namespace scidb
{

class PhysicalConsume: public PhysicalOperator
{
public:
    PhysicalConsume(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    /***
     * Consume scans the input array in order to materialize all chunks.  It produces an empty result,
     * so it can be used to wrap a complex expression in order to benchmark the expressions performance
     * without including any data transfers.  Along with the input array, it receives an interger parameter
     * that determines the width of the vertical slice of attributes for the scan.  If the param is 1,
     * the scan is purely vertical, scanning each attribute entirely in turn.  If the param is equal to
     * the number of attributes, the scan is horizontal, scanning chunks across all attributes.
     *
     */
    shared_ptr<Array> execute(vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        assert(_parameters.size() <= 1);

        /* This parameter determines the width of the vertical slice that we use to scan
           Default is 1.
        */
        uint64_t attrStrideSize =
            (_parameters.size() == 1)
            ? ((shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getUint64()
            : 1;

        /* If the array is fully materialized, do nothing
         */
        shared_ptr<Array>& array = inputArrays[0];
        if (array->isMaterialized())
        {
            return shared_ptr<Array>();
        }

        /* Scan through the array in vertical slices of width "attrStrideSize"
         */
        size_t numRealAttrs = _schema.getAttributes(true).size();
        size_t baseAttr = 0;

        attrStrideSize = (attrStrideSize < 1) ? 1 : attrStrideSize;
        attrStrideSize = (attrStrideSize > numRealAttrs) ? numRealAttrs : attrStrideSize;

        while (baseAttr < numRealAttrs)
        {
            /* Get iterators for this vertical slice
             */
            size_t sliceSize = (numRealAttrs - baseAttr) > attrStrideSize
                ? attrStrideSize
                : numRealAttrs - baseAttr;
            std::vector<shared_ptr<ConstIterator> > arrayIters(sliceSize);

            for (size_t i = baseAttr;
                 i < (baseAttr + sliceSize);
                 i++)
            {
                arrayIters[i - baseAttr] = array->getConstIterator(i);
            }

            /* Scan each attribute one chunk at a time, use MultiConstIterator
               to handle possible gaps at each chunk position
            */
            MultiConstIterators multiIters(arrayIters);
            while (!multiIters.end())
            {
                std::vector<size_t> minIds;

                /* Get list of all iters where current chunk position is not empty
                 */
                multiIters.getIDsAtMinPosition(minIds);
                for (size_t i = 0; i < minIds.size(); i++)
                {
                    shared_ptr<ConstArrayIterator> currentIter =
                        static_pointer_cast<ConstArrayIterator, ConstIterator> (arrayIters[minIds[i]]);
                    const ConstChunk& chunk = currentIter->getChunk();

                    if (!chunk.isMaterialized()) {
                        shared_ptr<ConstChunkIterator> chunkIter =
                            chunk.getConstIterator(ConstChunkIterator::INTENDED_TILE_MODE |
                                                   ConstChunkIterator::IGNORE_EMPTY_CELLS);

                        while (!chunkIter->end()) {
                            Value& v = chunkIter->getItem();

                            v.isNull();                      // suppress compiler warning
                            ++(*chunkIter);
                        }
                    }
                }

                /* Advance to next chunk position
                 */
                ++multiIters;
            }

            /* Progress to the next vertical slice
             */
            baseAttr += sliceSize;
        }

        return shared_ptr<Array>();
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalConsume, "consume", "PhysicalConsume")

}  // namespace scidb
