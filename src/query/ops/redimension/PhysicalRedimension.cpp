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
 * PhysicalRedimension.cpp
 *
 *  Created on: Apr 16, 2010
 *  @author Knizhnik
 *  @author poliocough@gmail.com
 */

#include "RedimensionCommon.h"
#include <log4cxx/logger.h>

namespace scidb
{

using namespace std;
using namespace boost;

/**
 * Redimension operator
 */
class PhysicalRedimension: public RedimensionCommon
{
private:
    /**
     * True if this is a redimension with aggregates; false otherwise.
     */
    bool _haveAggregates;

public:
    /**
     * Vanilla; checks for aggregates.
     * @param logicalName the name of operator "redimension"
     * @param physicalName the name of the physical counterpart
     * @param parameters the operator parameters - the output schema and optional aggregates
     * @param schema the result of LogicalRedimension::inferSchema
     */
    PhysicalRedimension(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        RedimensionCommon(logicalName, physicalName, parameters, schema), _haveAggregates(false)
    {
        if(_parameters.size() > 1)
        {
            _haveAggregates = true;
        }
    }

    /**
     * Determine the distribution of the operator output: psRoundRobin with aggregates, psUndefined otherwise.
     * @return the output distribution
     */
    virtual ArrayDistribution getOutputDistribution(std::vector<ArrayDistribution> const&, std::vector< ArrayDesc> const&) const
    {
        if(_haveAggregates)
            return ArrayDistribution(psRoundRobin);
        return ArrayDistribution(psUndefined);
    }

    /**
     * Determine the chunking of the operator output: full with aggregates, partially-filled otherwise
     * @return the output chunking status
     */
    virtual bool outputFullChunks(std::vector< ArrayDesc> const&) const
    {
        return _haveAggregates;
    }

    /**
     * Execute the operator - redimension inputArrays[0] and return the result.
     * @return the redimensioned array.
     */
    shared_ptr<Array> execute(vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        shared_ptr<Array> srcArray = inputArrays[0];
        ArrayDesc const& srcArrayDesc = srcArray->getArrayDesc();

        Attributes const& destAttrs = _schema.getAttributes(true); // true = exclude empty tag.
        Dimensions const& destDims = _schema.getDimensions();

        vector<AggregatePtr> aggregates (destAttrs.size());
        vector<size_t> attrMapping(destAttrs.size());
        vector<size_t> dimMapping(_schema.getDimensions().size());

        setupMappings(srcArrayDesc, aggregates, attrMapping, dimMapping, destAttrs, destDims);
        vector< shared_ptr<AttributeMultiMap> > coordinateMultiIndices(destDims.size());
        vector< shared_ptr<AttributeMap> > coordinateIndices(destDims.size());
        ElapsedMilliSeconds timing;

        return redimensionArray(srcArray,
                                attrMapping,
                                dimMapping,
                                aggregates,
                                query,
                                coordinateMultiIndices,
                                coordinateIndices,
                                timing,
                                false);
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalRedimension, "redimension", "PhysicalRedimension")
}  // namespace ops
