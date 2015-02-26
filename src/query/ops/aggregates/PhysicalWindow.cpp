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
 * PhysicalWindow.cpp
 *
 *  Created on: Apr 11, 2010
 *      Author: Knizhnik, poliocough@gmail.com
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "WindowArray.h"


namespace scidb {

using namespace boost;
using namespace std;

class PhysicalWindow: public  PhysicalOperator
{
private:
    vector<WindowBoundaries> _window;

public:
    PhysicalWindow(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
	     PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
        size_t nDims = _schema.getDimensions().size();
        _window = vector<WindowBoundaries>(nDims);
        for (size_t i = 0, size = nDims * 2, boundaryNo = 0; i < size; i+=2, ++boundaryNo)
        {
            _window[boundaryNo] = WindowBoundaries(
                    ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i])->getExpression()->evaluate().getInt64(),
                    ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i+1])->getExpression()->evaluate().getInt64()
                    );
        }
	}

    virtual bool requiresRepart(ArrayDesc const& inputSchema) const
    {
        Dimensions const& dims = inputSchema.getDimensions();
        for (size_t i = 0; i < dims.size(); i++)
        {
            DimensionDesc const& srcDim = dims[i];
            if(srcDim.getChunkOverlap() < std::max(_window[i]._boundaries.first, _window[i]._boundaries.second))
            {
                return true;
            }
        }
        return false;
    }

    virtual ArrayDesc getRepartSchema(ArrayDesc const& inputSchema) const
    {
        Attributes attrs = inputSchema.getAttributes();

        Dimensions dims;
        for (size_t i =0; i<inputSchema.getDimensions().size(); i++)
        {
            DimensionDesc inDim = inputSchema.getDimensions()[i];

            size_t overlap = inDim.getChunkOverlap() >=
                std::max(_window[i]._boundaries.first, _window[i]._boundaries.second)
                ? inDim.getChunkOverlap()
                : std::max(_window[i]._boundaries.first, _window[i]._boundaries.second);

            dims.push_back( DimensionDesc(inDim.getBaseName(),
                                          inDim.getNamesAndAliases(),
                                          inDim.getStartMin(),
                                          inDim.getCurrStart(),
                                          inDim.getCurrEnd(),
                                          inDim.getEndMax(),
                                          inDim.getChunkInterval(),
                                          overlap,
                                          inDim.getType(),
                                          inDim.getFlags(),
                                          inDim.getMappingArrayName(),
                                          inDim.getComment(),
                                          inDim.getFuncMapOffset(),
                                          inDim.getFuncMapScale()));
        }

        return ArrayDesc(inputSchema.getName(), attrs, dims);
    }


    //FIXME: Why this is checking during execution?!
    void verifyInputSchema(ArrayDesc const& input) const
    {
        Dimensions const& dims = input.getDimensions();
        for (size_t i = 0, n = dims.size(); i < n; i++)
        {
            DimensionDesc const& srcDim = dims[i];
            if (srcDim.getChunkOverlap() < std::max(_window[i]._boundaries.first, _window[i]._boundaries.second))
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_WINDOW_ERROR2);
        }
    }

	/***
	 * Window is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
	 * that overrides the chunkiterator method.
	 */
	boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);

        if (inputArrays[0]->getSupportedAccess() == Array::SINGLE_PASS)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_UNSUPPORTED_INPUT_ARRAY) << getLogicalName();
        }

        ArrayDesc const& inDesc = inputArrays[0]->getArrayDesc();
        verifyInputSchema(inDesc);

        vector<AttributeID> inputAttrIDs;
        vector<AggregatePtr> aggregates;

        for (size_t i = inDesc.getDimensions().size() * 2, size = _parameters.size(); i < size; i++)
        {
            AttributeID inAttId;

            AggregatePtr agg = resolveAggregate((shared_ptr <OperatorParamAggregateCall> const&) _parameters[i],
                                                inDesc.getAttributes(),
                                                &inAttId,
                                                0);

            aggregates.push_back(agg);

            if (inAttId == (AttributeID) -1)
            {
                //for count(*); optimize later
                inputAttrIDs.push_back(0);
            }
            else
            {
                inputAttrIDs.push_back(inAttId);
            }
        }

        return boost::shared_ptr<Array>(new WindowArray(_schema, inputArrays[0], _window, inputAttrIDs, aggregates));
    }
};
    
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalWindow, "window", "physicalWindow")

}  // namespace scidb
