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
 * LogicalXgrid.cpp
 *
 *  Created on: Jul 19, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "system/Exceptions.h"

namespace scidb {

using namespace std;

/***
 * Helper function to generate descriptor of xgrid array
 ***/
    inline ArrayDesc createXgridDesc(ArrayDesc const& desc, vector<int> const& grid)
{
    Dimensions const& dims = desc.getDimensions();
    Dimensions newDims(dims.size());
    for (size_t i = 0, n = dims.size(); i < n; i++) {
        DimensionDesc const& srcDim = dims[i];
        newDims[i] = DimensionDesc(srcDim.getBaseName(),
                                   srcDim.getNamesAndAliases(),
                                   srcDim.getStartMin(),
                                   srcDim.getCurrStart(),
                                   srcDim.getCurrStart() + srcDim.getLength()*grid[i] - 1,
                                   srcDim.getStartMin() + srcDim.getLength()*grid[i] - 1,
                                   srcDim.getChunkInterval()*grid[i], 0);
    }
        return ArrayDesc(desc.getName(), desc.getAttributes(), newDims);
}


class LogicalXgrid: public  LogicalOperator
{
  public:
    LogicalXgrid(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
        {
        ADD_PARAM_INPUT()
        ADD_PARAM_VARIES()
        }

        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
        {
                std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
                if (_parameters.size() == schemas[0].getDimensions().size())
                        res.push_back(END_OF_VARIES_PARAMS());
                else
                        res.push_back(PARAM_CONSTANT("int32"));
                return res;
        }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        assert(_parameters.size() == schemas[0].getDimensions().size());

        ArrayDesc const& desc = schemas[0];
        size_t nDims = desc.getDimensions().size();

        vector<int> grid(nDims);
        for (size_t i = 0; i < nDims; i++)
        {
            if (desc.getDimensions()[i].getLength() == INFINITE_LENGTH)
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_XGRID_ERROR1);
            grid[i] = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[i])->getExpression(),
                               query, TID_INT32).getInt32();
            if (grid[i] <= 0)
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_XGRID_ERROR2);
        }
        return createXgridDesc(desc, grid);
        }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalXgrid, "xgrid")


}  // namespace scidb
