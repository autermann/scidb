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
 * LogicalRegrid.cpp
 *
 *  Created on: Jul 25, 2011
 *      Author: poliocough@gmail.com
 */

#include <boost/shared_ptr.hpp>

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "query/LogicalExpression.h"

namespace scidb {

using namespace std;


class LogicalRegrid: public LogicalOperator
{
public:
    LogicalRegrid(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
        ADD_PARAM_VARIES()
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector<ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        if (_parameters.size() < schemas[0].getDimensions().size())
        {
            res.push_back(PARAM_CONSTANT("int64"));
        }
        else if (_parameters.size() == schemas[0].getDimensions().size())
        {
            res.push_back(PARAM_AGGREGATE_CALL());
        }
        else
        {
            res.push_back(END_OF_VARIES_PARAMS());
            res.push_back(PARAM_AGGREGATE_CALL());
        }
        return res;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
    {
        assert(schemas.size() == 1);

        ArrayDesc const& inputDesc = schemas[0];
        size_t nDims = inputDesc.getDimensions().size();
        Dimensions outDims(nDims);

        for (size_t i = 0; i < nDims; i++)
        {
            int64_t interval = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[i])->getExpression(),
                                        query, TID_INT64).getInt64();
            if (interval <= 0)
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REGRID_ERROR1,
                                           _parameters[i]->getParsingContext());
            DimensionDesc const& srcDim = inputDesc.getDimensions()[i];
            outDims[i] = DimensionDesc( srcDim.getBaseName(),
                                        srcDim.getNamesAndAliases(),
                                        srcDim.getStart(),
                                        srcDim.getStart(),
                                        srcDim.getEndMax() == MAX_COORDINATE ? MAX_COORDINATE : srcDim.getStart() + (srcDim.getLength() + interval - 1)/interval - 1,
                                        srcDim.getEndMax() == MAX_COORDINATE ? MAX_COORDINATE : srcDim.getStart() + (srcDim.getLength() + interval - 1)/interval - 1,
                                        (srcDim.getChunkInterval() + interval - 1) / interval,
                                        0  );
        }

        ArrayDesc outSchema(inputDesc.getName(), Attributes(), outDims);

        for (size_t i = nDims; i<_parameters.size(); i++)
        {
            addAggregatedAttribute( (shared_ptr <OperatorParamAggregateCall> &) _parameters[i], inputDesc, outSchema);
        }

        return outSchema;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalRegrid, "regrid")


}  // namespace ops
