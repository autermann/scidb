
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
 * LogicalWindow.cpp
 *
 *  Created on: Apr 11, 2010
 *      Author: Knizhnik, poliocough@gmail.com
 */

#include <boost/shared_ptr.hpp>

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "query/LogicalExpression.h"

namespace scidb
{

using namespace std;

class LogicalWindow: public LogicalOperator
{
public:
    LogicalWindow(const std::string& logicalName, const std::string& alias):
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

    inline ArrayDesc createWindowDesc(ArrayDesc const& desc, vector<size_t> const& window)
    {
        Dimensions const& dims = desc.getDimensions();
        Dimensions aggDims(dims.size());
        for (size_t i = 0, n = dims.size(); i < n; i++)
        {
            DimensionDesc const& srcDim = dims[i];
            aggDims[i] = DimensionDesc(srcDim.getBaseName(),
                                       srcDim.getNamesAndAliases(),
                                       srcDim.getStartMin(),
                                       srcDim.getCurrStart(),
                                       srcDim.getCurrEnd(),
                                       srcDim.getEndMax(),
                                       srcDim.getChunkInterval(), 
                                       0,
                                       srcDim.getType(),
                                       srcDim.getFlags(),
                                       srcDim.getMappingArrayName(),
                                       srcDim.getComment(),
                                       srcDim.getFuncMapOffset(),
                                       srcDim.getFuncMapScale());
        }

        ArrayDesc output (desc.getName(), Attributes(), aggDims);

        for (size_t i = dims.size(); i<_parameters.size(); i++)
        {
            addAggregatedAttribute( (shared_ptr <OperatorParamAggregateCall> &) _parameters[i], desc, output);
        }

        if ( desc.getEmptyBitmapAttribute())
        {
            AttributeDesc const* eAtt = desc.getEmptyBitmapAttribute();
            output.addAttribute(AttributeDesc( output.getAttributes().size(), eAtt->getName(), eAtt->getType(), eAtt->getFlags(), eAtt->getDefaultCompressionMethod()));
        }

        return output;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
    {
        assert(schemas.size() == 1);

        ArrayDesc const& desc = schemas[0];
        size_t nDims = desc.getDimensions().size();
        vector<size_t> window(nDims);
        for (size_t i = 0; i < nDims; i++)
        {
           window[i] = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[i])->getExpression(),
                                query, TID_INT64).getInt64();
           if ((ssize_t)window[i] <= 0)
               throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_WINDOW_ERROR1,
                                          _parameters[i]->getParsingContext());
        }

        return createWindowDesc(desc, window);
        }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalWindow, "window")


}  // namespace ops
