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
 * LogicalCast.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "system/SystemCatalog.h"
#include "system/Exceptions.h"

using namespace std;

namespace scidb
{

class LogicalCast: public LogicalOperator
{
public:
    LogicalCast(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        _properties.tile = true;
        ADD_PARAM_INPUT()
    	ADD_PARAM_SCHEMA()
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        assert(_parameters.size() == 1);

        ArrayDesc schemaParam = ((boost::shared_ptr<OperatorParamSchema>&)_parameters[0])->getSchema();

        ArrayDesc const& srcArrayDesc = schemas[0];
        Attributes const& srcAttributes = srcArrayDesc.getAttributes();
        Dimensions const& srcDimensions = srcArrayDesc.getDimensions();
        Attributes const& dstAttributes = schemaParam.getAttributes();
        Dimensions dstDimensions = schemaParam.getDimensions();

        if (schemaParam.getName().size() == 0)
        {
            schemaParam.setName(srcArrayDesc.getName());
        }

        const boost::shared_ptr<ParsingContext> &pc = _parameters[0]->getParsingContext();
        if (srcAttributes.size() != dstAttributes.size() && srcAttributes.size() != schemaParam.getAttributes(true).size())
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CAST_ERROR1, pc);
        for (size_t i = 0, n = srcAttributes.size(); i < n; i++) {
            if (srcAttributes[i].getType() != dstAttributes[i].getType())
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CAST_ERROR2, pc);
            if  ( dstAttributes[i].getFlags()!= srcAttributes[i].getFlags() &&
                  dstAttributes[i].getFlags()!= (srcAttributes[i].getFlags() | AttributeDesc::IS_NULLABLE ))
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CAST_ERROR3, pc);
        }

        if (srcDimensions.size() != dstDimensions.size())
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CAST_ERROR4, pc);

        for (size_t i = 0, n = srcDimensions.size(); i < n; i++) {
            DimensionDesc const& srcDim = srcDimensions[i];
            DimensionDesc const& dstDim = dstDimensions[i];
            if (srcDim.getType() != dstDim.getType() && dstDim.getType() != TID_INT64)
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CAST_ERROR9, pc);
            if (!(srcDim.getEndMax() == dstDim.getEndMax() 
                  || (srcDim.getEndMax() < dstDim.getEndMax() 
                      && ((srcDim.getLength() % srcDim.getChunkInterval()) == 0
                          || srcArrayDesc.getEmptyBitmapAttribute() != NULL))))
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CAST_ERROR5, pc);
            if (srcDim.getStart() != dstDim.getStart())
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CAST_ERROR6, pc);
            if (srcDim.getChunkInterval() != dstDim.getChunkInterval())
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CAST_ERROR7, pc);
            if (srcDim.getChunkOverlap() != dstDim.getChunkOverlap())
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CAST_ERROR8, pc);
            
            if (!srcDim.getEndMax() != dstDim.getEndMax()) {
                _properties.tile = false;
            }
            dstDimensions[i] = DimensionDesc(dstDim.getBaseName(),
                                             dstDim.getNamesAndAliases(),
                                             dstDim.getStartMin() == MIN_COORDINATE && srcDim.getCurrStart() != MAX_COORDINATE ? srcDim.getCurrStart() : dstDim.getStartMin(), 
                                             srcDim.getCurrStart(), 
                                             srcDim.getCurrEnd(), 
                                             dstDim.getEndMax() == MAX_COORDINATE && srcDim.getCurrEnd() != MIN_COORDINATE ? srcDim.getCurrEnd() : dstDim.getEndMax(), 
                                             dstDim.getChunkInterval(), 
                                             dstDim.getChunkOverlap(), 
                                             dstDim.getType(), 
                                             dstDim.getFlags(), 
                                             srcDim.getMappingArrayName(), 
                                             dstDim.getComment(),
                                             srcDim.getFuncMapOffset(),
                                             srcDim.getFuncMapScale());
        }
        return ArrayDesc(schemaParam.getName(), dstAttributes, dstDimensions, schemaParam.getFlags());
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalCast, "cast")

} //namespace
