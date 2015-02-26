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
    	ADD_PARAM_INPUT()
    	ADD_PARAM_SCHEMA()
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        assert(_parameters.size() == 1);

        ArrayDesc dstArrayDesc = ((boost::shared_ptr<OperatorParamSchema>&)_parameters[0])->getSchema();

        ArrayDesc const& srcArrayDesc = schemas[0];
        Attributes const& srcAttributes = srcArrayDesc.getAttributes();
        Dimensions const& srcDimensions = srcArrayDesc.getDimensions();
        Attributes const& dstAttributes = dstArrayDesc.getAttributes();
        Dimensions const& dstDimensions = dstArrayDesc.getDimensions();

        if (dstArrayDesc.getName().size() == 0)
        {
            dstArrayDesc.setName(srcArrayDesc.getName());
        }

        const boost::shared_ptr<ParsingContext> &pc = _parameters[0]->getParsingContext();
        if (srcAttributes.size() != dstAttributes.size())
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
            if (srcDimensions[i].getLength() != dstDimensions[i].getLength())
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CAST_ERROR5, pc);
            if (srcDimensions[i].getStart() != dstDimensions[i].getStart())
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CAST_ERROR6, pc);
            if (srcDimensions[i].getChunkInterval() != dstDimensions[i].getChunkInterval())
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CAST_ERROR7, pc);
            if (srcDimensions[i].getChunkOverlap() != dstDimensions[i].getChunkOverlap())
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CAST_ERROR8, pc);
        }
        return dstArrayDesc;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalCast, "cast")

} //namespace
