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
 * LogicalConcat.cpp
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

    class LogicalConcat: public LogicalOperator
    {
    public:
        LogicalConcat(const string& logicalName, const std::string& alias):
            LogicalOperator(logicalName, alias)
        {
            ADD_PARAM_INPUT()
            ADD_PARAM_INPUT()
        }

        ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
        {
            size_t i;
            assert(schemas.size() == 2);
            ArrayDesc const& leftArrayDesc = schemas[0];
            ArrayDesc const& rightArrayDesc = schemas[1];

            // Check that attrbiutes are the same
            Attributes leftAttributes = leftArrayDesc.getAttributes();
            Attributes rightAttributes = rightArrayDesc.getAttributes();

            if (leftAttributes.size() != rightAttributes.size()
                    && (leftAttributes.size() != rightAttributes.size()+1
                        || !leftAttributes[leftAttributes.size()-1].isEmptyIndicator())
                    && (leftAttributes.size()+1 != rightAttributes.size()
                        || !rightAttributes[rightAttributes.size()-1].isEmptyIndicator()))
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);

            size_t nAttrs = min(leftAttributes.size(), rightAttributes.size());
            for (i = 0; i < nAttrs; i++)
            {
                if (leftAttributes[i].getName() != rightAttributes[i].getName()
                        ||  leftAttributes[i].getType() != rightAttributes[i].getType()
                        ||  leftAttributes[i].getFlags() != rightAttributes[i].getFlags())
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
            }
            Attributes const& newAttributes = (leftAttributes.size() > rightAttributes.size() ? leftAttributes : rightAttributes);
            // Check dimensions
            Dimensions const& leftDimensions = leftArrayDesc.getDimensions();
            Dimensions const& rightDimensions = rightArrayDesc.getDimensions();
            if (leftDimensions.size() != rightDimensions.size())
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
            size_t nDims = leftDimensions.size();
            Dimensions newDimensions(nDims);

            if (Coordinate(leftDimensions[0].getLength()) == MAX_COORDINATE
                    || Coordinate(rightDimensions[0].getLength()) == MAX_COORDINATE)
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CONCAT_ERROR1);
            newDimensions[0] = DimensionDesc(leftDimensions[0].getBaseName(),
                                             leftDimensions[0].getNamesAndAliases(),
                                             leftDimensions[0].getStartMin(), 
                                             leftDimensions[0].getCurrStart(), 
                                             leftDimensions[0].getCurrEnd() + rightDimensions[0].getLength(), 
                                             leftDimensions[0].getEndMax() + rightDimensions[0].getLength(), 
                                             leftDimensions[0].getChunkInterval(), 0);
            if (leftDimensions[0].getChunkInterval() != rightDimensions[0].getChunkInterval())
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);

            for (i = 1; i < nDims; i++) {
                if (leftDimensions[i].getLength() != rightDimensions[i].getLength()
                        || leftDimensions[i].getStart() != rightDimensions[i].getStart()
                        || leftDimensions[i].getChunkInterval() != rightDimensions[i].getChunkInterval())
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
                newDimensions[i] = leftDimensions[i];
            }
            return ArrayDesc(leftArrayDesc.getName() + rightArrayDesc.getName(), newAttributes, newDimensions);
        }
    };

    DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalConcat, "concat")

} //namespace
