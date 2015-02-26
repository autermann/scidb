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
 * LogicalJoin.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "system/SystemCatalog.h"
#include "system/Exceptions.h"
#include "array/Metadata.h"

using namespace std;

namespace scidb
{

class LogicalJoin: public LogicalOperator
{
  public:
    LogicalJoin(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_INPUT()
    	ADD_PARAM_INPUT()
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 2);

        ArrayDesc const& leftArrayDesc = schemas[0];
        ArrayDesc const& rightArrayDesc = schemas[1];
        Attributes const& leftAttributes = leftArrayDesc.getAttributes();
        Dimensions leftDimensions = leftArrayDesc.getDimensions();
        Attributes const& rightAttributes = rightArrayDesc.getAttributes();
        Dimensions const& rightDimensions = rightArrayDesc.getDimensions();
        size_t totalAttributes = leftAttributes.size() + rightAttributes.size();
        int nBitmaps = 0;
        nBitmaps += (leftArrayDesc.getEmptyBitmapAttribute() != NULL);
        nBitmaps += (rightArrayDesc.getEmptyBitmapAttribute() != NULL); 
        if (nBitmaps == 2) { 
            totalAttributes -= 1;
        }
        if (nBitmaps == 0) {
            for (size_t i = 0, n = leftDimensions.size(); i < n; i++) {
                if (leftDimensions[i].getType() != TID_INT64) { 
                    totalAttributes += 1;
                    break;
                }
            }
        }
        Attributes joinAttributes(totalAttributes);

        size_t j = 0;
        for (size_t i = 0, n = leftAttributes.size(); i < n; i++) {
            AttributeDesc const& attr = leftAttributes[i];
            if (!attr.isEmptyIndicator()) {
                joinAttributes[j] = AttributeDesc(j, attr.getName(), attr.getType(), attr.getFlags(),
                    attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                    attr.getDefaultValueExpr());
                joinAttributes[j].addAlias(leftArrayDesc.getName());
                j += 1;
            }
        }
        for (size_t i = 0, n = rightAttributes.size(); i < n; i++, j++) {
            AttributeDesc const& attr = rightAttributes[i];
            joinAttributes[j] = AttributeDesc(j, attr.getName(), attr.getType(), attr.getFlags(),
                attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                attr.getDefaultValueExpr());
            joinAttributes[j].addAlias(rightArrayDesc.getName());
        }
        if (j < totalAttributes) { 
            joinAttributes[j] = AttributeDesc(j, DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,  TID_INDICATOR,
                AttributeDesc::IS_EMPTY_INDICATOR, 0);
        }

        if(leftDimensions.size() != rightDimensions.size())
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_LOGICAL_JOIN_ERROR1);
        }
        for (size_t i = 0, n = leftDimensions.size(); i < n; i++) {
            if(!(leftDimensions[i].getType() ==  rightDimensions[i].getType()
                       && (leftDimensions[i].getType() != TID_INT64 
                           || (leftDimensions[i].getStart() == rightDimensions[i].getStart()
                              && leftDimensions[i].getChunkInterval() == rightDimensions[i].getChunkInterval()
                              && leftDimensions[i].getChunkOverlap() == rightDimensions[i].getChunkOverlap()))))
           {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_LOGICAL_JOIN_ERROR2);
           }
           leftDimensions[i].addAlias(leftArrayDesc.getName());
           Coordinate newCurrStart = max(leftDimensions[i].getCurrStart(), rightDimensions[i].getCurrStart());
           Coordinate newCurrEnd = min(leftDimensions[i].getCurrEnd(), rightDimensions[i].getCurrEnd());
           Coordinate newEndMax = min(leftDimensions[i].getEndMax(), rightDimensions[i].getEndMax());
           leftDimensions[i].setCurrStart(newCurrStart);
           leftDimensions[i].setCurrEnd(newCurrEnd);
           leftDimensions[i].setEndMax(newEndMax);

           BOOST_FOREACH(const ObjectNames::NamesPairType &rDimName, rightDimensions[i].getNamesAndAliases())
           {
               BOOST_FOREACH(const string &alias, rDimName.second)
               {
                   leftDimensions[i].addAlias(alias, rDimName.first);
               }
           }
        }
        return ArrayDesc(leftArrayDesc.getName() + rightArrayDesc.getName(), joinAttributes, leftDimensions);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalJoin, "join")


} //namespace
