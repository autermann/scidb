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
 * LogicalCross.cpp
 *
 *  Created on: Jul 19, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "system/SystemCatalog.h"
#include "system/Exceptions.h"
#include "array/Metadata.h"

namespace scidb
{

    using namespace std;
    

    class LogicalCross: public LogicalOperator
    {
      public:
        LogicalCross(const string& logicalName, const std::string& alias)
        : LogicalOperator(logicalName, alias)
        {
        	ADD_PARAM_INPUT()
            ADD_PARAM_INPUT()
        }

        ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
        {
        	assert(schemas.size() == 2);
            assert(_parameters.size() == 0);

            ArrayDesc const& leftArrayDesc = schemas[0];
            ArrayDesc const& rightArrayDesc = schemas[1];
            Attributes const& leftAttributes = leftArrayDesc.getAttributes();
            Dimensions const& leftDimensions = leftArrayDesc.getDimensions();
            Attributes const& rightAttributes = rightArrayDesc.getAttributes();
            Dimensions const& rightDimensions = rightArrayDesc.getDimensions();
            Attributes crossAttributes(leftAttributes.size() + rightAttributes.size());
            Dimensions crossDimensions(leftDimensions.size() + rightDimensions.size());

            size_t j = 0;
            for (size_t i = 0, n = leftAttributes.size(); i < n; i++, j++) { 
                AttributeDesc const& attr = leftAttributes[i];
                crossAttributes[j] = AttributeDesc(j, attr.getName(), attr.getType(), attr.getFlags(), attr.getDefaultCompressionMethod(), 
                                                   attr.getAliases(), &attr.getDefaultValue(), attr.getDefaultValueExpr());
                crossAttributes[j].addAlias(leftArrayDesc.getName());
            }
            for (size_t i = 0, n = rightAttributes.size(); i < n; i++, j++) { 
                AttributeDesc const& attr = rightAttributes[i];
                crossAttributes[j] = AttributeDesc(j, attr.getName(), attr.getType(), attr.getFlags(), attr.getDefaultCompressionMethod(), 
                                                   attr.getAliases(), &attr.getDefaultValue(), attr.getDefaultValueExpr());
                crossAttributes[j].addAlias(rightArrayDesc.getName());
            }
            
            j = 0;
            for (size_t i = 0, n = leftDimensions.size(); i < n; i++, j++) { 
                crossDimensions[j] = leftDimensions[i];
                BOOST_FOREACH(const DimensionDesc::NamesPairType& leftDimName, crossDimensions[j].getNamesAndAliases())
                {
                    crossDimensions[j].addAlias(leftDimName.first, leftArrayDesc.getName());
                }
            }
            for (size_t i = 0, n = rightDimensions.size(); i < n; i++, j++) { 
                crossDimensions[j] = rightDimensions[i];
                BOOST_FOREACH(const DimensionDesc::NamesPairType& rightDimName, crossDimensions[j].getNamesAndAliases())
                {
                    crossDimensions[j].addAlias(rightDimName.first, rightArrayDesc.getName());
                }
            }
            return ArrayDesc(leftArrayDesc.getName() + rightArrayDesc.getName(), crossAttributes, crossDimensions);
        }
    };

    DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalCross, "cross")

} //namespace
