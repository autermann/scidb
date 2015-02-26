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
 * LogicalFilter.cpp
 *
 *  Created on: Apr 11, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"

namespace scidb {

using namespace std;

/***
 * Helper function to add empty tag attribute to the array
 * Constructs a new array descriptor with empty tag attribute
 ***/
inline ArrayDesc addEmptyTagAttribute(ArrayDesc const& desc)
{
    Attributes const& inputAttributes = desc.getAttributes();
	Attributes newAttributes;
    for (size_t i = 0, n = inputAttributes.size(); i < n; i++) { 
        newAttributes.push_back(inputAttributes[i]);
    }
    if (desc.getEmptyBitmapAttribute() == NULL) { 
        newAttributes.push_back(AttributeDesc((AttributeID)newAttributes.size(),
                                             DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,  TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0));
    }
	return ArrayDesc(desc.getName(), newAttributes, desc.getDimensions());
}


class Filter: public  LogicalOperator
{
public:
	Filter(const std::string& logicalName, const std::string& alias):
	        LogicalOperator(logicalName, alias)
    {
        _properties.tile = true;
		ADD_PARAM_INPUT()
        ADD_PARAM_EXPRESSION("bool")
    }

    virtual bool compileParamInTileMode(size_t paramNo) { 
        return paramNo == 0;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
	{
		assert(schemas.size() == 1);
		assert(_parameters.size() == 1);
		assert(_parameters[0]->getParamType() == PARAM_LOGICAL_EXPRESSION);

		return addEmptyTagAttribute(schemas[0]);
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(Filter, "filter")


}  // namespace scidb
