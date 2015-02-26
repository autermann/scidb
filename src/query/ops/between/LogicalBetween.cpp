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
 * LogicalBetween.cpp
 *
 *  Created on: Oct 22, 2010
 *      Author: knizhnik@garret.ru
 */

#include "query/Operator.h"
#include "system/Exceptions.h"


namespace scidb {

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

class LogicalBetween: public  LogicalOperator
{
  public:
	LogicalBetween(const std::string& logicalName, const std::string& alias) : LogicalOperator(logicalName, alias)
	{
        _properties.tile = true;
		ADD_PARAM_INPUT()
		ADD_PARAM_VARIES()
	}

	std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector<ArrayDesc> &schemas)
	{
		std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        size_t i = _parameters.size();
        Dimensions const& dims = schemas[0].getDimensions();
        size_t nDims = dims.size();
		if (i < nDims*2)
			res.push_back(PARAM_CONSTANT(dims[i < nDims ? i : i - nDims].getType()));
		else
			res.push_back(END_OF_VARIES_PARAMS());
		return res;
	}

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
	{
		assert(schemas.size() == 1);
        return addEmptyTagAttribute(schemas[0]);
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalBetween, "between")


}  // namespace scidb
