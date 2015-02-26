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
 * LogicalBernoulli.cpp
 *
 *  Created on: Feb 14, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "system/SystemCatalog.h"
#include "system/Exceptions.h"

using namespace std;

namespace scidb {

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
    

class LogicalBernoulli: public LogicalOperator
{
public:
    LogicalBernoulli(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_INPUT()
        ADD_PARAM_CONSTANT("double");
        ADD_PARAM_VARIES() 
    }

	std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
	{
		std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        res.push_back(PARAM_CONSTANT("int64"));
        return res;
	}

    ArrayDesc inferSchema(vector<ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        return addEmptyTagAttribute(schemas[0]);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalBernoulli, "bernoulli")


} //namespace
