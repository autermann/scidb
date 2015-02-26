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
 * LogicalUnpack.cpp
 *
 *  Created on: Mar 9, 2010
 *      Author: Emad
 */

#include "query/Operator.h"
#include "system/Exceptions.h"

namespace scidb
{

using namespace std;

/***
 * Helper function to construct array descriptor for the unpacked array
 ***/
inline ArrayDesc addAttributes(ArrayDesc const& desc, string const& dimName)
{
    Attributes const& oldAttributes = desc.getAttributes();
    Dimensions const& dims = desc.getDimensions();
    Attributes newAttributes(oldAttributes.size() + dims.size()); 
    size_t i = 0;
    uint64_t arrayLength = 1;
    for (size_t j = 0; j < dims.size(); j++, i++)
    {
        arrayLength *= dims[j].getLength();
        newAttributes[i] = AttributeDesc(i, dims[j].getBaseName(), dims[j].getType(), 0, 0);
    }
    for (size_t j = 0; j < oldAttributes.size(); j++, i++) { 
        AttributeDesc const& attr = oldAttributes[j];
        newAttributes[i] = AttributeDesc((AttributeID)i, attr.getName(), attr.getType(), attr.getFlags(),
            attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
            attr.getDefaultValueExpr());
    }
    Dimensions newDimensions(1);
    newDimensions[0] = DimensionDesc(dimName, 0, 0, arrayLength-1, arrayLength-1, dims[dims.size()-1].getChunkInterval(), 0);
	return ArrayDesc(desc.getName(), newAttributes, newDimensions);
}
    

class LogicalUnpack: public LogicalOperator
{
public:
	LogicalUnpack(const string& logicalName, const std::string& alias):
	    LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
        ADD_PARAM_OUT_DIMENSION_NAME() //0
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        assert(_parameters.size() == 1);
        assert(((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getParamType() == PARAM_DIMENSION_REF);

        const string &dimName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
		return addAttributes(schemas[0], dimName);
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalUnpack, "unpack")

} //namespace
