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
 * LogicalInverse.cpp
 *
 */

#include "query/Operator.h"


namespace scidb
{

class Inverse : public  LogicalOperator
{
public:
	Inverse(const std::string& logicalName, const std::string& alias):
	    LogicalOperator(logicalName, alias)
	{
		ADD_PARAM_INPUT()
	}

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        assert(_parameters.size() == 0);

        if (schemas[0].getAttributes().size() != 1)
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_INVERSE_ERROR3);
        if (schemas[0].getAttributes()[0].getType() != TID_DOUBLE)
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_INVERSE_ERROR3);
        if (schemas[0].getDimensions().size() != 2)
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_INVERSE_ERROR5);
        if ( schemas[0].getDimensions()[0].getLength() != schemas[0].getDimensions()[1].getLength())
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_INVERSE_ERROR6);

		Attributes atts;
		AttributeDesc multAttr((AttributeID)0, "v",  TID_DOUBLE, 0, 0);
		atts.push_back(multAttr);

		Dimensions dims;
		DimensionDesc d1 = schemas[0].getDimensions()[0];
		DimensionDesc d2 = schemas[0].getDimensions()[1];

        if (d2.getLength() == INFINITE_LENGTH || d2.getLength() == INFINITE_LENGTH)
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_INVERSE_ERROR7);

        dims.push_back(DimensionDesc(d1.getBaseName(), d1.getNamesAndAliases(), d1.getStartMin(), d1.getCurrStart(), d1.getCurrEnd(), d1.getEndMax(), d1.getChunkInterval(), 0));
        dims.push_back(DimensionDesc(d2.getBaseName(), d2.getNamesAndAliases(), d1.getStartMin(), d1.getCurrStart(), d2.getCurrEnd(), d2.getEndMax(), d2.getChunkInterval(), 0));

		 ArrayDesc array_desc("inverse",atts,dims);
		return array_desc;
	}

};

DECLARE_LOGICAL_OPERATOR_FACTORY(Inverse, "inverse")

} //namespace scidb
