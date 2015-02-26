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
 * LogicalMerge.cpp
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

class LogicalMerge: public LogicalOperator
{
public:
    LogicalMerge(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_INPUT()
		ADD_PARAM_INPUT()
		ADD_PARAM_VARIES()
    }

	std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
	{
		std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
		res.push_back(PARAM_INPUT());
		res.push_back(END_OF_VARIES_PARAMS());
		return res;
	}

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() >= 2);
        assert(_parameters.size() == 0);

        Attributes const& leftAttributes = schemas[0].getAttributes();
        Dimensions const& leftDimensions = schemas[0].getDimensions();
        Attributes const* newAttributes = &leftAttributes;

        for (size_t j = 1; j < schemas.size(); j++) {
            Attributes const& rightAttributes = schemas[j].getAttributes();
            Dimensions const& rightDimensions = schemas[j].getDimensions();

            if (leftDimensions.size() != rightDimensions.size())
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);

            for (size_t i = 0, n = leftDimensions.size(); i < n; i++) {
                if (leftDimensions[i].getLength() != rightDimensions[i].getLength()
                        || leftDimensions[i].getType() != rightDimensions[i].getType()
                        || leftDimensions[i].getStart() != rightDimensions[i].getStart()
                        || leftDimensions[i].getChunkInterval() != rightDimensions[i].getChunkInterval()
                        || leftDimensions[i].getChunkOverlap() != rightDimensions[i].getChunkOverlap())
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
            }

            if (leftAttributes.size() != rightAttributes.size()
                    && (leftAttributes.size() != rightAttributes.size()+1
                        || !leftAttributes[leftAttributes.size()-1].isEmptyIndicator())
                    && (leftAttributes.size()+1 != rightAttributes.size()
                        || !rightAttributes[rightAttributes.size()-1].isEmptyIndicator()))
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
            size_t nAttrs = min(leftAttributes.size(), rightAttributes.size());
            if (rightAttributes.size() > newAttributes->size()) { 
                newAttributes = &rightAttributes;
            }
            for (size_t i = 0; i < nAttrs; i++)
            {
                if (leftAttributes[i].getType() != rightAttributes[i].getType()
                    || leftAttributes[i].getFlags() != rightAttributes[i].getFlags())
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
            }
        }
        return ArrayDesc(schemas[0].getName(), *newAttributes, leftDimensions);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalMerge, "merge")

} //namespace
