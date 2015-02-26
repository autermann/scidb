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
 * LogicalCrossJoin.cpp
 *
 *  Created on: Mar 09, 2011
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "system/SystemCatalog.h"
#include "system/Exceptions.h"
#include "array/Metadata.h"

using namespace std;

namespace scidb
{

class LogicalCrossJoin: public LogicalOperator
{
  public:
    LogicalCrossJoin(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_INPUT()
    	ADD_PARAM_INPUT()
		ADD_PARAM_VARIES()           
    }

	std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
	{
		std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        res.push_back(PARAM_IN_DIMENSION_NAME());
        return res;
	}

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert((_parameters.size() & 1) == 0);
        assert(schemas.size() == 2);

        ArrayDesc const& leftArrayDesc = schemas[0];
        ArrayDesc const& rightArrayDesc = schemas[1];
        Attributes const& leftAttributes = leftArrayDesc.getAttributes();
        Dimensions leftDimensions = leftArrayDesc.getDimensions();
        Attributes const& rightAttributes = rightArrayDesc.getAttributes();
        Dimensions const& rightDimensions = rightArrayDesc.getDimensions();
        size_t totalAttributes = leftAttributes.size() + rightAttributes.size();
        AttributeDesc const* leftBitmap = leftArrayDesc.getEmptyBitmapAttribute();
        AttributeDesc const* rightBitmap = rightArrayDesc.getEmptyBitmapAttribute();
        if (leftBitmap && rightBitmap) { 
            totalAttributes -= 1;
        }
        Attributes CrossJoinAttributes(totalAttributes);

        size_t j = 0;
        for (size_t i = 0, n = leftAttributes.size(); i < n; i++) {
            AttributeDesc const& attr = leftAttributes[i];
            if (!attr.isEmptyIndicator()) {
                CrossJoinAttributes[j] = AttributeDesc(j, attr.getName(), attr.getType(), attr.getFlags(),
                    attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                    attr.getDefaultValueExpr());
                CrossJoinAttributes[j].addAlias(leftArrayDesc.getName());
                j += 1;
            }
        }
        for (size_t i = 0, n = rightAttributes.size(); i < n; i++, j++) {
            AttributeDesc const& attr = rightAttributes[i];
            CrossJoinAttributes[j] = AttributeDesc(j, attr.getName(), attr.getType(), attr.getFlags(),
                attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                attr.getDefaultValueExpr());
            CrossJoinAttributes[j].addAlias(rightArrayDesc.getName());
        }
        if (leftBitmap && !rightBitmap) { 
            AttributeDesc const& attr = *leftBitmap;
            CrossJoinAttributes[j] = AttributeDesc(j, attr.getName(), attr.getType(), attr.getFlags(),
                attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                attr.getDefaultValueExpr());
            CrossJoinAttributes[j].addAlias(leftArrayDesc.getName());
        }
        size_t nRightDims = rightDimensions.size();
        size_t nLeftDims = leftDimensions.size();
        Dimensions CrossJoinDimensions(nLeftDims + nRightDims - _parameters.size()/2);
        vector<int> CrossJoinOnDimensions(nRightDims, -1);
        uint64_t leftCrossJoinOnMask = 0;
        uint64_t rightCrossJoinOnMask = 0;
        for (size_t p = 0, np = _parameters.size(); p < np; p += 2) { 
            shared_ptr<OperatorParamDimensionReference> leftDim = (shared_ptr<OperatorParamDimensionReference>&)_parameters[p];
            shared_ptr<OperatorParamDimensionReference> rightDim = (shared_ptr<OperatorParamDimensionReference>&)_parameters[p+1];
            
            const string &leftDimName = leftDim->getObjectName();
            const string &rightDimName = rightDim->getObjectName();
            const string &leftDimArray = leftDim->getArrayName();
            const string &rightDimArray = rightDim->getArrayName();
            size_t l = nLeftDims;
            do {
                if (l == 0)
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_NOT_EXIST,
                                               leftDim->getParsingContext()) << leftDimName;
            } while (!leftDimensions[--l].hasNameOrAlias(leftDimName, leftDimArray));
            
            if ((leftCrossJoinOnMask & ((uint64_t)1 << l)) != 0)
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CROSSJOIN_ERROR1,
                                           leftDim->getParsingContext());

            leftCrossJoinOnMask |=  (uint64_t)1 << l;
            
            size_t r = nRightDims;
            do {
                if (r == 0)
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_NOT_EXIST,
                                               rightDim->getParsingContext()) << rightDimName;
            } while (!rightDimensions[--r].hasNameOrAlias(rightDimName, rightDimArray));
            
            if ((rightCrossJoinOnMask & ((uint64_t)1 << r)) != 0)
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CROSSJOIN_ERROR1,
                                           rightDim->getParsingContext());
            rightCrossJoinOnMask |=  (uint64_t)1 << r;
            
            if (leftDimensions[l].getType() != TID_INT64 || rightDimensions[r].getType() != TID_INT64
                    || leftDimensions[l].getLength() != rightDimensions[r].getLength()
                    || leftDimensions[l].getStart() != rightDimensions[r].getStart()
                    || leftDimensions[l].getChunkInterval() != rightDimensions[r].getChunkInterval()
                    || leftDimensions[l].getChunkOverlap() != rightDimensions[r].getChunkOverlap())
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
            
            if (CrossJoinOnDimensions[r] >= 0)
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CROSSJOIN_ERROR1);
            CrossJoinOnDimensions[r] = (int)l;
        }                                           
        j = 0;
        for (size_t i = 0; i < nLeftDims; i++) { 
            CrossJoinDimensions[j] = leftDimensions[i];
            CrossJoinDimensions[j].addAlias(leftArrayDesc.getName());
            ++j;
        }
        for (size_t i = 0; i < nRightDims; i++) { 
            if (CrossJoinOnDimensions[i] < 0) {
                CrossJoinDimensions[j] = rightDimensions[i];
                CrossJoinDimensions[j].addAlias(rightArrayDesc.getName());
                ++j;
            }
        }
        return ArrayDesc(leftArrayDesc.getName() + rightArrayDesc.getName(), CrossJoinAttributes, CrossJoinDimensions);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalCrossJoin, "cross_join")


} //namespace
