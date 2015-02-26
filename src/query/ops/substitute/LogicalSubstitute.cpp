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
 * LogicalSubstitute.cpp
 *
 *  Created on: Mar 10, 2011
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "system/SystemCatalog.h"
#include "system/Exceptions.h"
#include "array/Metadata.h"

using namespace std;

namespace scidb
{

class LogicalSubstitute: public LogicalOperator
{
  public:
    LogicalSubstitute(const string& logicalName, const std::string& alias):
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
        res.push_back(PARAM_IN_ATTRIBUTE_NAME("void"));
        return res;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 2);
        ArrayDesc const& inputDesc = schemas[0];
        ArrayDesc const& substDesc = schemas[1];

        Dimensions const& substDims = substDesc.getDimensions();
        Attributes const& inputAttrs = inputDesc.getAttributes();
        Attributes const& substAttrs = substDesc.getAttributes(true);

        if (substDims.size() != 1)
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_SUBSTITUTE_ERROR1) << substDesc.getName();
        }
        if (substAttrs.size() != 1)
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_SUBSTITUTE_ERROR2) << substDesc.getName();
        }
        if (!substDims[0].isInteger())
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_SUBSTITUTE_ERROR3) <<
                (substDesc.getName() + "." + substDims[0].getBaseName());

        }
        if (substDims[0].getStart() != 0)
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_SUBSTITUTE_ERROR4) <<
                (substDesc.getName() + "." + substDims[0].getBaseName());
        }

        //if no parameters are given, we assume we are substituting all nullable attributes
        vector<bool> substituteAttrs (inputAttrs.size(), _parameters.size() == 0 ? true : false);
        for (size_t i = 0, n = _parameters.size(); i < n; i++)
        {
            size_t attId = ((boost::shared_ptr<OperatorParamReference>&)_parameters[i])->getObjectNo();
            substituteAttrs[attId] = true;
        }

        Attributes newAttributes(inputAttrs.size());
        for (size_t i = 0, n = inputAttrs.size(); i < n; i++)
        {
            if ( substituteAttrs[i] )
            {
                AttributeDesc const& inputAttr = inputAttrs[i];
                if (inputAttr.isNullable() && inputAttr.getType() != substAttrs[0].getType())
                {
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_SUBSTITUTE_ERROR5)
                        << inputAttr.getName() << substAttrs[0].getName();
                }
                newAttributes[i] = AttributeDesc(i,
                                                 inputAttr.getName(),
                                                 inputAttr.getType(),
                                                 inputAttr.getFlags() & ~AttributeDesc::IS_NULLABLE,
                                                 inputAttr.getDefaultCompressionMethod(),
                                                 inputAttr.getAliases(),
                                                 inputAttr.getDefaultValue().isNull() ? NULL : &inputAttr.getDefaultValue());
            }
            else
            {
                newAttributes[i] = inputAttrs[i];
            }
        }
        return ArrayDesc(inputDesc.getName() + "_subst", newAttributes, inputDesc.getDimensions());
    }


};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSubstitute, "substitute")


} //namespace
