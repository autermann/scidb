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
 * LogicalAggregate.cpp
 *
 *  Created on: Jul 7, 2011
 *      Author: poliocough@gmail.com
 */

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "query/LogicalExpression.h"
#include "query/TypeSystem.h"
#include "query/Aggregate.h"

#include <log4cxx/logger.h>

namespace scidb {

using namespace std;
using namespace boost;

class LogicalAggregate: public  LogicalOperator
{
public:
	LogicalAggregate(const std::string& logicalName, const std::string& alias):
	    LogicalOperator(logicalName, alias)
	{
        _properties.tile = true;
		ADD_PARAM_INPUT()
		ADD_PARAM_VARIES()
	}

	std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
	{
		std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
		res.push_back(END_OF_VARIES_PARAMS());
		if (_parameters.size() == 0)
		{
			res.push_back(PARAM_AGGREGATE_CALL());
		}
		else
		{
			boost::shared_ptr<OperatorParam> lastParam = _parameters[_parameters.size() - 1];
			if (lastParam->getParamType() == PARAM_AGGREGATE_CALL)
			{
				res.push_back(PARAM_AGGREGATE_CALL());
				res.push_back(PARAM_IN_DIMENSION_NAME());
			}
			else if (lastParam->getParamType() == PARAM_DIMENSION_REF)
			{
				res.push_back(PARAM_IN_DIMENSION_NAME());
			}
		}

		return res;
	}

	void addDimension(Dimensions const& inputDims,
	                  Dimensions& outDims,
	                  shared_ptr<OperatorParam> const& param)
	{
	    boost::shared_ptr<OperatorParamReference> const& reference =
	                            (boost::shared_ptr<OperatorParamReference> const&) param;

        string const& dimName = reference->getObjectName();
        string const& dimAlias = reference->getArrayName();

        for (size_t j = 0; j < inputDims.size(); j++)
        {
            if (inputDims[j].hasNameOrAlias(dimName, dimAlias))
            {
                //no overlap
                outDims.push_back(DimensionDesc( inputDims[j].getBaseName(),
                                                 inputDims[j].getStartMin(),
                                                 inputDims[j].getEndMax(),
                                                 inputDims[j].getChunkInterval(),
                                                 0,
                                                 inputDims[j].getType(),
                                                 inputDims[j].getSourceArrayName()));
                return;
            }
        }
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_DIMENSION_NOT_EXIST) << dimName;
	}

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
		assert(schemas.size() == 1);
		ArrayDesc const& input = schemas[0];

		Dimensions const& inputDims = input.getDimensions();

		Attributes outAttrs;
		Dimensions outDims;

		for (size_t i =0; i<_parameters.size(); i++)
		{
		    if (_parameters[i]->getParamType() == PARAM_DIMENSION_REF)
		    {
		        addDimension(inputDims, outDims, _parameters[i]);
		    }
		}

		if (outDims.size() == 0)
        {
            outDims.push_back(DimensionDesc("i", 0, 0, 0, 0, 1, 0));
        } 
        else 
        {
            _properties.tile = false;
        }

		ArrayDesc outSchema(input.getName(), Attributes(), outDims);
		for (size_t i =0; i<_parameters.size(); i++)
		{
		    if (_parameters[i]->getParamType() == PARAM_AGGREGATE_CALL)
		    {
		        addAggregatedAttribute( (shared_ptr <OperatorParamAggregateCall> &) _parameters[i], input, outSchema);
		    }
		}
		return outSchema;
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalAggregate, "aggregate")

}  // namespace scidb
