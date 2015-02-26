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
 * LogicalAdddim.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "system/SystemCatalog.h"
#include "system/Exceptions.h"

using namespace std;

namespace scidb {

    

class LogicalAdddim: public LogicalOperator
{
public:
    LogicalAdddim(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_INPUT()
    	ADD_PARAM_OUT_DIMENSION_NAME()
    }

    ArrayDesc inferSchema(vector<ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        assert(_parameters.size() == 1);
        assert(((boost::shared_ptr<OperatorParam>&)_parameters[0])->getParamType() == PARAM_DIMENSION_REF);

        const string &dimensionName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        ArrayDesc const& srcArrayDesc = schemas[0];
        Dimensions const& srcDimensions = srcArrayDesc.getDimensions();
        Dimensions dstDimensions(srcDimensions.size()+1);

        for (size_t i = 0, n = srcDimensions.size(); i < n; i++) {
            dstDimensions[i+1] = srcDimensions[i];
        }
        dstDimensions[0] = DimensionDesc(dimensionName, 0, 0, 0, 0, 1, 0);
        return ArrayDesc(srcArrayDesc.getName(), srcArrayDesc.getAttributes(), dstDimensions);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalAdddim, "adddim")


} //namespace
