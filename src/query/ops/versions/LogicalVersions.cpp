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
 * @file LogicalVersions.cpp
 *
 * @author knizhnik@garret.ru
 *
 * Get list of updatable array versions
 */

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "array/Metadata.h"
#include "system/SystemCatalog.h"

namespace scidb
{

using namespace std;
using namespace boost;

class LogicalVersions: public LogicalOperator
{
public:
    LogicalVersions(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_IN_ARRAY_NAME()
        _properties.ddl = true;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> inputSchemas, boost::shared_ptr<Query> query)
    {
        assert(inputSchemas.size() == 0);
        assert(_parameters.size() == 1);

        const string &arrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        ArrayDesc arrayDesc;
        SystemCatalog::getInstance()->getArrayDesc(arrayName, arrayDesc);

        size_t nVersions = SystemCatalog::getInstance()->getArrayVersions(arrayDesc.getId()).size();
        Attributes attributes(2);
        attributes[0] = AttributeDesc((AttributeID)0, "version_id", TID_INT64, 0, 0);
        attributes[1] = AttributeDesc((AttributeID)1, "timestamp", TID_DATETIME, 0, 0);
        vector<DimensionDesc> dimensions(1);
        dimensions[0] = DimensionDesc("VersionNo", 1, 1, nVersions, nVersions, nVersions, 0);
        return ArrayDesc("Versions", attributes, dimensions);
    }

};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalVersions, "versions")


} //namespace
