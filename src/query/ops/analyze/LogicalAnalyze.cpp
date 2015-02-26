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
 * LogicalAnalyze.cpp
 *
 *  Created on: Feb 1, 2012
 *      Author: egor.pugin@gmail.com
 */

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "PhysicalAnalyze.h"

using namespace std;
using namespace boost;

namespace scidb
{

class LogicalAnalyze : public LogicalOperator
{
public:
	LogicalAnalyze(const std::string& logicalName, const std::string& alias)
        : LogicalOperator(logicalName, alias)
	{
		ADD_PARAM_INPUT()
		ADD_PARAM_VARIES()
	}

	vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const vector<ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        res.push_back(PARAM_IN_ATTRIBUTE_NAME("void"));

        return res;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
    {
        Attributes atts;
        atts.push_back(AttributeDesc(0, "attribute_name", TID_STRING, 0, 0));
        atts.push_back(AttributeDesc(1, "min", TID_STRING, 0, 0));
        atts.push_back(AttributeDesc(2, "max", TID_STRING, 0, 0));
        atts.push_back(AttributeDesc(3, "distinct_count", TID_UINT64, 0, 0));
        atts.push_back(AttributeDesc(4, "non_null_count", TID_UINT64, 0, 0));

        const AttributeDesc *emptyIndicator = schemas[0].getEmptyBitmapAttribute();

        set<string> a_s;
        for (size_t i = 0; i < _parameters.size(); i++)
        {
            string attName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[i])->getObjectName();
                        
            if (emptyIndicator && emptyIndicator->getName() == attName)
                continue;

            a_s.insert(attName);
        }

        size_t attsCount = (a_s.size() == 0 ? (emptyIndicator ? schemas[0].getAttributes().size() - 1 : schemas[0].getAttributes().size()) : a_s.size()) - 1;

        Dimensions dims;
        dims.push_back(DimensionDesc("attribute_number", 0, attsCount, ANALYZE_CHUNK_SIZE, 0));

        return ArrayDesc(schemas[0].getName() + "_analyze", atts, dims);
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalAnalyze, "analyze")

} //namespace scidb
