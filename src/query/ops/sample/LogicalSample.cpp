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
 * LogicalSample.cpp
 *
 *  Created on: Feb 14, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "system/SystemCatalog.h"
#include "system/Exceptions.h"

using namespace std;

namespace scidb
{

/**
 * @brief The operator: sample().
 *
 * @par Synopsis:
 *   sample( srcArray, probability [, seed] )
 *
 * @par Summary:
 *   Produces a result array containing randomly sampled chunks from srcArray.
 *
 * @par Input:
 *   - srcArray: the source array with srcAttrs and srcDims.
 *   - probability: a double value from 0 to 1, as the probability that a chunk is selected.
 *   - seed: an int64 value as the seed to the random number generator.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   srcDims
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalSample: public LogicalOperator
{
public:
    LogicalSample(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_INPUT()
        ADD_PARAM_CONSTANT("double")
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
        return schemas[0];
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSample, "sample")


} //namespace
