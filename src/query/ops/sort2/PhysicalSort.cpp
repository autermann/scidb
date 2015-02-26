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
 * PhysicalSort2.cpp
 *
 *  Created on: Aug 15, 2010
 *      Author: knizhnik@garret.ru
 */
#include "query/Operator.h"
#include "query/QueryProcessor.h"
#include "array/Metadata.h"
#include "MergeSortArray.h"

using namespace boost;

namespace scidb
{

class PhysicalSort2 : public  PhysicalOperator
{
public:
    PhysicalSort2(std::string const& logicalName,
                  std::string const& physicalName,
                  Parameters const& parameters,
                  ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual bool outputFullChunks(std::vector< ArrayDesc> const&) const
    {
        return false;
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const&,
            std::vector< ArrayDesc> const&) const
    {
        return ArrayDistribution(psUndefined);
    }

    virtual PhysicalBoundaries getOutputBoundaries(
            std::vector<PhysicalBoundaries> const& inputBoundaries,
            std::vector< ArrayDesc> const& inputSchemas) const
    {
        return inputBoundaries[0];
    }

    boost::shared_ptr<Array> execute(std::vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        if (inputArrays.size() > 1) {
            SortContext* ctx = (SortContext*)query->userDefinedContext;
            boost::shared_ptr<Array> result = boost::shared_ptr<Array>(new MergeSortArray(query, _schema, inputArrays, ctx->keys));
            delete ctx;
            return result;
        }
        return inputArrays[0];
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSort2, "sort2", "physicalSort2")

} //namespace scidb
