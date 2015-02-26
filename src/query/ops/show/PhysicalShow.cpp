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

/**
 * @file
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief Shows object. E.g. schema of array.
 */

#include "query/Operator.h"
#include "query/OperatorLibrary.h"
#include "array/MemArray.h"
#include "system/SystemCatalog.h"

using namespace std;
using namespace boost;

namespace scidb
{

class PhysicalShow: public PhysicalOperator
{
public:
    PhysicalShow(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
        _result = boost::shared_ptr<MemArray>(new MemArray(_schema));
    }

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution>& inputDistributions,
                                                 const std::vector< ArrayDesc>& inputSchemas) const
    {
        return ArrayDistribution(psLocalInstance);
    }

    void preSingleExecute(boost::shared_ptr<Query> query)
    {
        assert(_parameters.size() == 1);

        stringstream ss;

        ArrayDesc desc;
        // we want to "show" only the persistent contents (i.e. the catalog contents)
        printSchema(ss, ((const shared_ptr<OperatorParamSchema>&)_parameters[0])->getSchema());

        boost::shared_ptr<ArrayIterator> arrIt = _result->getIterator(0);
        Coordinates coords;
        coords.push_back(0);
        Chunk& chunk = arrIt->newChunk(coords);
        boost::shared_ptr<ChunkIterator> chunkIt = chunk.getIterator(query);
        Value v(TypeLibrary::getType(TID_STRING));
        v.setString(ss.str().c_str());
        chunkIt->writeItem(v);
        chunkIt->flush();
    }

    boost::shared_ptr<Array> execute(
        std::vector<boost::shared_ptr<Array> >& inputArrays,
        boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);
        return _result;
    }

private:
    boost::shared_ptr<Array> _result;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalShow, "show", "impl_show")

} //namespace
