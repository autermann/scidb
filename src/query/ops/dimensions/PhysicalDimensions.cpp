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
 * @file PhysicalExample.cpp
 *
 * @author knizhnik@garret.ru
 *
 * Physical implementation of DIMENSIONS operator for dimensionsing data from text files
 */

#include <string.h>

#include "query/Operator.h"
#include "array/TupleArray.h"
#include "system/SystemCatalog.h"

using namespace std;
using namespace boost;

namespace scidb
{

class PhysicalDimensions: public PhysicalOperator
{
public:
    PhysicalDimensions(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual bool isDistributionPreserving(const std::vector<ArrayDesc> & inputSchemas) const
    {
        return false;
    }

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                 const std::vector< ArrayDesc> & inputSchemas) const
    {
        return ArrayDistribution(psLocalInstance);
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);
        assert(_parameters.size() == 1);

        string arrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        SystemCatalog& catalog = * SystemCatalog::getInstance();
        ArrayDesc arrayDesc;
        catalog.getArrayDesc(arrayName, LAST_VERSION, arrayDesc);
        Coordinates lowBoundary = catalog.getLowBoundary(arrayDesc.getId());
        Coordinates highBoundary = catalog.getHighBoundary(arrayDesc.getId());
        Dimensions const& dims = arrayDesc.getDimensions();
        
        vector< boost::shared_ptr<Tuple> > tuples(dims.size());
        for (size_t i = 0; i < dims.size(); i++) { 
            Tuple& tuple = *new Tuple(8);
            tuples[i] = boost::shared_ptr<Tuple>(&tuple);
            tuple[0].setData(dims[i].getBaseName().c_str(), dims[i].getBaseName().length() + 1);
            tuple[1] = Value(TypeLibrary::getType(TID_INT64));
            tuple[1].setInt64(dims[i].getStart());
            tuple[2] = Value(TypeLibrary::getType(TID_UINT64));
            tuple[2].setUint64(dims[i].getLength());
            tuple[3] = Value(TypeLibrary::getType(TID_INT32));
            tuple[3].setInt32(dims[i].getChunkInterval());
            tuple[4] = Value(TypeLibrary::getType(TID_INT32));
            tuple[4].setInt32(dims[i].getChunkOverlap());
            tuple[5] = Value(TypeLibrary::getType(TID_INT64));
            tuple[5].setInt64(lowBoundary[i]);
            tuple[6] = Value(TypeLibrary::getType(TID_INT64));
            tuple[6].setInt64(highBoundary[i]);
            tuple[7].setData(dims[i].getType().c_str(), dims[i].getType().length() + 1);

        } 
        return boost::shared_ptr<Array>(new TupleArray(_schema, tuples));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalDimensions, "dimensions", "physicalDimensions")

} //namespace
