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
 * Physical implementation of ATTRIBUTES operator for attributesing data from text files
 */

#include <string.h>

#include "query/Operator.h"
#include "array/TupleArray.h"
#include "system/SystemCatalog.h"

using namespace std;
using namespace boost;

namespace scidb
{

class PhysicalAttributes: public PhysicalOperator
{
public:
    PhysicalAttributes(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                 const std::vector< ArrayDesc> & inputSchemas) const
    {
        return ArrayDistribution(psLocalNode);
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);
        assert(_parameters.size() == 1);

        const string &arrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        ArrayDesc arrayDesc;
        SystemCatalog::getInstance()->getArrayDesc(arrayName, arrayDesc);
        Attributes const& attrs = arrayDesc.getAttributes();
        
        vector< boost::shared_ptr<Tuple> > tuples(attrs.size());
        for (size_t i = 0; i < attrs.size(); i++) { 
            Tuple& tuple = *new Tuple(3);
            tuples[i] = boost::shared_ptr<Tuple>(&tuple);
            tuple[0].setString(attrs[i].getName().c_str());
            tuple[1] = Value(TypeLibrary::getType(TID_STRING));
            tuple[1].setString(attrs[i].getType().c_str());
            tuple[2] = Value(TypeLibrary::getType(TID_BOOL));
            tuple[2].setBool(attrs[i].isNullable());
        } 
        return boost::shared_ptr<Array>(new TupleArray(_schema, tuples));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalAttributes, "attributes", "physicalAttributes")

} //namespace
