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
 * @file PhysicalSetopt.cpp
 *
 * @author knizhnik@garret.ru
 *
 * Physical implementation of SETOPT operator for setopting data from text files
 */

#include <string.h>

#include "query/Operator.h"
#include "query/OperatorLibrary.h"
#include "array/TupleArray.h"
#include "system/Config.h"

using namespace std;
using namespace boost;

namespace scidb
{
    
class PhysicalSetopt: public PhysicalOperator
{
  public:
    PhysicalSetopt(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        std::string name = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getString();
        vector< boost::shared_ptr<Tuple> > tuples(1);        
        if (_parameters.size() == 2) { 
            std::string newValue = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1])->getExpression()->evaluate().getString();
            std::string oldValue = Config::getInstance()->setOptionValue(name, newValue);
            Tuple& tuple = *new Tuple(2);
            tuple[0].setString(oldValue.c_str());
            tuple[1].setString(newValue.c_str());
            tuples[0] = boost::shared_ptr<Tuple>(&tuple);
        } else { 
            std::string oldValue = Config::getInstance()->getOptionValue(name);
            Tuple& tuple = *new Tuple(1);
            tuple[0].setString(oldValue.c_str());
            tuples[0] = boost::shared_ptr<Tuple>(&tuple);
        }
        return boost::shared_ptr<Array>(new TupleArray(_schema, tuples, Coordinate(query->getInstanceID())));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSetopt, "setopt", "physicalSetopt")

} //namespace
