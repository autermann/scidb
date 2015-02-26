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
 * @file PhysicalOrrderedTranspose.cpp
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include "query/Operator.h"
#include "PhysicalTranspose.h"
#include "TransposeArray.h"


using namespace std;
using namespace boost;

namespace scidb {

class PhysicalOrderedTranspose: public PhysicalTranspose
{
  public:
    PhysicalOrderedTranspose(std::string const& logicalName,
                      std::string const& physicalName,
                      Parameters const& parameters,
                      ArrayDesc const& schema):
        PhysicalTranspose(logicalName, physicalName, parameters, schema)
    {
    }

	/***
	 * Transpose is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
	 * that overrides the chunkiterator method.
	 */
	boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
		assert(inputArrays.size() == 1);
        if (inputArrays[0]->getSupportedAccess() != Array::RANDOM)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_UNSUPPORTED_INPUT_ARRAY) << getLogicalName();
        }

		return boost::shared_ptr<Array>(new OrderedTransposeArray(_schema, inputArrays[0]));
    }
};
    
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalOrderedTranspose, "transpose", "physicalOrderedTranspose")

}  // namespace scidb
