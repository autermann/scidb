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
 * PhysicalBetween.cpp
 *
 *  Created on: May 20, 2010
 *      Author: knizhnik@garret.ru
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "BetweenArray.h"


namespace scidb {

class PhysicalBetween: public  PhysicalOperator
{
public:
        PhysicalBetween(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
             PhysicalOperator(logicalName, physicalName, parameters, schema)
        {
        }

    Coordinates getWindowStart(const boost::shared_ptr<Query>& query) const
    {
        Dimensions const& dims = _schema.getDimensions();
        size_t nDims = dims.size();
        Coordinates result(nDims);
        for (size_t i = 0; i < nDims; i++)
        {
            Value const& coord = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i])->getExpression()->evaluate();
            if (coord.isNull()) {
                result[i] = dims[i].getLowBoundary();
            } else {
               result[i] = _schema.getOrdinalCoordinate(i, coord, cmLowerBound, query);
                if (dims[i].getStart() != MIN_COORDINATE && result[i] < dims[i].getStart())
                {
                    result[i] = dims[i].getStart();
                }
            }
        }
        return result;
    }

    Coordinates getWindowEnd(const boost::shared_ptr<Query>& query) const
    {
        Dimensions const& dims = _schema.getDimensions();
        size_t nDims = dims.size();
        Coordinates result(nDims);
        for (size_t i = 0; i < nDims; i++)
        {
            Value const& coord = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i + nDims])->getExpression()->evaluate();
            if (coord.isNull()) {
                result[i] = dims[i].getHighBoundary();
            } else {
               result[i] = _schema.getOrdinalCoordinate(i, coord, cmUpperBound, query);
                if (result[i] > dims[i].getEndMax())
                {
                    result[i] = dims[i].getEndMax();
                }
            }
        }
        return result;
    }

   virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                  const std::vector< ArrayDesc> & inputSchemas) const
    {
       boost::shared_ptr<Query> query(_query.lock());
       PhysicalBoundaries window(getWindowStart(query), getWindowEnd(query));
       return inputBoundaries[0].intersectWith(window);
    }

   /***
    * Between is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
    * that overrides the chunkiterator method.
    */
   boost::shared_ptr< Array> execute(std::vector< boost::shared_ptr< Array> >& inputArrays,
                                     boost::shared_ptr<Query> query)
   {
      assert(inputArrays.size() == 1);

      if (inputArrays[0]->getSupportedAccess() != Array::RANDOM)
      {
          throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_UNSUPPORTED_INPUT_ARRAY) << getLogicalName();
      }

      Coordinates lowPos = getWindowStart(query);
      Coordinates highPos = getWindowEnd(query);

      return boost::shared_ptr< Array>(new BetweenArray(_schema, lowPos, highPos, inputArrays[0], _tileMode));
   }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalBetween, "between", "physicalBetween")

}  // namespace scidb
