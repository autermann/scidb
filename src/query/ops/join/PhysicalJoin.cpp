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
 * PhysicalApply.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "JoinArray.h"
#include "MapJoinArray.h"


using namespace std;
using namespace boost;

namespace scidb {

class JoinArrayIterator : public DelegateArrayIterator
{
  public:
        virtual bool end()
    {
        return !hasCurrent;
    }

    virtual void operator ++()
    {
        assert(hasCurrent);
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_POSITION);
        ++(*inputIterator);
        while (!inputIterator->end()) {
            if (joinIterator->setPosition(inputIterator->getPosition())) {
                return;
            }
            ++(*inputIterator);
        }
        hasCurrent = false;
    }

    virtual bool setPosition(Coordinates const& pos)
    {
        return hasCurrent = inputIterator->setPosition(pos) && joinIterator->setPosition(pos);
    }

        virtual void reset() {
        inputIterator->reset();
        while (!inputIterator->end()) {
            if (joinIterator->setPosition(inputIterator->getPosition())) {
                hasCurrent = true;
                return;
            }
            ++(*inputIterator);
        }
        hasCurrent = false;
    }

    JoinArrayIterator(DelegateArray const& array, AttributeID attrID, shared_ptr<ConstArrayIterator> inputIterator,
                      shared_ptr<ConstArrayIterator> pairIterator)
    : DelegateArrayIterator(array, attrID, inputIterator),
      joinIterator(pairIterator)
    {
        reset();
    }

  private:
    shared_ptr<ConstArrayIterator> joinIterator;
    bool hasCurrent;
};

class JoinArray : public DelegateArray
{
public:
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const
    {
        return new JoinArrayIterator(*this, id,
                                     id < nLeftAttributes ? leftArray->getConstIterator(id) : rightArray->getConstIterator(id - nLeftAttributes),
                                     id < nLeftAttributes ? rightArray->getConstIterator(0) : leftArray->getConstIterator(0));
    }

    JoinArray(ArrayDesc const& desc, boost::shared_ptr<Array> left, boost::shared_ptr<Array> right)
    : DelegateArray(desc, left),
      leftArray(left),
      rightArray(right),
      nLeftAttributes(left->getArrayDesc().getAttributes().size())
    {
    }

  private:
    boost::shared_ptr<Array> leftArray;
    boost::shared_ptr<Array> rightArray;
    size_t nLeftAttributes;
};

class PhysicalJoin: public PhysicalOperator
{
public:
    PhysicalJoin(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    virtual DistributionRequirement getDistributionRequirement (const std::vector< ArrayDesc> & inputSchemas) const
    {
        return DistributionRequirement(DistributionRequirement::Collocated);
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        if (inputBoundaries[0].isEmpty() || inputBoundaries[1].isEmpty())
        {
            return PhysicalBoundaries::createEmpty(_schema.getDimensions().size());
        }
        return inputBoundaries[0].intersectWith(inputBoundaries[1]);
    }

    /***
     * Join is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
     * that overrides the chunkiterator method.
     */
    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 2);

        if (inputArrays[0]->getSupportedAccess() != Array::RANDOM ||
            inputArrays[1]->getSupportedAccess() != Array::RANDOM)
        {
            //We use setPosition to align data, so input arrays must support it
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_UNSUPPORTED_INPUT_ARRAY) << getLogicalName();
        }

        boost::shared_ptr<Array> left = inputArrays[0];
        boost::shared_ptr<Array> right = inputArrays[1];
        Dimensions const& rightDimensions = right->getArrayDesc().getDimensions();
        size_t nRightDims = rightDimensions.size();
        for (size_t i = 0; i < nRightDims; i++) {
            if (rightDimensions[i].getType() != TID_INT64) {
                return boost::shared_ptr<Array>(new MapJoinArray(_schema, left, redistribute(right, query, psReplication), query));
            }
        }
        return boost::shared_ptr<Array>(_schema.getEmptyBitmapAttribute() == NULL
                                        ? (Array*)new JoinArray(_schema, left, right)
                                        : (Array*)new JoinEmptyableArray(_schema, left, right));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalJoin, "join", "physicalJoin")

}  // namespace scidb
