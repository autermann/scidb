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
 *  Created on: Feb 15, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/DelegateArray.h"


using namespace std;
using namespace boost;

namespace scidb {

class SampleArrayIterator : public DelegateArrayIterator
{
  public:
	virtual void operator ++()
    {
        ++(*inputIterator);
        while (!inputIterator->end()) { 
            if (rand_r(&seed) <= threshold) { 
                return;
            }
            ++(*inputIterator);
        }
    }

	virtual void reset() { 
        inputIterator->reset();
        while (!inputIterator->end()) { 
            if (rand_r(&seed) <= threshold) { 
                return;
            }
            ++(*inputIterator);
        }
    }
    
    SampleArrayIterator(DelegateArray const& array, AttributeID attrID, shared_ptr<ConstArrayIterator> inputIterator,
                        double probability, int rndGenSeed)
    : DelegateArrayIterator(array, attrID, inputIterator),
      threshold((int)(RAND_MAX*probability)), seed(rndGenSeed)
    {
        reset();
    }

  private:
    double probability;
    int threshold;
    unsigned int seed;
};

class SampleArray : public DelegateArray
{
  public:
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const 
    {
        return new SampleArrayIterator(*this, id, inputArray->getConstIterator(id), probability, seed);
    }

    SampleArray(ArrayDesc const& desc, boost::shared_ptr<Array> input, double prob, int rndGenSeed) 
    : DelegateArray(desc, input),
      probability(prob), seed(rndGenSeed)
    {
    }
    
  private:
    double probability;
    int seed;
};

class PhysicalSample: public PhysicalOperator
{
public:
	PhysicalSample(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
	    PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

	/***
	 * Sample is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
	 * that overrides the chunkiterator method.
	 */
	boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
		assert(inputArrays.size() == 1);
        int seed = _parameters.size() == 2
            ? (int)((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1])->getExpression()->evaluate().getInt64()
            : (int)time(NULL);
        double probability = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getDouble();
        if (seed < 0)
            throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OP_SAMPLE_ERROR1);
        if (probability <= 0 || probability > 1)
            throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OP_SAMPLE_ERROR2);

        if (inputArrays[0]->getSupportedAccess() == Array::SINGLE_PASS)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_UNSUPPORTED_INPUT_ARRAY) << getLogicalName();
        }

  		return boost::shared_ptr<Array>(new SampleArray(_schema, inputArrays[0], probability, seed));
    }
};
    
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSample, "sample", "physicalSample")

}  // namespace scidb
