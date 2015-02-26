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
 * PhysicalSort.cpp
 *
 *  Created on: May 6, 2010
 *      Author: Knizhnik
 *      Author: poliocough@gmail.com
 */

#include <vector>

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/TupleArray.h"
#include "array/FileArray.h"
#include "network/NetworkManager.h"
#include "system/Config.h"
#include "query/ops/sort2/MergeSortArray.h"
#include "array/ParallelAccumulatorArray.h"

using namespace std;
using namespace boost;

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.sort"));

class PhysicalSort: public PhysicalOperator
{
  public:
	PhysicalSort(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
	    PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

	/**
	 * A SortJob is one Job that sorts part of an array.
	 * SortJob produces a vector of TupleArrays (each one of which fits in memory).
	 * Note that the input array of a sort job may or may not have an empty tag, but the sort job produces a result with an empty tag.
	 */
    class SortJob : public Job, protected SelfStatistics
    {
      private:
        size_t shift;
        size_t step;
        size_t memLimit;
        size_t tupleSize;
        ArrayDesc const& outputDesc;
        boost::shared_ptr<Array> input;
        shared_ptr < vector< shared_ptr<Array> > > result;
        vector< boost::shared_ptr<ConstArrayIterator> > arrayIterators;
        vector<Key>& keys;

      public:
        /**
         * The input array may not have an empty tag,
         * but the output array has an empty tag.
         */
        SortJob(shared_ptr<Query> query, size_t id, size_t nJobs, boost::shared_ptr<Array> array,
                shared_ptr< vector <shared_ptr <Array> > >& result, vector<Key>& sortKeys,
                size_t memLimit, size_t tupleSize, ArrayDesc const& outputDesc)
        : Job(query), shift(id), step(nJobs), memLimit(memLimit), tupleSize(tupleSize), outputDesc(outputDesc), input(array),
          result(result), arrayIterators(array->getArrayDesc().getAttributes().size()), keys(sortKeys)
        {
            assert(outputDesc.getEmptyBitmapAttribute());

            for (size_t i = 0; i < array->getArrayDesc().getAttributes().size(); i++) {
                arrayIterators[i] = array->getConstIterator(i);
            }
        }

        /**
         * TupleArray must handle the case that outputDesc.getAttributes().size() is 1 larger than arrayIterators.size(),
         * i.e. the case when the input array does not have an empty tag (but the output array does).
         */
        virtual void run()
        {
            boost::shared_ptr<TupleArray> buffer = boost::shared_ptr<TupleArray>(new TupleArray(outputDesc, arrayIterators, 0));
            for (size_t j = shift; j != 0 && !arrayIterators[0]->end(); --j)
            {
                for (size_t i = 0; i < arrayIterators.size(); i++) {
                    ++(*arrayIterators[i]);
                }
            }
            while (!arrayIterators[0]->end())
            {
                buffer->append(arrayIterators, 1);
                size_t currentSize = buffer->getNumberOfTuples() * tupleSize;
                if(currentSize > memLimit)
                {
                    buffer->sort(keys);
                    buffer->truncate();
                    result->push_back(shared_ptr<Array> (new FileArray(buffer)));
                    buffer.reset(new TupleArray(outputDesc, arrayIterators, 0));
                }
                for (size_t j = step-1; j != 0 && !arrayIterators[0]->end(); --j) {
                    for (size_t i = 0; i < arrayIterators.size(); i++) {
                        ++(*arrayIterators[i]);
                    }
                }
            }

            if (buffer->getNumberOfTuples())
            {
                buffer->sort(keys);
                buffer->truncate();
                result->push_back(shared_ptr<Array> (new FileArray(buffer)));
            }
        }
    };

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        uint64_t numCells = inputBoundaries[0].getNumCells();
        if (numCells == 0)
        {
            return PhysicalBoundaries::createEmpty(1);
        }

        Coordinates start(1);
        start[0] = _schema.getDimensions()[0].getStartMin();
        Coordinates end(1);
        end[0] = _schema.getDimensions()[0].getStartMin() + numCells -1 ;
        return PhysicalBoundaries(start,end);
    }

    /***
     * Sort is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
     * that overrides the chunkiterator method.
     */
    boost::shared_ptr< Array> execute(vector< boost::shared_ptr< Array> >& inputArrays,
                                             boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1); 
        vector<Key> keys;
        Attributes const& attrs = _schema.getAttributes();
        for (size_t i = 0, n=_parameters.size(); i < n; i++)
        {
            if(_parameters[i]->getParamType() != PARAM_ATTRIBUTE_REF)
            {
                continue;
            }
            shared_ptr<OperatorParamAttributeReference> sortColumn = ((boost::shared_ptr<OperatorParamAttributeReference>&)_parameters[i]);
            Key k;
            k.columnNo = sortColumn->getObjectNo();
            k.ascent = sortColumn->getSortAscent();
            if ((size_t)k.columnNo >= attrs.size())
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_SORT_ERROR2);
            keys.push_back(k);
        }

        if(keys.size()==0)
        {
            //No attribute is specified... so let's sort by first attribute ascending
            Key k;
            k.columnNo =0;
            k.ascent=true;
            keys.push_back(k);
        }

        if ( query->getInstancesCount() > 1) { 
            // Prepare context for second phase
            SortContext* ctx = new SortContext();
            ctx->keys = keys;
            query->userDefinedContext = ctx;
        }

        size_t tupleSize = TupleArray::getTupleFootprint(_schema.getAttributes());
        bool parallelSort = Config::getInstance()->getOption<bool>(CONFIG_PARALLEL_SORT) && inputArrays[0]->getSupportedAccess() == Array::RANDOM;
        size_t numJobs = parallelSort ?  Config::getInstance()->getOption<int>(CONFIG_PREFETCHED_CHUNKS) : 1;
        size_t memLimit = Config::getInstance()->getOption<int>(CONFIG_MERGE_SORT_BUFFER)*MB;
        //We do NOT divide mem limit by the number of threads -- that's the behavior of the rest of the system!
        time_t start = time(NULL);
        vector< shared_ptr<Array> > tempArrays;
        boost::shared_ptr<JobQueue> queue = PhysicalOperator::getGlobalQueueForOperators();
        vector< shared_ptr<SortJob> > jobs(numJobs);

        LOG4CXX_DEBUG(logger, "[Sort:] Creating " << numJobs << " sort jobs...");

        if (numJobs == 1)
        {
            shared_ptr < vector < shared_ptr< Array > > > result( new vector <shared_ptr < Array > > ());
            boost::shared_ptr<SortJob> job(new SortJob(query, 0, numJobs, inputArrays[0], result, keys,
                                                       memLimit, tupleSize, _schema));
            job->run();
            tempArrays.insert(tempArrays.end(), result->begin(), result->end());
            result.reset();
        }
        else
        {
            vector< shared_ptr< vector < shared_ptr< Array > > > > results (numJobs);       
            for (size_t i = 0; i < numJobs; i++)
            {
                results[i].reset( new vector < shared_ptr < Array > > () );
                queue->pushJob(jobs[i] = boost::shared_ptr<SortJob>(new SortJob(query, i, numJobs, inputArrays[0],
                                                                                results[i], keys, memLimit,
                                                                                tupleSize, _schema)));
            }
            for (size_t i = 0; i < numJobs; i++)
            {
                jobs[i]->wait();
                tempArrays.insert(tempArrays.end(), results[i]->begin(), results[i]->end());
                results[i].reset();
            }
        }

        LOG4CXX_DEBUG(logger, "Time for concurrent partial sort: " << (time(NULL) - start) << " seconds with "<<tempArrays.size()<<" parts");

        //There used to be a piece of code that returned tempArrays[0]
        //if there is only one such array. It was removed because tempArrays
        //contains TupleArrays whose length is truncated to the actual number
        //of non-empty elements. Thus, returning such a TupleArray would create
        //an incorrect result on single-node single-chunk sort queries - it
        //would not properly print the last several parentheses of the empty cells.

        //The rationale is that if there's only one tempArray, then there is only one chunk of data,
        //and appending one chunk of data to a MemArray is an overhead we can live with.

        //true means the array contains local data only (this is the first phase)
        shared_ptr<Array> sortArray(new MergeSortArray(query, _schema, tempArrays, keys, true));

        //false means perform a horizontal copy (copy all attributes for chunk 1, all attributes for chunk 2,...)
        shared_ptr<Array> ret(new MemArray(sortArray,false));

        return ret;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSort, "sort", "physicalSort")

}  // namespace scidb
