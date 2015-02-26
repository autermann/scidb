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

    class SortJob : public Job, protected SelfStatistics 
    {
      private:
        size_t shift;
        size_t step;
        boost::shared_ptr<Array> input;
        boost::shared_ptr<Array> result;
        vector< boost::shared_ptr<ConstArrayIterator> > arrayIterators;
        vector<Key>& keys;

      public:
        boost::shared_ptr<Array> getResult() { 
            return result;
        }

        SortJob(shared_ptr<Query> query, size_t id, size_t nJobs, boost::shared_ptr<Array> array, 
                size_t nAttrs, vector<Key>& sortKeys)
        : Job(query), shift(id), step(nJobs), input(array), arrayIterators(nAttrs), keys(sortKeys)
        {
            for (size_t i = 0; i < nAttrs; i++) { 
                arrayIterators[i] = array->getConstIterator(i);
            }
        }

        virtual void run() 
        { 
            boost::shared_ptr<TupleArray> memArray = boost::shared_ptr<TupleArray>(new TupleArray(input->getArrayDesc(), arrayIterators, shift, step));
            memArray->sort(keys);
            memArray->truncate();
            result = boost::shared_ptr<Array>(new FileArray(memArray));
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
        vector<Key> keys(_parameters.size());
        Attributes const& attrs = _schema.getAttributes();
        for (size_t i = 0; i < keys.size(); i++) {
            shared_ptr<OperatorParamAttributeReference> sortColumn = ((boost::shared_ptr<OperatorParamAttributeReference>&)_parameters[i]);
            keys[i].columnNo = sortColumn->getObjectNo();
            keys[i].ascent = sortColumn->getSortAscent();
            if ((size_t)keys[i].columnNo >= attrs.size())
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_SORT_ERROR2);
        }
        if ( query->getNodesCount() > 1) { 
            // Prepare context for second phase
            SortContext* ctx = new SortContext();
            ctx->keys = keys;
            query->userDefinedContext = ctx;
        }

        uint64_t tupleArraySize = _schema.getSize();
        if (tupleArraySize != INFINITE_LENGTH) { 
            size_t nAttrs = attrs.size();
            tupleArraySize = tupleArraySize*(sizeof(Tuple) + sizeof(Value)*nAttrs) + _schema.getUsedSpace(); 
            size_t bufSize = Config::getInstance()->getOption<int>(CONFIG_MERGE_SORT_BUFFER)*MB;
            bool parallelSort = Config::getInstance()->getOption<bool>(CONFIG_PARALLEL_SORT);
            if (parallelSort) { 
                bufSize /= Config::getInstance()->getOption<int>(CONFIG_EXEC_THREADS);
            }
            if (bufSize != 0 && bufSize < tupleArraySize) { 
                size_t nParts = (tupleArraySize + bufSize - 1) / bufSize;

                time_t start = time(NULL);
                vector< shared_ptr<Array> > tempArrays(nParts);
                
                if (parallelSort) { 
                    boost::shared_ptr<JobQueue> queue = ParallelAccumulatorArray::getQueue();
                    vector< shared_ptr<SortJob> > jobs(nParts);
                    for (size_t i = 0; i < nParts; i++) {
                        queue->pushJob(jobs[i] = boost::shared_ptr<SortJob>(new SortJob(query, i, nParts, inputArrays[0], nAttrs, keys)));
                    }
                    
                    for (size_t i = 0; i < nParts; i++) {
                        jobs[i]->wait();
                        tempArrays[i] = jobs[i]->getResult();
                    }
                } else { 
                    for (size_t i = 0; i < nParts; i++) {
                        SortJob job(query, i, nParts, inputArrays[0], nAttrs, keys);
                        job.run();
                        tempArrays[i] = job.getResult();
                    }
                }
                LOG4CXX_DEBUG(logger, "Time for concurrent partial sort: " << (time(NULL) - start) << " seconds");
                return boost::shared_ptr<Array>(new FileArray(boost::shared_ptr<Array>(new MergeSortArray(query, _schema, tempArrays, keys, true)), false));
            }
        }
        time_t start = time(NULL);
        TupleArray* array = new TupleArray(_schema, inputArrays[0]);
        array->sort(keys);
        LOG4CXX_DEBUG(logger, "Time for sorting of local array: " << (time(NULL) - start) << " seconds");
	    return boost::shared_ptr<Array>(array);
	 }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSort, "sort", "physicalSort")

}  // namespace scidb
