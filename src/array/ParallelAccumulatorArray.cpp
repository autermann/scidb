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
 * @file ParallelAccumulatorArray.cpp
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include <stdio.h>
#include <vector>
#include <log4cxx/logger.h>
#include "array/ParallelAccumulatorArray.h"
#include "system/Exceptions.h"
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"
#include "query/Operator.h"

namespace scidb
{
    using namespace boost;
    using namespace std;

    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.processor"));

    //
    // ParallelAccumulatorArray
    //
    ConstChunk const* ParallelAccumulatorArray::ChunkPrefetchJob::getResult()
    {
        wait(true, false);
        if (!resultChunk) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        }
        return resultChunk;
    }

    void ParallelAccumulatorArray::ChunkPrefetchJob::cleanup()
    {
        iterator.reset();
    }

    ParallelAccumulatorArray::ChunkPrefetchJob::ChunkPrefetchJob(const boost::shared_ptr<ParallelAccumulatorArray>& array,
                                                                 AttributeID attr,
                                                                 const boost::shared_ptr<Query>& query)
        : Job(query), _arrayLink(array), attrId(attr), resultChunk(NULL), iterator(array->pipe->getConstIterator(attrId)) {}


    void ParallelAccumulatorArray::ChunkPrefetchJob::run()
    {
        static int pass = 0; // DEBUG ONLY
        pass++;
        StatisticsScope sScope(_statistics);
        boost::shared_ptr<ParallelAccumulatorArray> arrayPtr = _arrayLink.lock();
        if (!arrayPtr) {
            return;
        }
        ParallelAccumulatorArray& acc = *arrayPtr;
        try {
            if (iterator->setPosition(pos)) {
                ConstChunk const& inputChunk = iterator->getChunk();
                if (inputChunk.isMaterialized()) {
                    resultChunk = &inputChunk;
                } else {
                    Address addr(attrId, inputChunk.getFirstPosition(false));
                    accChunk.initialize(&acc, &acc.desc, addr, inputChunk.getCompressionMethod());
                    accChunk.setBitmapChunk((Chunk*)&inputChunk);
                    boost::shared_ptr<ConstChunkIterator> src = inputChunk.getConstIterator(ChunkIterator::INTENDED_TILE_MODE|ChunkIterator::IGNORE_EMPTY_CELLS);
                    boost::shared_ptr<ChunkIterator> dst = accChunk.getIterator(_query, (src->getMode() & ChunkIterator::TILE_MODE)|ChunkIterator::NO_EMPTY_CHECK|(inputChunk.isSparse()?ChunkIterator::SPARSE_CHUNK:0)|ChunkIterator::SEQUENTIAL_WRITE);
                    bool vectorMode = src->supportsVectorMode() && dst->supportsVectorMode();
                    src->setVectorMode(vectorMode);
                    dst->setVectorMode(vectorMode);
                    size_t count = 0;
                    while (!src->end()) {
                        if (dst->setPosition(src->getPosition())) {
                            Value& v = src->getItem();
                            dst->writeItem(v);
                            count += 1;
                        }
                        ++(*src);
                    }
                    if (!vectorMode && !acc.desc.containsOverlaps()) {
                        accChunk.setCount(count);
                    }
                    dst->flush();
                    resultChunk = &accChunk;
                }
            } else {
                _error = SYSTEM_EXCEPTION_SPTR(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            }
        } catch (Exception const& x) {
            _error = x.copy();
        }
    }


ParallelAccumulatorArray::ParallelAccumulatorArray(boost::shared_ptr<Array> array)
    : StreamArray(array->getArrayDesc(), false),
      pipe(array),
      iterators(array->getArrayDesc().getAttributes().size()),
      activeJobs(iterators.size()),
      completedJobs(iterators.size())
    {
        if (iterators.size() <= 0) {
            LOG4CXX_FATAL(logger, "Array descriptor arrId = " << array->getArrayDesc().getId()
                          << " name = " << array->getArrayDesc().getId()
                          << " has no attributes ");
            assert(0);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INCONSISTENT_ARRAY_DESC);
        }
    }

void ParallelAccumulatorArray::start(const boost::shared_ptr<Query>& query)
 {
    PhysicalOperator::getGlobalQueueForOperators();
    size_t nAttrs = iterators.size();
    assert(nAttrs>0);
    for (size_t i = 0; i < nAttrs; i++) {
       iterators[i] = pipe->getConstIterator(i);
    }
    int nPrefetchedChunks = Config::getInstance()->getOption<int>(CONFIG_PREFETCHED_CHUNKS);
    do {
       for (size_t i = 0; i < nAttrs; i++) {
          doNewJob(boost::shared_ptr<ChunkPrefetchJob>(new ChunkPrefetchJob(shared_from_this(), i, query)));
       }
    } while ((nPrefetchedChunks -= nAttrs) > 0);
 }

    ParallelAccumulatorArray::~ParallelAccumulatorArray()
    {
        for (size_t i = 0; i < activeJobs.size(); i++) {
            list< shared_ptr<ChunkPrefetchJob> >& jobs = activeJobs[i];
            for (list< shared_ptr<ChunkPrefetchJob> >::iterator j = jobs.begin(); j != jobs.end(); ++j) {
                (*j)->skip();
            }
        }
    }

    void ParallelAccumulatorArray::doNewJob(boost::shared_ptr<ChunkPrefetchJob> job)
    {
        AttributeID attrId = job->getAttributeID();
        if (!iterators[attrId]->end()) {
            job->setPosition(iterators[attrId]->getPosition());
            PhysicalOperator::getGlobalQueueForOperators()->pushJob(job);
            activeJobs[attrId].push_back(job);
            ++(*iterators[attrId]);
        }
    }


    ConstChunk const* ParallelAccumulatorArray::nextChunk(AttributeID attId, MemChunk& chunk)
    {
        if (completedJobs[attId]) {
            doNewJob(completedJobs[attId]);
            completedJobs[attId].reset();
        }
        if (activeJobs[attId].empty()) {
            return NULL;
        }
        completedJobs[attId] = activeJobs[attId].front();
        activeJobs[attId].pop_front();
        return completedJobs[attId]->getResult();
    }

}
