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
#include "array/ParallelAccumulatorArray.h"
#include "system/Exceptions.h"
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"

namespace scidb
{
    using namespace boost;
    using namespace std;

    //
    // ParallelAccumulatorArray
    //
    ConstChunk const* ParallelAccumulatorArray::ChunkPrefetchJob::getResult()
    {
        wait(true, false);
        if (!resultChunk)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        return resultChunk;
    }

    void ParallelAccumulatorArray::ChunkPrefetchJob::cleanup()
    {
        iterator.reset();
    }

    ParallelAccumulatorArray::ChunkPrefetchJob::ChunkPrefetchJob(ParallelAccumulatorArray& array, AttributeID attr, const boost::shared_ptr<Query>& query)
        : Job(query), acc(array), attrId(attr), resultChunk(NULL), iterator(acc.pipe->getConstIterator(attrId)) {}


    void ParallelAccumulatorArray::ChunkPrefetchJob::run()
    {
        static int pass = 0; // DEBUG ONLY
        pass++;
        StatisticsScope sScope(_statistics);
        try {
            if (iterator->setPosition(pos)) {
                ConstChunk const& inputChunk = iterator->getChunk();
                if (inputChunk.isMaterialized()) {
                    resultChunk = &inputChunk;
                } else {
                    Address addr(acc.desc.getId(), attrId, inputChunk.getFirstPosition(false));
                    accChunk.initialize(&acc, &acc.desc, addr, inputChunk.getCompressionMethod());
                    accChunk.setBitmapChunk((Chunk*)&inputChunk);
                    boost::shared_ptr<ConstChunkIterator> src = inputChunk.getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS);
                    boost::shared_ptr<ChunkIterator> dst = accChunk.getIterator(_query, (src->getMode() & ChunkIterator::TILE_MODE)|ChunkIterator::NO_EMPTY_CHECK|(inputChunk.isSparse()?ChunkIterator::SPARSE_CHUNK:0));
                    bool vectorMode = src->supportsVectorMode();
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

    boost::shared_ptr<ThreadPool> ParallelAccumulatorArray::threadPool;
    boost::shared_ptr<JobQueue> ParallelAccumulatorArray::queue;
    Mutex ParallelAccumulatorArray::initCriticalSection;


    boost::shared_ptr<JobQueue> ParallelAccumulatorArray::getQueue()
    {
        ScopedMutexLock cs(initCriticalSection);
        if (!threadPool) {
            queue = boost::shared_ptr<JobQueue>(new JobQueue());
            threadPool = boost::shared_ptr<ThreadPool>(new ThreadPool(Config::getInstance()->getOption<int>(CONFIG_EXEC_THREADS), queue));
            threadPool->start();
        }
        return queue;
    }

ParallelAccumulatorArray::ParallelAccumulatorArray(boost::shared_ptr<Array> array)
    : StreamArray(array->getArrayDesc(), false),
      pipe(array),
      iterators(array->getArrayDesc().getAttributes().size()),
      activeJobs(iterators.size()),
      completedJobs(iterators.size())
    { }

void ParallelAccumulatorArray::start(const boost::shared_ptr<Query>& query)
 {
    getQueue();
    int nAttrs = (int)iterators.size();
    for (int i = 0; i < nAttrs; i++) {
       iterators[i] = pipe->getConstIterator(i);
    }
    int nPrefetchedChunks = Config::getInstance()->getOption<int>(CONFIG_PREFETCHED_CHUNKS);
    do {
       for (int i = 0; i < nAttrs; i++) {
          doNewJob(boost::shared_ptr<ChunkPrefetchJob>(new ChunkPrefetchJob(*this, i, query)));
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
        for (size_t i = 0; i < activeJobs.size(); i++) {
            list< shared_ptr<ChunkPrefetchJob> >& jobs = activeJobs[i];
            for (list< shared_ptr<ChunkPrefetchJob> >::iterator j = jobs.begin(); j != jobs.end(); ++j) {
                (*j)->wait(false, false);
                (*j)->cleanup();
            }
        }
        for (size_t i = 0; i < completedJobs.size(); i++) {
            if (completedJobs[i]) { 
                completedJobs[i]->cleanup();
            }
        }
    }

    void ParallelAccumulatorArray::doNewJob(boost::shared_ptr<ChunkPrefetchJob> job)
    {
        AttributeID attrId = job->getAttributeID();
        if (!iterators[attrId]->end()) {
            job->setPosition(iterators[attrId]->getPosition());
            queue->pushJob(job);
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
        if (activeJobs[attId].size() == 0) {
            return NULL;
        }
        completedJobs[attId] = activeJobs[attId].front();
        activeJobs[attId].pop_front();
        return completedJobs[attId]->getResult();
    }

}
