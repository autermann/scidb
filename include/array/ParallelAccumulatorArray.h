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
 * @file ParallelAccumulatorArray.h
 *
 */

#ifndef PARALLEL_ACCUMULATOR_ARRAY_H_
#define PARALLEL_ACCUMULATOR_ARRAY_H_

#include <vector>
#include <boost/enable_shared_from_this.hpp>

#include "array/MemArray.h"
#include "util/JobQueue.h"
#include "util/Semaphore.h"
#include "util/ThreadPool.h"
#include "array/StreamArray.h"


namespace scidb
{
    using namespace std;
    using namespace boost;

    //
    // Array implementation materializing current chunk
    //
    class ParallelAccumulatorArray : public StreamArray, public boost::enable_shared_from_this<ParallelAccumulatorArray>
    {
      public:
        ParallelAccumulatorArray(boost::shared_ptr<Array> pipe);
        ~ParallelAccumulatorArray();
        void start(const boost::shared_ptr<Query>& query);

      protected:
        virtual ConstChunk const* nextChunk(AttributeID attId, MemChunk& chunk);

      private:
        class ChunkPrefetchJob : public Job,
#ifndef NO_SUPPPORT_FOR_SWIG_TARGETS_THAT_CANT_HANDLE_PROTECTED_BASE_CLASSES
          public SelfStatistics
#else
          protected SelfStatistics
#endif // NO_SUPPPORT_FOR_SWIG_TARGETS_THAT_CANT_ACCEPT_PROTECTED_BASE_CLASSES
        {
          private:
            boost::weak_ptr<ParallelAccumulatorArray> _arrayLink;
            Coordinates pos;
            AttributeID attrId;
            MemChunk accChunk;
            ConstChunk const* resultChunk;
            boost::shared_ptr<ConstArrayIterator> iterator;

          public:
            ChunkPrefetchJob(const boost::shared_ptr<ParallelAccumulatorArray>& array,
                             AttributeID attr, const boost::shared_ptr<Query>& query);

            void setPosition(Coordinates const& coord) {
                resultChunk = NULL;
                pos = coord;
            }

            AttributeID getAttributeID() const {
                return attrId;
            }

            ConstChunk const* getResult();

            virtual void run();

            void cleanup();
        };

        void doNewJob(boost::shared_ptr<ChunkPrefetchJob> job);

        boost::shared_ptr<Array> pipe;
        vector< boost::shared_ptr<ConstArrayIterator> > iterators;
        vector< list< boost::shared_ptr<ChunkPrefetchJob> > > activeJobs;
        vector< boost::shared_ptr<ChunkPrefetchJob> > completedJobs;
   };
}

#endif
