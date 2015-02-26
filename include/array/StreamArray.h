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
 * @file StreamArray.h
 *
 * @brief Array receiving chunks from abstract stream
 */

#ifndef STREAM_ARRAY_H_
#define STREAM_ARRAY_H_

#include <vector>

#include "array/MemArray.h"
#include "util/JobQueue.h"
#include "util/Semaphore.h"
#include "util/ThreadPool.h"

namespace scidb
{
    using namespace std;
    using namespace boost;

    class StreamArrayIterator;

    /**
     * Abstract stream array iterator
     */
    class StreamArray: public Array
    {
        friend class StreamArrayIterator;
      public:
        virtual string const& getName() const;
        virtual ArrayID getHandle() const;

        virtual ArrayDesc const& getArrayDesc() const;

        /**
         * Get the least restrictive access mode that the array supports.
         * @return SINGLE_PASS
         */
        virtual Access getSupportedAccess() const
        {
            return SINGLE_PASS;
        }

        virtual boost::shared_ptr<ArrayIterator> getIterator(AttributeID attId);
        virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attId) const;

        StreamArray(ArrayDesc const& arr, bool emptyCheck = true);
        StreamArray(const StreamArray& other);

      protected:
        virtual ConstChunk const* nextChunk(AttributeID attId, MemChunk& chunk) = 0;

        ArrayDesc desc;
        bool emptyCheck;
        vector< boost::shared_ptr<ConstArrayIterator> > iterators;
        ConstChunk const* currentBitmapChunk;
        size_t nPrefetchedChunks;
    };

    /**
     * Stream array iterator
     */
    class StreamArrayIterator : public ConstArrayIterator 
    {
        StreamArray& array;
        AttributeID attId;
        ConstChunk const* currentChunk;
        MemChunk dataChunk;
        MemChunk bitmapChunk;
        bool moved;

        void moveNext();

      public:
        StreamArrayIterator(StreamArray& arr, AttributeID attId);
        ConstChunk const& getChunk();
        bool end();
        void operator ++();
        Coordinates const& getPosition();
    };

    //
    // Array implementation materializing current chunk
    //
    class AccumulatorArray : public StreamArray
    {
      public:
        AccumulatorArray(boost::shared_ptr<Array> pipe);

      protected:
        virtual ConstChunk const* nextChunk(AttributeID attId, MemChunk& chunk);

      private:
        boost::shared_ptr<Array> pipe;
        vector< boost::shared_ptr<ConstArrayIterator> > iterators;        
    };

    //
    // Merging different streams
    //
    class MultiStreamArray : public StreamArray
    {
        size_t nStreams;
        vector< vector<Coordinates> > chunkPos;
        MemChunk mergeChunk;
        MemChunk joinBitmapChunk;
      public:
        MultiStreamArray(size_t nStreams, ArrayDesc const& arr, bool emptyCheck = false);
        MultiStreamArray(const MultiStreamArray& other);

      protected:
        virtual ConstChunk const* nextChunk(AttributeID attId, MemChunk& chunk);

        virtual ConstChunk const* nextChunkBody(size_t i, AttributeID attId, MemChunk& chunk, Coordinates const& pos) = 0;
        virtual bool nextChunkPos(size_t i, AttributeID attId, Coordinates& pos) = 0;

        size_t getStreamsCount() {
            return nStreams;
        }
    };

    class MergeStreamArray : public StreamArray
    {
      protected:
        vector< boost::shared_ptr<Array> > inputArrays;
        vector< vector< boost::shared_ptr<ConstArrayIterator> > > inputIterators;

        virtual ConstChunk const* nextChunk(AttributeID attId, MemChunk& chunk);
        virtual void merge(boost::shared_ptr<ChunkIterator> dst, boost::shared_ptr<ConstChunkIterator> src);

      public:
        MergeStreamArray(ArrayDesc const& desc, vector< boost::shared_ptr<Array> > inputArrays);
    };
}

#endif
