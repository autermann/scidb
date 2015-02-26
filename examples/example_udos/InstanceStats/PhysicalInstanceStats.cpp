/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2014 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

/**
 * @file PhysicalInstanceStats.cpp
 * The physical implementation of the instance_stats operator.
 * @see LogicalInstanceStats.cpp
 * @author apoliakov@paradigm4.com
 */

#include "InstanceStatsSettings.h"
#include <query/Operator.h>
#include <query/Network.h>

namespace scidb
{

/*
 * A static-linkage logger object we can use to write data to scidb.log.
 * Lookup the log4cxx package for more information.
 */
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.toy_operators.instance_stats"));

class PhysicalInstanceStats : public PhysicalOperator
{
public:
    /**
     * An inner struct used to gather the information we need to output. To facilitate the "global" option, this struct
     * may be marshalled and un-marshalled into a memory buffer.
     */
    struct Stats
    {
        /* Self-explanatory; see LogicalInstanceStats.cpp for more info. */
        size_t chunkCount;
        size_t cellCount;
        size_t nonNullCount;
        double sum;

        Stats():
            chunkCount(0),
            cellCount(0),
            nonNullCount(0),
            sum(0)
        {}

        /**
         * Unmarshall stats from a flat buffer.
         * Note: the scidb::SharedBuffer is a thin wrapper over a block of allocated memory.
         * @param statData the buffer of data; must be exactly getMarshalledSize() bytes.
         */
        Stats(shared_ptr<SharedBuffer>& statData)
        {
            /* assert-like defensive exception */
            if (statData->getSize() != getMarshalledSize())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                      << "Received a statistics data buffer of incorrect size";
            }
            size_t* ptr = static_cast<size_t*> (statData->getData());
            chunkCount = *ptr;
            ++ptr;
            cellCount = *ptr;
            ++ptr;
            nonNullCount = *ptr;
            ++ptr;
            double* dptr = reinterpret_cast<double*> (ptr);
            sum = *dptr;
        }

        /**
         * Marshall stats into a buffer
         * @return a buffer of size getMarshalledSize() containing the data.
         */
        shared_ptr<SharedBuffer> marshall()
        {
            shared_ptr <SharedBuffer> result (new MemoryBuffer(NULL, getMarshalledSize()));
            size_t* ptr = static_cast<size_t*> (result->getData());
            *ptr = chunkCount;
            ++ptr;
            *ptr = cellCount;
            ++ptr;
            *ptr = nonNullCount;
            ++ptr;
            double* dptr = reinterpret_cast<double*> (ptr);
            *dptr = sum;
            return result;
        }

        /**
         * @return the marshalled size of the struct, in bytes.
         */
        static size_t getMarshalledSize()
        {
            return 3*sizeof(size_t) + sizeof(double);
        }

        /**
         * Add data from another Stats object to this.
         * @param other a set of stats collected on a different instance.
         */
        void merge(Stats const& other)
        {
            chunkCount += other.chunkCount;
            cellCount += other.cellCount;
            nonNullCount += other.nonNullCount;
            sum += other.sum;
        }
    };

    PhysicalInstanceStats(string const& logicalName,
                          string const& physicalName,
                          Parameters const& parameters,
                          ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    /**
     * Read data from inputArray, compute and return a set of statistics on it; also, log the data values if
     * settings.dumpToLog() is set. This routine provides the simple example of reading data from an input array.
     * @param inputArray the array to read
     * @param settings the operator settings
     * @return an object that describes the data
     */
    Stats computeLocalStats(shared_ptr<Array>& inputArray, InstanceStatsSettings const& settings)
    {
        /*
         * This flag is passed to the chunk.getConstIterator() method. When reading data from arrays, the option
         * IGNORE_EMPTY_CELLS must always be set; not providing it may lead to indeterminate results. The option
         * IGNORE_OVERLAPS will not iterate over the overlap region of the chunk.
         */
        int iterationMode = settings.includeOverlaps() ? ChunkIterator::IGNORE_EMPTY_CELLS :
                                               ChunkIterator::IGNORE_EMPTY_CELLS | ChunkIterator::IGNORE_OVERLAPS;
        Stats result;

        /* The ConstArrayIterator allows one to read the array data, one attribute at a time.
         * We obtain the iterator for attribute 0.
         */
        shared_ptr<ConstArrayIterator> arrayIter = inputArray->getConstIterator(0);
        while (!arrayIter->end())
        {
            /* The ConstArrayIterator will iterate once for every chunk in the array, in row-major order */
            ++result.chunkCount;

            /* Get the current chunk and construct a ConstChunkIterator over it */
            shared_ptr<ConstChunkIterator> chunkIter = arrayIter->getChunk().getConstIterator(iterationMode);
            while (!chunkIter->end())
            {
                /* the ConstChunkIterator will iterate once for every cell in the chunk, in row-major order */
                ++result.cellCount;

                /* Get the coordinates of the current cell */
                Coordinates const& position = chunkIter->getPosition();

                /* Get the value of the current cell's attribute 0 */
                Value const& item = chunkIter->getItem();

                if (settings.dumpDataToLog())
                {
                    ostringstream logOutput;
                    logOutput<<CoordsToStr(position)<<" -> ";
                    if (item.isNull()) /* covers all the possible SciDB null codes */
                    {
                        logOutput<<"NULL";
                    }
                    else
                    {
                        logOutput<<item.getDouble();
                    }
                    LOG4CXX_DEBUG(logger, logOutput.str());
                }
                if (!item.isNull())
                {
                    ++result.nonNullCount;
                    result.sum += item.getDouble();
                }
                ++(*chunkIter); /* advance chunk iterator */
            }
            ++(*arrayIter); /* advance array iterator */
        }

        /* Note: both ConstArrayIterator and ConstChunkIterator support a setPosition() method for random-access reading
         * See PhysicalIndexLookup for example usage.
         */
        return result;
    }

    /**
     * Record a set of statistics into a MemArray.
     * @param stats the statistics to record
     * @param query the query context
     */
    shared_ptr<Array> writeStatsToMemArray(Stats const& stats, shared_ptr<Query>& query)
    {
        /* This is very similar to the write code seen in PhysicalHelloInstances, except we are writing multiple
         * attributes - all at the same position.
         */
        shared_ptr<Array> outputArray(new MemArray(_schema, query));
        shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(0);
        Coordinates position(1, query->getInstanceID());

        /* The first attribute is opened with only SEQUENTIAL_WRITE. Other attributes are also opened with
         * NO_EMPTY_CHECK. So the empty tag is populated implicitly from the first attribute.
         *
         * Note: since there's only one cell to write, SEQUENTIAL_WRITE is not so relevant, though it is faster.
         */
        shared_ptr<ChunkIterator> outputChunkIter = outputArrayIter->newChunk(position).getIterator(query,
                                                                                       ChunkIterator::SEQUENTIAL_WRITE);
        outputChunkIter->setPosition(position);
        Value value;
        value.setUint32(stats.chunkCount);
        outputChunkIter->writeItem(value);
        outputChunkIter->flush();
        outputArrayIter = outputArray->getIterator(1);
        outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE |
                                                                                 ChunkIterator::NO_EMPTY_CHECK);
        outputChunkIter->setPosition(position);
        value.setUint64(stats.cellCount);
        outputChunkIter->writeItem(value);
        outputChunkIter->flush();
        outputArrayIter = outputArray->getIterator(2);
        outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE |
                                                                                 ChunkIterator::NO_EMPTY_CHECK);
        outputChunkIter->setPosition(position);
        value.setUint64(stats.nonNullCount);
        outputChunkIter->writeItem(value);
        outputChunkIter->flush();
        outputArrayIter = outputArray->getIterator(3);
        outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE |
                                                                                 ChunkIterator::NO_EMPTY_CHECK);
        outputChunkIter->setPosition(position);
        if (stats.nonNullCount > 0)
        {
            value.setDouble(stats.sum / stats.nonNullCount);
        }
        else
        {
            value.setNull(0);
        }
        outputChunkIter->writeItem(value);
        outputChunkIter->flush();
        return outputArray;
    }

    /**
     * Exchange the statistics between instances.
     * @param[in|out] myStats starts with the local information and is populated with the aggregation of the global
     *                        information on instance 0. Not changed on other instances.
     * @param query the query context
     */
    void exchangeStats(Stats& myStats, shared_ptr<Query>& query)
    {
        if (query->getInstanceID() != 0)
        {
            /* I am not instance 0, so send my stuff to instance 0 */
            shared_ptr<SharedBuffer> buf = myStats.marshall();
            /* Non-blocking send. Must be matched by a BufReceive call on the recipient */
            BufSend(0, buf, query);
        }
        else
        {
            /*I am instance 0, receive stuff rom all other instances */
            for (InstanceID i = 1; i<query->getInstancesCount(); ++i)
            {
                /* Blocking receive. */
                shared_ptr<SharedBuffer> buf = BufReceive(i, query);
                Stats otherInstanceStats(buf);
                /* add data to myStats */
                myStats.merge(otherInstanceStats);
            }
        }

        /* Note: at the moment instance 0 IS synonymous with "coordinator". In the future we may move to a more
         * advanced multiple-coordinator scheme.
         */
    }

    shared_ptr<Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        InstanceStatsSettings settings(_parameters, false, query);
        Stats stats = computeLocalStats(inputArrays[0], settings);
        if (settings.global())
        {
            /* Exchange data between instances */
            exchangeStats(stats, query);
            if (query->getInstanceID() == 0)
            {
                return writeStatsToMemArray(stats, query);
            }
            else
            {
                /* Just return an empty array if I am not instance 0*/
                return shared_ptr<Array>(new MemArray(_schema, query));
            }
        }
        else
        {
            /* Just return local stats*/
            return writeStatsToMemArray(stats, query);
        }


    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalInstanceStats, "instance_stats", "PhysicalInstanceStats");

} //namespace scidb
