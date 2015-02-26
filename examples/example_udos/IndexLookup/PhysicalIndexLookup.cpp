/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2013 SciDB, Inc.
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

/*
 * @file PhysicalIndexLookup.cpp
 * The implementation of the index_lookup operator.
 *
 * @par Algorithm:
 * <br>
 * <br>
 * It is assumed that the index_array (second argument) is small enough to fit entirely on disk on any of the instances.
 * Our first step is to call redistribute() to make a copy of the index on every instance.
 *
 * We then create an rb-tree (map) that contains some of the values from the index_array. Each node in the map contains
 * a value from the index and its corresponding coordinate. The tree always contains the first and the last value of
 * each chunk. In addition, we insert a random sampling of values from the index into the tree, not to exceed
 * MEMORY_LIMIT bytes of memory used. The tree is ordered based on the "<" comparison operator for the particular
 * datatype.
 *
 * Having built the tree, we create a virtual array that is computed as it is iterated over. Every time the client
 * requests for data from the output attribute, we first obtain the corresponding value from the input attribute. We
 * try to find the matching value in the tree. If not successful, we find the position of the next largest value and
 * the next smallest value in the tree. We use those coordinates to select a chunk in the index array. We then use
 * binary search over the chunk to find the value.
 *
 * @author apoliakov@paradigm4.com
 */

#include "IndexLookupSettings.h"
#include <query/Operator.h>
#include <query/Network.h>
#include <array/DelegateArray.h>

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.toy_operators.index_lookup"));

class PhysicalIndexLookup : public PhysicalOperator
{
private:
    /**
     * The rb-tree used as a cache; contains some of the data from the index array.
     * The AttributeComparator is provided by SciDB as a convenience way to execute "<" over two values of the same
     * type.
     */
    typedef map<Value, Coordinate, AttributeComparator> PartialValueMap;

public:
    PhysicalIndexLookup(string const& logicalName,
                        string const& physicalName,
                        Parameters const& parameters,
                        ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    /**
     * An object that contains a pointer to the PartialValueMap and a pointer to the index array, and can be used
     * to look up the coordinate of a particular value.
     */
    class ValueIndex
    {
    private:
        shared_ptr<Array> _indexArray;
        AttributeComparator _lessThan;
        //Important: the map stays constant throughout the process and the ValueIndex may not mutate it.
        shared_ptr<PartialValueMap const> _partialMap;
        shared_ptr<ConstArrayIterator> _indexArrayIter;
        shared_ptr<ConstChunkIterator> _indexChunkIter; //we keep one chunk open at any particular time to save RAM
        Coordinates _currentChunkPosition; //the position of the currently opened chunk

        //move our iterators to a new chunk position; close current chunk if any
        void repositionIterators(Coordinates const& desiredChunkPos)
        {
            _indexChunkIter.reset();
            _indexArrayIter->setPosition(desiredChunkPos);
            _indexChunkIter = _indexArrayIter->getChunk().getConstIterator();
            _currentChunkPosition = desiredChunkPos;
        }

        /**
         * Find the position of input in _indexArray, searching between the coordinates start and end. The
         * coordinates start and end must be in the same chunk.
         * @param input the value to look for
         * @param start the starting coordinate in the array at which to look for
         * @param end the ending coordinate in the array
         * @param[out] result set to the position of input if it is found
         * @return true if input was found, false otherwise
         */
        bool findPositionInArray(Value const&input, Coordinate start, Coordinate end, Coordinate& result)
        {
            //convert start to the chunk position and reposition the iterator if necessary
            Coordinates chunkPos(1,start);
            _indexArray->getArrayDesc().getChunkPositionFor(chunkPos);
            if (_currentChunkPosition.size() == 0 || _currentChunkPosition[0] != chunkPos[0])
            {
                repositionIterators(chunkPos);
            }
            while (start < end) //binary search
            {
                Coordinates midPoint (1, (start+end) / 2);
                _indexChunkIter->setPosition(midPoint);
                Value& item = _indexChunkIter->getItem();
                if (item == input)
                {
                    result = midPoint[0];
                    return true;
                }
                else if(_lessThan(input, item)) //input < item
                {
                    end = midPoint[0];
                }
                else //input > item
                {
                    start = midPoint[0] + 1;
                }
            }
            return false;
        }

    public:
        ValueIndex(shared_ptr<Array> const& indexArray, shared_ptr<PartialValueMap const> const& partialMap):
            _indexArray(indexArray),
            _lessThan(indexArray->getArrayDesc().getAttributes()[0].getType()),
            _partialMap(partialMap),
            _indexArrayIter(indexArray->getConstIterator(0))
        {}

        /**
         * Find the position of input in the index, first looking at the map, then at the array chunks.
         * @param input the value to look for
         * @param[out] result set to the position of input if found
         * @return true if the value was found, false otherwise
         */
        bool findPosition(Value const& input, Coordinate& result)
        {
            PartialValueMap::const_iterator iter = _partialMap->lower_bound(input);
            if (iter == _partialMap->end())
            {
                return false;
            }
            Value const& lbVal = iter->first;
            Coordinate lbPos = iter->second;
            if (input == lbVal)
            {
                result = lbPos;
                return true;
            }
            //lbVal must be greater than input
            if (iter == _partialMap->begin())
            {
                return false;
            }
            --iter;
            Coordinate prevPos = iter->second;
            //go look in the array
            return findPositionInArray(input, prevPos, lbPos, result);
        }
    };

    /**
     * A special ChunkIterator used to lookup the coordinates of values.
     * The DelegateArray class, with its friends DelegateArrayIterator and DelegateChunkIterator provide facilities
     * for returning a slightly modified version of the input array in a streaming on-demand fashion. The returned data
     * is not materialized until it is requested by the client of the array.
     */
    class IndexLookupChunkIterator : public DelegateChunkIterator
    {
    private:
        /**
         * The index object. It is important to note that multiple threads may create multiple iterators to the same
         * array, which is why this cannot be a pointer to a shared object. All indeces however do contain a pointer to
         * the same shared map and are very cerful not to alter it.
         */
        ValueIndex _index;

        /**
         * A placeholder for the returned value.
         */
        Value _buffer;

    public:
        IndexLookupChunkIterator(DelegateChunk const* chunk,
                                 int iterationMode,
                                 shared_ptr<Array> const& indexArray,
                                 shared_ptr<PartialValueMap const> const& partialMap):
            DelegateChunkIterator(chunk, iterationMode),
            _index(indexArray, partialMap)
        {}

        virtual Value& getItem()
        {
            //The inputIterator is constructed by the DelegateChunkIterator and happens to be an iterator to the
            //corresponding chunk of the input attribute.
            Value const& input = inputIterator->getItem();
            Coordinate output;
            //Perform the index lookup
            if (!input.isNull() && _index.findPosition(input, output))
            {
                _buffer.setInt64(output);
            }
            else
            {
                _buffer.setNull();
            }
            return _buffer;
        }

        //Note: all of the other ConstChunkIterator methods - getPosition, setPosition, end, ... do not need to be
        //overwritten for this case
    };

    /**
     * The virtual array that simply returns the underlying iterators to all the data, unless the client asks for
     * the new index attribute, in which case the IndexLookupChunkIterator is returned.
     */
    class IndexLookupArray : public DelegateArray
    {
    private:
        /**
         * The id of the looked-up attribute.
         */
        AttributeID const _sourceAttributeId;

        /**
         * The id of the output attribute that contains the looked-up coordinates.
         */
        AttributeID const _dstAttributeId;

        /**
         * A pointer to the index array.
         */
        shared_ptr<Array>const _indexArray;

        /**
         * A pointer to the partial map.
         */
        shared_ptr<PartialValueMap const> const _partialMap;

    public:
        IndexLookupArray(ArrayDesc const& desc,
                         shared_ptr<Array>& input,
                         AttributeID const sourceAttribute,
                         shared_ptr<Array> indexArray,
                         shared_ptr<PartialValueMap const> partialMap):
            DelegateArray(desc, input, true),
            _sourceAttributeId(sourceAttribute),
            _dstAttributeId(desc.getAttributes(true).size() -1),
            _indexArray(indexArray),
            _partialMap(partialMap)
        {}

        virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const
        {
            if (id == _dstAttributeId)
            {
                //pass "false" to the "clone" field indicating that this chunk is NOT a copy of the underlying chunk
                return new DelegateChunk(*this, *iterator, id, false);
            }
            return DelegateArray::createChunk(iterator, id);
        }

        virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const
        {
            if (id == _dstAttributeId)
            {
                //pass an iterator to the source attribute so the chunk iterator can have access to the looked up data
                return new DelegateArrayIterator(*this, id, inputArray->getConstIterator(_sourceAttributeId));
            }
            else if (id == _dstAttributeId+1)
            {
                //client must be asking for the empty tag, whose id is now shifted up by one
                return new DelegateArrayIterator(*this, id, inputArray->getConstIterator(id - 1));
            }
            return DelegateArray::createArrayIterator(id);
        }

        virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
        {
            if (chunk->getAttributeDesc().getId() == _dstAttributeId)
            {
                return new IndexLookupChunkIterator(chunk, iterationMode, _indexArray, _partialMap);
            }
            return DelegateArray::createChunkIterator(chunk, iterationMode);
        }
    };

    /**
     * A guide to tell us how many values may be placed into the map.
     */
    struct MapLimits
    {
        /**
         * The probability that any value is inserted (the fraction of the total number of values), between 0 and 1.
         */
        double insertionProbability;

        /**
         * The upper limit on the number of inserted values, in addition to the required two values for each chunk.
         */
        size_t numOptionalValues;

        MapLimits():
            insertionProbability(0),
            numOptionalValues(0)
        {}
    };

    /**
     * Compute a MapLimits object based on the memory limit and some information about index data.
     */
    MapLimits computeMapLimits(shared_ptr<Array>& indexArray, double memLimit)
    {
        MapLimits result;
        size_t declaredValueSize = indexArray->getArrayDesc().getAttributes()[0].getSize(); //0 means variable size
        bool isIntegralType = (declaredValueSize > 0 && declaredValueSize<=8);
        ssize_t cellCount = 0, chunkCount = 0, totalSize = 0;
        for(shared_ptr<ConstArrayIterator> indexArrayIter = indexArray->getConstIterator(0);
            !indexArrayIter->end();
            ++(*indexArrayIter))
        {
            //just iterate over the chunks
            ++chunkCount;
            ConstChunk const& chunk = indexArrayIter->getChunk();
            //we know the array has been distributed, therefore it is very likely a MemArray, in which case the methods
            //count and getSize run in constant time.
            cellCount += chunk.count();
            totalSize += chunk.getSize();
        }
        double averageValueSize = totalSize * 1.0 / cellCount;
        double averageMapMemberSize;
        if (isIntegralType) //if it is fixed-size and under 8 bytes, it is stored inside the Value class
        {
            averageMapMemberSize = sizeof(Coordinate) + sizeof(Value) + sizeof(_Rb_tree_node_base);
        }
        else //otherwise Value points to it
        {
            //averageValueSize includes some chunk overhead, so it is a slight over-estimate; err on the side of caution
            averageMapMemberSize = sizeof(Coordinate) + sizeof(Value) + sizeof(_Rb_tree_node_base) + averageValueSize;
        }
        ssize_t valuesThatFitInLimit = floor( memLimit / averageMapMemberSize);
        valuesThatFitInLimit -= (2*chunkCount);
        if (valuesThatFitInLimit <= 0)
        {}
        else if (valuesThatFitInLimit > cellCount) //the good case
        {
            result.insertionProbability = 1.0;
            result.numOptionalValues = valuesThatFitInLimit;
        }
        else
        {
            result.insertionProbability = valuesThatFitInLimit * 1.0 / cellCount;
            result.numOptionalValues = valuesThatFitInLimit;
        }
        LOG4CXX_DEBUG(logger, "Map Limits: cellCount "<<cellCount
                              <<" avgValueSize "<<averageValueSize
                              <<" avgMapMemberSize "<<averageMapMemberSize
                              <<" optValuesLimit "<<result.numOptionalValues
                              <<" insertionProb "<<result.insertionProbability);
        return result;
    }

    /**
     * Scan the data from the index array and insert a portion of it into the map.
     */
    shared_ptr<PartialValueMap const> buildPartialMap(shared_ptr<Array>& indexArray, MapLimits const& limits)
    {
        AttributeComparator comparator(indexArray->getArrayDesc().getAttributes()[0].getType());
        shared_ptr<PartialValueMap> partialMap (new PartialValueMap(comparator));
        pair<Value, Coordinate> itemToInsert;
        size_t optionalValuesInserted = 0;
        for(shared_ptr<ConstArrayIterator> indexArrayIter = indexArray->getConstIterator(0);
            !indexArrayIter->end();
            ++(*indexArrayIter))
        {
            Coordinate chunkStart = indexArrayIter->getPosition()[0];
            Coordinate nextChunkStart = chunkStart + indexArray->getArrayDesc().getDimensions()[0].getChunkInterval();
            for (shared_ptr<ConstChunkIterator> indexChunkIter = indexArrayIter->getChunk().getConstIterator();
                 !indexChunkIter->end();
                 ++(*indexChunkIter))
            {
                itemToInsert.first = indexChunkIter->getItem();
                itemToInsert.second = indexChunkIter->getPosition()[0];
                if(itemToInsert.second == chunkStart || itemToInsert.second == nextChunkStart - 1)
                {   //start or end of chunk
                    partialMap->insert(itemToInsert);
                }
                else if (limits.insertionProbability >= 1.0 ||
                         (optionalValuesInserted < limits.numOptionalValues &&
                          (rand() / (RAND_MAX + 1.0)) < limits.insertionProbability))
                {
                    partialMap->insert(itemToInsert);
                    ++ optionalValuesInserted;
                }
            }
        }
        //make sure to insert the very last value in case it doesn't line up with the end of the last chunk
        partialMap->insert(itemToInsert);
        LOG4CXX_DEBUG(logger, "Partial map built. Inserted "<<optionalValuesInserted<<" values");
        return partialMap;
    }

    /**
     * See the same method in PhysicalUniq.cpp
     */
    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                          const std::vector< ArrayDesc> & inputSchemas) const
    {
       return inputBoundaries[0];
    }

    shared_ptr<Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        ArrayDesc const& inputSchema = inputArrays[0]->getArrayDesc();
        ArrayDesc const& indexSchema = inputArrays[1]->getArrayDesc();
        IndexLookupSettings settings(inputSchema, indexSchema, _parameters, false, query);
        //replicate the index array; note this returns a new array pointer
        shared_ptr<Array> replicatedIndexData = redistribute(inputArrays[1], query, psReplication);
        MapLimits mapLimits = computeMapLimits(replicatedIndexData, settings.getMemoryLimit());
        shared_ptr<PartialValueMap const> partialMap = buildPartialMap(replicatedIndexData, mapLimits);
        return shared_ptr<Array>(new IndexLookupArray(_schema, inputArrays[0], settings.getInputAttributeId(),
                                                      replicatedIndexData, partialMap));
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalIndexLookup, "index_lookup", "PhysicalIndexLookup");

} //namespace scidb
