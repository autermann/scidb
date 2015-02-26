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
 * AggPartitioningOperator.h
 *
 *  Created on: Jul 25, 2011
 *      Author: poliocough@gmail.com
 */

#ifndef _AGGREGATOR_H
#define _AGGREGATOR_H

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/MemArray.h"
#include "query/QueryProcessor.h"
#include "network/NetworkManager.h"
#include "query/Aggregate.h"
#include "array/DelegateArray.h"

#include <boost/unordered_map.hpp>
#include <boost/foreach.hpp>
#include <log4cxx/logger.h>

namespace scidb
{

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr aggregateLogger(log4cxx::Logger::getLogger("scidb.qproc.aggregator"));

using namespace boost;
using namespace std;

class FinalResultChunkIterator : public DelegateChunkIterator
{
private:
    AggregatePtr _agg;
    Value _outputValue;

public:
    FinalResultChunkIterator (DelegateChunk const* sourceChunk, int iterationMode, AggregatePtr const& agg):
       DelegateChunkIterator(sourceChunk, iterationMode), _agg(agg->clone()), _outputValue(_agg->getResultType())
    {}

    virtual ~FinalResultChunkIterator()
    {}

    virtual Value &getItem()
    {
        Value input = inputIterator->getItem();
        _agg->finalResult(_outputValue, input);
        return _outputValue;
    }
};

class FinalResultMapCreator : public DelegateChunkIterator, protected CoordinatesMapper
{
private:
    RLEEmptyBitmap _bm;
    RLEEmptyBitmap::iterator _iter;
    Value _boolValue;
    Coordinates _coords;

public:
    FinalResultMapCreator(DelegateChunk const* sourceChunk, int iterationMode):
        DelegateChunkIterator(sourceChunk, iterationMode), CoordinatesMapper(*sourceChunk), _bm(NULL, 0)
    {
        ConstChunk const& srcChunk = sourceChunk->getInputChunk();
        PinBuffer scope(srcChunk);
        ConstRLEPayload payload((char*)srcChunk.getData());
        ConstRLEPayload::iterator iter = payload.getIterator();
        while (!iter.end())
        {
            if(iter.isNull() && iter.getMissingReason()==0)
            {}
            else
            {
                RLEEmptyBitmap::Segment seg;
                seg.lPosition=iter.getPPos();
                seg.pPosition=iter.getPPos();
                seg.length = iter.getSegLength();
                _bm.addSegment(seg);
            }
            iter.toNextSegment();
        }
        _iter = _bm.getIterator();
        _boolValue.setBool(true);
        _coords.resize(sourceChunk->getArrayDesc().getDimensions().size());
        reset();
    }

    virtual ~FinalResultMapCreator()
    {}

    Value& getItem()
    {
        if(_iter.end())
        {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }

        return _boolValue;
    }

    bool isEmpty()
    {
        if(_iter.end())
        {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }

        return false;
    }

    bool end()
    {
        return _iter.end();
    }

    void operator ++()
    {
        ++_iter;
    }

    Coordinates const& getPosition()
    {
        pos2coord(_iter.getLPos(), _coords);
        return _coords;
    }

    bool setPosition(Coordinates const& pos)
    {
        position_t p = coord2pos(pos);
        return _iter.setPosition(p);
    }

    void reset()
    {
        _iter.reset();
    }
};

class EmptyFinalResultChunkIterator : public FinalResultMapCreator
{
private:
    AggregatePtr _agg;
    Value _outputValue;

public:
    EmptyFinalResultChunkIterator (DelegateChunk const* sourceChunk, int iterationMode, AggregatePtr const& agg):
        FinalResultMapCreator(sourceChunk,iterationMode),
        _agg(agg->clone()), _outputValue(_agg->getResultType())
    {}

    virtual ~EmptyFinalResultChunkIterator()
    {}

    virtual Value &getItem()
    {
        inputIterator->setPosition(getPosition());
        Value input = inputIterator->getItem();
        _agg->finalResult(_outputValue, input);
        return _outputValue;
    }
};



class FinalResultArray : public DelegateArray
{
private:
    vector <AggregatePtr> _aggs;
    bool _createEmptyMap;
    AttributeID _emptyMapScapegoat;

public:
    FinalResultArray (ArrayDesc const& desc, shared_ptr<Array> const& stateArray, vector<AggregatePtr> const& aggs, bool createEmptyMap = false):
      DelegateArray (desc, stateArray), _aggs(aggs), _createEmptyMap(createEmptyMap), _emptyMapScapegoat(0)
    {
        if(_createEmptyMap)
        {
            if(!desc.getEmptyBitmapAttribute())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "improper use of FinalResultArray";
            }

            for(AttributeID i =0, n=desc.getAttributes().size(); i<n; i++)
            {
                if (_aggs[i].get())
                {
                    _emptyMapScapegoat=i;
                    break;
                }

                if (i==desc.getAttributes().size()-1)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "improper use of FinalResultArray";
                }
            }
        }
    }

    virtual ~FinalResultArray()
    {}

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
    {
        return new DelegateChunk(*this, *iterator, attrID, false);
    }

    virtual DelegateArrayIterator* createArrayIterator(AttributeID attrID) const
    {
        if(_createEmptyMap && attrID == desc.getEmptyBitmapAttribute()->getId())
        {
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(_emptyMapScapegoat));
        }
        return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(attrID));
    }

    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        StatisticsScope sScope(_statistics);
        AggregatePtr agg = _aggs[chunk->getAttributeDesc().getId()];
        if (agg.get())
        {
            if(_createEmptyMap)
            {
                return new EmptyFinalResultChunkIterator(chunk, iterationMode, agg);
            }
            else
            {
                return new FinalResultChunkIterator(chunk, iterationMode, agg);
            }
        }
        else if(_createEmptyMap && chunk->getAttributeDesc().getId() == desc.getEmptyBitmapAttribute()->getId())
        {
            return new FinalResultMapCreator(chunk, iterationMode);
        }
        else
        {
            return new DelegateChunkIterator(chunk, iterationMode);
        }
    }
};


struct AggIOMapping
{
    AggIOMapping(): inputAttributeId(-2), outputAttributeIds(0), aggregates(0)
    {}

    AggIOMapping(AttributeID inAttId, AttributeID outAttId, AggregatePtr agg):
        inputAttributeId(inAttId), outputAttributeIds(1, outAttId), aggregates(1, agg)
    {}

    int64_t inputAttributeId;
    vector<AttributeID> outputAttributeIds;
    vector<AggregatePtr> aggregates;
};


struct AggregationFlags
{
    int iterationMode;
    bool countOnly;
    vector<bool>shapeCountOverride;
    vector<bool>nullBarrier;
};

/**
 * The aggregator computes a distributed aggregation to the input array, based on several parameters.
 * The pieces of the puzzle are:
 *  - one or more AGGREGATE_CALLs in the given parameters
 *  - input schema
 *  - output schema
 *  - the transformCoordinates() function
 */
class AggregatePartitioningOperator: public  PhysicalOperator
{
  private:
     vector <AggIOMapping> _ioMappings;
     vector <AggregatePtr> _aggs;

  public:
    AggregatePartitioningOperator(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
         PhysicalOperator(logicalName, physicalName, parameters, schema),
         _ioMappings(0)
    {
    }

    virtual ~AggregatePartitioningOperator()
    {}

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const&,
            std::vector< ArrayDesc> const&) const
    {
        return ArrayDistribution(psRoundRobin);
    }

    virtual void initializeOperator(ArrayDesc const& inputSchema)
    {
        _aggs = vector<AggregatePtr>(_schema.getAttributes().size());
        AggIOMapping countMapping;

        AttributeID attID = 0;
        for (size_t i =0, n=_parameters.size(); i<n; i++)
        {
            if (_parameters[i]->getParamType() == PARAM_AGGREGATE_CALL)
            {
                shared_ptr <OperatorParamAggregateCall>const& ac = (shared_ptr <OperatorParamAggregateCall> const&) _parameters[i];
                AttributeID inAttributeId;
                AggregatePtr agg = resolveAggregate(ac, inputSchema.getAttributes(), &inAttributeId);
                _aggs[attID] = agg;

                if (inAttributeId == ((AttributeID) -1))
                {
                    //this is for count(*) - set it aside in the countMapping pile
                    countMapping.inputAttributeId = -1;
                    countMapping.outputAttributeIds.push_back(attID);
                    countMapping.aggregates.push_back(agg);
                }
                else
                {
                    //is anyone else scanning inAttributeId?
                    size_t k, kn;
                    for(k=0, kn=_ioMappings.size(); k<kn; k++)
                    {
                        if (inAttributeId == _ioMappings[k].inputAttributeId)
                        {
                            _ioMappings[k].outputAttributeIds.push_back(attID);
                            _ioMappings[k].aggregates.push_back(agg);
                            break;
                        }
                    }

                    if (k == _ioMappings.size())
                    {
                        _ioMappings.push_back(AggIOMapping(inAttributeId, attID, agg));
                    }
                }
                attID++;
            }
        }

        if (countMapping.inputAttributeId == -1)
        {
            //We have things in the countMapping pile - find an input for it
            int64_t minSize = -1;
            size_t j=0;
            if (_ioMappings.size())
            {
                //We're scanning other attributes - let's piggyback on one of them (the smallest)
                for (size_t i=0, n=_ioMappings.size(); i<n; i++)
                {
                    size_t attributeSize = inputSchema.getAttributes()[_ioMappings[i].inputAttributeId].getSize();
                    if (attributeSize > 0)
                    {
                        if (minSize == -1 || minSize > (int64_t) attributeSize)
                        {
                            minSize = attributeSize;
                            j = i;
                        }
                    }
                }
                for (size_t i=0, n=countMapping.outputAttributeIds.size(); i<n; i++)
                {
                    _ioMappings[j].outputAttributeIds.push_back(countMapping.outputAttributeIds[i]);
                    _ioMappings[j].aggregates.push_back(countMapping.aggregates[i]);
                }
            }
            else
            {
                //We're not scanning other attributes - let'pick the smallest attribute out of the input
                int64_t minSize = -1;
                for (size_t i =0, n=inputSchema.getAttributes().size(); i<n; i++)
                {
                    size_t attributeSize = inputSchema.getAttributes()[i].getSize();
                    if (attributeSize > 0 && inputSchema.getAttributes()[i].getType() != TID_INDICATOR)
                    {
                        if (minSize == -1 || minSize > (int64_t) attributeSize)
                        {
                            minSize = attributeSize;
                            j = i;
                        }
                    }
                }
                countMapping.inputAttributeId = j;
                _ioMappings.push_back(countMapping);
            }
        }
    }

    virtual void transformCoordinates(Coordinates const & inPos, Coordinates & outPos) = 0;

    ArrayDesc createStateDesc()
    {
        Attributes outAttrs;
        for (size_t i=0, n=_schema.getAttributes().size(); i<n; i++)
        {
            if (_schema.getEmptyBitmapAttribute() == NULL || _schema.getEmptyBitmapAttribute()->getId() != i)
            {
                Value defaultNull;
                defaultNull.setNull(0);
                outAttrs.push_back(AttributeDesc (i,
                                                  _schema.getAttributes()[i].getName(),
                                                  _aggs[i]->getStateType().typeId(),
                                                  AttributeDesc::IS_NULLABLE,
                                                  0, std::set<std::string>(), &defaultNull, "", ""));
            }
        }

        return ArrayDesc(_schema.getName(), outAttrs, _schema.getDimensions(), _schema.getFlags());
    }


    inline void initializeOutput( boost::shared_ptr<ArrayIterator>& stateArrayIterator,
                                  boost::shared_ptr<ChunkIterator>& stateChunkIterator,
                                  Coordinates const& outPos)
    {
        Chunk& stateChunk = stateArrayIterator->newChunk(outPos);
        boost::shared_ptr<Query> query(stateArrayIterator->getQuery());
        stateChunkIterator = stateChunk.getIterator(query);
    }

    inline void setOutputPosition(  boost::shared_ptr<ArrayIterator>& stateArrayIterator,
                                    boost::shared_ptr<ChunkIterator>& stateChunkIterator,
                                    Coordinates const& outPos,
                                    AttributeID i)
    {
        if (stateChunkIterator.get() == NULL)
        {
            initializeOutput(stateArrayIterator, stateChunkIterator, outPos);
        }

        if (!stateChunkIterator->setPosition(outPos))
        {
            stateChunkIterator->flush();
            if (!stateArrayIterator->setPosition(outPos))
            {
                initializeOutput(stateArrayIterator, stateChunkIterator, outPos);
            }
            else
            {
                Chunk& stateChunk = stateArrayIterator->updateChunk();
                boost::shared_ptr<Query> query(stateArrayIterator->getQuery());
                stateChunkIterator = stateChunk.getIterator(query, ChunkIterator::APPEND_CHUNK);
            }
            if (!stateChunkIterator->setPosition(outPos))
                throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_OPERATION_FAILED) << "setPosition";
        }
    }

    typedef unordered_map <Coordinates, vector<Value> > AggregateMap;
    typedef unordered_map <Coordinates, RLEPayloadAppender> RLEAggregateMap;

    inline void drainBuckets (  AggregateMap& buckets,
                                boost::shared_ptr<ArrayIterator>& stateArrayIterator,
                                boost::shared_ptr<ChunkIterator>& stateChunkIterator,
                                AttributeID i )
    {
        BOOST_FOREACH(AggregateMap::value_type& bucket, buckets)
        {
            setOutputPosition(stateArrayIterator, stateChunkIterator, bucket.first, i);
            Value& state = stateChunkIterator->getItem();
            if (state.getMissingReason()==0)
            {
                _aggs[i]->initializeState(state);
            }
            _aggs[i]->accumulate(state, bucket.second);
            stateChunkIterator->writeItem(state);
        }
        buckets.clear();
    }

    inline void drainBuckets (  RLEAggregateMap& buckets,
                                boost::shared_ptr<ArrayIterator>& stateArrayIterator,
                                boost::shared_ptr<ChunkIterator>& stateChunkIterator,
                                AttributeID i )
    {
        BOOST_FOREACH(RLEAggregateMap::value_type bucket, buckets)
        {
            setOutputPosition(stateArrayIterator, stateChunkIterator, bucket.first, i);
            Value& state = stateChunkIterator->getItem();
            const RLEPayload* payload = bucket.second.getPayload();
            if (state.getMissingReason()==0)
            {
                _aggs[i]->initializeState(state);
            }
            _aggs[i]->accumulatePayload(state, payload);
            stateChunkIterator->writeItem(state);
        }
    }


    AggregationFlags composeFlags(shared_ptr<Array> const& inputArray, AggIOMapping const& mapping)
    {
        AttributeID inAttID = mapping.inputAttributeId;
        AttributeDesc const& inputAttributeDesc = inputArray->getArrayDesc().getAttributes()[inAttID];

        bool arrayEmptyable = (inputArray->getArrayDesc().getEmptyBitmapAttribute() != NULL);
        bool attributeNullable = inputAttributeDesc.isNullable();

        bool countOnly = true;
        bool readZeroes = false;
        bool readNulls = false;

        size_t const nAggs = mapping.aggregates.size();

        //first pass: set countOnly, iterateWithoutZeroes, iterateWithoutNulls
        for(size_t i =0; i<nAggs; i++)
        {
            AggregatePtr agg = mapping.aggregates[i];
            if (agg->isCounting() == false)
            {
                countOnly = false;
                if(agg->ignoreZeroes() == false)
                {   readZeroes = true; }
                if(agg->ignoreNulls() == false && attributeNullable)
                {   readNulls = true;  }
            }
            else
            {
                CountingAggregate* cagg = (CountingAggregate*) agg.get();
                if (cagg->needsAccumulate())
                {   countOnly = false;  }
                if (arrayEmptyable) //we can't infer count from shape
                {
                    readZeroes = true;
                    if (cagg->ignoreNulls()==false && attributeNullable) //nulls must be included in count
                    {   readNulls = true; }
                }
                else if (attributeNullable && cagg->ignoreNulls())
                {   readNulls=true; readZeroes = true; }
            }
        }

        vector<bool>shapeCountOverride (nAggs,false);
        vector<bool>nullBarrier(nAggs,false);

        for(size_t i =0; i<nAggs; i++)
        {
            AggregatePtr agg = mapping.aggregates[i];
            if(readNulls && agg->ignoreNulls())
            {   nullBarrier[i] = true;    }
            if (agg->isCounting())
            {
                CountingAggregate* cagg = (CountingAggregate*) agg.get();
                if(!arrayEmptyable &&  ((attributeNullable && cagg->ignoreNulls() == false) || !attributeNullable) )
                {   shapeCountOverride[i] = true; }
            }
        }

        AggregationFlags result;
        result.countOnly = countOnly;
        result.iterationMode = ConstChunkIterator::IGNORE_EMPTY_CELLS | ConstChunkIterator::IGNORE_OVERLAPS;
        if (!readNulls)
        {
            result.iterationMode |= ConstChunkIterator::IGNORE_NULL_VALUES;
        }
        if (!readZeroes && inputAttributeDesc.getDefaultValue().isZero())
        {
            result.iterationMode |= ConstChunkIterator::IGNORE_DEFAULT_VALUES;
        }
        result.nullBarrier=nullBarrier;
        result.shapeCountOverride=shapeCountOverride;
        return result;
    }

    AggregationFlags composeGroupedFlags(shared_ptr<Array> const& inputArray, AggIOMapping const& mapping)
    {
        AttributeID inAttID = mapping.inputAttributeId;
        AttributeDesc const& inputAttributeDesc = inputArray->getArrayDesc().getAttributes()[inAttID];

        bool attributeNullable = inputAttributeDesc.isNullable();

        bool countOnly = false;
        bool readZeroes = false;
        bool readNulls = false;

        size_t const nAggs = mapping.aggregates.size();

        //first pass: set countOnly, iterateWithoutZeroes, iterateWithoutNulls
        for(size_t i =0; i<nAggs; i++)
        {
            AggregatePtr agg = mapping.aggregates[i];
            if(agg->ignoreZeroes() == false)
            {   readZeroes = true; }
            if(agg->ignoreNulls() == false && attributeNullable)
            {   readNulls = true;  }
        }

        vector<bool>shapeCountOverride (nAggs,false);
        vector<bool>nullBarrier(nAggs,false);

        for(size_t i =0; i<nAggs; i++)
        {
            AggregatePtr agg = mapping.aggregates[i];
            if(readNulls && agg->ignoreNulls())
            {   nullBarrier[i] = true;    }
        }

        AggregationFlags result;
        result.countOnly = countOnly;
        result.iterationMode = ConstChunkIterator::IGNORE_EMPTY_CELLS | ConstChunkIterator::IGNORE_OVERLAPS;
        if (!readNulls)
        {
            result.iterationMode |= ConstChunkIterator::IGNORE_NULL_VALUES;
        }
        if (!readZeroes && inputAttributeDesc.getDefaultValue().isZero())
        {
            result.iterationMode |= ConstChunkIterator::IGNORE_DEFAULT_VALUES;
        }
        result.nullBarrier=nullBarrier;
        result.shapeCountOverride=shapeCountOverride;
        return result;
    }

    void grandCount(Array* stateArray, shared_ptr<Array> & inputArray, AggIOMapping const& mapping, AggregationFlags const& aggFlags)
    {
        shared_ptr<ConstArrayIterator> inArrayIterator = inputArray->getConstIterator(mapping.inputAttributeId);
        size_t nAggs = mapping.aggregates.size();

        vector<uint64_t> counts(nAggs,0);
        bool dimBasedCount = true;
        for(size_t i=0; i<nAggs; i++)
        {
            if(aggFlags.shapeCountOverride[i] == false)
            {
                dimBasedCount = false;
                break;
            }
        }

        if (dimBasedCount)
        {
            while (!inArrayIterator->end())
            {
                {
                    ConstChunk const& chunk = inArrayIterator->getChunk();
                    uint64_t chunkCount = chunk.getNumberOfElements(false);
                    for (size_t i=0; i<nAggs; i++)
                    {
                        counts[i]+=chunkCount;
                    }
                }
                ++(*inArrayIterator);
            }
        }
        else
        {
            while (!inArrayIterator->end())
            {
                {
                    ConstChunk const& chunk = inArrayIterator->getChunk();
                    uint64_t itemCount = 0;
                    uint64_t noNullCount = 0;
                    
                    uint64_t chunkCount = chunk.getNumberOfElements(false);
                    shared_ptr <ConstChunkIterator> inChunkIterator = chunk.getConstIterator(aggFlags.iterationMode);
                    while(!inChunkIterator->end())
                    {
                        Value& v = inChunkIterator->getItem();
                        if(!v.isNull())
                        {
                            noNullCount++;
                        }
                        itemCount++;
                        ++(*inChunkIterator);
                    }
                    for (size_t i=0; i<nAggs; i++)
                    {
                        if (aggFlags.shapeCountOverride[i])
                        {
                            counts[i]+=chunkCount;
                        }
                        else if (aggFlags.nullBarrier[i])
                        {
                            counts[i]+=noNullCount;
                        }
                        else
                        {
                            counts[i]+=itemCount;
                        }
                    }
                }
                ++(*inArrayIterator);
            }
        }

        Coordinates outPos(_schema.getDimensions().size());
        for(size_t i =0, n=outPos.size(); i<n; i++)
        {
            outPos[i]=_schema.getDimensions()[i].getStartMin();
        }

        for(size_t i =0; i<nAggs; i++)
        {
            shared_ptr<ArrayIterator> stateArrayIterator = stateArray->getIterator(mapping.outputAttributeIds[i]);
            shared_ptr<ChunkIterator> stateChunkIterator;
            initializeOutput(stateArrayIterator, stateChunkIterator, outPos);
            stateChunkIterator->setPosition(outPos);
            Value state;
            mapping.aggregates[i]->initializeState(state);
            ((CountingAggregate*)mapping.aggregates[i].get())->overrideCount(state,counts[i]);
            stateChunkIterator->writeItem(state);
            stateChunkIterator->flush();
        }
    }

    void grandTileAggregate(Array* stateArray, shared_ptr<Array> & inputArray, AggIOMapping const& mapping, AggregationFlags const& aggFlags)
    {
        shared_ptr<ConstArrayIterator> inArrayIterator = inputArray->getConstIterator(mapping.inputAttributeId);
        size_t nAggs = mapping.aggregates.size();
        vector<Value> states(nAggs);

        while (!inArrayIterator->end())
        {
            {
                ConstChunk const& inChunk = inArrayIterator->getChunk();
                shared_ptr <ConstChunkIterator> inChunkIterator = inChunk.getConstIterator(ChunkIterator::TILE_MODE|aggFlags.iterationMode);
                while (!inChunkIterator->end())
                {
                    Value &v = inChunkIterator->getItem();
                    RLEPayload *tile = v.getTile();
                    if (tile->count())
                    {
                        for (size_t i=0; i<nAggs; i++)
                        {
                            if (states[i].getMissingReason() == 0) {
                                mapping.aggregates[i]->initializeState(states[i]);
                            }
                            mapping.aggregates[i]->accumulatePayload(states[i], tile);
                        }
                    }
                    
                    ++(*inChunkIterator);
                }
            }
            ++(*inArrayIterator);
        }

        Coordinates outPos(_schema.getDimensions().size());
        for(size_t i =0, n=outPos.size(); i<n; i++)
        {
            outPos[i]=_schema.getDimensions()[i].getStartMin();
        }

        for(size_t i =0; i<nAggs; i++)
        {
            shared_ptr<ArrayIterator> stateArrayIterator = stateArray->getIterator(mapping.outputAttributeIds[i]);
            shared_ptr<ChunkIterator> stateChunkIterator;
            initializeOutput(stateArrayIterator, stateChunkIterator, outPos);
            stateChunkIterator->setPosition(outPos);
            stateChunkIterator->writeItem(states[i]);
            stateChunkIterator->flush();
        }
    }

    void groupedTileFixedSizeAggregate(Array* stateArray, shared_ptr<Array> & inputArray, AggIOMapping const& mapping, AggregationFlags const& aggFlags, size_t attSize)
    {
        shared_ptr<ConstArrayIterator> inArrayIterator = inputArray->getConstIterator(mapping.inputAttributeId);
        size_t const nAggs = mapping.aggregates.size();
        vector<Value> states(nAggs);
        Coordinates outPos(_schema.getDimensions().size());

        vector <shared_ptr<ArrayIterator> > stateArrayIterators(nAggs);
        for (size_t i =0; i<nAggs; i++)
        {
            stateArrayIterators[i] = stateArray->getIterator(mapping.outputAttributeIds[i]);
        }

        vector <shared_ptr<ChunkIterator> > stateChunkIterators(nAggs, shared_ptr<ChunkIterator>());
        RLEAggregateMap aggregateMap;

        while (!inArrayIterator->end())
        {
            {
                ConstChunk const& inChunk = inArrayIterator->getChunk();
                shared_ptr <ConstChunkIterator> inChunkIterator = inChunk.getConstIterator(aggFlags.iterationMode);
                while (!inChunkIterator->end())
                {
                    transformCoordinates(inChunkIterator->getPosition(), outPos);
                    Value &v = inChunkIterator->getItem();
                    if (aggregateMap.count(outPos)==0)
                    {
                        pair<Coordinates,RLEPayloadAppender> p (outPos, RLEPayloadAppender(attSize*8));
                        aggregateMap.insert(p);
                    }
                    aggregateMap.find(outPos)->second.append(v);
                    ++(*inChunkIterator);
                }
            }

            BOOST_FOREACH(RLEAggregateMap::value_type &bucket, aggregateMap)
            {
                bucket.second.finalize();
            }

            for (size_t i = 0; i < nAggs; i++)
            {
                drainBuckets(aggregateMap, stateArrayIterators[i], stateChunkIterators[i], mapping.outputAttributeIds[i]);
            }
            aggregateMap.clear();
            ++(*inArrayIterator);
        }

        for (size_t i = 0; i <nAggs; i++)
        {
            if (stateChunkIterators[i].get())
            {
                stateChunkIterators[i]->flush();
            }
        }
    }

    void grandAggregate(Array* stateArray, shared_ptr<Array> & inputArray, AggIOMapping const& mapping, AggregationFlags const& aggFlags)
    {
        shared_ptr<ConstArrayIterator> inArrayIterator = inputArray->getConstIterator(mapping.inputAttributeId);
        size_t const nAggs = mapping.aggregates.size();
        Value null;
        null.setNull(0);
        vector<Value> states(nAggs,null);
        int64_t chunkCount = 0;
        bool noNulls = aggFlags.iterationMode & ChunkIterator::IGNORE_NULL_VALUES;

        while (!inArrayIterator->end())
        {
            {
                ConstChunk const& inChunk = inArrayIterator->getChunk();
                chunkCount += inChunk.getNumberOfElements(false);
                shared_ptr <ConstChunkIterator> inChunkIterator = inChunk.getConstIterator(aggFlags.iterationMode);
                while (!inChunkIterator->end())
                {
                    Value &v = inChunkIterator->getItem();
                    if (noNulls && v.isNull())
                    {
                        ++(*inChunkIterator);
                        continue;
                    }
                    
                    for (size_t i =0; i<nAggs; i++)
                    {
                        if ( !(aggFlags.nullBarrier[i] && v.isNull()) )
                        {
                            if(states[i].getMissingReason()==0)
                            {
                                mapping.aggregates[i]->initializeState(states[i]);
                            }
                            mapping.aggregates[i]->accumulate(states[i], v);
                        }
                    }
                    ++(*inChunkIterator);
                }
            }
            ++(*inArrayIterator);
        }

        Coordinates outPos(_schema.getDimensions().size());
        for(size_t i =0, n=outPos.size(); i<n; i++)
        {
            outPos[i]=_schema.getDimensions()[i].getStartMin();
        }

        for(size_t i =0; i<nAggs; i++)
        {
            shared_ptr<ArrayIterator> stateArrayIterator = stateArray->getIterator(mapping.outputAttributeIds[i]);
            shared_ptr<ChunkIterator> stateChunkIterator;
            initializeOutput(stateArrayIterator, stateChunkIterator, outPos);
            stateChunkIterator->setPosition(outPos);
            if(aggFlags.shapeCountOverride[i])
            {
                if (states[i].getMissingReason()==0)
                {
                    mapping.aggregates[i]->initializeState(states[i]);
                }
                ((CountingAggregate*)mapping.aggregates[i].get())->overrideCount(states[i], chunkCount);
            }
            stateChunkIterator->writeItem(states[i]);
            stateChunkIterator->flush();
        }
    }

    void groupedAggregate(Array* stateArray, shared_ptr<Array> & inputArray, AggIOMapping const& mapping, AggregationFlags const& aggFlags)
    {
        boost::shared_ptr<ConstArrayIterator> inArrayIterator = inputArray->getConstIterator(mapping.inputAttributeId);
        size_t const nAggs = mapping.aggregates.size();

        bool noNulls = aggFlags.iterationMode & ChunkIterator::IGNORE_NULL_VALUES;

        vector <shared_ptr<ArrayIterator> > stateArrayIterators(nAggs);
        for (size_t i =0; i<nAggs; i++)
        {
            stateArrayIterators[i] = stateArray->getIterator(mapping.outputAttributeIds[i]);
        }

        vector <shared_ptr<ChunkIterator> > stateChunkIterators(nAggs, shared_ptr<ChunkIterator>());
        vector <AggregateMap> aggValues(nAggs);
        Coordinates outPos(_schema.getDimensions().size());

        while (!inArrayIterator->end())
        {
            {
                boost::shared_ptr <ConstChunkIterator> inChunkIterator = inArrayIterator->getChunk().getConstIterator( aggFlags.iterationMode);
                while (!inChunkIterator->end())
                {
                    transformCoordinates(inChunkIterator->getPosition(), outPos);
                    Value &v = inChunkIterator->getItem();
                    if (noNulls && v.isNull())
                    {
                        ++(*inChunkIterator);
                        continue;
                    }
                    for (size_t i =0; i<nAggs; i++)
                    {
                        if ( !(aggFlags.nullBarrier[i] && v.isNull()) )
                        {
                            aggValues[i][outPos].push_back(v);
                        }
                    }
                    ++(*inChunkIterator);
                }
            }
            for (size_t i = 0; i < nAggs; i++)
            {
                drainBuckets(aggValues[i], stateArrayIterators[i], stateChunkIterators[i], mapping.outputAttributeIds[i]);
            }
            ++(*inArrayIterator);
        }

        for (size_t i = 0; i <nAggs; i++)
        {
            if (stateChunkIterators[i].get())
            {
                stateChunkIterators[i]->flush();
            }
        }
    }

    void logMapping(AggIOMapping const& mapping, AggregationFlags const& flags)
    {
        LOG4CXX_DEBUG(aggregateLogger, "AggIOMapping input " <<mapping.inputAttributeId
                                            << " countOnly " <<flags.countOnly
                                            << " iterMode "  <<flags.iterationMode);

        for (size_t i=0, n=mapping.aggregates.size(); i<n; i++)
        {
            LOG4CXX_DEBUG(aggregateLogger, ">>aggregate "  <<mapping.aggregates[i]->getName()
                                         <<" outputatt "  <<mapping.outputAttributeIds[i]
                                         <<" nullbarrier " <<flags.nullBarrier[i]
                                         <<" sco "         <<flags.shapeCountOverride[i]);
        }
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        ArrayDesc const& inArrayDesc = inputArrays[0]->getArrayDesc();
        initializeOperator(inArrayDesc);

        ArrayDesc stateDesc = createStateDesc();
        shared_ptr<MemArray> stateArray (new MemArray(stateDesc));

        if (inputArrays[0]->getSupportedAccess() == Array::SINGLE_PASS && _ioMappings.size() > 1)
        {
            //Input only allows single pass and we are aggregating over more than one attribute. Can't do it now.
            //TODO: write a loop that supports this in the future!
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_UNSUPPORTED_INPUT_ARRAY) << getLogicalName();
        }

        if (_schema.getSize()==1)
        {
            for (size_t i=0; i<_ioMappings.size(); i++)
            {
                AggregationFlags aggFlags = composeFlags(inputArrays[0], _ioMappings[i]);
                logMapping(_ioMappings[i],aggFlags);

                if (_tileMode)
                {
                    grandTileAggregate(stateArray.get(),inputArrays[0], _ioMappings[i], aggFlags);
                }
                else
                {
                    if(aggFlags.countOnly)
                    {
                        grandCount(stateArray.get(), inputArrays[0], _ioMappings[i], aggFlags);
                    }
                    else
                    {
                        grandAggregate(stateArray.get(),inputArrays[0], _ioMappings[i], aggFlags);
                    }
                }
            }
        }
        else
        {
            for (size_t i=0, n=_ioMappings.size(); i<n; i++)
            {
                AggregationFlags aggFlags = composeGroupedFlags( inputArrays[0], _ioMappings[i]);
                logMapping(_ioMappings[i], aggFlags);

                size_t attributeSize = inArrayDesc.getAttributes()[_ioMappings[i].inputAttributeId].getSize();
                if (inArrayDesc.getAttributes()[_ioMappings[i].inputAttributeId].getType() != TID_BOOL && attributeSize>0)
                {
                    groupedTileFixedSizeAggregate(stateArray.get(), inputArrays[0], _ioMappings[i], aggFlags, attributeSize);
                }
                else
                {
                    groupedAggregate(stateArray.get(), inputArrays[0], _ioMappings[i], aggFlags);
                }
            }
        }

        shared_ptr<Array> mergedArray = redistributeAggregate(stateArray, query, _aggs);
        stateArray.reset();

        shared_ptr<Array> finalResultArray (new FinalResultArray(_schema, mergedArray, _aggs, _schema.getEmptyBitmapAttribute()));
        if (_tileMode)
        {
            return shared_ptr<Array> (new MaterializedArray(finalResultArray, MaterializedArray::RLEFormat));
        }
        return finalResultArray;
    }
};

}  // namespace scidb

#endif
