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
 * PhysicalRank.cpp
 *  Created on: Mar 11, 2011
 *      Author: poliocough@gmail.com
 *  Revision 1: May 2012
 *      Author: Donghui
 *      Revision note:
 *          Adding the ability to deal with big data, i.e. when the data does not fit in memory.
 */

#ifndef RANK_COMMON
#define RANK_COMMON

#include <boost/shared_ptr.hpp>
#include <boost/foreach.hpp>
#include <log4cxx/logger.h>
#include <sys/time.h>
#include <boost/unordered_map.hpp>

#include <query/Operator.h>
#include <system/Exceptions.h>
#include <query/LogicalExpression.h>
#include <array/Metadata.h>
#include <system/Cluster.h>
#include <array/DelegateArray.h>
#include <array/FileArray.h>
#include <query/Network.h>
#include <array/RowCollection.h>

namespace scidb
{

typedef unordered_map <Coordinates, uint64_t> CountsMap;

shared_ptr<SharedBuffer> rMapToBuffer( CountsMap const& input, size_t nCoords);
void updateRmap(CountsMap& input, shared_ptr<SharedBuffer> buf, size_t nCoords);

class RankingStats
{
public:
    CountsMap counts;
};

class BlockTimer
{
  public:
    BlockTimer(int64_t* globalCounter): _globalCounter(globalCounter)
    {
         struct timeval tv;
         gettimeofday(&tv,0);
         _startTime = tv.tv_sec * 1000000 + tv.tv_usec;
    }

    ~BlockTimer()
    {
         struct timeval tv;
         gettimeofday(&tv,0);
         int64_t endTime = tv.tv_sec * 1000000 + tv.tv_usec;
         *_globalCounter += (endTime - _startTime);
    }

  private:
    int64_t _startTime;
    int64_t* _globalCounter;
};


class PreSortMap
{
public:
    PreSortMap(shared_ptr<Array>& input, AttributeID neededAttributeID, Dimensions const& groupedDims):
        _dimGrouping(input->getArrayDesc().getDimensions(), groupedDims)
    {}

    virtual ~PreSortMap()
    {}
    virtual double lookupRanking( Value const& input, Coordinates const& inCoords) = 0;
    virtual double lookupHiRanking( Value const& input, Coordinates const& inCoords) = 0;
    Coordinates getGroupCoords( Coordinates const& pos) const
    {
        return _dimGrouping.reduceToGroup(pos);
    }

protected:
    DimensionGrouping _dimGrouping;
};


class ValuePreSortMap : public PreSortMap
{
public:
    typedef unordered_map<Coordinates, shared_ptr<AttributeMultiSet> > MultiSets;
    typedef unordered_map<Coordinates, shared_ptr<AttributeMultiMap> > MultiMaps;

    ValuePreSortMap(shared_ptr<Array>& input, AttributeID neededAttributeID, Dimensions const& groupedDims):
        PreSortMap(input, neededAttributeID, groupedDims)
    {
        ArrayDesc const& inputSchema = input->getArrayDesc();
        TypeId tid = inputSchema.getAttributes()[neededAttributeID].getType();

        size_t numPresorts = 0;
        size_t actualValues = 0;
        size_t distinctValues = 0;

        unordered_map<Coordinates, shared_ptr < map<Value, uint64_t, AttributeComparator> > >::iterator mIter;
        map<Value, uint64_t, AttributeComparator>::iterator iter;
        {
            shared_ptr<ConstArrayIterator> arrayIterator = input->getConstIterator(neededAttributeID);
            while (!arrayIterator->end())
            {
                {
                    shared_ptr<ConstChunkIterator> chunkIterator = arrayIterator->getChunk().getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS|ConstChunkIterator::IGNORE_EMPTY_CELLS|ConstChunkIterator::IGNORE_NULL_VALUES);
                    while (!chunkIterator->end())
                    {
                        Value &v = chunkIterator->getItem();
                        if (v.isNull())
                        {
                            ++(*chunkIterator);
                            continue;
                        }

                        actualValues++;
                        Coordinates pos = _dimGrouping.reduceToGroup(chunkIterator->getPosition());

                        mIter = _preSortMaps.find(pos);
                        if (mIter == _preSortMaps.end())
                        {
                            shared_ptr < map< Value, uint64_t, AttributeComparator> > ptr (new map<Value,uint64_t, AttributeComparator> (AttributeComparator(tid)));
                            mIter=_preSortMaps.insert( pair<Coordinates, shared_ptr < map<Value, uint64_t, AttributeComparator> > >(pos, ptr)).first;
                            numPresorts++;
                        }

                        iter = mIter->second->find(v);
                        if (iter == mIter->second->end())
                        {
                            mIter->second->insert(pair<Value,uint64_t>(v,1));
                            distinctValues++;
                        }
                        else
                        {
                            iter->second++;
                        }
                        ++(*chunkIterator);
                    }
                }
                ++(*arrayIterator);
            }
        }

        LOG4CXX_DEBUG(logger, "Processed "<<actualValues<<" values into " << numPresorts << " presort maps with "<< distinctValues <<" distinct values");

        mIter = _preSortMaps.begin();
        while (mIter != _preSortMaps.end())
        {
           map<Value, uint64_t, AttributeComparator>::iterator iter = mIter->second->begin();
           uint64_t count = 0, tmp = 0;
           while (iter != mIter->second->end())
           {
               tmp=iter->second;
               (*iter).second=count;
               count += tmp;
               iter++;
           }

           Coordinates const& pos = mIter->first;
           _maxMap[pos] = count;
           mIter++;
        }

        LOG4CXX_DEBUG(logger, "Computed counts");
    }

    virtual ~ValuePreSortMap()
    {}

    virtual double lookupRanking( Value const& input, Coordinates const& inCoords)
    {
        Coordinates pos = getGroupCoords(inCoords);
        unordered_map<Coordinates, shared_ptr< map<Value, uint64_t, AttributeComparator> > >::iterator iter = _preSortMaps.find(pos);
        if(iter == _preSortMaps.end())
        {
            return 0;
        }
        else
        {
           shared_ptr<map<Value, uint64_t, AttributeComparator> >& innerMap = iter->second;
           map<Value, uint64_t, AttributeComparator>::iterator innerIter = innerMap->lower_bound(input);
           if(innerIter == innerMap->end())
           {
               return (double) _maxMap[pos];
           }
           return (double) innerIter->second;
        }

        return 0;   // dummy code to avoid warning
    }

    virtual double lookupHiRanking( Value const& input, Coordinates const& inCoords)
    {
        Coordinates pos = getGroupCoords(inCoords);
        unordered_map<Coordinates, shared_ptr< map<Value, uint64_t, AttributeComparator> > >::iterator iter = _preSortMaps.find(pos);
        if(iter == _preSortMaps.end())
        {
            return 0;
        }
        else
        {
           shared_ptr<map<Value, uint64_t, AttributeComparator> > & innerMap = iter->second;
           map<Value, uint64_t, AttributeComparator>::iterator innerIter = innerMap->upper_bound(input);
           if(innerIter == innerMap->end())
           {
               return (double) _maxMap[pos];
           }
           return (double) innerIter->second;
        }
    }

private:
    unordered_map<Coordinates, shared_ptr < map<Value, uint64_t, AttributeComparator> > > _preSortMaps;
    unordered_map<Coordinates, uint64_t > _maxMap;
};

template <class T>
struct IsFP
{
    static const bool value = false;
};

template<>
struct IsFP <float>
{
    static const bool value = true;
};

template<>
struct IsFP <double>
{
    static const bool value = true;
};

//TODO: we could reorganize the templates better and get rid of virtual methods, BUT THEN the RankArray class would have to be
//templatized - and there will be no reduction in the number of virtual function calls -- UNTIL we change RankArray to work
//in tile mode. Soon.
template <typename INPUT>
class PrimitivePreSortMap : public PreSortMap
{
public:
    PrimitivePreSortMap(shared_ptr<Array>& input, AttributeID neededAttributeID, Dimensions const& groupedDims):
        PreSortMap(input, neededAttributeID, groupedDims)
    {
        ArrayDesc const& inputSchema = input->getArrayDesc();
        TypeId tid = inputSchema.getAttributes()[neededAttributeID].getType();

        size_t numPresorts = 0;
        size_t actualValues = 0;
        size_t distinctValues = 0;

        typename unordered_map<Coordinates, shared_ptr<map<INPUT, uint64_t> > >::iterator mIter;
        typename map<INPUT, uint64_t>::iterator iter;
        {
            shared_ptr<ConstArrayIterator> arrayIterator = input->getConstIterator(neededAttributeID);
            while (!arrayIterator->end())
            {
                {
                    shared_ptr<ConstChunkIterator> chunkIterator = arrayIterator->getChunk().getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS|ConstChunkIterator::IGNORE_EMPTY_CELLS|ConstChunkIterator::IGNORE_NULL_VALUES);
                    while (!chunkIterator->end())
                    {
                        Value v = chunkIterator->getItem();
                        if (v.isNull() || (IsFP<INPUT>::value && isnan( *(INPUT*) v.data())))
                        {
                            ++(*chunkIterator);
                            continue;
                        }

                        actualValues++;
                        Coordinates pos = _dimGrouping.reduceToGroup(chunkIterator->getPosition());

                        mIter = _preSortMaps.find(pos);
                        if (mIter == _preSortMaps.end())
                        {
                            mIter=_preSortMaps.insert( pair<Coordinates, shared_ptr<map<INPUT, uint64_t> > >(pos, shared_ptr<map<INPUT,uint64_t> >(new map <INPUT, uint64_t> ()))).first;
                            numPresorts++;
                        }

                        INPUT* val = (INPUT*)chunkIterator->getItem().data();
                        iter = mIter->second->find(*val);
                        if (iter == mIter->second->end())
                        {
                            mIter->second->insert(pair<INPUT,uint64_t>(*val,1));
                            distinctValues++;
                        }
                        else
                        {
                            iter->second++;
                        }
                        ++(*chunkIterator);
                    }
                }
                ++(*arrayIterator);
            }
        }

        LOG4CXX_DEBUG(logger, "Processed "<<actualValues<<" values into " << numPresorts << " presort maps with "<< distinctValues <<" distinct values");

        mIter = _preSortMaps.begin();
        while (mIter != _preSortMaps.end())
        {
            typename map<INPUT, uint64_t>::iterator iter = mIter->second->begin();
            uint64_t count = 0, tmp = 0;
            while (iter != mIter->second->end())
            {
                tmp=iter->second;
                (*iter).second=count;
                count += tmp;
                iter++;
            }

            Coordinates const& pos = mIter->first;
            _maxMap[pos] = count;
            mIter++;
        }

        LOG4CXX_DEBUG(logger, "Computed counts");
    }

    virtual ~PrimitivePreSortMap()
    {}

    virtual double lookupRanking( Value const& input, Coordinates const& inCoords)
    {
        INPUT* val = (INPUT*) input.data();
        if(IsFP<INPUT>::value && isnan(*val))
        {
            return -1;
        }

        Coordinates pos = getGroupCoords(inCoords);
        typename unordered_map<Coordinates, shared_ptr <map<INPUT, uint64_t> > >::iterator iter = _preSortMaps.find(pos);
        if(iter == _preSortMaps.end())
        {
            return 0;
        }
        else
        {
           shared_ptr< map<INPUT, uint64_t> >& innerMap = iter->second;

           typename map<INPUT, uint64_t>::iterator innerIter = innerMap->lower_bound(*val);
           if(innerIter == innerMap->end())
           {
               return (double) _maxMap[pos];
           }
           return (double) innerIter->second;
        }
    }

    virtual double lookupHiRanking( Value const& input, Coordinates const& inCoords)
    {
        INPUT* val = (INPUT*) input.data();
        if(IsFP<INPUT>::value && isnan(*val))
        {
            return -1;
        }

        Coordinates pos = getGroupCoords(inCoords);
        typename unordered_map<Coordinates, shared_ptr <map<INPUT, uint64_t> > >::iterator iter = _preSortMaps.find(pos);
        if(iter == _preSortMaps.end())
        {
            return 0;
        }
        else
        {
           shared_ptr < map<INPUT, uint64_t> > & innerMap = iter->second;
           typename map<INPUT, uint64_t>::iterator innerIter = innerMap->upper_bound(*val);
           if(innerIter == innerMap->end())
           {
               return (double) _maxMap[pos];
           }
           return (double) innerIter->second;
        }
    }

private:
    unordered_map<Coordinates, shared_ptr<map<INPUT, uint64_t> > > _preSortMaps;
    unordered_map<Coordinates, uint64_t > _maxMap;
};


class RankChunkIterator : public DelegateChunkIterator
{
public:
    RankChunkIterator (DelegateChunk const* sourceChunk,
                       int iterationMode,
                       shared_ptr<PreSortMap> preSortMap,
                       shared_ptr<Array> mergerArray,
                       shared_ptr<RankingStats> rStats):
       DelegateChunkIterator(sourceChunk, (iterationMode & ~IGNORE_DEFAULT_VALUES) | IGNORE_OVERLAPS ),
       _preSortMap(preSortMap),
       _outputValue(TypeLibrary::getType(TID_DOUBLE)),
       _rStats(rStats)
    {
        if (mergerArray.get())
        {
            _mergerArrayIterator = mergerArray->getConstIterator(1);
            if (!_mergerArrayIterator->setPosition(sourceChunk->getFirstPosition(false))) { 
                throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            }
            _mergerIterator = _mergerArrayIterator->getChunk().getConstIterator(iterationMode & ~IGNORE_DEFAULT_VALUES);
        }
    }

    virtual ~RankChunkIterator()
    {}

    virtual Value &getItem()
    {
        Value input = inputIterator->getItem();
        if (input.isNull())
        {
            _outputValue.setNull();
        }
        else
        {
            double ranking = _preSortMap->lookupRanking(input, getPosition());
            if(ranking < 0)
            {
                //special case for non-null values that do not compare (i.e. double NAN)
                _outputValue.setNull();
            }
            else
            {
                if (_mergerIterator.get())
                {
                    if (!_mergerIterator->setPosition(getPosition()))
                        throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                    double mergedRanking = _mergerIterator->getItem().getDouble();
                    ranking += mergedRanking;
                }
                else
                {
                    ranking = ranking + 1.0;
                }

                if (_rStats.get())
                {
                    Coordinates groupCoords = _preSortMap->getGroupCoords(getPosition());
                    _rStats->counts[groupCoords]++;
                }

                _outputValue.setDouble(ranking);
            }
        }
        return _outputValue;
    }

protected:
    shared_ptr<PreSortMap> _preSortMap;
    Value _outputValue;            
    shared_ptr<ConstArrayIterator> _mergerArrayIterator;
    shared_ptr<ConstChunkIterator> _mergerIterator;
    shared_ptr<RankingStats> _rStats;
};

class HiRankChunkIterator : public RankChunkIterator
{
public:
    HiRankChunkIterator (DelegateChunk const* sourceChunk,
                          int iterationMode,
                          shared_ptr<PreSortMap> preSortMap,
                          shared_ptr<Array> mergerArray,
                          shared_ptr<RankingStats> rStats):
        RankChunkIterator(sourceChunk, iterationMode, preSortMap, mergerArray, rStats)
    {
        if (mergerArray.get())
        {
            _mergerArrayIterator = mergerArray->getConstIterator(2);
            if (!_mergerArrayIterator->setPosition(sourceChunk->getFirstPosition(false))) { 
                throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            }
            _mergerIterator = _mergerArrayIterator->getChunk().getConstIterator(iterationMode & ~IGNORE_DEFAULT_VALUES);
        }
    }

    virtual ~HiRankChunkIterator()
    {}

    virtual Value &getItem()
    {
        Value input = inputIterator->getItem();
        if (input.isNull())
        {
            _outputValue.setNull();
        }
        else
        {
            double ranking = _preSortMap->lookupHiRanking(input, getPosition());
            if(ranking < 0)
            {
                //special case for non-null values that do not compare (i.e. double NAN)
                _outputValue.setNull();
            }
            else
            {
                if (_mergerIterator.get())
                {
                    if (!_mergerIterator->setPosition(getPosition()))
                        throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                    double mergedRanking = _mergerIterator->getItem().getDouble();
                    ranking += mergedRanking;
                }
                if (_rStats.get())
                {
                    Coordinates groupCoords = _preSortMap->getGroupCoords(getPosition());
                    _rStats->counts[groupCoords]++;
                }
                _outputValue.setDouble(ranking);
            }
        }
        return _outputValue;
    }
};

class AvgRankChunkIterator : public DelegateChunkIterator
{
public:
    AvgRankChunkIterator (DelegateChunk const* sourceChunk,
                          int iterationMode,
                          shared_ptr<Array> mergerArray):
       DelegateChunkIterator(sourceChunk, iterationMode | IGNORE_OVERLAPS)
    {
        _mergerArrayIterator = mergerArray->getConstIterator(2);
        if (!_mergerArrayIterator->setPosition(sourceChunk->getFirstPosition(false))) { 
            throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_OPERATION_FAILED) << "setPosition";
        }
        _mergerIterator = _mergerArrayIterator->getChunk().getConstIterator(iterationMode & ~IGNORE_DEFAULT_VALUES);
    }

    virtual ~AvgRankChunkIterator()
    {}

    virtual Value &getItem()
    {
        Value input = inputIterator->getItem();
        if (input.isNull())
        {
            _outputValue.setNull();
        }
        else
        {
            double ranking = input.getDouble();

            if (!_mergerIterator->setPosition(getPosition()))
                throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            double mergedRanking = _mergerIterator->getItem().getDouble();
            ranking = (ranking + mergedRanking) / 2;

            _outputValue.setDouble(ranking);
        }
        return _outputValue;
    }

private:
    Value _outputValue;
    shared_ptr<ConstChunkIterator> _mergerIterator;
    shared_ptr<ConstArrayIterator> _mergerArrayIterator;
};

class RankArray : public DelegateArray
{
public:
    RankArray (ArrayDesc const& desc,
               shared_ptr<Array> const& inputArray,
               shared_ptr<PreSortMap> preSortMap,
               AttributeID inputAttributeID,
               bool merger,
               shared_ptr<RankingStats> rStats):
        DelegateArray(desc, inputArray),
        _preSortMap(preSortMap),
        _inputAttributeID(inputAttributeID),
        _merger(merger),
        _rStats(rStats)
    {
        _inputHasOlap = false;
        ArrayDesc const& inputDesc = inputArray->getArrayDesc();
        for(size_t i=0; i<inputDesc.getDimensions().size(); i++)
        {
            if(inputDesc.getDimensions()[i].getChunkOverlap()>0)
            {
                _inputHasOlap=true;
                break;
            }
        }
    }

    virtual ~RankArray()
    {}

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
    {
        bool isClone = (attrID != 1 && _inputHasOlap == false);
        return new DelegateChunk(*this, *iterator, attrID, isClone);
    }

    virtual DelegateArrayIterator* createArrayIterator(AttributeID attrID) const
    {
        if (attrID == 0 || attrID == 1)
        {
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(_inputAttributeID));
        }
        else
        {
            //Client must be asking for empty tag
            if (!inputArray->getArrayDesc().getEmptyBitmapAttribute()) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_DLA_ERROR13);
            }
            AttributeID etID = inputArray->getArrayDesc().getEmptyBitmapAttribute()->getId();
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(etID));
        }
    }

    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        StatisticsScope sScope(_statistics);

        if (chunk->getAttributeDesc().getId() == 1)
        {
            shared_ptr<Array> mergerArray;
            if (_merger)
            {
                mergerArray = inputArray;
            }

            return new RankChunkIterator(chunk, iterationMode, _preSortMap, mergerArray, _rStats);
        }
        else
        {
            return DelegateArray::createChunkIterator(chunk, iterationMode | ConstChunkIterator::IGNORE_OVERLAPS);
        }
    }

protected:
    shared_ptr<PreSortMap> _preSortMap;
    AttributeID _inputAttributeID;
    bool _merger;
    shared_ptr<RankingStats> _rStats;
    bool _inputHasOlap;
};


class DualRankArray : public RankArray
{
public:
    DualRankArray (ArrayDesc const& desc,
                   shared_ptr<Array> const& inputArray,
                   shared_ptr<PreSortMap> preSortMap,
                   AttributeID inputAttributeID,
                   bool merger,
                   shared_ptr<RankingStats> rStats):
       RankArray (desc, inputArray, preSortMap, inputAttributeID, merger, rStats)
    {}

    virtual ~DualRankArray()
    {}

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
    {
        bool isClone = (attrID != 1 && attrID != 2 && _inputHasOlap == false);
        return new DelegateChunk(*this, *iterator, attrID, isClone);
    }

    virtual DelegateArrayIterator* createArrayIterator(AttributeID attrID) const
    {
        if (attrID == 0 || attrID == 1 || attrID == 2)
        {
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(_inputAttributeID));
        }
        else
        {
            //Client must be asking for empty tag
            if (!inputArray->getArrayDesc().getEmptyBitmapAttribute()) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_DLA_ERROR13);
            }
            AttributeID etID = inputArray->getArrayDesc().getEmptyBitmapAttribute()->getId();
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(etID));
        }
    }

    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        StatisticsScope sScope(_statistics);

        if (chunk->getAttributeDesc().getId() == 1)
        {
            shared_ptr<Array> mergerArray;
            if (_merger)
            {
                mergerArray = inputArray;
            }
            return new RankChunkIterator(chunk, iterationMode, _preSortMap, mergerArray, _rStats);
        }
        else if (chunk->getAttributeDesc().getId() == 2)
        {
            shared_ptr<Array> mergerArray;
            if (_merger)
            {
                mergerArray = inputArray;
            }
            return new HiRankChunkIterator(chunk, iterationMode, _preSortMap, mergerArray, _rStats);
        }
        else
        {
            return DelegateArray::createChunkIterator(chunk, iterationMode | ConstChunkIterator::IGNORE_OVERLAPS);
        }
    }
};

class AvgRankArray : public DelegateArray
{
public:
    AvgRankArray (ArrayDesc const& desc,
                  shared_ptr<Array> const& inputArray):
      DelegateArray (desc, inputArray)
    {}

    virtual ~AvgRankArray()
    {}

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
    {
        bool isClone = attrID != 1;
        return new DelegateChunk(*this, *iterator, attrID, isClone);
    }

    virtual DelegateArrayIterator* createArrayIterator(AttributeID attrID) const
    {
        if (attrID == 0 || attrID == 1)
        {
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(attrID));
        }
        else
        {
            //Client must be asking for empty tag
            if (!inputArray->getArrayDesc().getEmptyBitmapAttribute()) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_DLA_ERROR13);
            }
            AttributeID etID = inputArray->getArrayDesc().getEmptyBitmapAttribute()->getId();
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(etID));
        }
    }

    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        StatisticsScope sScope(_statistics);

        if (chunk->getAttributeDesc().getId() == 1)
        {
            shared_ptr<Array> mergerArray = inputArray;
            return new AvgRankChunkIterator(chunk, iterationMode, mergerArray);
        }
        else
        {
            return DelegateArray::createChunkIterator(chunk, iterationMode | ConstChunkIterator::IGNORE_OVERLAPS);
        }
    }
};

ArrayDesc getRankingSchema(ArrayDesc const& inputSchema, AttributeID rankedAttributeID, bool dualRank = false);

//inputArray must be distributed round-robin
shared_ptr<Array> buildRankArray(shared_ptr<Array>& inputArray,
                                 AttributeID rankedAttributeID,
                                 Dimensions const& grouping,
                                 shared_ptr<Query> query,
                                 shared_ptr<RankingStats> rstats = shared_ptr<RankingStats>());
//inputArray must be distributed round-robin
shared_ptr<Array> buildDualRankArray(shared_ptr<Array>& inputArray,
                                     AttributeID rankedAttributeID,
                                     Dimensions const& grouping,
                                     shared_ptr<Query> query,
                                     shared_ptr<RankingStats> rstats = shared_ptr<RankingStats>());

/**
 * AllRankedOneChunkIterator.
 *   An iterator for AllRankedOneArray to deal with big data.
 *   Every rank is set to be 1.
 */
class AllRankedOneChunkIterator : public DelegateChunkIterator
{
public:
    AllRankedOneChunkIterator (DelegateChunk const* sourceChunk):
       DelegateChunkIterator(sourceChunk, ChunkIterator::IGNORE_OVERLAPS|ChunkIterator::IGNORE_EMPTY_CELLS ),
       _outputValue(TypeLibrary::getType(TID_DOUBLE))
    {
        TypeId strType = sourceChunk->getAttributeDesc().getType();
        _type = getDoubleFloatOther(strType);
    }

    virtual ~AllRankedOneChunkIterator()
    {}

    virtual Value &getItem()
    {
        Value input = inputIterator->getItem();
        if (isNullOrNan(input, _type))
        {
            _outputValue.setNull();
        }
        else
        {
            _outputValue.setDouble(1);
        }
        return _outputValue;
    }

protected:
    Value _outputValue;
    DoubleFloatOther _type;
};

/**
 * AllRankedOneArray.
 *   The Array that deals with big data, which adds an attribute with name = RANKEDATTIRBUTE_ranked, type = double, and value = 1.
 */
class AllRankedOneArray : public DelegateArray
{
public:
    AllRankedOneArray (ArrayDesc const& outputSchema,
               shared_ptr<Array> const& inputArray,
               AttributeID inputAttributeID):
        DelegateArray(outputSchema, inputArray),
        _inputAttributeID(inputAttributeID)
    {
        _inputHasOlap = false;
        ArrayDesc const& inputDesc = inputArray->getArrayDesc();
        for(size_t i=0; i<inputDesc.getDimensions().size(); i++)
        {
            if(inputDesc.getDimensions()[i].getChunkOverlap()>0)
            {
                _inputHasOlap=true;
                break;
            }
        }
    }

    virtual ~AllRankedOneArray()
    {}

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
    {
        bool isClone = (attrID != 1 && _inputHasOlap == false);
        return new DelegateChunk(*this, *iterator, attrID, isClone);
    }

    virtual DelegateArrayIterator* createArrayIterator(AttributeID attrID) const
    {
        if (attrID == 0 || attrID == 1)
        {
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(_inputAttributeID));
        }
        else
        {
            //Client must be asking for empty tag
            if (!inputArray->getArrayDesc().getEmptyBitmapAttribute()) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_DLA_ERROR13);
            }
            AttributeID etID = inputArray->getArrayDesc().getEmptyBitmapAttribute()->getId();
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(etID));
        }
    }

    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        StatisticsScope sScope(_statistics);

        if (chunk->getAttributeDesc().getId() == 1)
        {
            return new AllRankedOneChunkIterator(chunk);
        }
        else
        {
            return DelegateArray::createChunkIterator(chunk, ChunkIterator::IGNORE_EMPTY_CELLS | ChunkIterator::IGNORE_OVERLAPS);
        }
    }

protected:
    AttributeID _inputAttributeID;
    bool _inputHasOlap;
};

/**
 * SimpleProjectArray.
 *   The Array that projects certain attributes from an existing array.
 */
class SimpleProjectArray : public DelegateArray
{
protected:
    // A vector of attributeIDs to project on, not including the empty tag.
    vector<AttributeID> _projection;

    // Whether the input array has overlap in any of the dimensions.
    bool _inputHasOlap;

public:
    /**
     * Constructor.
     * @param   outputSchema    Must contain empty tag. Must be a subset of the inputArray's schema.
     * @param   inputArray      Must contain empty tag.
     * @param   projection      A vector of attributeIDs to project on, not including the empty tag.
     */
    SimpleProjectArray (ArrayDesc const& outputSchema,
               shared_ptr<Array> const& inputArray,
               vector<AttributeID> const& projection):
        DelegateArray(outputSchema, inputArray),
        _projection(projection)
    {
        // Input array must have an empty tag
        assert( inputArray->getArrayDesc().getEmptyBitmapAttribute() );
        assert( inputArray->getArrayDesc().getEmptyBitmapAttribute()->getId() + 1 == inputArray->getArrayDesc().getAttributes().size());

        // Suppose inputArray has 2 attributes in addition to the empty tag.
        // Suppose outputSchema also has the three attributes.
        // projection will have two elements: projection[0]=0; projection[1]=1.
        assert(projection.size()>0);
        assert(outputSchema.getAttributes().size() == projection.size()+1);
        assert(outputSchema.getAttributes().size() <= inputArray->getArrayDesc().getAttributes().size());
        assert(projection[projection.size()-1] + 1 < inputArray->getArrayDesc().getAttributes().size());

        _inputHasOlap = false;
        ArrayDesc const& inputDesc = inputArray->getArrayDesc();
        for(size_t i=0; i<inputDesc.getDimensions().size(); i++)
        {
            if(inputDesc.getDimensions()[i].getChunkOverlap()>0)
            {
                _inputHasOlap=true;
                break;
            }
        }
    }

    virtual ~SimpleProjectArray()
    {}

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
    {
        bool isClone = (_inputHasOlap == false);
        return new DelegateChunk(*this, *iterator, attrID, isClone);
    }

    virtual DelegateArrayIterator* createArrayIterator(AttributeID attrID) const
    {
        size_t attrIDInput = 0;
        if (attrID+1 < desc.getAttributes().size()) { // not EmptyTag
            attrIDInput = _projection[attrID];
        } else {                                         // EmptyTag
            attrIDInput = inputArray->getArrayDesc().getAttributes().size()-1;
        }
        return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(attrIDInput));
    }

    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        return DelegateArray::createChunkIterator(chunk, ChunkIterator::IGNORE_EMPTY_CELLS | ChunkIterator::IGNORE_OVERLAPS);
    }
};

typedef RowCollection<size_t> RCChunk;      // every chunk is a row
typedef RowIterator<size_t> RIChunk;
typedef boost::unordered_map<Coordinates, size_t> MapChunkPosToID;
typedef boost::unordered_map<Coordinates, size_t>::iterator MapChunkPosToIDIter;

/**
 * ChunkIterator for GroupbyRankArray, to assign ranks from a RowCollection (one per chunk).
 */
class GroupbyRankChunkIterator : public DelegateChunkIterator
{
public:
    GroupbyRankChunkIterator (DelegateChunk const* sourceChunk,
                       const boost::shared_ptr<RCChunk>& pRCChunk,
                       size_t chunkID):
       DelegateChunkIterator(sourceChunk, ChunkIterator::IGNORE_EMPTY_CELLS|ChunkIterator::IGNORE_OVERLAPS),
       _pRCChunk(pRCChunk), _chunkID(chunkID),
       _outputValue(TypeLibrary::getType(TID_DOUBLE))
    {
        _rcIterator = boost::shared_ptr<RIChunk>(_pRCChunk->openRow(_pRCChunk->rowIdFromExistingGroup(_chunkID)));
    }

    virtual ~GroupbyRankChunkIterator()
    {
        _rcIterator.reset();
    }

    /**
     * Forget about incrementing inputIterator().
     */
    virtual void operator++() {
        ++(*_rcIterator);
    }
    virtual Value &getItem()
    {
        assert(! _rcIterator->end());
        vector<Value> itemInRCChunk(2);
        _rcIterator->getItem(itemInRCChunk);
        _outputValue = itemInRCChunk[0];
        return _outputValue;
    }

    virtual bool setPosition(const Coordinates& pos) {
        assert(false);
        return false;
    }

    virtual void reset() {
        assert(false);
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "GroupbyRankChunkIterator::reset()";
    }

    virtual bool end() {
        return _rcIterator->end();
    }

protected:
    boost::shared_ptr<RCChunk> const _pRCChunk;
    size_t _chunkID;
    Value _outputValue;
    boost::shared_ptr<RIChunk> _rcIterator;
};

/**
 * An array that returns the ranked value (from the input array) and the ranks of each field (from RCChunk).
 *
 * @note    The array can ONLY be scanned sequentially. setPosition() will fail.
 */
class GroupbyRankArray : public DelegateArray
{
protected:
    boost::shared_ptr<RCChunk> _pRCChunk;
    AttributeID _inputAttributeID;
    bool _inputHasOlap;
    boost::shared_ptr<MapChunkPosToID> _mapChunkPosToID;

public:
    GroupbyRankArray (ArrayDesc const& desc,
               boost::shared_ptr<Array> const& inputArray,
               boost::shared_ptr<RCChunk>& pRCChunk,
               AttributeID inputAttributeID,
               boost::shared_ptr<MapChunkPosToID>& mapChunkPosToID
               ):
        DelegateArray(desc, inputArray),
        _pRCChunk(pRCChunk),
        _inputAttributeID(inputAttributeID),
        _mapChunkPosToID(mapChunkPosToID)
    {
        _inputHasOlap = false;
        ArrayDesc const& inputDesc = inputArray->getArrayDesc();
        for(size_t i=0; i<inputDesc.getDimensions().size(); i++)
        {
            if(inputDesc.getDimensions()[i].getChunkOverlap()>0)
            {
                _inputHasOlap=true;
                break;
            }
        }
    }

    virtual ~GroupbyRankArray() {
    }

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
    {
        bool isClone = (attrID != 1 && _inputHasOlap == false);
        return new DelegateChunk(*this, *iterator, attrID, isClone);
    }

    virtual DelegateArrayIterator* createArrayIterator(AttributeID attrID) const
    {
        if (attrID == 0 || attrID == 1 )   // value
        {
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(_inputAttributeID));
        }
        else
        {
            //Client must be asking for empty tag
            if (!inputArray->getArrayDesc().getEmptyBitmapAttribute()) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_DLA_ERROR13);
            }
            AttributeID etID = inputArray->getArrayDesc().getEmptyBitmapAttribute()->getId();
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(etID));
        }
    }

    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        if (chunk->getAttributeDesc().getId() == 1)
        {
            MapChunkPosToIDIter iter = _mapChunkPosToID->find(chunk->getFirstPosition(false));
            assert(iter!=_mapChunkPosToID->end());
            return new GroupbyRankChunkIterator(chunk, _pRCChunk, iter->second);
        }
        return DelegateArray::createChunkIterator(chunk, ChunkIterator::IGNORE_EMPTY_CELLS|ChunkIterator::IGNORE_OVERLAPS);
    }
};

}
#endif
