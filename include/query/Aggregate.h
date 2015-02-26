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
 * @file Aggregate.h
 *
 * @author poliocough@gmail.com
 *
 * @brief Aggregate, Aggregate Factory and Aggregate Library headers
 */


#ifndef __AGGREGATE_H__
#define __AGGREGATE_H__

#include <map>

#include "query/TypeSystem.h"
#include "array/Metadata.h"
#include "array/RLE.h"
#include "query/TileFunctions.h"

namespace scidb
{

typedef boost::shared_ptr<class Aggregate> AggregatePtr;

class Aggregate
{
protected:
    std::string _aggregateName;
    Type _inputType;
    Type _resultType;

    Aggregate( std::string const& aggregateName,
               Type const& inputType,
               Type const& resultType):
        _aggregateName(aggregateName),
        _inputType(inputType),
        _resultType(resultType)
    {}

public:
    virtual ~Aggregate() {}

    virtual AggregatePtr clone() const        = 0;
    virtual AggregatePtr clone(Type const& aggregateType) const        = 0;

    const std::string& getName() const
    {
        return _aggregateName;
    }

    const Type& getAggregateType() const
    {
        return _inputType;
    }

    virtual Type getStateType() const = 0;

    const Type& getResultType() const
    {
        return _resultType;
    }

    virtual bool supportAsterisk() const { return false; }

    /**
     * This is supposed to be removed.
     */
    virtual bool ignoreZeroes() const
    {
        return false;
    }

    virtual bool ignoreNulls() const
    {
        return false;
    }

    virtual bool isCounting() const
    {
        return false;
    }

    virtual void initializeState(Value& state) = 0;

    /**
     * Accumulate an input value to a state.
     */
    virtual void accumulate(Value& state, Value const& input) = 0;

    /**
     * Accumulate all input values to a state.
     */
    virtual void accumulate(Value& state, std::vector<Value> const& input)
    {
        for (size_t i = 0; i< input.size(); i++)
        {
            accumulate(state, input[i]);
        }
    }

    /**
     * Whether a value qualifies to be accumulated.
     * @note The method does NOT check ignoreZeroes(), because that is supposed to be dead code and should be removed.
     */
    virtual bool qualifyAccumulate(Value const& input) {
        return !(ignoreNulls() && input.isNull());
    }

    /**
     * Call accumulate on a single value, if qualify.
     */
    virtual void tryAccumulate(Value& state, Value const& input) {
        if (qualifyAccumulate(input)) {
            accumulate(state, input);
        }
    }

    /**
     * Call accumulate on multiple values, if qualify.
     */
    virtual void tryAccumulate(Value& state, std::vector<Value> const& input)
    {
        for (size_t i = 0; i< input.size(); i++)
        {
            tryAccumulate(state, input[i]);
        }
    }

    virtual void accumulatePayload(Value& state, ConstRLEPayload const* tile)
    {
        ConstRLEPayload::iterator iter = tile->getIterator();
        bool noNulls = ignoreNulls();

        Value val;
        while (!iter.end())
        {
            if (iter.isNull() == false || noNulls == false)
            {
                iter.getItem(val);
                accumulate(state, val);
                ++iter;
            }
            else
            {
                iter.toNextSegment();
            }
        }
    }

    virtual void merge(Value& dstState, Value const& srcState)  = 0;
    virtual void finalResult(Value& result, Value const& state) = 0;
};

template<typename T> inline
bool skipValue(T value)
{
    return false;
}

template<> inline
bool skipValue<double>(double value)
{
    return isnan(value);
}

template<> inline
bool skipValue<float>(float value)
{
    return isnan(value);
}

template<template <typename TS, typename TSR> class A, typename T, typename TR, bool asterisk = false>
class BaseAggregate: public Aggregate
{
public:
    BaseAggregate(const std::string& name, Type const& aggregateType, Type const& resultType): Aggregate(name, aggregateType, resultType)
    {}

    AggregatePtr clone() const
    {
        return AggregatePtr(new BaseAggregate(getName(), getAggregateType(), getResultType()));
    }

    AggregatePtr clone(Type const& aggregateType) const
    {
        return AggregatePtr(new BaseAggregate(getName(), aggregateType, _resultType.typeId() == TID_VOID ? aggregateType : _resultType));
    }

    bool ignoreNulls() const
    {
        return true;
    }

    Type getStateType() const
    {
        Type stateType(TID_BINARY, sizeof(typename A<T, TR>::State) << 3);
        return stateType;
    }

    bool supportAsterisk() const
    {
        return asterisk;
    }

    void initializeState(Value& state)
    {
        state.setVector(sizeof(typename A<T, TR>::State));
        A<T, TR>::init(*static_cast<typename A<T, TR>::State* >(state.data()));
        state.setNull(-1);
    }

    void accumulate(Value& state, Value const& input)
    {
        T value = *reinterpret_cast<T*>(input.data());
        if (!skipValue(value)) {
            A<T, TR>::aggregate(*static_cast< typename A<T, TR>::State* >(state.data()), value);
        }
    }

    virtual void accumulatePayload(Value& state, ConstRLEPayload const* tile)
    {
        typename A<T, TR>::State& s = *static_cast< typename A<T, TR>::State* >(state.data());
        for (size_t i = 0; i < tile->nSegments(); i++)
        {
            const RLEPayload::Segment& v = tile->getSegment(i);
            if (v.null)
                continue;
            if (v.same) {
                T value = getPayloadValue<T>(tile, v.valueIndex);
                if (!skipValue(value)) {
                    A<T, TR>::multAggregate(s, value, v.length());
                }
            } else {
                const size_t end = v.valueIndex + v.length();
                for (size_t j = v.valueIndex; j < end; j++) {
                    T value = getPayloadValue<T>(tile, j);
                    if (!skipValue(value)) {
                        A<T, TR>::aggregate(s, value);
                    }
                }
            }
        }
    }

    void merge(Value& dstState, Value const& srcState)
    {
        A<T, TR>::merge(*static_cast< typename A<T, TR>::State* >(dstState.data()), *static_cast< typename A<T, TR>::State* >(srcState.data()));
    }

    void finalResult(Value& result, Value const& state)
    {
        result.setVector(sizeof(TR));
        if (!A<T, TR>::final(*static_cast< typename A<T, TR>::State* >(state.data()), state.isNull(), *static_cast< TR* >(result.data()))) {
            result.setNull();
        } else {
            result.setNull(-1);
        }
    }
};

template<template <typename TS, typename TSR> class A, typename T, typename TR, bool asterisk = false>
class BaseAggregateInitByFirst: public Aggregate
{
public:
    BaseAggregateInitByFirst(const std::string& name, Type const& aggregateType, Type const& resultType): Aggregate(name, aggregateType, resultType)
    {}

    AggregatePtr clone() const
    {
        return AggregatePtr(new BaseAggregateInitByFirst(getName(), getAggregateType(), getResultType()));
    }

    AggregatePtr clone(Type const& aggregateType) const
    {
        return AggregatePtr(new BaseAggregateInitByFirst(getName(), aggregateType, _resultType.typeId() == TID_VOID ? aggregateType : _resultType));
    }

    bool ignoreNulls() const
    {
        return true;
    }

    Type getStateType() const
    {
        Type stateType(TID_BINARY, sizeof(typename A<T, TR>::State));
        return stateType;
    }

    bool supportAsterisk() const
    {
        return asterisk;
    }

    void initializeState(Value& state)
    {
        //Here we use missing code 1 for special meaning. It means there have been values
        //accumulated but no valid state yet. This is used by aggregates min() and max() so
        //that min(null, null) returns null. We can't use missing code 0 because that's
        //reserved by the system for groups that do not exist.
        state.setNull(1);
    }

    void accumulate(Value& state, Value const& input)
    {
        //ignoreNulls is true so null input is not allowed
        if (state.isNull())
        {
            T value = *reinterpret_cast<T*>(input.data());
            if (!skipValue(value)) {
                state.setVector(sizeof(typename A<T, TR>::State));
                A<T, TR>::init(*static_cast<typename A<T, TR>::State* >(state.data()), value);
                state.setNull(-1);
            } else {
                return;
            }
        }
        A<T, TR>::aggregate(*static_cast< typename A<T, TR>::State* >(state.data()), *reinterpret_cast<T*>(input.data()));
    }

    virtual void accumulatePayload(Value& state, ConstRLEPayload const* tile)
    {
        if (!tile->payloadSize()) {
            return;
        }

        if (state.isNull())
        {
            for (size_t i = 0; i < tile->payloadCount(); i++) {
                T value = getPayloadValue<T>(tile, i);
                if (!skipValue(value)) {
                    state.setVector(sizeof(typename A<T, TR>::State));
                    typename A<T, TR>::State& si = *static_cast< typename A<T, TR>::State* >(state.data());
                    A<T, TR>::init(si, value);
                    state.setNull(-1);
                    break;
                }
            }
        }
        if (state.isNull()) {
            return;
        }

        typename A<T, TR>::State& s = *static_cast< typename A<T, TR>::State* >(state.data());
        for (size_t i = 0; i < tile->nSegments(); i++)
        {
            const RLEPayload::Segment& v = tile->getSegment(i);
            if (v.null)
                continue;
            if (v.same) {
                A<T, TR>::multAggregate(s, getPayloadValue<T>(tile, v.valueIndex), v.length());
            } else {
                const size_t end = v.valueIndex + v.length();
                for (size_t j = v.valueIndex; j < end; j++) {
                    A<T, TR>::aggregate(s, getPayloadValue<T>(tile, j));
                }
            }
        }
    }

    void merge(Value& dstState, Value const& srcState)
    {
        if(srcState.isNull()) {
            return;
        }
        if(dstState.isNull()) {
            dstState = srcState;
            return;
        }
        A<T, TR>::merge(*static_cast< typename A<T, TR>::State* >(dstState.data()), *static_cast< typename A<T, TR>::State* >(srcState.data()));
    }

    void finalResult(Value& result, Value const& state)
    {
        result.setVector(sizeof(TR));
        if (!A<T, TR>::final(*static_cast< typename A<T, TR>::State* >(state.data()), state.isNull(), *static_cast< TR* >(result.data()))) {
            result.setNull();
        } else {
            result.setNull(-1);
        }
    }
};

class CountingAggregate : public Aggregate
{
protected:
    CountingAggregate(std::string const& aggregateName,
               Type const& inputType,
               Type const& resultType):
        Aggregate(aggregateName, inputType, resultType)
    {}

public:
    virtual bool isCounting() const
    {
        return true;
    }

    virtual bool needsAccumulate() const
    {
        return true;
    }

    virtual void overrideCount(Value& state, uint64_t newCount)   = 0;
};

class AggregateLibrary: public Singleton<AggregateLibrary>
{
private:
    // Map of aggregate factories.
    // '*' for aggregate type means universal aggregate operator which operates by expressions (slow universal implementation).
    typedef std::map < std::string, std::map<TypeId, AggregatePtr>, __lesscasecmp > FactoriesMap;
    FactoriesMap _registeredFactories;

public:
    AggregateLibrary();

    virtual ~AggregateLibrary()
    {}

    void addAggregate(AggregatePtr const& aggregate);

    void getAggregateNames(std::vector<std::string>& names) const;

    size_t getNumAggregates() const
    {
        return _registeredFactories.size();
    }

    bool hasAggregate(std::string const& aggregateName) const
    {
        return _registeredFactories.find(aggregateName) != _registeredFactories.end();
    }

    AggregatePtr createAggregate(std::string const& aggregateName, Type const& aggregateType) const;
};

} //namespace scidb

#endif
