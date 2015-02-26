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

#include "query/TypeSystem.h"
#include "array/Metadata.h"
#include "array/RLE.h"
#include <map>

namespace scidb
{

class Aggregate
{
protected:
    std::string _aggregateName;
    Type _inputType;
    Type _stateType;
    Type _resultType;
    size_t _stateVarSize;

    Aggregate( std::string const& aggregateName,
               Type const& inputType,
               Type const& stateType,
               Type const& resultType,
               size_t stateVarSize = 0):
        _aggregateName(aggregateName),
        _inputType(inputType),
        _stateType(stateType),
        _resultType(resultType),
        _stateVarSize(stateVarSize)
    {}

public:
    virtual ~Aggregate()
    {}

    virtual boost::shared_ptr<Aggregate> clone() const        = 0;

    inline std::string getName() const
    {
        return _aggregateName;
    }

    inline Type getInputType() const
    {
        return _inputType;
    }

    inline Type getStateType() const
    {
        return _stateType;
    }

    inline Type getResultType() const
    {
        return _resultType;
    }

    inline size_t getStateVarSize() const
    {
        return _stateVarSize;
    }

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

    virtual void initializeState(Value& state)                  = 0;
    virtual void accumulate(Value& state, Value const& input)   = 0;

    virtual void accumulate(Value& state, std::vector<Value> const& input)
    {
        for (size_t i = 0; i< input.size(); i++)
        {
            accumulate(state, input[i]);
        }
    }

    virtual void accumulatePayload(Value& state, RLEPayload const* tile)
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

class CountingAggregate : public Aggregate
{
protected:
    CountingAggregate( std::string const& aggregateName,
               Type const& inputType,
               Type const& stateType,
               Type const& resultType,
               size_t stateVarSize = 0):
        Aggregate(aggregateName, inputType, stateType, resultType, stateVarSize)
    {}

public:
    virtual ~CountingAggregate()
    {}

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



typedef boost::shared_ptr<Aggregate> AggregatePtr;

class AggregateFactory
{
private:
    std::string _aggregateName;

public:
    AggregateFactory (std::string const& aggName):
        _aggregateName(aggName)
    {}

    virtual ~AggregateFactory()
    {}

    inline std::string getName() const
    {
        return _aggregateName;
    }

    virtual bool supportsInputType (Type const& input)  const      = 0;
    virtual AggregatePtr createAggregate (Type const& input) const = 0;
};

typedef boost::shared_ptr<AggregateFactory> AggregateFactoryPtr;

class AggregateLibrary: public Singleton<AggregateLibrary>
{
private:
    std::map <std::string, AggregateFactoryPtr> _registeredFactories;

public:
    AggregateLibrary();

    virtual ~AggregateLibrary()
    {}

    void addAggregateFactory( AggregateFactoryPtr const& aggregateFactory)
    {
        if (aggregateFactory.get() == 0)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_ADD_NULL_FACTORY);

        if (_registeredFactories.count(aggregateFactory->getName()) != 0)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_DUPLICATE_AGGREGATE_FACTORY);

        _registeredFactories[aggregateFactory->getName()] = aggregateFactory;
    }

    std::vector<std::string> getAggregateNames() const
    {
        std::vector<std::string> result;
        std::map <std::string, AggregateFactoryPtr>::const_iterator iter;
        for (iter = _registeredFactories.begin(); iter != _registeredFactories.end(); iter++)
        {
            result.push_back((*iter).first);
        }
        return result;
    }

    inline size_t getNumAggregates() const
    {
        return _registeredFactories.size();
    }

    inline bool hasAggregate(std::string const& aggregateName) const
    {
        return (_registeredFactories.count(aggregateName) > 0);
    }

    AggregatePtr createAggregate ( std::string const& aggregateName, Type const& inputType) const
    {
        AggregatePtr null_ptr;
        if ( _registeredFactories.count(aggregateName) == 0 )
        {
            throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_AGGREGATE_NOT_FOUND) << aggregateName;
        }

        AggregateFactoryPtr const& factory = _registeredFactories.find(aggregateName)->second;
        if ( !factory->supportsInputType(inputType))
        {
            std::string errorstr;
            if (TID_VOID == inputType.typeId())
            {
                throw USER_EXCEPTION(SCIDB_SE_TYPE, SCIDB_LE_AGGREGATE_DOESNT_SUPPORT_ASTERISK)
                        << aggregateName;
            }
            else
            {
                throw USER_EXCEPTION(SCIDB_SE_TYPE, SCIDB_LE_AGGREGATE_DOESNT_SUPPORT_TYPE)
                        << aggregateName << inputType.typeId();
            }
        }
        else
        {
            return factory->createAggregate(inputType);
        }
    }
};

} //namespace scidb

#endif
