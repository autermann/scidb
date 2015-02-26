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
 * @file BuiltinAggregates.cpp
 *
 * @author poliocough@gmail.com
 */

#include "query/Aggregate.h"
#include "query/FunctionLibrary.h"
#include "query/Expression.h"

#include <math.h>
#include <log4cxx/logger.h>

using boost::shared_ptr;
using namespace std;

namespace scidb
{

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.builtin_aggregates"));

/**
 *
 * Sum.
 *
 */
class SumAggregate : public Aggregate
{
protected:
    Expression        _accumulateExpression;
    ExpressionContext _accumulateContext;

    Expression        _mergeExpression;
    ExpressionContext _mergeContext;

    SumAggregate(Type const& inputType, Type const& stateType, Type const& resultType,
                 Expression const& accumulateExpression, Expression const& mergeExpression ):
        Aggregate("sum", inputType, stateType, resultType),
        _accumulateExpression(accumulateExpression),
        _accumulateContext(_accumulateExpression),
        _mergeExpression(mergeExpression),
        _mergeContext(_mergeExpression)
    {}

public:
    friend class SumAggregateFactory;

    virtual ~SumAggregate()
    {}

    virtual AggregatePtr clone() const
    {
        //Expression Contexts cannot be shallow-copied
        return AggregatePtr(new SumAggregate(getInputType(), getStateType(), getResultType(),
                                             _accumulateExpression, _mergeExpression));
    }

    bool ignoreNulls() const
    {
        return true;
    }

    bool ignoreZeroes() const
    {
        return true;
    }

    void initializeState(Value& state)
    {
        state = Value(getStateType());
        state.setZero();
        state.setNull(0);
    }

    void accumulate(Value& state, Value const& input)
    {
        state.setNull(-1);
        _accumulateContext[0] = state;
        _accumulateContext[1] = input;
        state = _accumulateExpression.evaluate(_accumulateContext);
    }

    virtual void accumulatePayload(Value& state, RLEPayload const* tile)
    {
        ConstRLEPayload::iterator iter = tile->getIterator();
        Value val;
        while (!iter.end())
        {
            if (iter.isNull())
            {
                iter.toNextSegment();
            }
            else
            {
                state.setNull(-1);
                _accumulateContext[0]=state;
                iter.getItem(_accumulateContext[1]);
                state = _accumulateExpression.evaluate(_accumulateContext);
                ++iter;
            }
        }
    }

    void merge(Value& dstState, Value const& srcState)
    {
        _mergeContext[0] = dstState;
        _mergeContext[1] = srcState;
        dstState = _mergeExpression.evaluate(_mergeContext);
    }

    void finalResult(Value& result, Value const& state)
    {
        if (state.isNull())
        {
            result.setZero();
            return;
        }

        result = state;
    }
};

class MultiplicativeSumAggregate : public SumAggregate
{
private:
    Expression        _multiplyByInt;
    ExpressionContext _multiplyByIntContext;

    MultiplicativeSumAggregate(Type const& inputType, Type const& stateType, Type const& resultType,
                               Expression const& accumulateExpression, Expression const& mergeExpression, Expression const& mulExpression):
        SumAggregate(inputType, stateType, resultType, accumulateExpression, mergeExpression),
        _multiplyByInt(mulExpression),
        _multiplyByIntContext(_multiplyByInt)
    {}

public:
    friend class SumAggregateFactory;

    ~MultiplicativeSumAggregate()
    {}

    AggregatePtr clone() const
    {
        //Expression Contexts cannot be shallow-copied
        return AggregatePtr(new MultiplicativeSumAggregate(getInputType(), getStateType(), getResultType(),
                                                           _accumulateExpression, _mergeExpression, _multiplyByInt));
    }

    void accumulatePayload(Value& state, RLEPayload const* tile)
    {
        ConstRLEPayload::iterator iter = tile->getIterator();
        while (!iter.end())
        {
            if (iter.isNull())
            {
                iter.toNextSegment();
            }
            else if (iter.isSame())
            {
                state.setNull(-1);
                iter.getItem(_multiplyByIntContext[0]);
                _multiplyByIntContext[1].setUint64(iter.getRepeatCount());

                _mergeContext[0]=state;
                _mergeContext[1]=_multiplyByInt.evaluate(_multiplyByIntContext);

                state = _accumulateExpression.evaluate(_accumulateContext);
                iter.toNextSegment();
            }
            else
            {
                state.setNull(-1);
                _accumulateContext[0]=state;
                iter.getItem(_accumulateContext[1]);
                state = _accumulateExpression.evaluate(_accumulateContext);
                ++iter;
            }
        }
    }
};

template <typename INPUT_TYPE, typename STATE_TYPE>
class PrimitiveSumAggregate : public Aggregate
{
private:
    PrimitiveSumAggregate(Type const& inputType, Type const& resType):
        Aggregate("sum", inputType, resType, resType)
    {}

public:
    friend class SumAggregateFactory;

    ~PrimitiveSumAggregate()
    {}

    AggregatePtr clone() const
    {
        //Expression Contexts cannot be shallow-copied
        return AggregatePtr(new PrimitiveSumAggregate<INPUT_TYPE,STATE_TYPE>(getInputType(), getStateType()));
    }

    bool ignoreNulls() const
    {
        return true;
    }

    bool ignoreZeroes() const
    {
        return true;
    }

    void initializeState(Value& state)
    {
        state = Value(getStateType());
        state.setZero();
        state.setNull(0);
    }

    void accumulate(Value& state, Value const& input)
    {
        state.setNull(-1);
        (*((STATE_TYPE*) state.data()))+= *((INPUT_TYPE*) input.data());
    }

    void accumulatePayload(Value& state, RLEPayload const* tile)
    {
        STATE_TYPE* dst = (STATE_TYPE*)state.data();
        if (tile->elementSize() == 0 || tile->isBool()) { 
            ConstRLEPayload::iterator iter = tile->getIterator();
            size_t s;
            while (!iter.end())
            {
                if (iter.isNull())
                {
                    iter.toNextSegment();
                }
                else 
                { 
                    uint64_t repeat = iter.getRepeatCount();
                    state.setNull(-1);
                    if (iter.isSame())
                    {
                        *dst += STATE_TYPE(*(INPUT_TYPE*)iter.getRawValue(s) * repeat);
                        iter.toNextSegment();
                    }
                    else
                    {
                        do { 
                            *dst += *(INPUT_TYPE*)iter.getRawValue(s);
                            ++iter;
                        } while (--repeat != 0);
                    }
                }
            }
        } else { 
            INPUT_TYPE* src = (INPUT_TYPE*)tile->getFixData();
            for (size_t i = 0, n = tile->nSegments(); i < n; i++) { 
                ConstRLEPayload::Segment const& segm = tile->getSegment(i);
                size_t len = segm.length();
                if (!segm.null)
                {
                    state.setNull(-1);
                    if (segm.same)
                    {
                        *dst += STATE_TYPE(*src++ * len);
                    }
                    else
                    {
                        for (size_t j = 0; j < len; j++) { 
                            *dst += *src++;
                        }
                    }
                }
            }
        }
    }

    void accumulate(Value& state, std::vector<Value> const& input)
    {
        state.setNull(-1);
        STATE_TYPE *sum = (STATE_TYPE*) state.data();
        for(size_t i =0; i<input.size(); i++)
        {
            *sum += *((INPUT_TYPE*) input[i].data());
        }
    }

    virtual void merge(Value& dstState, Value const& srcState)
    {
        (*((STATE_TYPE*) dstState.data()))+= *((STATE_TYPE*) srcState.data());
    }

    virtual void finalResult(Value& result, Value const& state)
    {
        if (state.isNull())
        {
            result.setZero();
            return;
        }
        result = state;
    }
};

class SumAggregateFactory : public AggregateFactory
{
public:
    SumAggregateFactory():
        AggregateFactory("sum")
    {}

    virtual ~SumAggregateFactory()
    {}

    virtual bool supportsInputType (Type const& input) const
    {
        if (TID_VOID == input.typeId())
            return false;

        vector<TypeId> inputTypes(2);
        inputTypes[0] = input.typeId();
        inputTypes[1] = input.typeId();

        FunctionDescription functionDesc;
        vector<FunctionPointer> converters;
        bool found = FunctionLibrary::getInstance()->findFunction("+", inputTypes, functionDesc, converters, false);

        return found;
    }

    virtual AggregatePtr createAggregate (Type const& input) const
    {
        if (!supportsInputType(input))
            throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_AGGREGATE_DOESNT_SUPPORT_TYPE) << getName() << input;
        Type propagatedType = TypeLibrary::getType( propagateType(input.typeId()) );

        if( input.typeId() == TID_DOUBLE )
        {
            return AggregatePtr (new PrimitiveSumAggregate<double,double>(input, propagatedType));
        }
        else if (input.typeId() == TID_FLOAT)
        {
            return AggregatePtr (new PrimitiveSumAggregate<float,double>(input, propagatedType));
        }
        else if (input.typeId() == TID_INT64)
        {
            return AggregatePtr (new PrimitiveSumAggregate<int64_t,int64_t>(input, propagatedType));
        }
        else if (input.typeId() == TID_INT32)
        {
            return AggregatePtr (new PrimitiveSumAggregate<int32_t,int64_t>(input, propagatedType));
        }
        else if (input.typeId() == TID_INT16)
        {
            return AggregatePtr (new PrimitiveSumAggregate<int16_t,int64_t>(input, propagatedType));
        }
        else if (input.typeId() == TID_INT8)
        {
            return AggregatePtr (new PrimitiveSumAggregate<int8_t,int64_t>(input, propagatedType));
        }
        else if (input.typeId() == TID_UINT64)
        {
            return AggregatePtr (new PrimitiveSumAggregate<uint64_t,uint64_t>(input, propagatedType));
        }
        else if (input.typeId() == TID_UINT32)
        {
            return AggregatePtr (new PrimitiveSumAggregate<uint32_t,uint64_t>(input, propagatedType));
        }
        else if (input.typeId() == TID_UINT16)
        {
            return AggregatePtr (new PrimitiveSumAggregate<uint16_t,uint64_t>(input, propagatedType));
        }
        else if (input.typeId() == TID_UINT8)
        {
            return AggregatePtr (new PrimitiveSumAggregate<uint8_t,uint64_t>(input, propagatedType));
        }

        Expression accumulation;
        accumulation.compile("+", false, propagatedType.typeId(), input.typeId(), propagatedType.typeId());

        Expression merge;
        merge.compile("+", false, propagatedType.typeId(), propagatedType.typeId());

        vector<TypeId> inputTypes(2);
        inputTypes[0] = input.typeId();
        inputTypes[1] = TID_UINT64;
        FunctionDescription functionDesc;
        vector<FunctionPointer> converters;
        bool found = FunctionLibrary::getInstance()->findFunction("*", inputTypes, functionDesc, converters, false);
        if (found)
        {
            Expression multiplyByInt;
            multiplyByInt.compile("*", false, input.typeId(), TID_UINT64, propagatedType.typeId());
            return AggregatePtr (new MultiplicativeSumAggregate(input, propagatedType, propagatedType, accumulation, merge, multiplyByInt));
        }

        return AggregatePtr (new SumAggregate(input, propagatedType, propagatedType, accumulation, merge));
    }
};

/**
 *
 * Avg.
 *
 */
class AvgAggregate : public CountingAggregate
{
protected:
    Expression        _addValueToState;
    ExpressionContext _addValueToStateContext;

    Expression        _addStateToState;
    ExpressionContext _addStateToStateContext;

    Expression        _divideStateByCount;
    ExpressionContext _divideStateByCountContext;

    size_t           _minStateSize;

    AvgAggregate(Type const& inputType, Type const& stateType, Type const& resultType, size_t stateVarSize,
                 Expression const& addValueToState,
                 Expression const& addStateToState,
                 Expression const& divideStateByCount):
         CountingAggregate("avg", inputType, stateType, resultType, stateVarSize),
        _addValueToState(addValueToState),
        _addValueToStateContext(_addValueToState),
        _addStateToState(addStateToState),
        _addStateToStateContext(_addStateToState),
        _divideStateByCount(divideStateByCount),
        _divideStateByCountContext(_divideStateByCount),
        _minStateSize(stateVarSize > 0 ? stateVarSize : sizeof(uint64_t))
    {}

public:
    friend class AvgAggregateFactory;

    virtual ~AvgAggregate()
    {}

    virtual AggregatePtr clone() const
    {
        //Expression Contexts cannot be shallow-copied
        return AggregatePtr(new AvgAggregate(getInputType(), getStateType(), getResultType(), getStateVarSize(),
                                             _addValueToState, _addStateToState, _divideStateByCount));
    }

    bool ignoreNulls() const
    {
        return true;
    }

    inline uint64_t* getCount(Value const& state)
    {
        return (uint64_t*) (((char*) state.data()) + state.size() - sizeof(uint64_t));
    }

    inline Value getSum (Value const& state)
    {
        return Value (state.data(), state.size() - sizeof(uint64_t), false);
    }

    void overrideCount(Value& state, uint64_t newCount)
    {
        state.setNull(-1);
        *getCount(state) = newCount;
    }

    inline void packState(Value& state, Value const& sum, uint64_t* count)
    {
        if (_stateVarSize)
        {
            memcpy(state.data(), sum.data(), _stateVarSize - sizeof(uint64_t));
            *((uint64_t*)((char*)state.data() + _stateVarSize - sizeof(uint64_t))) = *count;
        }
        else
        {
            Value result(sum.size() + sizeof(uint64_t));
            memcpy(result.data(), sum.data(), sum.size());
            *((uint64_t*)((char*)result.data()+sum.size())) = *count;
            state = result;
        }
    }

    void initializeState(Value& state)
    {
        state = Value(_minStateSize);
        state.setZero();
        state.setNull(0);
    }

    void accumulate(Value& state, Value const& input)
    {
        state.setNull(-1);
        uint64_t* count = getCount(state);
        (*count)++;

        if (input.isZero())
        {
            return;
        }

        Value sum = getSum(state);
        _addValueToStateContext[0] = input;
        _addValueToStateContext[1] = sum;
        sum = _addValueToState.evaluate(_addValueToStateContext);

        packState(state, sum, count);
    }

    virtual void accumulatePayload(Value& state, RLEPayload const* tile)
    {
        uint64_t *count = getCount(state);
        Value sum = getSum(state);

        ConstRLEPayload::iterator iter = tile->getIterator();
        while (!iter.end())
        {
            if (iter.isNull())
            {
                iter.toNextSegment();
            }
            else
            {
                state.setNull(-1);
                iter.getItem(_addValueToStateContext[0]);
                _addValueToStateContext[1]=sum;
                sum=_addValueToState.evaluate(_addValueToStateContext);
                (*count)++;
                ++iter;
            }
        }

        packState(state,sum,count);
    }

    void accumulate(Value& state, std::vector<Value> const& input)
    {
        state.setNull(-1);
        uint64_t* count = getCount(state);
        (*count)+= input.size();

        Value sum = getSum(state);

        for(size_t i =0; i<input.size(); i++)
        {
            if (input[i].isZero())
            {
                continue;
            }

            _addValueToStateContext[0] = input[i];
            _addValueToStateContext[1] = sum;
            sum = _addValueToState.evaluate(_addValueToStateContext);
        }

        packState(state, sum, count);
    }

    void merge(Value& dstState, Value const& srcState)
    {
        uint64_t* count =  getCount(dstState);
        *count += *getCount(srcState);

        Value lhsSum = getSum(dstState);
        Value rhsSum = getSum(srcState);

        _addStateToStateContext[0] = rhsSum;
        _addStateToStateContext[1] = lhsSum;

        rhsSum = _addStateToState.evaluate(_addStateToStateContext);

        packState(dstState, rhsSum, count);
    }

    void finalResult(Value& result, Value const& state)
    {
        if (state.isNull())
        {
            result.setNull();
            return;
        }

        Value sum = getSum(state);

        Value vCount;
        vCount.setUint64(*getCount(state));

        _divideStateByCountContext[0] = sum;
        _divideStateByCountContext[1] = vCount;

        Value rslt = _divideStateByCount.evaluate(_divideStateByCountContext);
        result = rslt;
    }
};

class MultiplicativeAvgAggregate : public AvgAggregate
{
private:
    Expression        _multiplyByInt;
    ExpressionContext _multiplyByIntContext;

    MultiplicativeAvgAggregate(Type const& inputType, Type const& stateType, Type const& resultType, size_t stateVarSize,
                               Expression const& addValueToState,
                               Expression const& addStateToState,
                               Expression const& divideStateByCount,
                               Expression const& multiplyByInt):
        AvgAggregate(inputType, stateType, resultType, stateVarSize, addValueToState, addStateToState, divideStateByCount),
        _multiplyByInt(multiplyByInt),
        _multiplyByIntContext(_multiplyByInt)
    {}

public:
    friend class AvgAggregateFactory;

    ~MultiplicativeAvgAggregate()
    {}

    AggregatePtr clone() const
    {
        //Expression Contexts cannot be shallow-copied
        return AggregatePtr(new MultiplicativeAvgAggregate(getInputType(), getStateType(), getResultType(), getStateVarSize(),
                                                           _addValueToState, _addStateToState, _divideStateByCount, _multiplyByInt));
    }

    void accumulatePayload(Value& state, RLEPayload const* tile)
    {
        uint64_t *count = getCount(state);
        Value sum = getSum(state);

        ConstRLEPayload::iterator iter = tile->getIterator();
        while (!iter.end())
        {
            if (iter.isNull())
            {
                iter.toNextSegment();
            }
            else if (iter.isSame())
            {
                state.setNull(-1);
                iter.getItem(_multiplyByIntContext[0]);
                _multiplyByIntContext[1].setUint64(iter.getRepeatCount());
                _addStateToStateContext[0]=_multiplyByInt.evaluate(_multiplyByIntContext);
                _addStateToStateContext[1]=sum;
                sum = _addStateToState.evaluate(_addStateToStateContext);
                (*count)+=iter.getRepeatCount();
                iter.toNextSegment();
            }
            else
            {
                state.setNull(-1);
                iter.getItem(_addValueToStateContext[0]);
                _addValueToStateContext[1]=sum;
                sum=_addValueToState.evaluate(_addValueToStateContext);
                (*count)++;
                ++iter;
            }
        }
        packState(state,sum,count);
    }
};

template <typename INPUT_TYPE, typename SUM_TYPE>
class PrimitiveAverageAggregate : public CountingAggregate
{
private:
    PrimitiveAverageAggregate(Type const& inputType, Type const& stateType):
        CountingAggregate("avg", inputType, stateType, TypeLibrary::getType(TID_DOUBLE), sizeof(SUM_TYPE) + sizeof(uint64_t))
    {}

public:
    friend class AvgAggregateFactory;

    ~PrimitiveAverageAggregate()
    {}

    AggregatePtr clone() const
    {
        //Expression Contexts cannot be shallow-copied
        return AggregatePtr(new PrimitiveAverageAggregate<INPUT_TYPE, SUM_TYPE>(getInputType(), getStateType()));
    }

    bool ignoreNulls() const
    {
        return true;
    }

    inline uint64_t* getCount(Value const& state)
    {
        return (uint64_t*) (((char*) state.data()) + sizeof(SUM_TYPE));
    }

    void overrideCount(Value& state, uint64_t newCount)
    {
        state.setNull(-1);
        *getCount(state) = newCount;
    }

    void initializeState(Value& state)
    {
        state = Value(getStateVarSize());
        state.setZero();
        state.setNull(0);
    }

    void accumulate(Value& state, Value const& input)
    {
        state.setNull(-1);
        (*getCount(state))++;
        *((SUM_TYPE*) state.data()) += *((INPUT_TYPE*) input.data());
    }


    virtual void accumulatePayload(Value& state, RLEPayload const* tile)
    {
        SUM_TYPE* dst = (SUM_TYPE*)state.data();
        uint64_t* count = getCount(state);

        if (tile->elementSize() == 0 || tile->isBool()) { 
            ConstRLEPayload::iterator iter = tile->getIterator();
            size_t s;
            while (!iter.end())
            {
                if (iter.isNull())
                {
                    iter.toNextSegment();
                }
                else 
                { 
                    uint64_t repeat = iter.getRepeatCount();
                    state.setNull(-1);
                    if (iter.isSame())
                    {
                        *dst += SUM_TYPE(*(INPUT_TYPE*)iter.getRawValue(s) * repeat);
                        *count += repeat;
                        iter.toNextSegment();
                    }
                    else
                    {
                        *count += repeat;
                        do { 
                            *dst += *(INPUT_TYPE*)iter.getRawValue(s);
                            ++iter;
                        } while (--repeat != 0);
                    }
                }
            }
        } else { 
            INPUT_TYPE* src = (INPUT_TYPE*)tile->getFixData();
            for (size_t i = 0, n = tile->nSegments(); i < n; i++) { 
                ConstRLEPayload::Segment const& segm = tile->getSegment(i);
                size_t len = segm.length();
                if (!segm.null)
                {
                    state.setNull(-1);
                    *count += len;
                    if (segm.same)
                    {
                        *dst += SUM_TYPE(*src++ * len);
                    }
                    else
                    {
                        for (size_t j = 0; j < len; j++) { 
                            *dst += *src++;
                        }
                    }
                }
            }
        }
    }

    void accumulate(Value& state, std::vector<Value> const& input)
    {
        state.setNull(-1);
        (*getCount(state)) += input.size();
        for(size_t i =0; i<input.size(); i++)
        {
            *((SUM_TYPE*) state.data()) += *((INPUT_TYPE*) input[i].data());
        }
    }

    void merge(Value& dstState, Value const& srcState)
    {
        (*getCount(dstState)) += (*getCount(srcState));
        *((SUM_TYPE*) dstState.data()) += *((SUM_TYPE*) srcState.data());
    }

    void finalResult(Value& result, Value const& state)
    {
        if (state.isNull())
        {
            result.setNull();
            return;
        }

        result.setDouble((double) (*(SUM_TYPE*) state.data()) / (double) (*getCount(state)));
    }
};

class AvgAggregateFactory : public AggregateFactory
{
public:
    AvgAggregateFactory():
        AggregateFactory("avg")
    {}

    virtual ~AvgAggregateFactory()
    {}

    virtual bool supportsInputType (Type const& input) const
    {
        if (TID_VOID == input.typeId())
            return false;

        vector<TypeId> inputTypes(2);
        inputTypes[0] = input.typeId();
        inputTypes[1] = input.typeId();

        FunctionDescription functionDesc;
        vector<FunctionPointer> converters;
        bool plusFound = FunctionLibrary::getInstance()->findFunction("+", inputTypes, functionDesc, converters, false);

        if (plusFound == false)
        {
            return false;
        }

        inputTypes[0] = functionDesc.getOutputArg();
        inputTypes[1] = TID_UINT64;

        bool divFound = FunctionLibrary::getInstance()->findFunction("/", inputTypes, functionDesc, converters, false);

        return divFound;
    }

    virtual AggregatePtr createAggregate (Type const& input) const
    {
        if (!supportsInputType(input))
            throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_AGGREGATE_DOESNT_SUPPORT_TYPE) << getName() << input;

        Type stateType = TypeLibrary::getType(TID_BINARY);

        if( input.typeId() == TID_DOUBLE )
        {
            return AggregatePtr (new PrimitiveAverageAggregate<double,double>(input, stateType));
        }
        else if (input.typeId() == TID_FLOAT)
        {
            return AggregatePtr (new PrimitiveAverageAggregate<float,double>(input, stateType));
        }
        else if (input.typeId() == TID_INT64)
        {
            return AggregatePtr (new PrimitiveAverageAggregate<int64_t,int64_t>(input, stateType));
        }
        else if (input.typeId() == TID_INT32)
        {
            return AggregatePtr (new PrimitiveAverageAggregate<int32_t,int64_t>(input, stateType));
        }
        else if (input.typeId() == TID_INT16)
        {
            return AggregatePtr (new PrimitiveAverageAggregate<int16_t,int64_t>(input, stateType));
        }
        else if (input.typeId() == TID_INT8)
        {
            return AggregatePtr (new PrimitiveAverageAggregate<int8_t,int64_t>(input, stateType));
        }
        else if (input.typeId() == TID_UINT64)
        {
            return AggregatePtr (new PrimitiveAverageAggregate<uint64_t,uint64_t>(input, stateType));
        }
        else if (input.typeId() == TID_UINT32)
        {
            return AggregatePtr (new PrimitiveAverageAggregate<uint32_t,uint64_t>(input, stateType));
        }
        else if (input.typeId() == TID_UINT16)
        {
            return AggregatePtr (new PrimitiveAverageAggregate<uint16_t,uint64_t>(input, stateType));
        }
        else if (input.typeId() == TID_UINT8)
        {
            return AggregatePtr (new PrimitiveAverageAggregate<uint8_t,uint64_t>(input, stateType));
        }

        Type sumType = TypeLibrary::getType( propagateTypeToReal(input.typeId()) );

        Expression addValueToState;
        addValueToState.compile("+", false, input.typeId(), sumType.typeId(), sumType.typeId());

        Expression addStateToState;
        addStateToState.compile("+", false, sumType.typeId(), sumType.typeId(), sumType.typeId());

        Expression divideStateByCount;
        divideStateByCount.compile("/", false, sumType.typeId(), TID_UINT64, sumType.typeId());

        size_t stateVarSize = sumType.byteSize() ? sumType.byteSize() + sizeof(uint64_t) : 0;

        vector<TypeId> inputTypes(2);
        inputTypes[0] = input.typeId();
        inputTypes[1] = TID_UINT64;
        FunctionDescription functionDesc;
        vector<FunctionPointer> converters;
        bool mulFound = FunctionLibrary::getInstance()->findFunction("*", inputTypes, functionDesc, converters, false);

        if (mulFound)
        {
            Expression multiplyByInt;
            multiplyByInt.compile("*", false, input.typeId(), TID_UINT64, sumType.typeId());
            return AggregatePtr (new MultiplicativeAvgAggregate(input, stateType, sumType, stateVarSize, addValueToState, addStateToState, divideStateByCount, multiplyByInt));
        }

        return AggregatePtr (new AvgAggregate(input, stateType, sumType, stateVarSize, addValueToState, addStateToState, divideStateByCount));
    }
};

/**
 *
 * Var.
 *
 */
class VarAggregate : public Aggregate
{
protected:
    Expression _add;
    ExpressionContext _addContext;

    Expression _sub;
    ExpressionContext _subContext;

    Expression _mul;
    ExpressionContext _mulContext;

    Expression _fmul;
    ExpressionContext _fmulContext;

    FunctionPointer _converter;
    size_t _vSize;

    VarAggregate(Type const& input, size_t stateSize, Type const& propType, FunctionPointer upConverter,
                 Expression add, Expression subtract, Expression multiply, Expression multiplyByFloat):
        Aggregate("var", input, TypeLibrary::getType(TID_BINARY), propType, stateSize),
        _add(add),
        _addContext(_add),
        _sub(subtract),
        _subContext(_sub),
        _mul(multiply),
        _mulContext(_mul),
        _fmul(multiplyByFloat),
        _fmulContext(_fmul),
        _converter(upConverter),
        _vSize(propType.byteSize())
    {}

public:
    friend class VarAggregateFactory;

    virtual ~VarAggregate()
    {}

    virtual AggregatePtr clone() const
    {
        return AggregatePtr(new VarAggregate(getInputType(), getStateVarSize(), getResultType(), _converter, _add, _sub, _mul, _fmul));
    }

    virtual bool ignoreNulls() const
    {
        return true;
    }

    inline void packState(Value& state, Value const& mean, Value const& m2, uint64_t count)
    {
        char* data = (char*) state.data();
        memcpy(data, mean.data(), mean.size());
        memcpy(data + _vSize, m2.data(), m2.size() );
        *((uint64_t*)(data+_vSize*2)) = count;
    }

    inline void unpackState(Value const& state, Value& mean, Value& m2, uint64_t& count)
    {
        char* data = (char*) state.data();
        memcpy(mean.data(), data, _vSize);
        data += _vSize;
        memcpy(m2.data(), data, _vSize);
        data += _vSize;
        count = *((uint64_t*)data);
    }

    virtual void initializeState(Value& state)
    {
        state = Value(getStateVarSize());
        state.setZero();
        state.setNull(0);
    }

    inline Value sub (Value const& a, Value const& b)
    {
        _subContext[0]=a;
        _subContext[1]=b;
        return _sub.evaluate(_subContext);
    }

    inline Value add (Value const& a, Value const& b)
    {
        _addContext[0]=a;
        _addContext[1]=b;
        return _add.evaluate(_addContext);
    }

    inline Value mul (Value const& a, Value const& b)
    {
        _mulContext[0]=a;
        _mulContext[1]=b;
        return _mul.evaluate(_mulContext);
    }

    inline Value mul (Value const& a, double b)
    {
        _mulContext[0]=a;
        Value vB;
        vB.setDouble(b);
        _mulContext[1]=vB;
        return _mul.evaluate(_mulContext);
    }

    virtual void accumulate(Value& state, Value const& input)
    {
        state.setNull(-1);
        Value mean(_vSize), m2(_vSize);
        uint64_t count;
        unpackState(state,mean,m2,count);
        Value x(_vSize);
        if(_converter)
        {
            const Value* v = &input;
            _converter(&v, &x, 0);
        }
        else
        {
            x = input;
        }

        count++;
        Value delta = sub(x, mean);
        mean = add(mean, mul(delta, 1.0 / (double) count));
        m2 = add(m2, mul(delta, sub(x,mean)));
        packState(state, mean, m2, count);
    }

    void accumulatePayload(Value& state, RLEPayload const* tile)
    {
        ConstRLEPayload::iterator iter = tile->getIterator();
        size_t s;

        Value curMean(_vSize), curM2(_vSize);
        uint64_t curCount;
        unpackState(state,curMean,curM2,curCount);

        Value x(_vSize);
        Value v;

        while (!iter.end())
        {
            if (iter.isNull())
            {
                iter.toNextSegment();
            }
            else if (iter.isSame())
            {
                //we got a run of same values, so mean = val, M2 = 0; take mean, M2 and count and perform merge
                state.setNull(-1);

                uint64_t newCount = curCount + iter.getRepeatCount();
                if(_converter)
                {
                    iter.getItem(v);
                    const Value* pVal = &v;
                    _converter(&pVal, &x, 0);
                }
                else
                {
                    char *ptr = iter.getRawValue(s);
                    x.setData(ptr, s);
                }

                Value delta = sub(x,curMean);
                curMean = mul(add(mul(curMean, (double) curCount), mul(x, (double) iter.getRepeatCount())), 1.0/(double) newCount);
                curM2 = add(curM2, mul( mul(delta,delta), (double) curCount * (double) iter.getRepeatCount() / (double) newCount ));
                curCount = newCount;

                iter.toNextSegment();
            }
            else
            {
                state.setNull(-1);
                if(_converter)
                {
                    iter.getItem(v);
                    const Value* pVal = &v;
                    _converter(&pVal, &x, 0);
                }
                else
                {
                    char *ptr = iter.getRawValue(s);
                    x.setData(ptr, s);
                }

                curCount++;
                Value delta = sub(x, curMean);
                curMean = add(curMean, mul(delta, 1.0 / (double) curCount));
                curM2 = add(curM2, mul(delta, sub(x,curMean)));

                ++iter;
            }
        }

        packState(state, curMean, curM2, curCount);
    }

    virtual void merge(Value& dstState, Value const& srcState)
    {
        Value srcMean(_vSize), srcM2(_vSize);
        uint64_t srcCount;
        unpackState(srcState,srcMean,srcM2,srcCount);

        Value dstMean(_vSize), dstM2(_vSize);
        uint64_t dstCount;
        unpackState(dstState,dstMean,dstM2,dstCount);

        uint64_t newCount = srcCount + dstCount;
        Value delta = sub(dstMean, srcMean);
        Value newMean = mul(add(mul(srcMean, (double) srcCount), mul(dstMean, (double)dstCount)), 1.0/(double)newCount);
        Value newM2 = add(add(srcM2, dstM2), mul( mul(delta,delta), (double) srcCount* (double) dstCount/ ((double)newCount)));

        packState(dstState, newMean, newM2, newCount);
    }

    virtual void finalResult(Value& result, Value const& state)
    {
        if (state.isNull())
        {
            result.setNull();
            return;
        }

        Value mean(_vSize), m2(_vSize);
        uint64_t count;

        unpackState(state,mean,m2,count);

        if (count == 1)
        {
            result.setNull();
            return;
        }

        result = mul(m2, 1.0 / ((double)( count - 1 )));
    }
};

template <typename INPUT_TYPE>
class PrimitiveVarAggregate : public Aggregate
{
protected:
    PrimitiveVarAggregate(Type const& input):
        Aggregate("var", input, TypeLibrary::getType(TID_BINARY), TypeLibrary::getType(TID_DOUBLE), sizeof(double) + sizeof(double) + sizeof(uint64_t))
    {}

public:
    friend class VarAggregateFactory;

    virtual ~PrimitiveVarAggregate()
    {}

    virtual AggregatePtr clone() const
    {
        return AggregatePtr(new PrimitiveVarAggregate<INPUT_TYPE>(getInputType()));
    }

    bool ignoreNulls() const
    {
        return true;
    }

    inline double* mean(Value const& state)
    {
        return (double*) state.data();
    }

    inline double* m2(Value const& state)
    {
        return (double*) (((char*) state.data()) + sizeof(double));
    }

    inline uint64_t* count(Value const& state)
    {
        return (uint64_t*) (((char*) state.data()) + 2 * sizeof(double));
    }

    void initializeState(Value& state)
    {
        state = Value(getStateVarSize());
        state.setZero();
        state.setNull(0);
    }

    void accumulate(Value& state, Value const& input)
    {
        state.setNull(-1);
        (*count(state))++;
        double x = *((INPUT_TYPE *) input.data());
        double delta = x - *mean(state);
        *mean(state) = *mean(state) + delta / (double)(*count(state));
        *m2(state) = *m2(state) + delta * (x - *mean(state));
    }

    void accumulate(Value& state, Value const& input, size_t ct)
    {
        state.setNull(-1);
        for(size_t i =0; i<ct; i++)
        {
            (*count(state))++;
            double x = *((INPUT_TYPE *) input.data());
            double delta = x - *mean(state);
            *mean(state) = *mean(state) + delta / (double)(*count(state));
            *m2(state) = *m2(state) + delta * (x - *mean(state));
        }
    }

    void accumulatePayload(Value& state, RLEPayload const* tile)
    {
        ConstRLEPayload::iterator iter = tile->getIterator();

        double curMean = *mean(state);
        double curM2 = *m2(state);
        uint64_t curCount = *count(state);

        size_t s;
        while (!iter.end())
        {
            if (iter.isNull())
            {
                iter.toNextSegment();
            }
            else if (iter.isSame())
            {
                //we got a run of same values, so mean = val, M2 = 0; take mean, M2 and count and perform merge
                state.setNull(-1);
                uint64_t newCount = curCount + iter.getRepeatCount();
                double val = *((INPUT_TYPE*) iter.getRawValue(s));
                double delta = curMean - val;

                curMean = (curMean * curCount + val * iter.getRepeatCount()) / (double) newCount;
                curM2 = curM2 + delta * delta * (double) (curCount) * (double) iter.getRepeatCount() / (double) newCount;
                curCount = newCount;

                iter.toNextSegment();
            }
            else
            {
                state.setNull(-1);
                curCount ++;
                double x = *((INPUT_TYPE *) iter.getRawValue(s));
                double delta = x - curMean;
                curMean = curMean + delta / (double) curCount;
                curM2 = curM2 + delta * (x - curMean);

                ++iter;
            }
        }

        *count(state) = curCount;
        *mean(state) = curMean;
        *m2(state) = curM2;
    }

    void merge(Value& dstState, Value const& srcState)
    {
        uint64_t newCount = (*count(dstState)) + (*count(srcState));

        double delta = (*mean(dstState) - *mean(srcState));
        double newMean = (*mean(srcState) * (*count(srcState)) + *mean(dstState) * (*count(dstState))) / (double) newCount;
        double newM2 = *m2(srcState) + *m2(dstState) + delta*delta * (double)  (*count(dstState)) * (double) (*count(srcState)) / (double) newCount;

        *count(dstState) = newCount;
        *mean(dstState) = newMean;
        *m2(dstState) = newM2;
    }

    virtual void finalResult(Value& result, Value const& state)
    {
        if (state.isNull())
        {
            result.setNull();
            return;
        }

        if (*count(state) == 1)
        {
            result.setNull();
            return;
        }

        result.setDouble(*m2(state) / ((double)( *count(state) - 1 )));
    }
};


class VarAggregateFactory : public AggregateFactory
{
public:
    VarAggregateFactory():
        AggregateFactory("var")
    {}

    virtual ~VarAggregateFactory()
    {}

    virtual bool supportsInputType (Type const& input) const
    {
        if (TID_VOID == input.typeId())
            return false;

        vector<TypeId> inputTypes(2);
        inputTypes[0] = input.typeId();
        inputTypes[1] = input.typeId();

        FunctionDescription functionDesc;
        vector<FunctionPointer> converters;
        bool plusFound = FunctionLibrary::getInstance()->findFunction("+", inputTypes, functionDesc, converters, false);
        bool mulFound = FunctionLibrary::getInstance()->findFunction("*", inputTypes, functionDesc, converters, false);
        bool minusFound = FunctionLibrary::getInstance()->findFunction("-", inputTypes, functionDesc, converters, false);
        inputTypes[1] = TID_DOUBLE;
        bool fmulFound = FunctionLibrary::getInstance()->findFunction("*", inputTypes, functionDesc, converters, false);

        return plusFound && mulFound && minusFound && fmulFound && input.byteSize() > 0;
    }

    virtual AggregatePtr createAggregate (Type const& input) const
    {
        if (!supportsInputType(input))
            throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_AGGREGATE_DOESNT_SUPPORT_TYPE) << getName() << input;

        if( input.typeId() == TID_DOUBLE )
        {
            return AggregatePtr (new PrimitiveVarAggregate<double>(input));
        }
        else if (input.typeId() == TID_FLOAT)
        {
            return AggregatePtr (new PrimitiveVarAggregate<float>(input));
        }
        else if (input.typeId() == TID_INT64)
        {
            return AggregatePtr (new PrimitiveVarAggregate<int64_t>(input));
        }
        else if (input.typeId() == TID_INT32)
        {
            return AggregatePtr (new PrimitiveVarAggregate<int32_t>(input));
        }
        else if (input.typeId() == TID_INT16)
        {
            return AggregatePtr (new PrimitiveVarAggregate<int16_t>(input));
        }
        else if (input.typeId() == TID_INT8)
        {
            return AggregatePtr (new PrimitiveVarAggregate<int8_t>(input));
        }
        else if (input.typeId() == TID_UINT64)
        {
            return AggregatePtr (new PrimitiveVarAggregate<uint64_t>(input));
        }
        else if (input.typeId() == TID_UINT32)
        {
            return AggregatePtr (new PrimitiveVarAggregate<uint32_t>(input));
        }
        else if (input.typeId() == TID_UINT16)
        {
            return AggregatePtr (new PrimitiveVarAggregate<uint16_t>(input));
        }
        else if (input.typeId() == TID_UINT8)
        {
            return AggregatePtr (new PrimitiveVarAggregate<uint8_t>(input));
        }

        FunctionPointer upConverter = NULL;
        Type propType = TypeLibrary::getType( propagateTypeToReal(input.typeId()) );

        if (propType.byteSize() <= 0)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_VARIABLE_SIZE_TYPES_NOT_SUPPORTED_BY_VAR);

        if (propType.name() != input.name())
        {
            bool vec; //to be revisited
            upConverter = FunctionLibrary::getInstance()->findConverter(input.typeId(), propType.typeId(), vec, false);
        }

        size_t stateSize = propType.byteSize() * 2 + sizeof(uint64_t);

        Expression subtract;
        subtract.compile("-", false, propType.typeId(), propType.typeId(), propType.typeId());

        Expression add;
        add.compile("+", false, propType.typeId(), propType.typeId(), propType.typeId());

        Expression multiply;
        multiply.compile("*", false, propType.typeId(), propType.typeId(), propType.typeId());

        Expression multiplyByFloat;
        multiplyByFloat.compile("*", false, propType.typeId(), TID_DOUBLE, propType.typeId());

        return AggregatePtr (new VarAggregate(input, stateSize, propType, upConverter, add, subtract, multiply, multiplyByFloat));
    }
};

/**
 *
 * Stdev.
 *
 */
class StdevAggregate : public VarAggregate
{
private:
    FunctionPointer _sqrt;

    StdevAggregate(Type const& input, size_t stateSize, Type const& propType, FunctionPointer upConverter,
                   Expression add, Expression subtract, Expression multiply, Expression multiplyByFloat, FunctionPointer sqrt):
        VarAggregate(input, stateSize, propType, upConverter, add, subtract, multiply, multiplyByFloat),
        _sqrt(sqrt)
    {
        _aggregateName = "stdev";
    }

public:
    friend class StdevAggregateFactory;

    ~StdevAggregate()
    {}

    AggregatePtr clone() const
    {
        return AggregatePtr(new StdevAggregate(getInputType(), getStateVarSize(), getResultType(), _converter, _add, _sub, _mul, _fmul, _sqrt));
    }

    void finalResult(Value& result, Value const& state)
    {
        result.setZero();
        VarAggregate::finalResult(result,state);
        if (result.isNull())
        {
            return;
        }

        Value fres(_vSize);
        const Value* v = &result;
        _sqrt(&v, &fres, 0);
        result = fres;
    }
};

template <typename INPUT_TYPE>
class PrimitiveStdevAggregate : public PrimitiveVarAggregate<INPUT_TYPE>
{
    typedef PrimitiveVarAggregate<INPUT_TYPE> super;

private:
    PrimitiveStdevAggregate(Type const& input):
        super(input)
    {
        super::_aggregateName = "stdev";
    }

public:
    friend class StdevAggregateFactory;

    ~PrimitiveStdevAggregate()
    {}

    AggregatePtr clone() const
    {
       return AggregatePtr(new PrimitiveStdevAggregate<INPUT_TYPE>(super::getInputType()));
    }

    void finalResult(Value& result, Value const& state)
    {
        result.setZero();
        super::finalResult(result,state);
        if (result.isNull())
        {
            return;
        }

        result.setDouble(sqrt(result.getDouble()));
    }

};

class StdevAggregateFactory : public AggregateFactory
{
public:
    StdevAggregateFactory():
        AggregateFactory("stdev")
    {}

    virtual ~StdevAggregateFactory()
    {}

    virtual bool supportsInputType (Type const& input) const
    {
        if (TID_VOID == input.typeId())
            return false;

        vector<TypeId> inputTypes(2);
        inputTypes[0] = input.typeId();
        inputTypes[1] = input.typeId();

        FunctionDescription functionDesc;
        vector<FunctionPointer> converters;
        bool plusFound = FunctionLibrary::getInstance()->findFunction("+", inputTypes, functionDesc, converters, false);
        bool mulFound = FunctionLibrary::getInstance()->findFunction("*", inputTypes, functionDesc, converters, false);
        bool minusFound = FunctionLibrary::getInstance()->findFunction("-", inputTypes, functionDesc, converters, false);
        inputTypes[1] = TID_DOUBLE;
        bool fmulFound = FunctionLibrary::getInstance()->findFunction("*", inputTypes, functionDesc, converters, false);

        vector<TypeId> inputType(1);
        inputType[0] = input.typeId();
        bool sqrtfound = FunctionLibrary::getInstance()->findFunction("sqrt", inputType, functionDesc, converters, false);

        return plusFound && mulFound && minusFound && fmulFound && input.byteSize() > 0 && sqrtfound;
    }

    virtual AggregatePtr createAggregate (Type const& input) const
    {
        if (!supportsInputType(input))
            throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_AGGREGATE_DOESNT_SUPPORT_TYPE) << getName() << input;

        if( input.typeId() == TID_DOUBLE )
        {
            return AggregatePtr (new PrimitiveStdevAggregate<double>(input));
        }
        else if (input.typeId() == TID_FLOAT)
        {
            return AggregatePtr (new PrimitiveStdevAggregate<float>(input));
        }
        else if (input.typeId() == TID_INT64)
        {
            return AggregatePtr (new PrimitiveStdevAggregate<int64_t>(input));
        }
        else if (input.typeId() == TID_INT32)
        {
            return AggregatePtr (new PrimitiveStdevAggregate<int32_t>(input));
        }
        else if (input.typeId() == TID_INT16)
        {
            return AggregatePtr (new PrimitiveStdevAggregate<int16_t>(input));
        }
        else if (input.typeId() == TID_INT8)
        {
            return AggregatePtr (new PrimitiveStdevAggregate<int8_t>(input));
        }
        else if (input.typeId() == TID_UINT64)
        {
            return AggregatePtr (new PrimitiveStdevAggregate<uint64_t>(input));
        }
        else if (input.typeId() == TID_UINT32)
        {
            return AggregatePtr (new PrimitiveStdevAggregate<uint32_t>(input));
        }
        else if (input.typeId() == TID_UINT16)
        {
            return AggregatePtr (new PrimitiveStdevAggregate<uint16_t>(input));
        }
        else if (input.typeId() == TID_UINT8)
        {
            return AggregatePtr (new PrimitiveStdevAggregate<uint8_t>(input));
        }

        FunctionPointer upConverter = NULL;
        Type propType = TypeLibrary::getType( propagateTypeToReal(input.typeId()) );

        if (propType.byteSize() <= 0)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_VARIABLE_SIZE_TYPES_NOT_SUPPORTED_BY_VAR);

        if (propType.name() != input.name())
        {
            bool vec; //to be revisited
            upConverter = FunctionLibrary::getInstance()->findConverter( input.typeId(), propType.typeId(), vec, false);
        }

        size_t stateSize = propType.byteSize() * 2 + sizeof(uint64_t);

        Expression subtract;
        subtract.compile("-", false, propType.typeId(), propType.typeId(), propType.typeId());

        Expression add;
        add.compile("+", false, propType.typeId(), propType.typeId(), propType.typeId());

        Expression multiply;
        multiply.compile("*", false, propType.typeId(), propType.typeId(), propType.typeId());

        Expression multiplyByFloat;
        multiplyByFloat.compile("*", false, propType.typeId(), TID_DOUBLE, propType.typeId());

        FunctionDescription functionDesc;
        vector<FunctionPointer> converters;
        vector<TypeId> inputType(1);
        inputType[0] = propType.typeId();
        FunctionLibrary::getInstance()->findFunction("sqrt", inputType, functionDesc, converters, false);

        return AggregatePtr (new StdevAggregate(input, stateSize, propType, upConverter, add, subtract, multiply, multiplyByFloat, functionDesc.getFuncPtr()));
    }
};

/**
 *
 * Min and Max.
 *
 */
class ComparisonAggregate : public Aggregate
{
private:
    Expression        _comparison;
    ExpressionContext _comparisonContext;

    ComparisonAggregate(string const& name, Type const& cType, Expression const& comparison):
        Aggregate(name, cType, cType, cType),
        _comparison(comparison),
        _comparisonContext(_comparison)
    {}

public:
    friend class MinAggregateFactory;
    friend class MaxAggregateFactory;

    ~ComparisonAggregate()
    {}

    AggregatePtr clone() const
    {
        //Expression Contexts cannot be shallow-copied
        return AggregatePtr(new ComparisonAggregate(getName(), getInputType(), _comparison));
    }

    bool ignoreNulls() const
    {
        return true;
    }

    void initializeState(Value& state)
    {
        state.setNull(0);
    }

    void accumulate(Value& state, Value const& input)
    {
        if (state.isNull())
        {
            state = input;
            return;
        }

        _comparisonContext[0] = input;
        _comparisonContext[1] = state;
        Value rslt = _comparison.evaluate(_comparisonContext);

        if (rslt.getBool())
        {
            state = input;
        }
    }

    virtual void accumulatePayload(Value& state, RLEPayload const* tile)
    {
        ConstRLEPayload::iterator iter = tile->getIterator();
        Value input;

        while (!iter.end())
        {
            if (iter.isNull())
            {
                iter.toNextSegment();
            }
            else
            {
                if(state.isNull())
                {
                    iter.getItem(state);
                }
                else
                {
                    iter.getItem(input);
                    _comparisonContext[0]=input;
                    _comparisonContext[1]=state;
                    if (_comparison.evaluate(_comparisonContext).getBool())
                    {
                        state = input;
                    }
                }

                if(iter.isSame())
                {
                    iter.toNextSegment();
                }
                else
                {
                    ++iter;
                }
            }
        }
    }

    void merge(Value& dstState, Value const& srcState)
    {
        _comparisonContext[0] = srcState;
        _comparisonContext[1] = dstState;
        Value rslt = _comparison.evaluate(_comparisonContext);

        if (rslt.getBool())
        {
            dstState = srcState;
        }
    }

    void finalResult(Value& result, Value const& state)
    {
        if (state.isNull())
        {
            result.setNull();
            return;
        }
        result = state;
    }
};


template <typename INPUT_TYPE, bool GREATER_THAN>
class PrimitiveComparisonAggregate : public Aggregate
{
private:
    PrimitiveComparisonAggregate(string const& name, Type const& cType):
        Aggregate(name, cType, cType, cType)
    {}

public:
    friend class MinAggregateFactory;
    friend class MaxAggregateFactory;

    ~PrimitiveComparisonAggregate()
    {}

    AggregatePtr clone() const
    {
        //Expression Contexts cannot be shallow-copied
        return AggregatePtr(new PrimitiveComparisonAggregate(getName(), getInputType()));
    }

    bool ignoreNulls() const
    {
        return true;
    }

    void initializeState(Value& state)
    {
        state = Value(getStateType());
        state.setNull(0);
    }

    void accumulate(Value& state, Value const& input)
    {
        if (state.isNull())
        {
            state = input;
            return;
        }

        if(GREATER_THAN && *((INPUT_TYPE*) input.data()) > *((INPUT_TYPE*) state.data()))
        {
            *((INPUT_TYPE*) state.data()) = *((INPUT_TYPE*) input.data());
        }
        else if (!GREATER_THAN && *((INPUT_TYPE*) input.data()) < *((INPUT_TYPE*) state.data()))
        {
            *((INPUT_TYPE*) state.data()) = *((INPUT_TYPE*) input.data());
        }
    }

    virtual void accumulatePayload(Value& state, RLEPayload const* tile)
    {
        ConstRLEPayload::iterator iter = tile->getIterator();

        bool stNull = state.isNull();
        INPUT_TYPE stVal = *((INPUT_TYPE*) state.data());
        size_t s;

        while (!iter.end())
        {
            if (iter.isNull())
            {
                iter.toNextSegment();
            }
            else
            {
                INPUT_TYPE inVal = *(INPUT_TYPE*) iter.getRawValue(s);
                if(stNull)
                {
                    stNull = false;
                    stVal = inVal;
                }
                else if (GREATER_THAN && inVal > stVal)
                {
                    stVal = inVal;
                }
                else if (!GREATER_THAN && inVal < stVal)
                {
                    stVal = inVal;
                }

                if(iter.isSame())
                {
                    iter.toNextSegment();
                }
                else
                {
                    ++iter;
                }
            }
        }

        if (stNull == false)
        {
            state.setNull(-1);
        }

        *((INPUT_TYPE*) state.data()) = stVal;
    }

    void merge(Value& dstState, Value const& srcState)
    {
        if(GREATER_THAN)
        {
            if ( *((INPUT_TYPE*) srcState.data()) > *((INPUT_TYPE*) dstState.data()) )
            {
                *((INPUT_TYPE*) dstState.data()) = *((INPUT_TYPE*) srcState.data());
            }
        }
        else
        {
            if ( (*((INPUT_TYPE*) srcState.data())) < (*((INPUT_TYPE*) dstState.data())))
            {
                *((INPUT_TYPE*) dstState.data()) = *((INPUT_TYPE*) srcState.data());
            }
        }
    }

    void finalResult(Value& result, Value const& state)
    {
        if (state.isNull())
        {
            result.setNull();
            return;
        }
        result = state;
    }
};


class MinAggregateFactory : public AggregateFactory
{
public:
    MinAggregateFactory():
        AggregateFactory("min")
    {}

    virtual ~MinAggregateFactory()
    {}

    virtual bool supportsInputType (Type const& input) const
    {
        if (TID_VOID == input.typeId())
            return false;

        vector<TypeId> inputTypes(2);
        inputTypes[0] = input.typeId();
        inputTypes[1] = input.typeId();

        FunctionDescription functionDesc;
        vector<FunctionPointer> converters;
        return FunctionLibrary::getInstance()->findFunction("<", inputTypes, functionDesc, converters, false);
    }

    virtual AggregatePtr createAggregate (Type const& input) const
    {
        if (!supportsInputType(input))
            throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_AGGREGATE_DOESNT_SUPPORT_TYPE) << getName() << input;

        if( input.typeId() == TID_DOUBLE )
        {
            return AggregatePtr (new PrimitiveComparisonAggregate<double, false>("min", input));
        }
        else if (input.typeId() == TID_FLOAT)
        {
            return AggregatePtr (new PrimitiveComparisonAggregate<float, false>("min", input));
        }
        else if (input.typeId() == TID_INT64)
        {
            return AggregatePtr (new PrimitiveComparisonAggregate<int64_t, false>("min", input));
        }
        else if (input.typeId() == TID_INT32)
        {
            return AggregatePtr (new PrimitiveComparisonAggregate<int32_t, false>("min", input));
        }
        else if (input.typeId() == TID_INT16)
        {
            return AggregatePtr (new PrimitiveComparisonAggregate<int16_t, false>("min", input));
        }
        else if (input.typeId() == TID_INT8)
        {
            return AggregatePtr (new PrimitiveComparisonAggregate<int8_t, false>("min", input));
        }
        else if (input.typeId() == TID_UINT64)
        {
            return AggregatePtr (new PrimitiveComparisonAggregate<uint64_t, false>("min", input));
        }
        else if (input.typeId() == TID_UINT32)
        {
            return AggregatePtr (new PrimitiveComparisonAggregate<uint32_t, false>("min", input));
        }
        else if (input.typeId() == TID_UINT16)
        {
            return AggregatePtr (new PrimitiveComparisonAggregate<uint16_t, false>("min", input));
        }
        else if (input.typeId() == TID_UINT8)
        {
            return AggregatePtr (new PrimitiveComparisonAggregate<uint8_t, false>("min", input));
        }

        Expression comparison;
        comparison.compile("<", false, input.typeId(), input.typeId(), TID_BOOL);

        return AggregatePtr (new ComparisonAggregate("min", input, comparison));
    }
};

class MaxAggregateFactory : public AggregateFactory
{
public:
    MaxAggregateFactory():
        AggregateFactory("max")
    {}

    virtual ~MaxAggregateFactory()
    {}

    virtual bool supportsInputType (Type const& input) const
    {
        if (TID_VOID == input.typeId())
            return false;

        vector<TypeId> inputTypes(2);
        inputTypes[0] = input.typeId();
        inputTypes[1] = input.typeId();

        FunctionDescription functionDesc;
        vector<FunctionPointer> converters;
        return FunctionLibrary::getInstance()->findFunction(">", inputTypes, functionDesc, converters, false);
    }

    virtual AggregatePtr createAggregate (Type const& input) const
    {
        if (!supportsInputType(input))
            throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_AGGREGATE_DOESNT_SUPPORT_TYPE) << getName() << input;

        if( input.typeId() == TID_DOUBLE )
        {
            return AggregatePtr (new PrimitiveComparisonAggregate<double, true>("max", input));
        }
        else if (input.typeId() == TID_FLOAT)
        {
            return AggregatePtr (new PrimitiveComparisonAggregate<float, true>("max", input));
        }
        else if (input.typeId() == TID_INT64)
        {
            return AggregatePtr (new PrimitiveComparisonAggregate<int64_t, true>("max", input));
        }
        else if (input.typeId() == TID_INT32)
        {
            return AggregatePtr (new PrimitiveComparisonAggregate<int32_t, true>("max", input));
        }
        else if (input.typeId() == TID_INT16)
        {
            return AggregatePtr (new PrimitiveComparisonAggregate<int16_t, true>("max", input));
        }
        else if (input.typeId() == TID_INT8)
        {
            return AggregatePtr (new PrimitiveComparisonAggregate<int8_t, true>("max", input));
        }
        else if (input.typeId() == TID_UINT64)
        {
            return AggregatePtr (new PrimitiveComparisonAggregate<uint64_t, true>("max", input));
        }
        else if (input.typeId() == TID_UINT32)
        {
            return AggregatePtr (new PrimitiveComparisonAggregate<uint32_t, true>("max", input));
        }
        else if (input.typeId() == TID_UINT16)
        {
            return AggregatePtr (new PrimitiveComparisonAggregate<uint16_t, true>("max", input));
        }
        else if (input.typeId() == TID_UINT8)
        {
            return AggregatePtr (new PrimitiveComparisonAggregate<uint8_t, true>("max", input));
        }

        Expression comparison;
        comparison.compile(">", false, input.typeId(), input.typeId(), TID_BOOL);

        return AggregatePtr (new ComparisonAggregate("max", input, comparison));
    }
};

/**
 *
 * Count.
 *
 */
class CountAggregate : public CountingAggregate
{
private:
    CountAggregate(Type const& input):
        CountingAggregate("count", input, TypeLibrary::getType(TID_UINT64), TypeLibrary::getType(TID_UINT64))
    {}

public:
    friend class CountAggregateFactory;

    virtual ~CountAggregate()
    {}

    virtual AggregatePtr clone() const
    {
        //Expression Contexts cannot be shallow-copied
        return AggregatePtr(new CountAggregate(getInputType()));
    }

    virtual void initializeState(Value& state)
    {
        state = Value(getStateType());
        state.setZero();
    }

    virtual bool needsAccumulate() const
    {
        return false;
    }

    virtual void accumulate(Value& state, Value const& input)
    {
        (*((uint64_t*) state.data()))++;
    }

    virtual void accumulate(Value& state, std::vector<Value> const& input)
    {
        (*((uint64_t*) state.data()))+= input.size();
    }

    virtual void accumulatePayload(Value& state, RLEPayload const* tile)
    {
        uint64_t count = *((uint64_t*)state.data());
        ConstRLEPayload::iterator iter = tile->getIterator();
        while (!iter.end())
        {
            if (!iter.isNull())
            {
                count += iter.getSegLength();
            }
            iter.toNextSegment();
        }
        *((uint64_t*)state.data())=count;
    }

    virtual void merge(Value& dstState, Value const& srcState)
    {
        (*((uint64_t*) dstState.data())) += srcState.getUint64();
    }

    void overrideCount(Value& state, uint64_t newCount)
    {
        (*((uint64_t*) state.data())) = newCount;
    }

    void finalResult(Value& result, Value const& state)
    {
        if (state.isNull())
        {
            result.setZero();
            return;
        }

        result = state;
    }

    virtual bool ignoreNulls() const
    {
        return true;
    }

    friend class CountAllAggregate;
};

//This is exact copy of CountAggregate, but we counting nulls too
class CountAllAggregate: public CountAggregate
{
private:
    CountAllAggregate(Type const& input):
        CountAggregate(input)
    {}
public:
    ~CountAllAggregate()
    {}

    friend class CountAggregateFactory;

    AggregatePtr clone() const
    {
        //Expression Contexts cannot be shallow-copied
        return AggregatePtr(new CountAllAggregate(getInputType()));
    }

    void accumulate(Value& state, Value const& input)
    {
        (*((uint64_t*) state.data()))++;
    }

    void accumulate(Value& state, Value const& input, size_t ct)
    {
        (*((uint64_t*) state.data()))+=ct;
    }

    void accumulate(Value& state, std::vector<Value> const& input)
    {
        (*((uint64_t*) state.data()))+= input.size();
    }

    virtual void accumulatePayload(Value& state, RLEPayload const* tile)
    {
        *((uint64_t*)state.data()) += tile->count();
    }

    bool ignoreNulls() const
    {
        return false;
    }

    bool needsAccumulate() const
    {
        return false;
    }
};

class CountAggregateFactory : public AggregateFactory
{
public:
    CountAggregateFactory():
        AggregateFactory("count")
    {}

    virtual ~CountAggregateFactory()
    {}

    virtual bool supportsInputType (Type const& input) const
    {
        return true;
    }

    virtual AggregatePtr createAggregate (Type const& input) const
    {
        // When VOID passed, we counting all cells without null handling
        if (TID_VOID == input.typeId())
        {
            return AggregatePtr (new CountAllAggregate(input));
        }
        // Otherwise checking every attribute on null value
        else
        {
            return AggregatePtr (new CountAggregate(input));
        }
    }
};

AggregateLibrary::AggregateLibrary()
{
    addAggregateFactory( AggregateFactoryPtr (new SumAggregateFactory()) );
    addAggregateFactory( AggregateFactoryPtr (new AvgAggregateFactory()) );
    addAggregateFactory( AggregateFactoryPtr (new MinAggregateFactory()) );
    addAggregateFactory( AggregateFactoryPtr (new MaxAggregateFactory()) );
    addAggregateFactory( AggregateFactoryPtr (new VarAggregateFactory()) );
    addAggregateFactory( AggregateFactoryPtr (new StdevAggregateFactory()) );
    addAggregateFactory( AggregateFactoryPtr (new CountAggregateFactory()));
}

} // namespace scidb
