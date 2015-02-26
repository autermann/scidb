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
#include "query/TileFunctions.h"

#include "query/ops/analyze/AnalyzeAggregate.h"

#include <math.h>
#include <log4cxx/logger.h>

using boost::shared_ptr;
using namespace std;

namespace scidb
{

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.builtin_aggregates"));

class ExpressionAggregate : public Aggregate
{
protected:
    Type _stateType;

    string _accumulateOp;
    Expression _accumulateExpression;
    boost::shared_ptr<ExpressionContext> _accumulateContext;

    string _mergeOp;
    Expression _mergeExpression;
    boost::shared_ptr<ExpressionContext> _mergeContext;

    bool _initByFirstValue;

public:
    ExpressionAggregate(const string& name, Type const& aggregateType, Type const& stateType, Type const& resultType,
                        const string& accumulateOp, const string& mergeOp, bool initByFirstValue = false):
        Aggregate(name, aggregateType, resultType), _stateType(stateType), _accumulateOp(accumulateOp), _mergeOp(mergeOp), _initByFirstValue(initByFirstValue)
    {
        vector<string> names(2);
        names[0] = "a";
        names[1] = "b";
        vector<TypeId> types(2);
        types[0] = stateType.typeId();
        types[1] = aggregateType.typeId();
        _accumulateExpression.compile(accumulateOp, names, types, stateType.typeId());
        _accumulateContext = boost::shared_ptr<ExpressionContext>(new ExpressionContext(_accumulateExpression));

        types[1] = stateType.typeId();
        _mergeExpression.compile(mergeOp, names, types);
        _mergeContext = boost::shared_ptr<ExpressionContext>(new ExpressionContext(_mergeExpression));
    }

    virtual AggregatePtr clone() const
    {
        return AggregatePtr(new ExpressionAggregate(getName(), getAggregateType(), getStateType(), getResultType(), _accumulateOp, _mergeOp, _initByFirstValue));
    }

    AggregatePtr clone(Type const& aggregateType) const
    {
        return AggregatePtr(new ExpressionAggregate(getName(), aggregateType, aggregateType, _resultType.typeId() == TID_VOID ? aggregateType : _resultType, _accumulateOp, _mergeOp, _initByFirstValue));
    }

    bool ignoreNulls() const
    {
        return true;
    }

    Type getStateType() const
    {
        return _stateType;
    }

    void initializeState(Value& state)
    {
        state = Value(getStateType());
        setDefaultValue(state, getStateType().typeId());
        if (_initByFirstValue) {
            //We use missing code 1 because missing code 0 has special meaning to the aggregate framework.
            state.setNull(1);
        }
    }

    void accumulate(Value& state, Value const& input)
    {
        if (_initByFirstValue && state.isNull()) {
            state = input;
        } else {
            (*_accumulateContext)[0] = state;
            (*_accumulateContext)[1] = input;
            state = _accumulateExpression.evaluate(*_accumulateContext);
        }
    }

    virtual void accumulatePayload(Value& state, ConstRLEPayload const* tile)
    {
        ConstRLEPayload::iterator iter = tile->getIterator();
        while (!iter.end())
        {
            if (iter.isNull())
            {
                iter.toNextSegment();
            }
            else
            {
                if (_initByFirstValue && state.isNull()) {
                    iter.getItem(state);
                } else {
                    (*_accumulateContext)[0]=state;
                    iter.getItem((*_accumulateContext)[1]);
                    state = _accumulateExpression.evaluate(*_accumulateContext);
                }
                ++iter;
            }
        }
    }

    void merge(Value& dstState, Value const& srcState)
    {
        (*_mergeContext)[0] = dstState;
        (*_mergeContext)[1] = srcState;
        dstState = _mergeExpression.evaluate(*_mergeContext);
    }

    void finalResult(Value& result, Value const& state)
    {
        result = state;
    }
};

class CountAggregate : public CountingAggregate
{
private:
    bool _ignoreNulls;

public:
    CountAggregate(Type const& aggregateType):
        CountingAggregate("count", aggregateType, TypeLibrary::getType(TID_UINT64)), _ignoreNulls(aggregateType.typeId() != TID_VOID)
    {}

    AggregatePtr clone() const
    {
        return AggregatePtr(new CountAggregate(getAggregateType()));
    }

    AggregatePtr clone(Type const& aggregateType) const
    {
        return AggregatePtr(new CountAggregate(aggregateType));
    }

    Type getStateType() const
    {
        return TypeLibrary::getType(TID_UINT64);
    }

    bool supportAsterisk() const
    {
        return true;
    }

    void initializeState(Value& state)
    {
        state = Value(getStateType());
        setDefaultValue(state, getStateType().typeId());
    }

    bool ignoreNulls() const
    {
        return _ignoreNulls;
    }

    bool needsAccumulate() const
    {
        return false;
    }

    void accumulate(Value& state, Value const& input)
    {
        (*((uint64_t*) state.data()))++;
    }

    void accumulate(Value& state, std::vector<Value> const& input)
    {
        (*((uint64_t*) state.data()))+= input.size();
    }

    void accumulatePayload(Value& state, ConstRLEPayload const* tile)
    {
        if (!ignoreNulls()) {
            *((uint64_t*)state.data()) += tile->count();
        } else {
            ConstRLEPayload::iterator iter = tile->getIterator();
            while (!iter.end())
            {
                if (!iter.isNull())
                {
                    *((uint64_t*)state.data()) += iter.getSegLength();
                }
                iter.toNextSegment();
            }
        }
    }

    void merge(Value& dstState, Value const& srcState)
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
            setDefaultValue(result, getResultType().typeId());
            return;
        }

        result = state;
    }
};

AggregateLibrary::AggregateLibrary()
{
    /** SUM **/
    addAggregate(AggregatePtr(new ExpressionAggregate("sum", TypeLibrary::getType(TID_VOID), TypeLibrary::getType(TID_VOID), TypeLibrary::getType(TID_VOID), "a+b", "a+b")));

    addAggregate(AggregatePtr(new BaseAggregate<AggSum, int8_t, int64_t>("sum", TypeLibrary::getType(TID_INT8), TypeLibrary::getType(TID_INT64))));
    addAggregate(AggregatePtr(new BaseAggregate<AggSum, int16_t, int64_t>("sum", TypeLibrary::getType(TID_INT16), TypeLibrary::getType(TID_INT64))));
    addAggregate(AggregatePtr(new BaseAggregate<AggSum, int32_t, int64_t>("sum", TypeLibrary::getType(TID_INT32), TypeLibrary::getType(TID_INT64))));
    addAggregate(AggregatePtr(new BaseAggregate<AggSum, int64_t, int64_t>("sum", TypeLibrary::getType(TID_INT64), TypeLibrary::getType(TID_INT64))));
    addAggregate(AggregatePtr(new BaseAggregate<AggSum, uint8_t, uint64_t>("sum", TypeLibrary::getType(TID_UINT8), TypeLibrary::getType(TID_UINT64))));
    addAggregate(AggregatePtr(new BaseAggregate<AggSum, uint16_t, uint64_t>("sum", TypeLibrary::getType(TID_UINT16), TypeLibrary::getType(TID_UINT64))));
    addAggregate(AggregatePtr(new BaseAggregate<AggSum, uint32_t, uint64_t>("sum", TypeLibrary::getType(TID_UINT32), TypeLibrary::getType(TID_UINT64))));
    addAggregate(AggregatePtr(new BaseAggregate<AggSum, uint64_t, uint64_t>("sum", TypeLibrary::getType(TID_UINT64), TypeLibrary::getType(TID_UINT64))));
    addAggregate(AggregatePtr(new BaseAggregate<AggSum, float, double>("sum", TypeLibrary::getType(TID_FLOAT), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggSum, double, double>("sum", TypeLibrary::getType(TID_DOUBLE), TypeLibrary::getType(TID_DOUBLE))));

    /** COUNT **/
    addAggregate(AggregatePtr(new CountAggregate(TypeLibrary::getType(TID_VOID))));

    /** AVG **/
    addAggregate(AggregatePtr(new BaseAggregate<AggAvg, int8_t, double>("avg", TypeLibrary::getType(TID_INT8), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggAvg, int16_t, double>("avg", TypeLibrary::getType(TID_INT16), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggAvg, int32_t, double>("avg", TypeLibrary::getType(TID_INT32), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggAvg, int64_t, double>("avg", TypeLibrary::getType(TID_INT64), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggAvg, uint8_t, double>("avg", TypeLibrary::getType(TID_UINT8), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggAvg, uint16_t, double>("avg", TypeLibrary::getType(TID_UINT16), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggAvg, uint32_t, double>("avg", TypeLibrary::getType(TID_UINT32), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggAvg, uint64_t, double>("avg", TypeLibrary::getType(TID_UINT64), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggAvg, float, double>("avg", TypeLibrary::getType(TID_FLOAT), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggAvg, double, double>("avg", TypeLibrary::getType(TID_DOUBLE), TypeLibrary::getType(TID_DOUBLE))));

    /** MIN **/
    addAggregate(AggregatePtr(new ExpressionAggregate("min", TypeLibrary::getType(TID_VOID), TypeLibrary::getType(TID_VOID), TypeLibrary::getType(TID_VOID), "iif(a < b, a, b)", "iif(a < b, a, b)", true)));

    addAggregate(AggregatePtr(new BaseAggregateInitByFirst<AggMin, int8_t, int8_t>("min", TypeLibrary::getType(TID_INT8), TypeLibrary::getType(TID_INT8))));
    addAggregate(AggregatePtr(new BaseAggregateInitByFirst<AggMin, int16_t, int16_t>("min", TypeLibrary::getType(TID_INT16), TypeLibrary::getType(TID_INT16))));
    addAggregate(AggregatePtr(new BaseAggregateInitByFirst<AggMin, int32_t, int32_t>("min", TypeLibrary::getType(TID_INT32), TypeLibrary::getType(TID_INT32))));
    addAggregate(AggregatePtr(new BaseAggregateInitByFirst<AggMin, int64_t, int64_t>("min", TypeLibrary::getType(TID_INT64), TypeLibrary::getType(TID_INT64))));
    addAggregate(AggregatePtr(new BaseAggregateInitByFirst<AggMin, uint8_t, uint8_t>("min", TypeLibrary::getType(TID_UINT8), TypeLibrary::getType(TID_UINT8))));
    addAggregate(AggregatePtr(new BaseAggregateInitByFirst<AggMin, uint16_t, uint16_t>("min", TypeLibrary::getType(TID_UINT16), TypeLibrary::getType(TID_UINT16))));
    addAggregate(AggregatePtr(new BaseAggregateInitByFirst<AggMin, uint32_t, uint32_t>("min", TypeLibrary::getType(TID_UINT32), TypeLibrary::getType(TID_UINT32))));
    addAggregate(AggregatePtr(new BaseAggregateInitByFirst<AggMin, uint64_t, uint64_t>("min", TypeLibrary::getType(TID_UINT64), TypeLibrary::getType(TID_UINT64))));
    addAggregate(AggregatePtr(new BaseAggregateInitByFirst<AggMin, float, float>("min", TypeLibrary::getType(TID_FLOAT), TypeLibrary::getType(TID_FLOAT))));
    addAggregate(AggregatePtr(new BaseAggregateInitByFirst<AggMin, double, double>("min", TypeLibrary::getType(TID_DOUBLE), TypeLibrary::getType(TID_DOUBLE))));

    /** MAX **/
    addAggregate(AggregatePtr(new ExpressionAggregate("max", TypeLibrary::getType(TID_VOID), TypeLibrary::getType(TID_VOID), TypeLibrary::getType(TID_VOID), "iif(a > b, a, b)", "iif(a > b, a, b)", true)));

    addAggregate(AggregatePtr(new BaseAggregateInitByFirst<AggMax, int8_t, int8_t>("max", TypeLibrary::getType(TID_INT8), TypeLibrary::getType(TID_INT8))));
    addAggregate(AggregatePtr(new BaseAggregateInitByFirst<AggMax, int16_t, int16_t>("max", TypeLibrary::getType(TID_INT16), TypeLibrary::getType(TID_INT16))));
    addAggregate(AggregatePtr(new BaseAggregateInitByFirst<AggMax, int32_t, int32_t>("max", TypeLibrary::getType(TID_INT32), TypeLibrary::getType(TID_INT32))));
    addAggregate(AggregatePtr(new BaseAggregateInitByFirst<AggMax, int64_t, int64_t>("max", TypeLibrary::getType(TID_INT64), TypeLibrary::getType(TID_INT64))));
    addAggregate(AggregatePtr(new BaseAggregateInitByFirst<AggMax, uint8_t, uint8_t>("max", TypeLibrary::getType(TID_UINT8), TypeLibrary::getType(TID_UINT8))));
    addAggregate(AggregatePtr(new BaseAggregateInitByFirst<AggMax, uint16_t, uint16_t>("max", TypeLibrary::getType(TID_UINT16), TypeLibrary::getType(TID_UINT16))));
    addAggregate(AggregatePtr(new BaseAggregateInitByFirst<AggMax, uint32_t, uint32_t>("max", TypeLibrary::getType(TID_UINT32), TypeLibrary::getType(TID_UINT32))));
    addAggregate(AggregatePtr(new BaseAggregateInitByFirst<AggMax, uint64_t, uint64_t>("max", TypeLibrary::getType(TID_UINT64), TypeLibrary::getType(TID_UINT64))));
    addAggregate(AggregatePtr(new BaseAggregateInitByFirst<AggMax, float, float>("max", TypeLibrary::getType(TID_FLOAT), TypeLibrary::getType(TID_FLOAT))));
    addAggregate(AggregatePtr(new BaseAggregateInitByFirst<AggMax, double, double>("max", TypeLibrary::getType(TID_DOUBLE), TypeLibrary::getType(TID_DOUBLE))));

    /** VAR **/
    addAggregate(AggregatePtr(new BaseAggregate<AggVar, int8_t, double>("var", TypeLibrary::getType(TID_INT8), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggVar, int16_t, double>("var", TypeLibrary::getType(TID_INT16), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggVar, int32_t, double>("var", TypeLibrary::getType(TID_INT32), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggVar, int64_t, double>("var", TypeLibrary::getType(TID_INT64), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggVar, uint8_t, double>("var", TypeLibrary::getType(TID_UINT8), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggVar, uint16_t, double>("var", TypeLibrary::getType(TID_UINT16), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggVar, uint32_t, double>("var", TypeLibrary::getType(TID_UINT32), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggVar, uint64_t, double>("var", TypeLibrary::getType(TID_UINT64), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggVar, float, double>("var", TypeLibrary::getType(TID_FLOAT), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggVar, double, double>("var", TypeLibrary::getType(TID_DOUBLE), TypeLibrary::getType(TID_DOUBLE))));

    /** STDEV **/
    addAggregate(AggregatePtr(new BaseAggregate<AggStDev, int8_t, double>("stdev", TypeLibrary::getType(TID_INT8), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggStDev, int16_t, double>("stdev", TypeLibrary::getType(TID_INT16), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggStDev, int32_t, double>("stdev", TypeLibrary::getType(TID_INT32), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggStDev, int64_t, double>("stdev", TypeLibrary::getType(TID_INT64), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggStDev, uint8_t, double>("stdev", TypeLibrary::getType(TID_UINT8), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggStDev, uint16_t, double>("stdev", TypeLibrary::getType(TID_UINT16), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggStDev, uint32_t, double>("stdev", TypeLibrary::getType(TID_UINT32), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggStDev, uint64_t, double>("stdev", TypeLibrary::getType(TID_UINT64), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggStDev, float, double>("stdev", TypeLibrary::getType(TID_FLOAT), TypeLibrary::getType(TID_DOUBLE))));
    addAggregate(AggregatePtr(new BaseAggregate<AggStDev, double, double>("stdev", TypeLibrary::getType(TID_DOUBLE), TypeLibrary::getType(TID_DOUBLE))));

    /** ApproxDC (ANALYZE) **/
    addAggregate(AggregatePtr(new AnalyzeAggregate()));
}


} // namespace scidb
