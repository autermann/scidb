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
 * @file TileFunctions.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Templates of implementation tile functions
 */

#ifndef TILEFUNCTIONS_H
#define TILEFUNCTIONS_H

#include <query/TypeSystem.h>

namespace scidb
{

struct VarValue
{
    boost::shared_ptr<Value> value;
    boost::shared_ptr< std::vector<char> > varPart;
};

/**
 * A set of helper templates for code generation related to adding and setting values
 */
template <typename T>
size_t addPayloadFixedValues(RLEPayload* p, size_t n = 1)
{
    return p->addRawValues(n);
}

template <>
size_t addPayloadFixedValues<bool>(RLEPayload* p, size_t n)
{
    return p->addBoolValues(n);
}

template <>
size_t addPayloadFixedValues<VarValue>(RLEPayload* p, size_t n)
{
    // Value will be really added via appendValue in set helper
    return 0;
}

template <typename T>
void setPayloadFixedValue(RLEPayload* p, size_t index, T value) {
    T* data = (T*)p->getRawValue(index);
    *data = value;
}
// Specialization for bool
template <>
void setPayloadFixedValue<bool>(RLEPayload* p, size_t index, bool value) {
    char* data = p->getFixData();
    if (value) {
        data[index >> 3] |= 1 << (index & 7);
    } else {
        data[index >> 3] &= ~(1 << (index & 7));
    }
}

template <>
void setPayloadFixedValue<VarValue>(RLEPayload* p, size_t index, VarValue value)
{
    p->appendValue(*value.varPart, *value.value, index);
}

template <typename T>
T getPayloadFixedValue(const RLEPayload* p, size_t index) {
    return *(T*)p->getRawValue(index);
}
// Specialization for bool
template <>
bool getPayloadFixedValue<bool>(const RLEPayload* p, size_t index) {
    return p->checkBit(index);
}

template <>
VarValue getPayloadFixedValue<VarValue>(const RLEPayload* p, size_t index) {
    VarValue res;
    res.value = boost::shared_ptr<Value>(new Value());
    p->getValueByIndex(*res.value, index);
    return res;
}

// UNARY TEMPLATE FUNCTIONS

template<typename T, typename TR>
struct UnaryFixedMinus
{
    static TR func(T v)
    {
        return -v;
    }
};

template<typename T, typename TR>
struct UnaryFixedFunctionCall
{
    typedef TR (*FunctionPointer)(T);
    template<FunctionPointer F>
    struct Function
    {
        template<typename T1, typename TR1>
        struct Op
        {
            static TR1 func(T1 v)
            {
                return F(v);
            }
        };
    };
};

template<typename T, typename TR>
struct UnaryFixedConverter
{
    static TR func(T v)
    {
        return v;
    }
};

/**
 * Template of function for unary operations.
 */
template<template <typename T, typename TR> class O, typename T, typename TR>
void rle_unary_fixed_func(const Value** args,  Value* result, void*)
{
    const Value& v = *args[0];
    Value& res = *result;
    res.getTile()->clear();
    res.getTile()->assignSegments(*v.getTile());
    const size_t valuesCount = v.getTile()->getValuesCount();
    addPayloadFixedValues<TR>(res.getTile(), valuesCount);
    size_t i = 0;
    T* s = (T*)v.getTile()->getFixData();
    T* end = s + valuesCount;
    while (s < end) {
        setPayloadFixedValue<TR>(res.getTile(), i++, O<T, TR>::func(*s++));
    }
}

void rle_unary_bool_not(const Value** args,  Value* result, void*)
{
    const Value& v = *args[0];
    Value& res = *result;
    res.getTile()->clear();
    res.getTile()->assignSegments(*v.getTile());
    const size_t valuesCount = v.getTile()->getValuesCount();
    addPayloadFixedValues<bool>(res.getTile(), valuesCount);
    const char* s = (const char*)v.getTile()->getFixData();
    char* r = (char*)res.getTile()->getFixData();
    const char* end = s + (valuesCount >> 3) + 1;
    // Probably can be optimized by using DWORD instead of char
    while (s < end) {
        *r++ = ~(*s++);
    }
}
// BINARY TEMPLATE FUNCTIONS

template<typename T1, typename T2, typename TR>
struct BinaryFixedPlus
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 + v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryFixedMinus
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 - v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryFixedMult
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 * v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryFixedDiv
{
    static TR func(T1 v1, T2 v2)
    {
        if (0 == v2)
            throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_DIVISION_BY_ZERO);
        return v1 / v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryFixedMod
{
    static TR func(T1 v1, T2 v2)
    {
        if (0 == v2)
            throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_DIVISION_BY_ZERO);
        return v1 % v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryFixedAnd
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 && v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryFixedOr
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 || v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryFixedLess
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 < v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryFixedLessOrEq
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 <= v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryFixedEq
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 == v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryFixedNotEq
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 != v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryFixedGreater
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 > v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryFixedGreaterOrEq
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 >= v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryFixedFunctionCall
{
    typedef TR (*FunctionPointer)(T1, T2);
    template<FunctionPointer F>
    struct Function
    {
        template<typename T1_, typename T2_, typename TR1_>
        struct Op
        {
            static TR1_ func(T1_ v1, T2_ v2)
            {
                return F(v1, v2);
            }
        };
    };
};


template<typename T1, typename T2, typename TR>
struct BinaryStringPlus
{
    static VarValue func(VarValue v1, VarValue v2)
    {
        const string str1(v1.value->getString());
        const string str2(v2.value->getString());
        VarValue res;
        res.value = boost::shared_ptr<Value>(new Value());
        res.value->setString((str1 + str2).c_str());
        return res;
    }
};

template<typename T>
void setVarPart(T& v, const boost::shared_ptr<std::vector<char> >& part)
{
    // Nothing for every datatype except VarType which is specified below
}

template<>
void setVarPart<VarValue>(VarValue& v, const boost::shared_ptr<std::vector<char> >& part)
{
    v.varPart = part;
}

/**
 * This template is for binary function for types with fixed size.
 * This function cannot preserve RLE structure.
 * Arguments of this function should be extracted using the same empty bitmask.
 * In other words they should be alligned during unpack.
 */
template<template <typename T1, typename T2, typename TR> class O, typename T1, typename T2, typename TR>
void rle_binary_fixed_func(const Value** args, Value* result, void*)
{
    const Value& v1 = *args[0];
    const Value& v2 = *args[1];
    Value& res = *result;
    res.getTile()->clear();
    boost::shared_ptr<std::vector<char> > varPart(new std::vector<char>());

    if (v1.getTile()->nSegments() == 0 ||  v2.getTile()->nSegments() == 0) {
        res.getTile()->flush(0);
        return;
    }

    size_t i1 = 0;
    size_t i2 = 0;
    RLEPayload::Segment ps1 = v1.getTile()->getSegment(i1);
    uint64_t ps1_length = v1.getTile()->getSegment(i1).length();
    RLEPayload::Segment ps2 = v2.getTile()->getSegment(i2);
    uint64_t ps2_length = v2.getTile()->getSegment(i2).length();
    uint64_t chunkSize = 0;

    if (ps1_length == INFINITE_LENGTH) {
        // v1 is constant with infinity length. Allign it pPosition to v2
        ps1.pPosition = ps2.pPosition;
    } else {
        if (ps2_length == INFINITE_LENGTH) {
            // v2 is constant with infinity length. Allign it pPosition to v1
            ps2.pPosition = ps1.pPosition;
        }
    }

    while (true)
    {
        // At this point ps1 and ps2 should alligned
        // Segment with less length will be iterated and with more length cut at the end of loop
        assert(ps1.pPosition == ps2.pPosition);
        const uint64_t length = std::min(ps1_length, ps2_length);
        RLEPayload::Segment rs;

        // Check NULL cases
        if ( (ps1.same && ps1.null) || (ps2.same && ps2.null) ) {
            rs.same = true;
            rs.null = true;
            rs.pPosition = ps1.pPosition;
            rs.valueIndex = ps1.null ? ps1.valueIndex : ps2.valueIndex; // which missingReason is to use?
        } else { // No one is NULL and we can evaluate
            rs.null = false;
            rs.pPosition = ps1.pPosition;
            if (ps1.same) {
                if (ps2.same) {
                    rs.same = true;
                    rs.valueIndex = addPayloadFixedValues<TR>(res.getTile());
                    TR r = O<T1, T2, TR>::func(getPayloadFixedValue<T1>(v1.getTile(), ps1.valueIndex),
                                               getPayloadFixedValue<T2>(v2.getTile(), ps2.valueIndex));
                    setVarPart<TR>(r, varPart);
                    setPayloadFixedValue<TR>(res.getTile(), rs.valueIndex, r);
                } else {
                    rs.same = false;
                    rs.valueIndex = addPayloadFixedValues<TR>(res.getTile(), length);
                    size_t i = rs.valueIndex;
                    size_t j = ps2.valueIndex;
                    const size_t end = j + length;
                    while (j < end) {
                        TR r = O<T1, T2, TR>::func(getPayloadFixedValue<T1>(v1.getTile(), ps1.valueIndex),
                                                   getPayloadFixedValue<T2>(v2.getTile(), j++));
                        setVarPart<TR>(r, varPart);
                        setPayloadFixedValue<TR>(res.getTile(), i++, r);
                    }
                }
            } else { // dense non-nullable data
                if (ps2.same) {
                    rs.same = false;
                    rs.valueIndex = addPayloadFixedValues<TR>(res.getTile(), length);
                    size_t i = rs.valueIndex;
                    size_t j = ps1.valueIndex;
                    const size_t end = j + length;
                    while (j < end) {
                        TR r = O<T1, T2, TR>::func(getPayloadFixedValue<T1>(v1.getTile(), j++),
                                                   getPayloadFixedValue<T2>(v2.getTile(), ps2.valueIndex));
                        setVarPart<TR>(r, varPart);
                        setPayloadFixedValue<TR>(res.getTile(), i++, r);
                    }
                } else {
                    rs.same = false;
                    rs.valueIndex = addPayloadFixedValues<TR>(res.getTile(), length);
                    size_t i = rs.valueIndex;
                    size_t j1 = ps1.valueIndex;
                    size_t j2 = ps2.valueIndex;
                    const size_t end = j1 + length;
                    while (j1 < end) {
                        // TODO: This is not optimal implementation for boolean. Must be changed to bit operations
                        TR r = O<T1, T2, TR>::func(getPayloadFixedValue<T1>(v1.getTile(), j1++),
                                                   getPayloadFixedValue<T2>(v2.getTile(), j2++));
                        setVarPart<TR>(r, varPart);
                        setPayloadFixedValue<TR>(res.getTile(), i++, r);
                    }
                }
            }
        }
        res.getTile()->addSegment(rs);
        chunkSize = rs.pPosition + length;

        // Moving to the next segments
        if (ps1_length == ps2_length) {
            if (++i1 >= v1.getTile()->nSegments())
                break;
            if (++i2 >= v2.getTile()->nSegments())
                break;
            ps1 = v1.getTile()->getSegment(i1);
            ps1_length = v1.getTile()->getSegment(i1).length();
            ps2 = v2.getTile()->getSegment(i2);
            ps2_length = v2.getTile()->getSegment(i2).length();
        } else {
            if (ps1_length < ps2_length) {
                if (++i1 >= v1.getTile()->nSegments())
                    break;
                ps1 = v1.getTile()->getSegment(i1);
                ps1_length = v1.getTile()->getSegment(i1).length();
                ps2.pPosition += length;
                ps2_length -= length;
                if (!ps2.same)
                    ps2.valueIndex += length;
            } else {
                if (++i2 >= v2.getTile()->nSegments())
                    break;
                ps2 = v2.getTile()->getSegment(i2);
                ps2_length = v2.getTile()->getSegment(i2).length();
                ps1.pPosition += length;
                ps1_length -= length;
                if (!ps1.same)
                    ps1.valueIndex += length;
            }
        }
    }
    res.getTile()->flush(chunkSize);
    if (varPart->size()) {
        res.getTile()->setVarPart(*varPart);
    }
}


/**
 * tile implementation of is_null function
 * this function is polymorphic and will require changes to provide inferring type result function.
 */
void inferIsNullArgTypes(const ArgTypes& factInputArgs, std::vector<ArgTypes>& possibleInputArgs, std::vector<TypeId>& possibleResultArgs)
{
    possibleInputArgs.resize(1);
    possibleInputArgs[0] = factInputArgs;
    possibleResultArgs.resize(1);
    possibleResultArgs[0] = TID_BOOL;
}

void rle_unary_bool_is_null(const Value** args, Value* result, void*)
{
    const RLEPayload* vTile = args[0]->getTile();
    RLEPayload* rTile =  result->getTile();
    rTile->clear();
    rTile->addBoolValues(2);
    *rTile->getFixData() = 2;
    const RLEPayload::Segment* v = NULL;
    position_t tail = 0;
    for (size_t i = 0; i < vTile->nSegments(); i++)
    {
        v = &vTile->getSegment(i);
        RLEPayload::Segment r;
        r.null = false;
        r.pPosition = v->pPosition;
        r.same = v->length() > 1;
        r.valueIndex = v->null ? 1 : 0;
        rTile->addSegment(r);
        tail = v->pPosition + v->length();
    }
    rTile->flush(tail);
}

void rle_unary_null_to_any(const Value** args, Value* result, void*)
{
    const RLEPayload* vTile = args[0]->getTile();
    RLEPayload* rTile =  result->getTile();
    rTile->clear();
    rTile->assignSegments(*vTile);
}

/* Aggregator classes */
template <typename TS, typename TSR>
class TileToSum
{
private:
    TSR _sum;

public:
    TileToSum() {
        memset(&_sum, 0, sizeof(_sum));
    }

    void init(const TS& value)
    {
        _sum = value;
    }

    void aggregate(const TS& value)
    {
        _sum += value;
    }

    void multAggregate(const TS& value, uint64_t count)
    {
        _sum += value * count;
    }

    const TSR& final()
    {
        return _sum;
    }
};

template <typename TS, typename TSR>
class TileToCount
{
private:
    uint64_t _count;

public:
    TileToCount() {
        _count = 0;
    }

    void init(const TS& value)
    {
        _count = 1;
    }

    void aggregate(const TS& value)
    {
        _count++;
    }

    void multAggregate(const TS& value, uint64_t count)
    {
        _count += count;
    }

    uint64_t final()
    {
        return _count;
    }
};

template <typename TS, typename TSR>
class TileToMin
{
private:
    TSR _min;

public:
    TileToMin() {
        memset(&_min, 0, sizeof(_min));
    }

    void init(const TS& value)
    {
        _min = value;
    }

    void aggregate(const TS& value)
    {
        if (value < _min)
            _min = value;
    }

    void multAggregate(const TS& value, uint64_t count)
    {
        if (value < _min)
            _min = value;
    }

    const TSR& final()
    {
        return _min;
    }
};

template <typename TS, typename TSR>
class TileToMax
{
private:
    TSR _max;

public:
    TileToMax() {
        memset(&_max, 0, sizeof(_max));
    }

    void init(const TS& value)
    {
        _max = value;
    }

    void aggregate(const TS& value)
    {
        if (value > _max)
            _max = value;
    }

    void multAggregate(const TS& value, uint64_t count)
    {
        if (value > _max)
            _max = value;
    }

    const TSR& final()
    {
        return _max;
    }
};

template <typename TS, typename TSR>
class TileToAvg
{
private:
    TSR _sum;
    uint64_t _count;

public:
    TileToAvg() {
        memset(&_sum, 0, sizeof(_sum));
        _count = 0;
    }

    void init(const TS& value)
    {
        _sum = value;
        _count = 1;
    }

    void aggregate(const TS& value)
    {
        _sum += value;
        _count++;
    }

    void multAggregate(const TS& value, uint64_t count)
    {
        _sum += value * count;
        _count += count;
    }

    TSR final()
    {
        return _sum / _count;
    }
};

/**
 * Template for implementation of tile->scalar functions
 * @param A is aggregator class.
 * @param T is typename which is processed by function.
 * @param TR is a type of result aggregation.
 */
template<template <typename TS, typename TSR> class A, typename T, typename TR>
void rle_tile_to_scalar(const Value** args, Value* result, void*)
{
    const RLEPayload* vTile = args[0]->getTile();
    A<T, TR> accumulator;
    bool isNull = true;
    size_t i = 0;
    // This first loop initialize the first value and process other values of segment except the first.
    for (; i < vTile->nSegments(); i++)
    {
        const RLEPayload::Segment& v = vTile->getSegment(i);
        if (!v.null) {
            accumulator.init(getPayloadFixedValue<T>(vTile, v.valueIndex));
            if (v.same) {
                if (v.length() > 1) {
                    accumulator.multAggregate(getPayloadFixedValue<T>(vTile, v.valueIndex), v.length() - 1);
                }
            } else {
                const size_t end = v.valueIndex + v.length();
                for (size_t j = v.valueIndex + 1; j < end; j++) {
                    accumulator.aggregate(getPayloadFixedValue<T>(vTile, j));
                }
            }
            isNull = false;
            i++;
            break;
        }
    }
    // Process other segments
    if (!isNull) {
        for (; i < vTile->nSegments(); i++)
        {
            const RLEPayload::Segment& v = vTile->getSegment(i);
            if (v.null)
                continue;
            if (v.same) {
                accumulator.multAggregate(getPayloadFixedValue<T>(vTile, v.valueIndex), v.length());
            } else {
                const size_t end = v.valueIndex + v.length();
                for (size_t j = v.valueIndex; j < end; j++) {
                    accumulator.aggregate(getPayloadFixedValue<T>(vTile, j));
                }
            }
        }
    }
    RLEPayload* rTile =  result->getTile();
    rTile->clear();
    RLEPayload::Segment r;
    r.null = isNull;
    r.pPosition = 0;
    r.same = !isNull;
    r.valueIndex = 0;
    rTile->addSegment(r);
    if (!isNull) {
        addPayloadFixedValues<TR>(rTile, 1);
        setPayloadFixedValue<TR>(rTile, 0, accumulator.final());
    }
    rTile->flush(1);
}

}

#endif // TILEFUNCTIONS_H
