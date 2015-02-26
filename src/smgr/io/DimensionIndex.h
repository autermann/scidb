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
 * InternalStorage.h
 *
 *  Created on: 06.01.2010
 *      Author: knizhnik@garret.ru
 *      Description: Internal storage manager interface
 */

#ifndef DIMENSION_INDEX_H_
#define DIMENSION_INDEX_H_


#include <set>
#include <map>
#include <boost/shared_ptr.hpp>
#include "system/Exceptions.h"
#include "query/TypeSystem.h"
#include "query/FunctionLibrary.h"

namespace scidb
{

using namespace std;
using namespace boost;

class AttributeComparator
{
  public:
    bool operator()(const Value& v1, const Value& v2) const
    {
        const Value* operands[2];
        Value result;
        operands[0] = &v1;
        operands[1] = &v2;
        less(operands, &result, NULL);
        return result.getBool();
    }

    AttributeComparator() 
    {
    }

    AttributeComparator(TypeId tid) 
    : typeID(tid), type(TypeLibrary::getType(tid))
    {
        vector<TypeId> inputTypes(2);
        inputTypes[0] = tid;
        inputTypes[1] = tid;
        FunctionDescription functionDesc;
        vector<FunctionPointer> converters;
        if (!FunctionLibrary::getInstance()->findFunction("<", inputTypes, functionDesc, converters, false) || converters.size())
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_NOT_FOUND) << "<" << tid;
        less = functionDesc.getFuncPtr();
    }

  protected:
    TypeId typeID;
    Type type;
    FunctionPointer less;
};


template < template < class Key, class Compare = less<Key>, class Allocator = allocator<Key> > class SetClass,
           class IteratorClass,
           bool sizing = true>
class __Attribute_XSet : AttributeComparator
{
  public:
    __Attribute_XSet(TypeId tid) : AttributeComparator(tid), totalSize(0), valueSet(*this)
    {
    }

    size_t size() const 
    { 
        return (typeID == TID_DOUBLE) ? doubleSet.size() : valueSet.size();
    }

    void add(Value const& item) 
    {
        if (typeID == TID_DOUBLE)
        {
            size_t origSize = doubleSet.size();
            doubleSet.insert(item.getDouble());
            if (doubleSet.size() > origSize )
            {
                totalSize += sizeof(double);
            }
        } else {
            size_t origSize = valueSet.size();
            valueSet.insert(item);
            if (valueSet.size() > origSize)
            {
                totalSize += item.size();
                if (type.variableSize()) { 
                    totalSize += item.size()-1 >= 0xFF ? 5 : 1;
                }
            }
        }
    }

    void add(void const* data, size_t size) 
    {
        if (typeID == TID_DOUBLE) {
            double* src = (double*)data;
            double* end = src + size/sizeof(double);
            while (src < end)
            {
                size_t origSize = doubleSet.size();
                doubleSet.insert(*src++);
                if (doubleSet.size()>origSize)
                {
                    totalSize += sizeof(double);
                }
            }
        } else {
            Value value;
            uint8_t* src = (uint8_t*)data;
            uint8_t* end = src + size;
            size_t attrSize = type.byteSize();
            if (attrSize == 0) { 
                while (src < end) {
                    if (*src == 0) { 
                        attrSize = (src[1] << 24) | (src[2] << 16) | (src[3] << 8) | src[4];
                        src += 5;
                    } else { 
                        attrSize = *src++;
                    }
                    value.setData(src, attrSize);

                    size_t origSize = valueSet.size();
                    valueSet.insert(value);
                    if(valueSet.size()>origSize)
                    {
                        totalSize += attrSize + (attrSize-1 >= 0xFF ? 5 : 1);
                    }
                    src += attrSize;
                }
            } else { 
                while (src < end) {
                    value.setData(src, attrSize);

                    size_t origSize = valueSet.size();
                    valueSet.insert(value);
                    if(valueSet.size()>origSize)
                    {
                        totalSize += attrSize;
                    }
                    src += attrSize;
                }
            }
        }
    }

    void add(shared_ptr<SharedBuffer> buf) 
    {
        if (buf) { 
            add(buf->getData(), buf->getSize());
        }
    }

    shared_ptr<SharedBuffer> sort(bool partial)
    {
        if (!partial)
        {
            if (type.variableSize())
            {
                totalSize += valueSet.size()*sizeof(int);
            }

            totalSize += sizeof(size_t);
        }

        MemoryBuffer* buf = new MemoryBuffer(NULL, totalSize);
        if (typeID == TID_DOUBLE) {
            double* dst = (double*)buf->getData();
            for (set<double>::iterator i = doubleSet.begin(); i != doubleSet.end(); ++i) { 
                *dst++ = *i;
            }

            if (!partial)
            {
                *((size_t *) dst) = size();
                dst = (double*)(((size_t*) dst)+1);
            }

            assert((char*)dst == (char*)buf->getData() + buf->getSize());
        } else {
            char* dst = (char*)buf->getData();
            size_t attrSize = type.byteSize();
            if (attrSize == 0) { 
                int* offsPtr = (int*)dst;
                char* base = NULL;
                if (!partial) { 
                    base = (char*)(offsPtr + valueSet.size());
                    dst = base;
                }
                for (IteratorClass i = valueSet.begin(); i != valueSet.end(); ++i) {
                    attrSize = i->size();
                    if (!partial) { 
                        *offsPtr++ = (int)(dst - base);
                    }
                    if (attrSize-1 >= 0xFF) {
                        *dst++ = '\0';
                        *dst++ = char(attrSize >> 24);
                        *dst++ = char(attrSize >> 16);
                        *dst++ = char(attrSize >> 8);
                    }
                    *dst++ = char(attrSize);
                    memcpy(dst, i->data(), attrSize);
                    dst += attrSize;
                }
            } else { 
                for (IteratorClass i = valueSet.begin(); i != valueSet.end(); ++i) {
                    memcpy(dst, i->data(), attrSize);
                    dst += attrSize;
                }
            }

            if (!partial)
            {
                *((size_t *) dst) = size();
                dst = (char*)(((size_t*) dst)+1);
            }

            assert(dst == (char*)buf->getData() + buf->getSize());
        }
        return shared_ptr<SharedBuffer>(buf);
    }
            
    TypeId getType()
    {
        return typeID;
    }

  private:
    SetClass <double> doubleSet;
    size_t totalSize;
    SetClass <Value, AttributeComparator> valueSet;
};

typedef __Attribute_XSet<set, set <Value, AttributeComparator>::iterator > AttributeSet;
typedef __Attribute_XSet<multiset, multiset <Value, AttributeComparator>::iterator, false> AttributeMultiSet;

template < template < class Key, class T, class Compare = less<Key>, class Allocator = allocator<pair<const Key,T> > > class MapClass,
           class ValueIteratorClass,
           class DoubleIteratorClass >
class __Attribute_XMap : AttributeComparator
{
  public:
    __Attribute_XMap(DimensionDesc const& dim, FunctionPointer to, FunctionPointer from) 
    {
        _toOrdinal = to;
        _fromOrdinal = from;
        _start = dim.getStart();
        _length = dim.getLength();
    }

    __Attribute_XMap(TypeId tid, Coordinate start, size_t nCoords, void const* data, size_t size)
    : AttributeComparator(tid), valueMap(*this), _start(start), _toOrdinal(NULL), _fromOrdinal(NULL)
    {        
        if (typeID == TID_DOUBLE) {
            double* src = (double*)data;
            double* end = src + size/sizeof(double);
            while (src < end) {
                doubleMap.insert(make_pair(*src++, start++));
            }
        } else {
            Value value;
            uint8_t* src = (uint8_t*)data;
            uint8_t* end = src + size;
            size_t attrSize = type.byteSize();
            if (attrSize == 0) { 
                int* offsPtr = (int*)src;
                uint8_t* base = src + nCoords*sizeof(int);
                for (size_t i = 0; i < nCoords; i++) {
                    src = base + offsPtr[i];
                    if (*src == 0) { 
                        attrSize = (src[1] << 24) | (src[2] << 16) | (src[3] << 8) | src[4];
                        src += 5;
                    } else { 
                        attrSize = *src++;
                    }
                    value.setData(src, attrSize);
                    valueMap.insert(make_pair(value,start++));
                }
            } else {
                while (src < end) {
                    value.setData(src, attrSize);
                    valueMap.insert(make_pair(value,start++));
                    src += attrSize;
                }
            }
        }
    }

    void getOriginalCoordinate(Value& value, Coordinate pos, bool throwException = true) const
    {
        assert(_fromOrdinal != NULL);
        Value params[3];
        params[0].setInt64(pos);
        params[1].setInt64(_start);
        params[2].setInt64(_length);
        const Value* pParams[3] = {&params[0], &params[1], &params[2]};
        _fromOrdinal(pParams, &value, NULL);
        if (throwException) {
            if (value.isNull())
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_MAPPING_FOR_COORDINATE);
        }
    }

    Coordinate get(Value const& value, CoordinateMappingMode mode = cmExact) const
    {
        if (value.isNull()) {
            if (mode != cmTest)
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_MAPPING_FOR_COORDINATE);
            return MIN_COORDINATE-1;
        }
        if (_toOrdinal != NULL) { 
            const Value* params[3];
            Value result;
            params[0] = &value;
            Value p1;
            p1.setInt64(_start);
            params[1] = &p1;
            Value p2;
            p2.setInt64(_length);
            params[2] = &p2;
            _toOrdinal(params, &result, NULL);
            if (result.isNull())
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_MAPPING_FOR_COORDINATE);
            return result.getInt64();
        }
        if (typeID == TID_DOUBLE) { 
//            map<double, Coordinate>::const_iterator i;
            DoubleIteratorClass i;
            double d = value.getDouble();
            switch (mode) { 
              case cmTest:
                i = doubleMap.find(d);
                return i == doubleMap.end() ? MIN_COORDINATE-1 : i->second;
              case cmExact:
                i = doubleMap.find(d);
                break;
              case cmLowerBound:
                //i = doubleMap.lower_bound(d);
                //break;
              case cmLowerCount:
                i = doubleMap.lower_bound(d);
                return i == doubleMap.end() ? _start + doubleMap.size() : i->second;
              case cmUpperCount:
                i = doubleMap.upper_bound(d);
                return  i == doubleMap.end() ? _start + doubleMap.size() : i->second;
              case cmUpperBound:
                i = doubleMap.upper_bound(d);
                if (i == doubleMap.begin()) { 
                    return i->second-1;
                }
                --i;
                return i == doubleMap.end() ? _start - 1 : i->second;
              default:
                assert(false);
            }
            if (i == doubleMap.end())
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_MAPPING_FOR_COORDINATE);
            return i->second;
        } else { 
//            map<Value, Coordinate, AttributeComparator>::const_iterator i;
            ValueIteratorClass i;
            switch (mode) { 
              case cmTest:
                i = valueMap.find(value);
                return i == valueMap.end() ? MIN_COORDINATE-1 : i->second;
              case cmExact:
                i = valueMap.find(value);
                break;
              case cmLowerBound:
                //i = valueMap.lower_bound(value);
                //break;
              case cmLowerCount:
                i = valueMap.lower_bound(value);
                return i == valueMap.end() ? _start + valueMap.size() : i->second;
              case cmUpperCount:
                i = valueMap.upper_bound(value);
                return  i == valueMap.end() ? _start + valueMap.size() : i->second;
              case cmUpperBound:
                i = valueMap.upper_bound(value);
                if (i == valueMap.begin()) { 
                    return i->second-1;
                }
                --i;
                return i == valueMap.end() ? _start - 1 : i->second;
              default:
                assert(false);
            }
            if (i == valueMap.end())
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_MAPPING_FOR_COORDINATE);
            return i->second;
        }
    }

    bool hasFunctionMapping() const { 
        return _toOrdinal != NULL;
    }

  private:
    MapClass <double, Coordinate> doubleMap;
    MapClass <Value, Coordinate, AttributeComparator> valueMap;
    Coordinate _start;
    Coordinate _length;

    FunctionPointer _toOrdinal;
    FunctionPointer _fromOrdinal;
};

typedef __Attribute_XMap <map, map<Value,Coordinate,AttributeComparator>::const_iterator, map<double,Coordinate>::const_iterator> AttributeMap;
typedef __Attribute_XMap <multimap, multimap<Value,Coordinate,AttributeComparator>::const_iterator, multimap<double,Coordinate>::const_iterator> AttributeMultiMap;



}

#endif
