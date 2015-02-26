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
 * @file TypeSystem.cpp
 *
 * @author roman.simakov@gmail.com
 */

#include <stdio.h>
#include <inttypes.h>
#include <stdarg.h>
#include <float.h>
#include <vector>

#include "log4cxx/logger.h"
#include "log4cxx/basicconfigurator.h"
#include "log4cxx/helpers/exception.h"

#include "query/TypeSystem.h"
#include "util/PluginManager.h"
#include "util/na.h"
#include "query/FunctionLibrary.h"
#include "query/LogicalExpression.h"

using namespace std;
using namespace boost;

namespace scidb
{

// to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.typesystem"));

std::ostream& operator<<(std::ostream& stream, const Type& ob )
{
    stream << ob.typeId();
    return stream;
}

//
// PGB: Note that this will only generate the subset of the input list 
//      of types that are actually in the TypeLibrary. 
std::ostream& operator<<(std::ostream& stream,
                         const std::vector< TypeId>& obs )
{
    for (size_t i = 0, l = obs.size(); i < l; i++) {
        if (i) {
            stream << ", " << TypeLibrary::getType(obs[i]);
        } else {
            stream << " " << TypeLibrary::getType(obs[i]);
        }
    }
    return stream;
}

std::ostream& operator<<(std::ostream& stream,
                         const std::vector< Type>& obs )
{
    for (size_t i = 0, l = obs.size(); i < l; i++) {
        if (i) {
            stream << ", " << obs[i];
        } else {
            stream << " " << obs[i];
        }
    }
    return stream;
}

bool Type::isSubtype(TypeId const& subtype, TypeId const& supertype) 
{
    return TypeLibrary::getType(subtype).isSubtypeOf(supertype);
}


/**
 * TypeLibrary implementation
 */

/*
** This is the list of type names and the bit-sizes for the built-in type 
** list. 
*/
const char* BuiltInNames[BUILTIN_TYPE_CNT+1] = {TID_INDICATOR, TID_CHAR, TID_INT8, TID_INT16, TID_INT32, TID_INT64, TID_UINT8, TID_UINT16, TID_UINT32, TID_UINT64, TID_FLOAT, TID_DOUBLE, TID_BOOL, TID_STRING, TID_DATETIME,    TID_VOID, TID_BINARY, TID_DATETIMETZ};
const size_t BuiltInBitSizes[BUILTIN_TYPE_CNT+1] = {1,         8,        8,        16,        32,        64,        8,         16,         32,         64,         32,        64,         1,        0,          sizeof(time_t)*8,  0,        0,         2*sizeof(time_t)*8};

TypeLibrary TypeLibrary::_instance;

TypeLibrary::TypeLibrary()
{
#ifdef SCIDB_CLIENT
    registerBuiltInTypes();
#endif
}

void TypeLibrary::registerBuiltInTypes()
{
    for (size_t i = 0; i <= BUILTIN_TYPE_CNT; i++) {
        Type type(BuiltInNames[i], BuiltInBitSizes[i]);
        _instance._registerType(type);
        _instance._builtinTypesById[BuiltInNames[i]] = type;
    }
    Type fixedStringType(TID_FIXED_STRING, 0, TID_STRING);
    _instance._registerType(fixedStringType);
    _instance._builtinTypesById[TID_FIXED_STRING] = fixedStringType;    
}

bool TypeLibrary::_hasType(TypeId typeId)
{
    if (_builtinTypesById.find(typeId) != _builtinTypesById.end()) { 
        return true;
    } else { 
        ScopedMutexLock cs(mutex);
        return _typesById.find(typeId) != _typesById.end();
    }
}

const Type& TypeLibrary::_getType(TypeId typeId)
{
    map<TypeId, Type, __lesscasecmp>::const_iterator i = _builtinTypesById.find(typeId);
    if (i != _builtinTypesById.end()) { 
        return i->second;
    } else { 
        ScopedMutexLock cs(mutex);
        i = _typesById.find(typeId);
        if (i == _typesById.end()) {
            size_t pos = typeId.find_first_of('_');
            if (pos != string::npos) { 
                string genericTypeId = typeId.substr(0, pos+1) + '*';
                i = _typesById.find(genericTypeId);
                if (i != _typesById.end()) {
                    Type limitedType(typeId, atoi(typeId.substr(pos+1).c_str())*8, i->second.baseType());
                    _typeLibraries.addObject(typeId); 
                    return _typesById[typeId] = limitedType;
                }
            }
            LOG4CXX_DEBUG(logger, "_getType('" << typeId << "') not found");
            throw SYSTEM_EXCEPTION(SCIDB_SE_TYPESYSTEM, SCIDB_LE_TYPE_NOT_REGISTERED) << typeId;
        }
        return i->second;
    }
}

void TypeLibrary::_registerType(const Type& type)
{
    ScopedMutexLock cs(mutex);
    map<string, Type, __lesscasecmp>::const_iterator i = _typesById.find(type.typeId());
    if (i == _typesById.end()) {
        _typesById[type.typeId()] = type;
        _typeLibraries.addObject(type.typeId());
    } else {
        if (i->second.bitSize() != type.bitSize() || i->second.baseType() != type.baseType())  {
            throw SYSTEM_EXCEPTION(SCIDB_SE_TYPESYSTEM, SCIDB_LE_TYPE_ALREADY_REGISTERED) << type.typeId();
        }
    }
}

size_t TypeLibrary::_typesCount()
{
    ScopedMutexLock cs(mutex);
    size_t count = 0;
    for (map<string, Type, __lesscasecmp>::const_iterator i = _typesById.begin();
         i != _typesById.end();
         ++i)
    {
        if (i->first[0] != '$')
            ++count;
    }
    return count;
}

std::vector<TypeId> TypeLibrary::_typeIds()
{
    ScopedMutexLock cs(mutex);
    std::vector<std::string> list;
    for (map<string, Type, __lesscasecmp>::const_iterator i = _typesById.begin(); i != _typesById.end(); 
         ++i)
    {
        if (i->first[0] != '$')
            list.push_back(i->first);
    }
    return list;
}

/**
 * Helper Value functions implementation
 * 
 * NOTE: This will only work efficiently for the built in types. If you try 
 *       use this for a UDT it needs to do a lookup to try and find a UDF.
 */
string ValueToString(const TypeId type, const Value& value, int precision)
{
    std::stringstream ss;

	/*
	** Start with the most common ones, and do the least common ones
	** last.
	*/
    if ( value.isNull() ) { 
        if (value.getMissingReason() == 0) { 
            ss << "null";
        } else { 
            ss << '?' << value.getMissingReason();
        }
    } else if ( TID_DOUBLE == type ) { 
        double val = value.getDouble();
        if (isNAonly(val)) {
            ss << "NA";
        } else {
            if (isnan(val) || val==0)
                val = abs(val);
            ss.precision(precision);
            ss << val;
        }
	} else if ( TID_INT64 == type ) {
       	ss << value.getInt64();
	} else if ( TID_INT32 == type ) { 
       	ss << value.getInt32();
	} else if ( TID_STRING == type ) { 
        char const* str = value.getString();
        if (str == NULL) { 
            ss << "null";
        } else { 
            const string s = str;
            ss << '\"';
            for (size_t i = 0; i < s.length(); i++) {
                const char ch = s[i];
                if (ch == '\"' || ch == '\\') {
                    ss << '\\';
                }
                ss << ch;
            }
            ss << '\"';
        }
	} else if ( TID_CHAR == type ) {

       	ss << '\'';
       	const char ch = value.getChar();
       	if (ch == '\0') {
           	ss << "\\0";
       	} else if (ch == '\n') {
           	ss << "\\n";
       	} else if (ch == '\r') {
           	ss << "\\r";
       	} else if (ch == '\t') {
           	ss << "\\t";
       	} else if (ch == '\f') {
           	ss << "\\f";
       	} else {
           	if (ch == '\'' || ch == '\\') {
               	ss << '\\';
           	}
           	ss << ch;
       	}
       	ss << '\'';

	} else if ( TID_FLOAT == type ) { 
        float val = value.getFloat();
        if (isNAonly(val)) {
            ss << "NA";
        } else {
            ss << val;
        }
	} else if (( TID_BOOL == type ) || ( TID_INDICATOR == type )) { 
        ss << (value.getBool() ? "true" : "false");
	} else if ( TID_DATETIME == type ) { 

        char buf[STRFTIME_BUF_LEN];
       	struct tm tm;
       	time_t dt = (time_t)value.getDateTime();

       	gmtime_r(&dt, &tm);
       	strftime(buf, sizeof(buf), DEFAULT_STRFTIME_FORMAT, &tm);
       	ss << '\"' << buf << '\"';

	} else if ( TID_DATETIMETZ == type) {

	    char buf[STRFTIME_BUF_LEN + 8];
	    time_t *seconds = (time_t*) value.data();
	    time_t *offset = seconds+1;

	    struct tm tm;
	    gmtime_r(seconds,&tm);
	    size_t offs = strftime(buf, sizeof(buf), DEFAULT_STRFTIME_FORMAT, &tm);

	    char sign = *offset > 0 ? '+' : '-';

	    time_t aoffset = *offset > 0 ? *offset : (*offset) * -1;

	    sprintf(buf+offs, " %c%02d:%02d",
	            sign,
	            (int32_t) aoffset/3600,
	            (int32_t) (aoffset%3600)/60);


	    ss << '\"' << buf << '\"';
	} else if ( TID_INT8 == type ) { 
       	ss << (int)value.getInt8();
	} else if ( TID_INT16 == type ) { 
       	ss << value.getInt16();
	} else if ( TID_UINT8 == type ) { 
       	ss << (int)value.getUint8();
	} else if ( TID_UINT16 == type ) { 
       	ss << value.getUint16();
	} else if ( TID_UINT32 == type ) { 
       	ss << value.getUint32();
	} else if ( TID_UINT64 == type ) { 
       	ss << value.getUint64();
	} else if ( TID_VOID == type ) { 
        ss << "<void>";
	} else  {
        ss << "<" << type << ">";
	}
    return ss.str();
}

inline void mStringToMonth(char* mString, int& month)
{
    if (mString[0]!=0)
    {
        switch (mString[0])
        {
            case 'J':
                if (mString[1]=='A')      { month=1; }
                else if (mString[2]=='N') { month=6; }
                else                      { month=7; }
                break;
            case 'F':                       month=2;
                break;
            case 'M':
                if(mString[2]=='R')       { month=3; }
                else                      { month=5; }
                break;
            case 'A':
                if (mString[1]=='P')      { month=4; }
                else                      { month=8; }
                break;
            case 'S':                       month=9;
                break;
            case 'O':                       month=10;
                break;
            case 'N':                       month=11;
                break;
            case 'D':                       month=12;
                break;
            default:
                throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_INVALID_MONTH_REPRESENTATION) << string(mString);
        }
    }
}

/**
 * Parse a string that contains (hopefully) a DateTime constant into
 * the internal representation.
 * @param string containing DateTime value
 * @return standard time_t.
 */
time_t parseDateTime(std::string const& str)
{
    struct tm t;
    time_t now = time(NULL);
    if (str == "now") {
        return now;
    }
    gmtime_r(&now, &t);
    int n;
    int sec_frac;
    char const* s = str.c_str();
    t.tm_mon += 1;
    t.tm_hour = t.tm_min = t.tm_sec = 0;
    char mString[4]="";
    char amPmString[3]="";

    if (( sscanf(s, "%d-%3s-%d %d.%d.%d %2s%n", &t.tm_mday, &mString[0], &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &amPmString[0], &n) == 7 ||
          sscanf(s, "%d%3s%d:%d:%d:%d%n", &t.tm_mday, &mString[0], &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &n) == 6 ) && n == (int) str.size())
    {
        mStringToMonth(&mString[0], t.tm_mon);
        if(amPmString[0]=='P')
        {
            t.tm_hour += 12;
        }
    }
    else
    {
        if((sscanf(s, "%d/%d/%d %d:%d:%d%n", &t.tm_mon, &t.tm_mday, &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &n) != 6 &&
                    sscanf(s, "%d.%d.%d %d:%d:%d%n", &t.tm_mday, &t.tm_mon, &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &n) != 6 &&
                    sscanf(s, "%d-%d-%d %d:%d:%d.%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &sec_frac, &n) != 7 &&
                    sscanf(s, "%d-%d-%d %d.%d.%d.%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &sec_frac, &n) != 7 &&
                    sscanf(s, "%d-%d-%d %d.%d.%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &n) != 6 &&
                    sscanf(s, "%d-%d-%d %d:%d:%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &n) != 6 &&
                    sscanf(s, "%d/%d/%d %d:%d%n", &t.tm_mon, &t.tm_mday, &t.tm_year, &t.tm_hour, &t.tm_min, &n) != 5 &&
                    sscanf(s, "%d.%d.%d %d:%d%n", &t.tm_mday, &t.tm_mon, &t.tm_year, &t.tm_hour, &t.tm_min, &n) != 5 &&
                    sscanf(s, "%d-%d-%d %d:%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &n) != 5 &&
                    sscanf(s, "%d-%d-%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &n) != 3 &&
                    sscanf(s, "%d/%d/%d%n", &t.tm_mon, &t.tm_mday, &t.tm_year, &n) != 3 &&
                    sscanf(s, "%d.%d.%d%n", &t.tm_mday, &t.tm_mon, &t.tm_year, &n) != 3 &&
                    sscanf(s, "%d:%d:%d%n", &t.tm_hour, &t.tm_min, &t.tm_sec, &n) != 3 &&
                    sscanf(s, "%d:%d%n", &t.tm_hour, &t.tm_min, &n) != 2)
                    || n != (int)str.size())
            throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_FAILED_PARSE_STRING);
    }

    if (!(t.tm_mon >= 1 && t.tm_mon <= 12  && t.tm_mday >= 1 && t.tm_mday <= 31 && t.tm_hour >= 0
          && t.tm_hour <= 23 && t.tm_min >= 0 && t.tm_min <= 59 && t.tm_sec >= 0 && t.tm_sec <= 60))
        throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_INVALID_SPECIFIED_DATE);

    t.tm_mon -= 1;
    if (t.tm_year >= 1900) {
        t.tm_year -= 1900;
    } else if (t.tm_year < 100) {
        t.tm_year += 100;
    }
    return timegm(&t);
}

void parseDateTimeTz(std::string const& str, Value& result)
{
    if (str == "now")
    {
        pair<time_t,time_t> r;
        time_t now = time(NULL);
        struct tm localTm;
        localtime_r(&now, &localTm);
        r.second = timegm(&localTm) - now;
        r.first = now + r.second;
        result.setData(&r, 2*sizeof(time_t));
    }

    struct tm t;
    int offsetHours, offsetMinutes, secFrac, n;
    char mString[4]="";
    char amPmString[3]="";

    char const* s = str.c_str();
    t.tm_mon += 1;
    t.tm_hour = t.tm_min = t.tm_sec = 0;

    if ((sscanf(s, "%d-%3s-%d %d.%d.%d %2s %d:%d%n", &t.tm_mday, &mString[0], &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &amPmString[0], &offsetHours, &offsetMinutes, &n) == 9)
        && n == (int)str.size())
    {
        mStringToMonth(&mString[0], t.tm_mon);
        if(amPmString[0]=='P')
        {
            t.tm_hour += 12;
        }
    }
    else
    {
        if((sscanf(s, "%d/%d/%d %d:%d:%d %d:%d%n", &t.tm_mon, &t.tm_mday, &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &offsetHours, &offsetMinutes, &n) != 8 &&
                sscanf(s, "%d.%d.%d %d:%d:%d %d:%d%n", &t.tm_mday, &t.tm_mon, &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &offsetHours, &offsetMinutes, &n) != 8 &&
                sscanf(s, "%d-%d-%d %d:%d:%d.%d %d:%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &secFrac, &offsetHours, &offsetMinutes, &n) != 9 &&
                sscanf(s, "%d-%d-%d %d:%d:%d %d:%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &offsetHours, &offsetMinutes, &n) != 8 &&
                sscanf(s, "%d-%d-%d %d.%d.%d.%d %d:%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &secFrac, &offsetHours, &offsetMinutes, &n) != 9 &&
                sscanf(s, "%d-%d-%d %d.%d.%d %d:%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &offsetHours, &offsetMinutes, &n) != 8 &&
                sscanf(s, "%d-%3s-%d %d.%d.%d %2s %d:%d%n", &t.tm_mday, &mString[0], &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &amPmString[0], &offsetHours, &offsetMinutes, &n) != 9)
              || n != (int)str.size())
            throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_FAILED_PARSE_STRING);
    }

    if (offsetHours < 0 && offsetMinutes > 0)
    {
        offsetMinutes *= -1;
    }

    if (!(t.tm_mon >= 1 && t.tm_mon <= 12  &&
          t.tm_mday >= 1 && t.tm_mday <= 31 &&
          t.tm_hour >= 0 && t.tm_hour <= 23 &&
          t.tm_min >= 0 && t.tm_min <= 59 &&
          t.tm_sec >= 0 && t.tm_sec <= 60 &&
          offsetHours>=-13 && offsetHours<=13 &&
          offsetMinutes>=-59 && offsetMinutes<=59))
        throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_INVALID_SPECIFIED_DATE);

    t.tm_mon -= 1;
    if (t.tm_year >= 1900) {
        t.tm_year -= 1900;
    } else if (t.tm_year < 100) {
        t.tm_year += 100;
    }

    pair<time_t,time_t> r;
    r.first = timegm(&t);
    r.second = (offsetHours * 3600 + offsetMinutes * 60);
    result.setData(&r, 2*sizeof(time_t));
}

bool isBuiltinType(const TypeId type)
{
	return TID_DOUBLE == type
        || TID_INT64 == type
        || TID_INT32 == type
        || TID_CHAR == type
        || TID_STRING == type
        || TID_FLOAT == type
        || TID_INT8 == type
        || TID_INT16 == type
        || TID_UINT8 == type
        || TID_UINT16 == type
        || TID_UINT32 == type
        || TID_UINT64 == type
        || TID_INDICATOR == type 
        || TID_BOOL == type
        || TID_DATETIME == type
        || TID_VOID == type
        || TID_DATETIMETZ == type;
}

void setDefaultValue(Value &value, const TypeId typeId)
{
    int fixed_str;
    if (isBuiltinType(typeId) || typeId == TID_BINARY || sscanf(typeId.c_str(), "string_%d", &fixed_str) == 1)
    {
        value.setZero();
    }
    else
    {
        FunctionDescription functionDesc;
        vector<FunctionPointer> converters;
        if (FunctionLibrary::getInstance()->findFunction(typeId, vector<TypeId>(), functionDesc, converters, false))
        {
            functionDesc.getFuncPtr()(0, &value, 0);
        }
        else
        {
            stringstream ss;
            ss << typeId << "()";
            throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_FUNCTION_NOT_FOUND) << ss.str();
        }
    }
}
 
TypeId propagateType(const TypeId type)
{
	return TID_INT8 == type || TID_INT16 == type || TID_INT32 == type
        ? TID_INT64
        : TID_UINT8 == type || TID_UINT16 == type || TID_UINT32 == type
        ? TID_UINT64
        : TID_FLOAT == type ? TID_DOUBLE : type;
}

TypeId propagateTypeToReal(const TypeId type)
{
	return TID_INT8 == type || TID_INT16 == type || TID_INT32 == type || TID_INT64 == type
        || TID_UINT8 == type || TID_UINT16 == type || TID_UINT32 == type || TID_UINT64 == type
        || TID_FLOAT == type ? TID_DOUBLE : type;
}

void StringToValue(const TypeId type, const string& str, Value& value)
{
    int n;
	if ( TID_DOUBLE == type ) {
	    if (str == string("NA") ) {
	        value.setDouble(NA::NAInfo<double>::value());
	    }else{
	        value.setDouble(atof(str.c_str()));
	    }
    } else if ( TID_INT64 == type ) { 
        int64_t val;
        if (sscanf(str.c_str(), "%"PRIi64"%n", &val, &n) != 1 || n != (int)str.size())
            throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_FAILED_PARSE_STRING);
        value.setInt64(val);
    } else if ( TID_INT32 == type ) { 
        int val;
        if (sscanf(str.c_str(), "%d%n", &val, &n) != 1 || n != (int)str.size())
            throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_FAILED_PARSE_STRING);
        value.setInt32(val);
    } else if (  TID_CHAR == type )  { 
        value.setChar(str[0]);
	} else if ( TID_STRING == type ) {
        value.setString(str.c_str());
    } else if ( TID_FLOAT == type ) { 
        if (str == string("NA") ) {
            value.setFloat(NA::NAInfo<double>::value());
        }else{
            value.setFloat(atof(str.c_str()));
        }
	} else if ( TID_INT8 == type ) { 
        int16_t val;
        if (sscanf(str.c_str(), "%hd%n", &val, &n) != 1 || n != (int)str.size() || val>127 || val<-127)
            throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_FAILED_PARSE_STRING);
        value.setInt8(static_cast<int8_t>(val));
    } else if (TID_INT16 == type) { 
        int16_t val;
        if (sscanf(str.c_str(), "%hd%n", &val, &n) != 1 || n != (int)str.size())
            throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_FAILED_PARSE_STRING);
        value.setInt16(val);
    } else if ( TID_UINT8 == type ) { 
        uint16_t val;
        if (sscanf(str.c_str(), "%hu%n", &val, &n) != 1 || n != (int)str.size() || val>255)
            throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_FAILED_PARSE_STRING);
        value.setUint8(static_cast<uint8_t>(val));
    } else if ( TID_UINT16 == type ) { 
        uint16_t val;
        if (sscanf(str.c_str(), "%hu%n", &val, &n) != 1 || n != (int)str.size())
            throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_FAILED_PARSE_STRING);
        value.setUint16(val);
    } else if ( TID_UINT32 == type ) {
        unsigned val;
        if (sscanf(str.c_str(), "%u%n", &val, &n) != 1 || n != (int)str.size())
            throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_FAILED_PARSE_STRING);
        value.setUint32(val);
    } else if ( TID_UINT64 == type ) { 
        uint64_t val;
        if (sscanf(str.c_str(), "%"PRIu64"%n", &val, &n) != 1 || n != (int)str.size())
            throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_FAILED_PARSE_STRING);
        value.setUint64(val);
	} else if (( TID_INDICATOR == type ) || ( TID_BOOL == type )) { 
        if (str == "true")
            value.setBool(true);
        else if (str == "false")
            value.setBool(false);
        else
            throw SYSTEM_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_TYPE_CONVERSION_ERROR2)
                << str << "string" << "bool";
	} else if ( TID_DATETIME == type ) {
        value.setDateTime(parseDateTime(str));
	} else if ( TID_DATETIMETZ == type) {
	    parseDateTimeTz(str, value);
	} else if ( TID_VOID == type ) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_TYPE_CONVERSION_ERROR2)
            << str << "string" << type;
	} else { 
        std::stringstream ss;
        ss << type;
        throw SYSTEM_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_TYPE_CONVERSION_ERROR2)
            << str << "string" << type;
    }
}

double ValueToDouble(const TypeId type, const Value& value)
{
    std::stringstream ss;
    if ( TID_DOUBLE == type ) { 
        return value.getDouble();
    } else if ( TID_INT64 == type ) { 
        return value.getInt64();
    } else if ( TID_INT32 == type ) { 
        return value.getInt32();
    } else if ( TID_CHAR == type ) { 
        return value.getChar();
    } else if ( TID_STRING == type ) { 
        double d;
        int n;
        char const* str = value.getString();
        if (sscanf(str, "%lf%n", &d, &n) != 1 || n != (int)strlen(str))
            throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_FAILED_PARSE_STRING);
        return d;
    } else if ( TID_FLOAT == type ) { 
        return value.getFloat();
    } else if ( TID_INT8 == type ) { 
        return value.getInt8();
    } else if ( TID_INT16 == type ) { 
        return value.getInt16();
    } else if ( TID_UINT8 == type ) { 
        return value.getUint8();
    } else if ( TID_UINT16 == type ) { 
        return value.getUint16();
    } else if ( TID_UINT32 == type ) { 
        return value.getUint32();
    } else if ( TID_UINT64 == type ) { 
        return value.getUint64();
    } else if (( TID_INDICATOR == type ) || ( TID_BOOL == type )) { 
        return value.getBool();
    } else if ( TID_DATETIME == type ) {
        return value.getDateTime();
    } else {
        throw SYSTEM_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_TYPE_CONVERSION_ERROR)
            << type << "double";
    }
}

void DoubleToValue(const TypeId type, double d, Value& value)
{
      if (  TID_DOUBLE == type ) {
        value.setDouble(d);
      } else if ( TID_INT64 == type ) {
        value.setInt64((int64_t)d);
      } else if ( TID_UINT32 == type ) {
        value.setUint32((uint32_t)d);
      } else if ( TID_CHAR == type ) {
        value.setChar((char)d);
      } else if ( TID_FLOAT == type ) {
        value.setFloat((float)d);
      } else if ( TID_INT8 == type ) {
        value.setInt8((int8_t)d);
      } else if ( TID_INT16 == type ) {
        value.setInt32((int32_t)d);
      } else if ( TID_UINT8 == type ) {
        value.setUint8((uint8_t)d);
      } else if ( TID_UINT16 == type ) {
        value.setUint16((uint16_t)d);
      } else if ( TID_UINT64 == type ) {
        value.setUint64((uint64_t)d);
      } else if (( TID_INDICATOR == type ) || ( TID_BOOL == type )) {
        value.setBool(d != 0.0);
      } else if ( TID_STRING == type ) {
          std::stringstream ss;
          ss << d;
          value.setString(ss.str().c_str());
      } else if (  TID_DATETIME == type ) {
        return value.setDateTime((time_t)d);
    } else {
        throw SYSTEM_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_TYPE_CONVERSION_ERROR)
            << "double" << type;
    }
}

void Value::makeTileConstant(const TypeId& typeId)
{
    assert(_tile == NULL);

    RLEPayload& p = *getTile(typeId);
    RLEPayload::Segment s;
    s.same = true;
    s.null = isNull();
    s.pPosition = 0;
    s.valueIndex = 0;
    if (!s.null) {
        std::vector<char> varPart;
        p.appendValue(varPart, *this, 0);
        p.setVarPart(varPart);
    }
    p.addSegment(s);
    p.flush(INFINITE_LENGTH);
}
} // namespace

