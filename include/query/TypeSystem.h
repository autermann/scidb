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
 * @file TypeSystem.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Type system of SciDB base of inline version of Value and looper
 * expression evaluator.
 */

#ifndef TYPESYSTEM_H_
#define TYPESYSTEM_H_

#include <stdlib.h>
#include <stdint.h>
#include <vector>
#include <map>
#include <boost/shared_ptr.hpp>
#include <assert.h>
#include <cstring>
#include <cmath>
#include "limits.h"

#include "system/Exceptions.h"
#include "util/Singleton.h"
#include "util/Mutex.h"
#include "util/StringUtil.h"
#include "util/PluginObjects.h"
#include <array/RLE.h>
#include <util/na.h>

namespace scidb
{

class LogicalExpression;

const size_t STRIDE_SIZE = 64*1024;

// Type identifier
typedef std::string TypeId;

const char TID_INDICATOR[] = "indicator";
const char TID_CHAR[] = "char";
const char TID_INT8[] = "int8";
const char TID_INT16[] = "int16";
const char TID_INT32[] = "int32";
const char TID_INT64[] = "int64";
const char TID_UINT8[] = "uint8";
const char TID_UINT16[] = "uint16";
const char TID_UINT32[] = "uint32";
const char TID_UINT64[] = "uint64";
const char TID_FLOAT[] = "float";
const char TID_DOUBLE[] = "double";
const char TID_BOOL[] = "bool";
const char TID_STRING[] = "string";
const char TID_DATETIME[] = "datetime";
const char TID_DATETIMETZ[] = "datetimetz";
const char TID_VOID[] = "void";
const char TID_BINARY[] = "binary";
const char TID_FIXED_STRING[] = "string_*";

#define IS_VARLEN(type) (type==TID_STRING || type==TID_BINARY ? true : false)

#define BUILTIN_TYPE_CNT 17

#define IS_NUMERIC(type) (\
    type == TID_INT8 || type == TID_INT16 || type == TID_INT32 || type == TID_INT64\
    || type == TID_UINT8 || type == TID_UINT16 || type == TID_UINT32 || type == TID_UINT64\
    || type == TID_FLOAT || type == TID_DOUBLE)

#define IS_REAL(type) (type == TID_FLOAT || type == TID_DOUBLE)

#define IS_SIGNED(type) (type == TID_INT8 || type == TID_INT16 || type == TID_INT32 || type == TID_INT64\
    || type == TID_FLOAT || type == TID_DOUBLE)

const size_t STRFTIME_BUF_LEN = 256;
const char* const DEFAULT_STRFTIME_FORMAT = "%F %T";

/**
 * Class to present description of a type of a value.
 * The class knows about size of data.
 * Objects of this class can be compared with each other.
 */
class Type
{
private:
    TypeId _typeId;     /**< type identificator */
    uint32_t _bitSize;  /**< bit size is used in storage manager. 0 - for variable size data */
    TypeId _baseType;

public:

    Type(const TypeId& typeId, const uint32_t bitSize, const TypeId baseType = TID_VOID):
    _typeId(typeId), _bitSize(bitSize), _baseType(baseType)
    {
    }

    Type(): _typeId(TypeId(TID_VOID)), _bitSize(0), _baseType(TypeId(TID_VOID))
    {
    }

    /**
     * @return type id
     */
    TypeId typeId() const {
        return _typeId;
    }

    /**
     * @return base type id
     */
    TypeId baseType() const { 
        return _baseType;
    }


     /**
     * Check if this supertype is base type for subtype 
     * return true if subtype is direct or indirect subtype of supertype
     */
   static bool isSubtype(TypeId const& subtype, TypeId const& supertype);

    /**
     * Check if this type is subtype of the specified type
     * return true if this type is direct or indirect subtype of the specified type
     */
    bool isSubtypeOf(TypeId const& type) const { 
        return _baseType != TID_VOID && (_baseType == type || isSubtype(_baseType, type));
    }

    /**
     * @return a name of type
     */
    const std::string& name() const
    {
        return _typeId;
    }

    /**
     * @return size of data in bits
     */
    uint32_t bitSize() const {
        return _bitSize;
    }

    /**
     * @return size of data in bytes
     */
    uint32_t byteSize() const {
        return (_bitSize + 7) >> 3;
    }

    bool variableSize() const {
        return _bitSize == 0;
    }

    bool isVoid() const {
		return (0 ==  _typeId.compare(TID_VOID));
	}

    bool operator<(const Type& ob) const {
        return (0 > _typeId.compare(ob.typeId()));
    }

    bool operator==(const Type& ob) const {
        return (0 == _typeId.compare(ob.typeId()));
    }

    bool operator==(const std::string& ob) const {
        return (0 == _typeId.compare(ob));
    }

    bool operator!=(const Type& ob) const {
        return (0 != _typeId.compare(ob.typeId()));
    }

    bool operator!=(const std::string& ob) const {
        return (0 != _typeId.compare(ob));
    }
};

struct Type_less {
    bool operator()(const Type &f1, const Type &f2) const
    {
        return ( f1 < f2 );
    }
};

std::ostream& operator<<(std::ostream& stream, const Type& ob );
std::ostream& operator<<(std::ostream& stream, const std::vector<Type>& ob );
std::ostream& operator<<(std::ostream& stream, const std::vector<TypeId>& ob );

/**
 * TypeLibrary is a container to registered types in the engine.
 */
class TypeLibrary
{
private:
    static TypeLibrary _instance;
    std::map<TypeId, Type, __lesscasecmp> _typesById;
    std::map<TypeId, Type, __lesscasecmp> _builtinTypesById;
    PluginObjects _typeLibraries;
    Mutex mutex;

    const Type& _getType(TypeId typeId);
    void _registerType(const Type& type);
    bool _hasType(TypeId typeId);
    size_t _typesCount();
    std::vector<TypeId> _typeIds();
    
public:
    TypeLibrary();

    static void registerBuiltInTypes();

    static const Type& getType(TypeId typeId)
    {
        return _instance._getType(typeId);
    }

    static bool hasType(const std::string& typeId)
    {
        return _instance._hasType(typeId);
    }

    static const std::vector<Type> getTypes(const std::vector<TypeId> typeIds)
    {
        std::vector<Type> result;
        for( size_t i = 0, l = typeIds.size(); i < l; i++ )
            result.push_back(_instance._getType(typeIds[i]));

        return result;
    }

    static void registerType(const Type& type)
    {
        _instance._registerType(type);
    }

    /**
     * Return the number of types currently registered in the TypeLibrary.
     */
    static size_t typesCount() { 
        return _instance._typesCount();
    }

    /**
     * Return a vector of typeIds registered in the library.
     */
    static std::vector<TypeId> typeIds() {
        return _instance._typeIds();
    }

    static const PluginObjects& getTypeLibraries() {
        return _instance._typeLibraries;
    }
};

/**
 * The Value class is data storage for type Type. It has only data, as the 
 * type descriptor will be stored separately.
 * 
 * The main goal of this class implementing is keep all methods of it inline.
 */
#define BUILTIN_METHODS(TYPE_NAME, METHOD_NAME) \
    TYPE_NAME get##METHOD_NAME() const { return *static_cast<TYPE_NAME*>((void*)&_builtinBuf); } \
    void set##METHOD_NAME(TYPE_NAME val) { _missingReason = -1; _size = sizeof(TYPE_NAME); *static_cast<TYPE_NAME*>((void*)&_builtinBuf) = val; }

class Value
{
private:
    typedef int64_t builtinbuf_t ;

    /**
     *  _missingReason is an overloaded element. It contains information 
	 *  related to the 'missing' code for data, but also details about 
     *  cases where the data is stored in a buffer allocated outside the 
     *  class instance, and merely linked to it here. 
	 * 
     * _missingReason >= 0 means value is NULL and _missingReason has a code 
	 *  of reason.
     * _missingReason = -1 means value is not NULL and data() returns relevant 
	 *  buffer with value.
     * _missingReason = -2 means _data contains linked vector data that should 
     *  not be freed. Methods changing *data() are disallowed.
     */
    int _missingReason;
	/*
	** For variable length data, the size of this data in bytes. 
	*/
    uint32_t _size;
	/*
	** A union type. If _missingReason is -2, or the _size > 8, then the data 
	** is found in a buffer pointed to out of _data. Otherwise, the data 
	** associated with this instance of the Value class is found in the 
	** 8-byte _builtinBuf. 
	*/
    union { 
        void*   _data;
        builtinbuf_t _builtinBuf;
    };

    /**
     * This is RLEEncoded payload of data which should be processed.
     * Payload data do not include empty elements.
     * Empty bitmask should be applied separately while unpacking data.
     */
    RLEPayload* _tile;

    inline void init (bool allocate = true) 
    {
        if (allocate && _size > sizeof(_builtinBuf))
        {
            _data = malloc(_size);
            if (!_data) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_TYPESYSTEM, SCIDB_LE_NO_MEMORY_FOR_VALUE);
            }
        }
    }

public:
    Value() : _missingReason(0), _size(0), _builtinBuf(0), _tile(NULL) {}
    
    /**
     * Construct Value for some size.
     */
    Value(size_t size):
        _missingReason(-1), _size(size), _builtinBuf(0), _tile(NULL)
    {
        init();
    }

    /**
     * Construct Value for some Type.
     */
    Value(const Type& type):
        _missingReason(-1), _size(type.byteSize()), _builtinBuf(0), _tile(NULL)
    {
        init(type.typeId()!=TID_VOID);
    }

    /**
     * Construct Value for some Type with tile mode support.
     */
    Value(const Type& type, bool tile):
        _missingReason(tile ? -3 : -1), _size(type.byteSize()), _builtinBuf(0), _tile(tile ? new RLEPayload(type) : NULL)
    {
        init(type.typeId() != TID_VOID);
    }

    /**
     * Construct value with linked data
     * @param data a pointer to linked data
     * @param size a size of linked data buffer
     */
    inline Value(void* data, size_t size, bool isVector = true)
        : _size(size), _builtinBuf(0), _tile(NULL)
    {
        if (isVector) {
            _missingReason = -2;
            _data = data;
        } else {
            _missingReason = -1;
            void* ptr;
            if (size > sizeof(_builtinBuf)) {
                _data = ptr = malloc(_size);
                if (!ptr) {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_TYPESYSTEM, SCIDB_LE_NO_MEMORY_FOR_VALUE);
                }
            } else { 
                ptr = (void*)&_builtinBuf;
            }
            memcpy(ptr, data, size);
        }
    }

	/**
	 * Copy constructor.
	 * @param Value object to be copied. 
	 */
    inline Value(const Value& val): 
        _missingReason(-1), _size(0), _builtinBuf(0), _tile(NULL) 
	{
        *this = val;
    }

    inline ~Value() 
    {
        if (_size > sizeof(_builtinBuf) && _missingReason != -2) {
            free(_data);
        }
        delete _tile;
    }

    /**
     * Get the total in-memory footprint of the Value.
     * @param dataSize the size of the stored data, in bytes
     * @return the total in-memory footprint that would be occupied by allocating new Value(size)
     */
    inline static size_t getFootprint(size_t dataSize)
    {
        //if the datatype is smaller than _builtinBuf, it's stored inside _builtinBuf.
        if (dataSize > sizeof(builtinbuf_t ))
        {
            return sizeof(Value) + dataSize;
        }
        else
        {
            return sizeof(Value);
        }
    }

	/**
	 * Link data buffer of some size to the Value object. 
	 * @param pointer to data buffer
	 * @param size of data buffer in bytes
	 */
    inline void linkData(void* data, size_t size)
    {
        if (((NULL != data) || (0 != size)) &&
            ((NULL == data) || (0 == size)))
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_LINK_DATA_TO_ZERO_BUFFER);
        }

        if (_size > sizeof(_builtinBuf) && _missingReason != -2) {
            free(_data);
        }
        _missingReason = -2;
        _size = size;
        _data = data;
    }

	/**
     * Get (void *) to data contents of Value object.
     * @return (void *) to data
	 */
    inline void* data() const 
    {
        return _missingReason != -2 && _size <= sizeof(_builtinBuf) ? (void*)&_builtinBuf : _data;
    }

	/**
	 * Get size of data contents in bytes.
	 * @return size_t length of data in bytes
	 */
    inline size_t size() const {
        return _size;
    }
	
	/**
	 * Equality operator
	 * Very basic, byte-wise equality. 
	 */
    inline bool operator == (Value const& other) const 
    {
        return _missingReason == other. _missingReason
               && (_missingReason >= 0 
                || (_size == other._size && (_size > sizeof(_builtinBuf) 
                                             ? memcmp(_data, other._data, _size) == 0
                                             : _builtinBuf == other._builtinBuf)));
    }

    inline bool operator != (Value const& other) const { 
        return !(*this == other);
    }

    /**
     * Check if value is zero
     * TODO: (RS) It can be optimized by using comparing of DWORDs
     */
    inline bool isZero() const
    {
        char* ptr = (char*)data();
        size_t n = size();
        while (n-- != 0) {
            if (*ptr++ != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Set the Value to zero. 
     */
    inline void setZero()
    {
        if (_missingReason == -2)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_MODIFY_VALUE_WITH_LINKED_DATA);

		// TODO: Fix this. Should be able to set a Vector of 
		//       values to default. 
        if (_missingReason == -3)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_SET_VALUE_VECTOR_TO_DEFAULT);

        _missingReason = -1;
        if (_size > sizeof(_builtinBuf)) { 
            memset(_data, 0, _size);
        } else { 
            _builtinBuf = 0;
        }
    }

	/**
	 * Set up memory to hold a vector of data values in this Value.
	 * @param size of the memory to hold the vector in bytes.
	*/
    inline void setVector(const size_t size)
    {
        if (_missingReason == -2)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_MODIFY_VALUE_WITH_LINKED_DATA);

        if (size > sizeof(_builtinBuf)) { 
            _data = (_size <= sizeof(_builtinBuf)) ? malloc(size) : realloc(_data, size);
            if (_data == NULL) { 
                throw SYSTEM_EXCEPTION(SCIDB_SE_TYPESYSTEM, SCIDB_LE_NO_MEMORY_FOR_VALUE);
            }
        } else { 
            if (_size > sizeof(_builtinBuf)) { 
                free(_data);
            }
        }
        _size = size;
//        _missingReason = -3;
    }

    /** 
     * Allocate space for value
	 * @param size in bytes of the data buffer.
	 */
    inline void setSize(const size_t size)
    {
        if (_missingReason == -2)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_MODIFY_VALUE_WITH_LINKED_DATA);

        if (size > sizeof(_builtinBuf)) { 
            _data = (_size <= sizeof(_builtinBuf)) ? malloc(size) : realloc(_data, size);
            if (_data == NULL) { 
                throw SYSTEM_EXCEPTION(SCIDB_SE_TYPESYSTEM, SCIDB_LE_NO_MEMORY_FOR_VALUE);
            }
        } else { 
            if (_size > sizeof(_builtinBuf)) { 
                free(_data);
            }
        }
        _size = size;
        _missingReason = -1;
    }

	/**
  	 * Copy data buffer into the value object.
     * @param (void *) to data buffer to be copied.
	 * @param size in bytes of the data buffer.
	 */
    inline void setData(const void* data, size_t size) 
    {
        void* ptr;

        if (_missingReason == -2)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_MODIFY_VALUE_WITH_LINKED_DATA);

        if (size > sizeof(_builtinBuf)) { 
            ptr = (_size <= sizeof(_builtinBuf)) ? malloc(size) : realloc(_data, size);
            if (ptr == NULL) { 
                throw SYSTEM_EXCEPTION(SCIDB_SE_TYPESYSTEM, SCIDB_LE_NO_MEMORY_FOR_VALUE);
            }
            _data = ptr;
        } else { 
            if (_size > sizeof(_builtinBuf)) { 
                free(_data);
            }
            ptr = (void*)&_builtinBuf;
            _builtinBuf = 0;
        }
        _size = size;
        memcpy(ptr, data, size);
        _missingReason = -1;
    }

    /**
     * Check if value represents 'missing' value
     */
    inline bool isNull() const {
        return _missingReason >= 0;
    }

    /**
     * Set 'missing value with optional explanation of value missing reason.
     */
    inline void setNull(int reason = 0) {
        _missingReason = reason;
    }

    /**
     * Get reason of missing value (if it is previously set by setNull method)
     */
    inline int getMissingReason() const {
        return _missingReason;
    }

    inline bool isVector() const {
        return _missingReason <= -2;
    }

	/**
	 * Assignment operator.
	 */
    inline Value& operator=(const Value& val) 
    {
        if (this == &val)
        {
            return *this;
        }

        if (val._missingReason != -2) {
            // TODO: It's better to have special indicator of using tile mode in vector.
            // I will add it later.
            if (val._tile != NULL) { 
                if (_tile == NULL) { 
                    _tile = new RLEPayload();
                }
                *_tile = *val._tile;
            } else {
                delete _tile;
                _tile = NULL;
            }
            if (_missingReason == -2)
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_MODIFY_VALUE_WITH_LINKED_DATA);

            if (val.isNull()) {
                setNull(val.getMissingReason());
            } else {
                if (val._size <= sizeof(_builtinBuf)) {
                    if (_size > sizeof(_builtinBuf)) {
                        free(_data);
                    }
                    _builtinBuf = val._builtinBuf;
                    _size = val._size;
                } else {
                    setData(val._data, val._size);
                }
                _missingReason = val._missingReason;
            }
        } else {
            // Here we have source with linked data buffer and just copy 
			// pointer and size to this.
            //
            // PGB: Woah. This is a bit risky, no? If you free the first
            //      Value object, and free the memory associated with it,
            //      then this second Value will have dodgy data. Can we
            //      make the _Data a boost::shared_ptr<void> ?
            if (_size > sizeof(_builtinBuf) && _missingReason != -2) {
                free(_data);
            }
            _data = val._data;
            _size = val._size;
            _missingReason = -2;
        }
        return *this;
    }

    void makeTileConstant(const TypeId& typeId);

	/*
	** Serialization of the Value object for I/O
	*/
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & _size;
        ar & _missingReason;

        // Serialization data
        if (!isNull()) {
        	char* ptr = NULL;
            if (Archive::is_loading::value) {
				ptr =_size > sizeof(_builtinBuf) ? (char*)(_data = malloc(_size)) : (char*)&_builtinBuf;
            } else {
        		ptr = (char*) data();
        	}
            for (size_t i = 0, n = _size; i < n; i++) {
				ar & ptr[i];
			}
        }

        // Serialization payload
        if (Archive::is_loading::value) {
            bool hasTile;
            ar & hasTile;
            if (hasTile) {
                _tile = new RLEPayload();
                ar & (*_tile);
            }
        } else {
            bool hasTile = _tile;
            ar & hasTile;
            if (hasTile) {
                ar & (*_tile);
            }
        }
    }

    // Methods for manipulating data content with casting to required C++ type
    BUILTIN_METHODS(char, Char)
    BUILTIN_METHODS(int8_t, Int8)
    BUILTIN_METHODS(int16_t, Int16)
    BUILTIN_METHODS(int32_t, Int32)
    BUILTIN_METHODS(int64_t, Int64)
    BUILTIN_METHODS(uint8_t, Uint8)
    BUILTIN_METHODS(uint16_t, Uint16)
    BUILTIN_METHODS(uint32_t, Uint32)
    BUILTIN_METHODS(uint64_t, Uint64)
    BUILTIN_METHODS(uint64_t, DateTime)
    BUILTIN_METHODS(float, Float)
    BUILTIN_METHODS(double, Double)
    BUILTIN_METHODS(bool, Bool)

	/**
 	* Return (char *) to to the contents of the Value object
	*/
    inline const char* getString() const
    {
        return _size == 0 ? "" : (const char*)data();
    }

	/**
 	* Set the data contents of the 
	* @param Pointer to null terminated 'C' string
	*/
    inline void setString(const char* str)
    {
        //
        // PGB: No, no, no. A string "" is *not* a missing string.
        // if (str && *str) { }
        if (str) {
            setData(str, strlen(str) + 1);
        }
        else {
            setNull();
        }
    }

/**
 * New RLE fields of Value
 */
public:
    inline RLEPayload* getTile(TypeId const& type) {
        if (_tile == NULL) { 
            _tile = new RLEPayload(TypeLibrary::getType(type));
        }
        return _tile;
    }

    inline RLEPayload* getTile() const {
        assert(_tile != NULL);
//TODO:        assert(_missingReason == -3);
        return _tile;
    }

    inline void setTile(RLEPayload* payload)
    {
        _size = payload->elementSize();
        delete _tile;
        _tile = payload;
        _missingReason = -3;
    }
};

/**
 * Helper Value functions
 */
/**
 * @param type a type of input value
 * @param value a value to be converted
 * @return string with value
 */
std::string ValueToString(const TypeId type, const Value& value, int precision = 6);


/**
 * @param type a type of output value
 * @param str a string to be converted
 * @param [out] value a value in which string will be converted
  */
void StringToValue(const TypeId type, const std::string& str, Value& value);

/**
 * @param type a type of input value
 * @param value a value to be converted
 * @return double value
 */
double ValueToDouble(const TypeId type, const Value& value);

/**
 * @param type a type of output value
 * @param d a double value to be converted
 * @param [out] value a value in which double will be converted
  */
void DoubleToValue(const TypeId type, double d, Value& value);

bool isBuiltinType(const TypeId type);

/**
 * Returns default value for specified type
 * @param[out] value Default value for this type
 * @param[in] typeId Input typeId
 */
void setDefaultValue(Value &value, const TypeId typeId);

TypeId propagateType(const TypeId type);
TypeId propagateTypeToReal(const TypeId type);

/**
 * Convert string to date time
 * @param str string woth data/time to be parsed
 * @return Unix time (time_t)
 */
time_t parseDateTime(std::string const& str);

void parseDateTimeTz(std::string const& str, Value& result);

/**
 * The three-value logic is introduced to improve efficiency for calls to isNan.
 * If isNan takes as input a TypeId, every time isNan is called on a value, string comparisions would be needed
 * to check if the type is equal to TID_DOUBLE and/or TID_FLOAT.
 * With the introduction of DoubleFloatOther, the caller can do the string comparison once for a collection of values.
 */
enum DoubleFloatOther
{
    DOUBLE_TYPE,
    FLOAT_TYPE,
    OTHER_TYPE
};

/**
 * Given a TypeId, tell whether it is double, float, or other.
 * @param[in] type   a string type
 * @return one constant in DoubleFloatOther
 */
inline DoubleFloatOther getDoubleFloatOther(TypeId const& type)
{
    if (type==TID_DOUBLE) {
        return DOUBLE_TYPE;
    } else if (type==TID_FLOAT) {
        return FLOAT_TYPE;
    }
    return OTHER_TYPE;
}

/**
 * A value can be in one of below, assuming null < na < nan < regular
 */
enum NullNaNanRegular
{
    NULL_VALUE,
    NA_VALUE,
    NAN_VALUE,
    REGULAR_VALUE
};

/**
 * Given a value, tell whether it is Null, Na, Nan, or a regular value.
 * @param[in] v      a value
 * @param[in] type   an enum DoubleFloatOther
 * @return one constant in NullNaNanRegular
 */
inline NullNaNanRegular getNullNaNanRegular(Value const& v, DoubleFloatOther type)
{
    if (v.isNull()) {
        return NULL_VALUE;
    }
    if (type==DOUBLE_TYPE) {
        double d = v.getDouble();
        if (isNAonly(d)) {
            return NA_VALUE;
        } else if (std::isnan(d)) {
            return NAN_VALUE;
        }
    } else if (type==FLOAT_TYPE) {
        float d = v.getFloat();
        if (isNAonly(d)) {
            return NA_VALUE;
        } else if (std::isnan(d)) {
            return NAN_VALUE;
        }
    }
    return REGULAR_VALUE;
}

/**
 * Check if a value is NaN.
 * @param[in] v     a value
 * @param[in] type  an enum DoubleFloatOther
 * @return    true iff the value is Nan
 *
 */
inline bool isNan(const Value& v, DoubleFloatOther type)
{
    if (type==DOUBLE_TYPE) {
        return std::isnan(v.getDouble());
    } else if (type==FLOAT_TYPE) {
        return std::isnan(v.getFloat());
    }
    return false;
}

/**
 * Check if a value is Null or NaN.
 * @param[in] v     a value
 * @param[in] type  an enum DoubleFloatOther
 * @return    true iff the value is either Null or Nan
 */
inline bool isNullOrNan(const Value& v, DoubleFloatOther type)
{
    return v.isNull() || isNan(v, type);
}

} //namespace

#endif /* TYPESYSTEM_H_ */
