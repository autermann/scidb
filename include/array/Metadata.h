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
 * @file Metadata.h
 *
 * @brief Structures for fetching and updating metadata of cluster.
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#ifndef METADATA_H_
#define METADATA_H_

#include <stdint.h>
#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <assert.h>
#include <set>

#include <boost/foreach.hpp>
#include <boost/serialization/set.hpp>
#include <boost/serialization/map.hpp>
#include <boost/shared_ptr.hpp>

#include "query/TypeSystem.h"

namespace scidb
{

class AttributeDesc;
class DimensionDesc;
class InstanceDesc;
class LogicalOpDesc;
class PhysicalOpDesc;
class Query;
class ObjectNames;

/**
 * Vector of AttributeDesc type
 */
typedef std::vector<AttributeDesc> Attributes;

/**
 * Vector of DimensionDesc type
 */
typedef std::vector<DimensionDesc> Dimensions;

/**
 * Vector of InstanceDesc type
 */
typedef std::vector<InstanceDesc> Instances;

typedef std::vector<LogicalOpDesc> LogicalOps;

typedef std::vector<PhysicalOpDesc> PhysicalOps;

/**
 * Instance identifier
 */
typedef uint64_t InstanceID;

/**
 * Array identifier
 */
typedef uint64_t ArrayID;

/**
 * Identifier of array version
 */
typedef uint64_t VersionID;

/**
 * Attribute identifier (attribute number in array description)
 */
typedef uint32_t AttributeID;

/**
 * Note: this id is used in messages serialized by GPB and be careful with changing this type.
 */
typedef uint64_t QueryID;

typedef uint64_t OpID;

/**
 * Array coordinate
 */
std::ostream& operator<<(std::ostream& stream,const Coordinates& ob);

const Coordinate MAX_COORDINATE = (uint64_t)-1 >> 2;
const Coordinate MIN_COORDINATE = -MAX_COORDINATE;
const uint64_t   INFINITE_LENGTH = MAX_COORDINATE;
const VersionID  LAST_VERSION = (VersionID)-1;
const VersionID  ALL_VERSIONS = (VersionID)-2;

const InstanceID CLIENT_INSTANCE = ~0;  // Connection with this instance id is client connection
const InstanceID INVALID_INSTANCE = ~0;  // Invalid instanceID for checking that it's not registered
const QueryID INVALID_QUERY_ID = ~0;
const ArrayID INVALID_ARRAY_ID = ~0;
const AttributeID INVALID_ATTRIBUTE_ID = ~0;
const size_t INVALID_DIMENSION_ID = ~0;
const InstanceID COORDINATOR_INSTANCE = INVALID_INSTANCE;
const std::string DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME = "EmptyTag";

/**
 * Partitioning schema show how an array is distributed through instances.
 * Every kind of partitioning has one or more parameters. These parameters
 * stored in vector of integers.
 */
enum PartitioningSchema
{
    psReplication,
    psRoundRobin,
    psLocalInstance,
    psByRow,
    psByCol,
    psUndefined
};

/**
 * Coordinates mapping mode
 */
enum CoordinateMappingMode
{
    cmUpperBound,
    cmLowerBound,
    cmExact,
    cmTest,
    cmLowerCount,
    cmUpperCount
};

/**
 * @brief Class containing all possible object names
 *
 * During array processing schemas can be merged in many ways. For example NATURAL JOIN contain all
 * attributes from both arrays and dimensions combined. Attributes in such example received same names
 * as from original schema and also aliases from original schema name if present, so it can be used
 * later for resolving ambiguity. Dimensions in output schema received not only aliases, but also
 * additional names, so same dimension in output schema can be referenced by old name from input schema.
 *
 * Despite object using many names and aliases catalog storing only one name - base name. This name
 * will be used also for returning in result schema. So query processor handling all names but storage
 * and user API using only one.
 *
 * @note Alias this is not full name of object! Basically it prefix received from schema name or user
 * defined alias name.
 */
class ObjectNames
{
public:
    typedef std::set<std::string> AliasesType;

    typedef std::map<std::string, AliasesType> NamesType;

    typedef std::pair<std::string, AliasesType> NamesPairType;

    ObjectNames();

    /**
     * Constructing initial name without aliases and/or additional names. This name will be later
     * used for returning to user or storing to catalog.
     *
     * @param baseName base object name
     */
    ObjectNames(const std::string &baseName);

    /**
     * Constructing full name
     *
     * @param baseName base object name
     * @param names other names and aliases
     */
    ObjectNames(const std::string &baseName, const NamesType &names);

    /**
     * Add new object name
     *
     * @param name object name
     */
    void addName(const std::string &name);

    /**
     * Add new alias name to object name
     *
     * @param alias alias name
     * @param name object name
     */
    void addAlias(const std::string &alias, const std::string &name);

    /**
     * Add new alias name to all object names
     *
     * @param alias alias name
     */
    void addAlias(const std::string &alias);

    /**
     * Check if object has such name or alias.
     *
     * @param name object name
     * @param alias alias name
     * @return true if has
     */
    bool hasNameOrAlias(const std::string &name, const std::string &alias = "") const;


    /**
     * Get all names and aliases of object
     *
     * @return names and aliases map
     */
    const NamesType& getNamesAndAliases() const;

    /**
     * Get base name of object.
     *
     * @return base name of object
     */
    const std::string& getBaseName() const;

    bool operator==(const ObjectNames &o) const;

    friend std::ostream& operator<<(std::ostream& stream, const ObjectNames &ob);
    friend void printSchema (std::ostream& stream, const ObjectNames &ob);

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & _baseName;
        ar & _names;
    }

protected:
    NamesType _names;

    std::string _baseName;
};

std::ostream& operator<<(std::ostream& stream, const ObjectNames::NamesType &ob);
/**
 * Print only the names
 */
void printNames(std::ostream& stream, const ObjectNames::NamesType &ob);
/**
 * Syntactic sugar to represent an n-dimensional vector.
 */
class DimensionVector
{
public:
        /**
         * Create a "null" vector.
         */
        DimensionVector()
        {}

        /**
         * Create a zero-length vector in numDims dimensions.
         * @param[in] numDims number of dimensions
         */
        DimensionVector(size_t numDims)
        {
                for (size_t i = 0; i< numDims; i++)
                {
                        _data.push_back(0);
                }
        }

        /**
         * Create a vector based on values.
         * @param[in] vector values
         */
        DimensionVector(Coordinates values):
                _data(values)
        {}

        /**
         * Copy.
         */
        DimensionVector(const DimensionVector & other):
                _data(other._data)
        {}

        ~DimensionVector()
        {}

        /**
         * Check if this is a "NULL" vector.
         * @return true if the vector is in 0 dimensions. False otherwise.
         */
        inline bool isEmpty() const
        {
                return _data.size() == 0;
        }

        /**
         * Get the number of dimensions.
         * @return the number of dimensions
         */
        inline size_t numDimensions() const
        {
                return _data.size();
        }

        Coordinate& operator[] (const size_t& index )
        {
                return _data[index];
        }

        const Coordinate& operator[] (const size_t& index ) const
        {
                return _data[index];
        }

        DimensionVector& operator= (const DimensionVector& rhs)
        {
                if ( this == &rhs )
                {       return *this; }

                _data = rhs._data;
                return *this;
        }

        friend DimensionVector& operator+= (DimensionVector& lhs, const DimensionVector& rhs)
        {
                if (lhs.isEmpty())
                {
                    lhs._data = rhs._data;
                }
                else if (rhs.isEmpty())
                {}
                else
                {
                    assert(lhs._data.size() == rhs._data.size());
                    for (size_t i = 0; i< lhs._data.size(); i++)
                    {
                        lhs._data[i] += rhs._data[i];
                    }
                }
                return lhs;
        }

        friend DimensionVector& operator-= (DimensionVector& lhs, const DimensionVector& rhs)
        {
                if (!lhs.isEmpty() && !rhs.isEmpty())
                {
                    assert(lhs._data.size() == rhs._data.size());
                    for (size_t i = 0; i< lhs._data.size(); i++)
                    {
                        lhs._data[i] -= rhs._data[i];
                    }
                }
                return lhs;
        }

        const DimensionVector operator+ (const DimensionVector & other) const
        {
                DimensionVector result(*this);
                return result += other;
        }

        friend bool operator== (const DimensionVector & lhs, const DimensionVector & rhs)
    {
            if ( (rhs.isEmpty() && !lhs.isEmpty()) || (!rhs.isEmpty() && lhs.isEmpty()) )
            {
                return false;
            }

            if (rhs.numDimensions() != lhs.numDimensions())
            {
                return false;
            }

            for (size_t i=0; i<rhs.numDimensions(); i++)
            {
                if (lhs[i]!=rhs[i])
                {
                    return false;
                }
            }
            return true;
    }

        friend bool operator!= (const DimensionVector & lhs, const DimensionVector & rhs)
    {
            return !(lhs == rhs);
    }

        void clear()
        {
            _data.clear();
        }

        operator Coordinates () const
        {
                return _data;
        }

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line.
     * @param[out] str buffer to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    void toString (std::ostringstream &str, int indent = 0) const
    {
        for(int i=0; i<indent; i++)
        {
            str<<" ";
        }

        if (isEmpty())
        {
            str<<"[empty]";
        }
        else
        {
            str<<"[";
            for(size_t i=0; i<_data.size(); i++)
            {
                str<<_data[i]<<" ";
            }
            str<<"]";
        }
    }

        template<class Archive>
        void serialize(Archive& ar, const unsigned int version)
        {
                ar & _data;
        }

private:
        Coordinates _data;
};


/**
 * Descriptor of array. Used for getting metadata of array from catalog.
 */
class ArrayDesc
{
    friend class DimensionDesc;
public:
    /**
     * Various array qualifiers
     */
    enum ArrayFlags {
        IMMUTABLE    = 0x01,
        LOCAL        = 0x02,
        TEMPORARY    = 0x04,
        DENSE        = 0x08,
        DETERIORATED = 0x10
    };


    /**
     * Construct empty array descriptor (for receiving metadata)
     */
    ArrayDesc();

    /**
     * Construct partial array descriptor (without id, for adding to catalog)
     *
     * @param name array name
     * @param attributes vector of attributes
     * @param dimensions vector of dimensions
     */
    ArrayDesc(const std::string &name, const Attributes& attributes, const Dimensions &dimensions,
              int32_t flags = 0);

    /**
     * Construct full descriptor (for returning metadata from catalog)
     *
     * @param id array identifier
     * @param name array name
     * @param attributes vector of attributes
     * @param dimensions vector of dimensions
     */
    ArrayDesc(ArrayID id, const std::string &name, const Attributes& attributes, const Dimensions &dimensions,
              int32_t flags = 0, std::string const& comment = std::string());

    /**
     * Copy constructor
     */
    ArrayDesc(ArrayDesc const& other);

    /**
     * Assignment operator
     */
    ArrayDesc& operator = (ArrayDesc const& other);

    /**
     * Get array identifier
     * @return array identifier
     */
    ArrayID getId() const;

    /**
     * Set array identifier
     */
    void setId(ArrayID id)
    {
        _id = id;
    }

    /**
     * Mark array descriptor as deteriaorated (needed for maintain array descriptor's cache)
     */
    void invalidate() 
    {
        _flags |= DETERIORATED;
    }

    bool isInvalidated() const
    {    
        return _flags & DETERIORATED;
    }

    /**
     * Get name of array
     * @return array name
     */
        const std::string& getName() const;

    /**
     * Set name of array
     * @param name array name
     */
    void setName(const std::string& name);

    /**
     * Get static array size (number of elements within static boundaries)
     * @return array size
     */
    uint64_t getSize() const;

    /**
     * Get actual array size (number of elements within actual boundaries)
     * @return array size
     */
    uint64_t getCurrSize() const;

    /**
     * Get array size in bytes (works only for arrays with fixed size dimensions and fixed size types)
     * @return array size in bytes
     */
    uint64_t getUsedSpace() const;

    /**
     * Get number of chunks in array
     * @return number of chunks in array
     */
    uint64_t getNumberOfChunks() const;

    /**
     * Get bitmap attribute used to mark empty cells
     * @return descriptor of the empty indicator attribute or NULL is array is regular
     */
        const AttributeDesc* getEmptyBitmapAttribute() const;

    /**
     * Get vector of array attributes
     * @return array attributes
     */
    const Attributes& getAttributes(bool excludeEmptyBitmap = false) const;

    /**
     * Get vector of array dimensions
     * @return array dimensions
     */
    const Dimensions& getDimensions() const;

    /**
     * Check if position belongs to the array boundaries
     */
    bool contains(Coordinates const& pos) const;

    /**
     * Get position of the chunk for the given coordinates
     * @param pos in: element position, out: chunk position (position if first chunk element not including overlaps)
     */
    void getChunkPositionFor(Coordinates& pos) const;

   /**
     * Get position of the chunk for the given coordinates
     * @param pos in: element position
     * @param box in: bounding box for offset distributions
     */
    uint64_t getChunkNumber(Coordinates const& pos) const;

    /**
     * Get flags associated with array
     * @return flags
     */
    int32_t getFlags() const
    {
        return _flags;
    }

    /**
     * Trim unbounded array to its actual boundaries
     */
    void trim();

    /**
     * Get array comment
     * @return array comment
     */
    const std::string& getComment() const;

    /**
     * Checks if arrya has non-zero overlap in any dimension
     */
    bool hasOverlap() const;
    
    /**
     * Check if array is updatable
     */
    bool isImmutable() const
    {
        return _flags & IMMUTABLE;
    }

    /**
     * Check if array is local array
     */
    bool isLocal() const
    {
        return _flags & LOCAL;
    }

    /**
     * Check if array contains overlaps
     */
    bool containsOverlaps() const;

    /**
     * Get partitioning schema
     */
    PartitioningSchema getPartitioningSchema() const
    {
        return _ps;
    }

    /**
     * Set partitioning schema
     */
    void setPartitioningSchema(PartitioningSchema ps)
    {
       _ps = ps;
    }

    /**
     * Map value of this coordinate to the integer value
     * @param dimensionNo dimension index
     * @param value original coordinate value
     * @param mode coordinate mapping mode
     * @return ordinal number to which this value is mapped
     * @todo move the query parameter into the constructor (and possibly split the client and server interfaces)
     */
    Coordinate getOrdinalCoordinate(size_t dimensionNo, Value const& value, CoordinateMappingMode mode,
                                    const boost::shared_ptr<Query>& query = boost::shared_ptr<Query>()) const;

    /**
     * Perform reverse mapping of integer dimension to the original dimension domain
     * @param dimensionNo dimension index
     * @param pos integer coordinate
     * @return original value for the coordinate
     * @todo move the query parameter into the constructor (and possibly split the client and server interfaces)
     */
    Value getOriginalCoordinate(size_t dimensionNo, Coordinate pos,
                                const boost::shared_ptr<Query>& query = boost::shared_ptr<Query>()) const;

    /**
     * Get descriptor of the array with coordinate index
     * @param dimensionNo dimension index
     * @param indexDesc [OUT] coordinate index array descriptor
     */
    void getMappingArrayDesc(size_t dimensionNo, ArrayDesc& indexDesc) const;

    /**
     * Get name of array with coordinates index
     * @param dimensionNo dimension index
     */
    std::string const& getMappingArrayName(size_t dimensionNo) const;

    /**
     * Create name of array with coordinates index
     * @param dimensionNo dimension index
     */
    std::string createMappingArrayName(size_t dimensionNo, VersionID version) const;


    

    /**
     * Add alias to all objects of schema
     *
     * @param alias alias name
     */
    void addAlias(const std::string &alias);

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
            ar & _id;
            ar & _name;
            ar & _attributes;
            ar & _dimensions;
            ar & _flags;
            ar & _comment;

            if (Archive::is_loading::value)
            {
                    locateBitmapAttribute();
            }
    }

    bool operator ==(ArrayDesc const& other) const;

    bool operator !=(ArrayDesc const& other) const {
        return !(*this == other);
    }

    void cutOverlap();
    Dimensions grabDimensions(VersionID version) const;

    bool coordsAreAtChunkStart(Coordinates const& coords) const;
    bool coordsAreAtChunkEnd(Coordinates const& coords) const;

    void addAttribute(AttributeDesc const& newAttribute);

    size_t _accessCount;
    ~ArrayDesc();

private:
    void locateBitmapAttribute();
    void initializeDimensions();

    ArrayID _id;
    std::string _name;
    Attributes _attributes;
    Attributes _attributesWithoutBitmap;
    Dimensions _dimensions;
    AttributeDesc* _bitmapAttr;
    int32_t _flags;
    std::string _comment;
    PartitioningSchema _ps;
};

std::ostream& operator<<(std::ostream& stream,const Attributes& ob);
std::ostream& operator<<(std::ostream& stream,const ArrayDesc& ob);

/**
 * Print only the persistent part of the array descriptor
 */
void printSchema(std::ostream& stream,const ArrayDesc& ob);

/**
 * Attribute descriptor
 */
class AttributeDesc
{
public:
    enum AttributeFlags {
        IS_NULLABLE = 1,
        IS_EMPTY_INDICATOR = 2
    };

    /**
     * Construct empty attribute descriptor (for receiving metadata)
     */
    AttributeDesc();
    virtual ~AttributeDesc() {}

    /*
     * Construct full attribute descriptor
     *
     * @param arrayId identifier of appropriate array
     * @param id attribute identifier
     * @param name attribute name
     * @param type attribute type
     * @param flags attribute flags
     * @param defaultCompressionMethod default compression method for this attribute
     * @param alias attribute alias
     * @param rereserve percent of chunk space reserved for future updates
     * @param defaultValue default attribute value (if NULL, then use predefined default value: zero for scalar types, empty for strings,...)
     * @param comment documentation comment
     */
    AttributeDesc(AttributeID id, const std::string &name, TypeId type, int16_t flags,
                  uint16_t defaultCompressionMethod,
                  const std::set<std::string> &aliases = std::set<std::string>(),
                  Value const* defaultValue = NULL,
                  const std::string &defaultValueExpr = std::string(),
                  std::string const& comment = std::string(),
                  size_t varSize = 0);


    /**
     * Construct full attribute descriptor
     *
     * @param arrayId identifier of appropriate array
     * @param id attribute identifier
     * @param name attribute name
     * @param type attribute type
     * @param flags attribute flags
     * @param defaultCompressionMethod default compression method for this attribute
     * @param alias attribute alias
     * @param rereserve percent of chunk space reserved for future updates
     * @param defaultValue default attribute value (if NULL, then use predefined default value: zero for scalar types, empty for strings,...)
     * @param comment documentation comment
     */
    AttributeDesc(AttributeID id, const std::string &name, TypeId type, int16_t flags,
                  uint16_t defaultCompressionMethod,
                  const std::set<std::string> &aliases,
                  int16_t reserve, Value const* defaultValue = NULL,
                  const std::string &defaultValueExpr = std::string(),
                  std::string const& comment = std::string(),
                  size_t varSize = 0);

    bool operator == (AttributeDesc const& other) const;
    bool operator != (AttributeDesc const& other) const
    {
        return !(*this == other);
    }

    /**
     * Get attribute identifier
     * @return attribute identifier
     */
    AttributeID getId() const;

    /**
     * Get attribute name
     * @return attribute name
     */
    const std::string& getName() const;

    /**
     * Get attribute aliases
     * @return attribute aliases
     */
    const std::set<std::string>& getAliases() const;

    /**
     * Assign new alias to attribute
     * @alias alias name
     */
    void addAlias(const std::string& alias);

    /**
     * Check if such alias present in aliases
     * @alias alias name
     * @return true if such alias present
     */
    bool hasAlias(const std::string& alias) const;

    /**
     * Get chunk reserved space percent
     * @return reserved percent of chunk size
     */
    int16_t getReserve() const;

    /**
     * Get attribute type
     * @return attribute type
     */
    TypeId getType() const;

    /**
     * Check if this attribute can have NULL values
     */
    bool isNullable() const;

    /**
     * Check if this arttribute is empty cell indicator
     */
    bool isEmptyIndicator() const;

    /**
     * Get default compression method for this attribute: it is possible to specify explictely different
     * compression methods for each chunk, but by default one returned by this method is used
     */
    uint16_t getDefaultCompressionMethod() const;

    /**
     * Get default attribute value
     */
    Value const& getDefaultValue() const;

    /**
     * Set default compression method for this attribute: it is possible to specify explictely different
     * compression methods for each chunk, but by default one set by this method is used
     * @param method default compression for this attribute
     */
    void setDefaultCompressionMethod(uint16_t method);

    /**
     * Get attribute flags
     * @return attribute flags
     */
    int getFlags() const;

    /**
     * Get attribute comment
     * @return attribute comment
     */
    const std::string& getComment() const;

    /**
     * Return type size or var size (in bytes) or 0 for truly variable size.
     */
    size_t getSize() const;

    /**
     * Get the optional variable size.v
     */
    size_t getVarSize() const;

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] str buffer to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    virtual void toString (std::ostringstream &str, int indent = 0) const
    {
        for ( int i = 0; i < indent; i++)
        {
                str<<" ";
        }
        str<<"[attDesc] id "<<_id
           <<" name "<<_name
           <<" aliases {";

        BOOST_FOREACH(const std::string& alias, _aliases)
        {
            str << _name << "." << alias << ", ";
        }

        str<<"} type "<<_type
           <<" flags "<<_flags
           <<" compression "<<_defaultCompressionMethod
           <<" reserve "<<_reserve
           <<" default "<<ValueToString(_type, _defaultValue)
           <<" comment "<<_comment<<"\n";
    }

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & _id;
        ar & _name;
        ar & _aliases;
        ar & _type;
        ar & _flags;
        ar & _defaultCompressionMethod;
        ar & _reserve;
        ar & _defaultValue;
        ar & _comment;
        ar & _varSize;
        ar & _defaultValueExpr;
    }

    /**
     * Return expression string which used for default value.
     *
     * @return expression string
     */
    const std::string& getDefaultValueExpr() const;

private:
    AttributeID _id;
    std::string _name;
    std::set<std::string> _aliases;
    TypeId _type;
    int16_t _flags;
    uint16_t _defaultCompressionMethod;
    int16_t _reserve;
    Value _defaultValue;
    std::string _comment;
    size_t _varSize;

    /**
     * Compiled and serialized expression for evaluating default value. Used only for storing/retrieving
     * to/from system catalog. Default value evaluated once after fetching metadata or during schema
     * construction in parser. Later only Value field passed between schemas.
     *
     * We not using Expression object because this class used on client.
     * actual value.
     */
    //TODO: May be good to have separate Metadata interface for client library
    std::string _defaultValueExpr;
};

std::ostream& operator<<(std::ostream& stream, const AttributeDesc& ob);

/**
 * Descriptor of dimension
 */
class DimensionDesc: public ObjectNames
{
public:
    enum DimensionFlags 
    {
        DISTINCT = 0,
        ALL = 1,
        COMPLEX_TRANSFORMATION = 2
    };

    /**
     * Construct empty dimension descriptor (for receiving metadata)
     */
    DimensionDesc();

    virtual ~DimensionDesc() {}

    /**
     * Construct full descriptor (for returning metadata from catalog)
     *
     * @param name dimension name
     * @param start dimension start
     * @param end dimension end
     * @param chunkInterval chunk size in this dimension
     * @param chunkOverlap chunk overlay in this dimension
     * @param type dimension type
     * @param mappingArrayName name of the persistent array from which this dimension was taken
     * @param comment documentation comment
     */
    DimensionDesc(const std::string &name,
                  Coordinate start, Coordinate end,
                  uint32_t chunkInterval, uint32_t chunkOverlap,
                  TypeId type = TID_INT64,
                  int flags = 0,
                  std::string const& mappingArrayName = std::string(),
                  std::string const& comment = std::string()
        );

    /**
     *
     *
     * @param name dimension names and/ aliases
     * @param start dimension start
     * @param end dimension end
     * @param chunkInterval chunk size in this dimension
     * @param chunkOverlap chunk overlay in this dimension
     * @param type dimension type
     * @param mappingArrayName name of the persistent array from which this dimension was taken
     * @param comment documentation comment
     */
    DimensionDesc(const std::string &baseName, const NamesType &names,
                  Coordinate start, Coordinate end,
                  uint32_t chunkInterval, uint32_t chunkOverlap,
                  TypeId type = TID_INT64,
                  int flags = 0,
                  std::string const& mappingArrayName = std::string(),
                  std::string const& comment = std::string()
        );

    /**
     * Construct full descriptor (for returning metadata from catalog)
     *
     * @param name dimension names and/ aliases
     * @param startMin dimension minimum start
     * @param currStart dimension current start
     * @param currMax dimension current end
     * @param endMax dimension maximum end
     * @param chunkInterval chunk size in this dimension
     * @param chunkOverlap chunk overlay in this dimension
     * @param type dimension type
     * @param mappingArrayName name of the persistent array from which this dimension was taken
     * @param comment documentation comment
     * @param funcMapOffset offset for coordinate map function (used for SUBARRAY, THIN)
     * @param funcMapScale scale for coordinate map function (used for THIN)
     */
    DimensionDesc(const std::string &name,
                  Coordinate startMin, Coordinate currStart,
                  Coordinate currEnd, Coordinate endMax,
                  uint32_t chunkInterval, uint32_t chunkOverlap,
                  TypeId type = TID_INT64,
                  int flags = 0,
                  std::string const& mappingArrayName =std::string(),
                  std::string const& comment = std::string(),
                  Coordinate funcMapOffset = 0,
                  Coordinate funcMapScale = 1
    );

    /**
     * Construct full descriptor (for returning metadata from catalog)
     *
     * @param name dimension names and/ aliases
     * @param startMin dimension minimum start
     * @param currStart dimension current start
     * @param currMax dimension current end
     * @param endMax dimension maximum end
     * @param chunkInterval chunk size in this dimension
     * @param chunkOverlap chunk overlay in this dimension
     * @param type dimension type
     * @param mappingArrayName name of the persistent array from which this dimension was taken
     * @param comment documentation comment
     * @param funcMapOffset offset for coordinate map function (used for SUBARRAY, THIN)
     * @param funcMapScale scale for coordinate map function (used for THIN)
     */
    DimensionDesc(const std::string &baseName, const NamesType &names,
                  Coordinate startMin, Coordinate currStart,
                  Coordinate currEnd, Coordinate endMax,
                  uint32_t chunkInterval, uint32_t chunkOverlap,
                  TypeId type = TID_INT64,
                  int flags = 0,
                  std::string const& mappingArrayName = std::string(),
                  std::string const& comment = std::string(),
                  Coordinate funcMapOffset = 0,
                  Coordinate funcMapScale = 1
    );

    bool operator == (DimensionDesc const& other) const;
    bool operator != (DimensionDesc const& other) const
    {
        return !(*this == other);
    }

    /**
     * Get minimum value for array index - _startMin
     * @return minimum dimension start
     */
    Coordinate getStartMin() const;

    /**
     * Get current start for array index - _currStart
     * @return current dimension start
     */
    Coordinate getCurrStart() const;

    /**
     * Get from catalog low boundary for the specified dimension
     * @return low boundary for the dimension
     */
    Coordinate getLowBoundary() const;

    /**
     * Get from catalog high boundary for the specified dimension
     * @return high boundary for the dimension
     */
    Coordinate getHighBoundary() const;

    /**
     * Get dimension start ( _startMin  )
     * @return dimension start
     */
    Coordinate getStart() const;

    /**
     * Get current end for array index - _currEnd
     * @return current dimension end
     */
    Coordinate getCurrEnd() const;

    /**
     * Get maximum end for array index - _endMax
     * @return maximum dimension end
     */
    Coordinate getEndMax() const;

    /**
     * Get dimension length
     * @return dimension length
     */
    uint64_t getLength() const;

    /**
     * Get current dimension length
     * @return dimension length
     */
    uint64_t getCurrLength() const;

    /**
     * Get length of chunk in this dimension (not including overlaps)
     * @return step of partitioning array into chunks for this dimension
     */
    uint32_t getChunkInterval() const;

    /**
     * Get chunk overlap in this dimension, so given base coordinate Xi,
     * chunk stores interval of array Ai=[Xi-getChunkOverlap(), Xi+getChunkInterval()+getChunkOverlap()]
     */
    uint32_t getChunkOverlap() const;
#ifndef SWIG
    /**
     * Get name of the persistent array from which this dimension was taken
     */
    std::string const& getMappingArrayName() const;

    /**
     * Get current origin of array along this dimension (not including overlap).
     * @return current starting offset of array int64_t index
     */
    inline int64_t start() const;

    /**
     * Get current length of array along this dimension (not including overlap).
     * @return difference between the current end and the current start of array along this dimension
     */
    inline uint64_t length() const;
#endif
    /**
     * Get type of the dimension coordinate
     * @return dimension coordinate type
     */
    TypeId getType() const;

    /**
     * Get dimnesion comment
     * @return dimension comment
     */
    std::string const& getComment() const;

    /**
     * Return false if this is a non-integer dimension.
     * @return true if this is an integer dimension; false otherwise.
     */
    inline bool isInteger() const
    {
        return _isInteger;
    }

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] str buffer to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    virtual void toString (std::ostringstream &str, int indent = 0) const
    {
        for ( int i = 0; i < indent; i++)
        {
                str<<" ";
        }
        str<<"[dimDesc] names "<<_names
           <<" startMin "<<_startMin
           <<" currStart "<<_currStart
           <<" currEnd "<<_currEnd
           <<" endMax "<<_endMax
           <<" chnkInterval "<<_chunkInterval
           <<" chnkOverlap "<<_chunkOverlap
           <<" type "<<_type
           <<" flags "<<_flags
           <<" mappingArrayName "<<_mappingArrayName
           <<" comment " << _comment
           <<" funcMapOffset " << _funcMapOffset
           <<" funcMapScale " << _funcMapScale << "\n";
    }

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<ObjectNames>(*this);
        ar & _startMin;
        ar & _currStart;
        ar & _currEnd;
        ar & _endMax;
        ar & _chunkInterval;
        ar & _chunkOverlap;
        ar & _type;
        ar & _flags;
        ar & _mappingArrayName;
        ar & _comment;
        ar & _isInteger;
        ar & _funcMapOffset;
        ar & _funcMapScale;        
    }

    int getFlags() const
    {
        return _flags;
    }

    bool isDistinct() const
    {
        return (_flags & ALL) == 0;
    }

    void setMappingArrayName(const std::string &mappingArrayName)
    {
        _mappingArrayName = mappingArrayName;
    }

    Coordinate getFuncMapOffset() const
    {
        return _funcMapOffset;
    }

    Coordinate getFuncMapScale() const
    {
        return _funcMapScale;
    }

    void setFuncMapOffset(Coordinate offs)
    { 
        _funcMapOffset = offs;
    }
    
    void setFuncMapScale(Coordinate scale)
    { 
        _funcMapScale = scale;
    }
    
    void setCurrStart(Coordinate currStart)
    {
        _currStart = currStart;
    }

    void setCurrEnd(Coordinate currEnd)
    {
        _currEnd = currEnd;
    }

    void setEndMax(Coordinate endMax)
    {
        _endMax = endMax;
    }

private:
    friend class ArrayDesc;

    Coordinate _startMin;
    Coordinate _currStart;

    Coordinate _currEnd;
    Coordinate _endMax;

    uint32_t _chunkInterval;
    uint32_t _chunkOverlap;

    ArrayDesc* _array;

    Coordinate _funcMapOffset;
    Coordinate _funcMapScale;
    
    TypeId _type;
    int  _flags;
    std::string _mappingArrayName;
    std::string _comment;
    bool _isInteger;
};

std::ostream& operator<<(std::ostream& stream,const Dimensions& ob);

/**
 * Print only the persistent part of the dimensions
 */
void printSchema(std::ostream& stream,const Dimensions& ob);
std::ostream& operator<<(std::ostream& stream,const DimensionDesc& ob);

/**
 * Print only the persistent part of the dimension descriptor
 */
void printSchema (std::ostream& stream,const DimensionDesc& ob);

/**
 * Descriptor of instance
 */
class InstanceDesc
{
public:
    /**
     * Construct empty instance descriptor (for receiving metadata)
     */
    InstanceDesc();

    /**
     * Construct partial instance descriptor (without id, for adding to catalog)
     *
     * @param host ip or hostname where instance running
     * @param port listening port
     * @param online instance status (online or offline)
     */
    InstanceDesc(const std::string &host, uint16_t port);

    /**
     * Construct full instance descriptor
     *
     * @param instance_id instance identifier
     * @param host ip or hostname where instance running
     * @param port listening port
     * @param online instance status (online or offline)
     */
    InstanceDesc(uint64_t instance_id, const std::string &host, uint16_t port, uint64_t onlineTs);

    /**
     * Get instance identifier
     * @return instance identifier
     */
    uint64_t getInstanceId() const;

    /**
     * Get instance hostname or ip
     * @return instance host
     */
    const std::string& getHost() const;

    /**
     * Get instance listening port number
     * @return port number
     */
    uint16_t getPort() const;

    /**
     * @return time when the instance marked itself online
     */
    uint64_t getOnlineSince() const;

private:
    uint64_t _instance_id;
    std::string _host;
    uint16_t _port;
    uint64_t _online;
};
std::ostream& operator<<(std::ostream& stream,const InstanceDesc& ob);

/**
 * Descriptor of pluggable logical operator
 */
class LogicalOpDesc
{
public:
    /**
     * Default constructor
     */
    LogicalOpDesc()
    {}

    /**
     * Construct descriptor for adding to catalog
     *
     * @param name Operator name
     * @param module Operator module
     * @param entry Operator entry in module
     */
    LogicalOpDesc(const std::string& name, const std::string& module, const std::string& entry) :
            _name(name),
            _module(module),
            _entry(entry)
    {}

    /**
     * Construct full descriptor
     *
     * @param logicalOpId Logical operator identifier
     * @param name Operator name
     * @param module Operator module
     * @param entry Operator entry in module
     */
    LogicalOpDesc(OpID logicalOpId, const std::string& name, const std::string& module,
                    const std::string& entry) :
            _logicalOpId(logicalOpId),
            _name(name),
            _module(module),
            _entry(entry)
    {}

    /**
     * Get logical operator identifier
     *
     * @return Operator identifier
     */
    OpID getLogicalOpId() const
    {
            return _logicalOpId;
    }

    /**
     * Get logical operator name
     *
     * @return Operator name
     */
    const std::string& getName() const
    {
            return _name;
    }

    /**
     * Get logical operator module
     *
     * @return Operator module
     */
    const std::string& getModule() const
    {
            return _module;
    }

    /**
     * Get logical operator entry in module
     *
     * @return Operator entry
     */
    const std::string& getEntry() const
    {
            return _entry;
    }

private:
    OpID _logicalOpId;
    std::string _name;
    std::string _module;
    std::string _entry;
};

class PhysicalOpDesc
{
public:
    /**
     * Default constructor
     */
    PhysicalOpDesc()
    {}

    PhysicalOpDesc(const std::string& logicalOpName, const std::string& name,
                const std::string& module, const std::string& entry) :
        _logicalOpName(logicalOpName),
        _name(name),
        _module(module),
        _entry(entry)
    {}

    /**
     * Construct full descriptor
     *
     * @param physicalOpId Operator identifier
     * @param logicalOpName Logical operator name
     * @param name Physical operator name
     * @param module Operator module
     * @param entry Operator entry in module
     * @return
     */
    PhysicalOpDesc(OpID physicalOpId, const std::string& logicalOpName,
                const std::string& name, const std::string& module, const std::string& entry) :
        _physicalOpId(physicalOpId),
        _logicalOpName(logicalOpName),
        _name(name),
        _module(module),
        _entry(entry)
    {}

    /**
     * Get physical operator identifier
     *
     * @return Operator identifier
     */
    OpID getId() const
    {
        return _physicalOpId;
    }

    /**
     * Get logical operator name
     *
     * @return Operator name
     */
    const std::string& getLogicalName() const
    {
        return _logicalOpName;
    }

    /**
     * Get physical operator name
     *
     * @return Operator name
     */
    const std::string& getName() const
    {
        return _name;
    }

    /**
     * Get physical operator module
     *
     * @return Operator module
     */
    const std::string& getModule() const
    {
        return _module;
    }

    /**
     * Get physical operator entry in module
     *
     * @return Operator entry
     */
    const std::string& getEntry() const
    {
        return _entry;
    }

  private:
    OpID _physicalOpId;
    std::string _logicalOpName;
    std::string _name;
    std::string _module;
    std::string _entry;
};

struct VersionDesc
{
  public:
    ArrayID getArrayID() const {
        return _arrayId;
    }

    VersionID getVersionID() const {
        return _versionId;
    }

    time_t getTimeStamp() const {
        return _timestamp;
    }

    VersionDesc(ArrayID arrayID, VersionID versionId, time_t timestamp) {
        _arrayId = arrayID;
        _versionId = versionId;
        _timestamp = timestamp;
    }
    VersionDesc(const VersionDesc& rhs)
    {
       _arrayId   = rhs._arrayId;
       _versionId = rhs._versionId;
       _timestamp = rhs._timestamp;
    }
    VersionDesc& operator=( const VersionDesc& rhs)
    {
       if ( this == &rhs ) {
          return *this;
       }
       _arrayId   = rhs._arrayId;
       _versionId = rhs._versionId;
       _timestamp = rhs._timestamp;
       return *this;
    }
    VersionDesc() {}

  private:
    ArrayID   _arrayId;
    VersionID _versionId;
    time_t    _timestamp;
};

/**
 * Split a given vesion array name in the form name@version into 'name' and version
 * @param versionName [in]
 * @param ver [out] converted version or 0 if versionName is the base name or the version cannot be extracted
 * @return base array name
 */
std::string splitArrayNameVersion(std::string const& versionName, VersionID& ver);

/**
 * Compose a version array name in the form name@version
 * @param versionName [in] base array name
 * @param ver [in] version (cannot be 0)
 * @return version array name
 */
std::string formArrayNameVersion(std::string const& arrayName, VersionID const& version);

} // namespace

#endif /* METADATA_H_ */
