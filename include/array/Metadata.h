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
 * @author poliocough@gmail.com
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

#include <system/Utils.h>
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
 * Unversioned Array identifier
 */
typedef uint64_t ArrayUAID;

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
 * Coordinates comparator to be used in std::map
 */
struct CoordinatesLess
{
    bool operator()(const Coordinates& c1, const Coordinates& c2) const
    {
        assert(c1.size() == c2.size());
        for (size_t i = 0, n = c1.size(); i < n; i++) {
            if (c1[i] != c2[i]) {
                return c1[i] < c2[i];
            }
        }
        return false;
    }
};

/**
 * Compare two coordinates and return a number indicating how they differ.
 * @param c1 the left side of the comparsion
 * @param c2 the right side of the comparsion
 * @param nDims the optional number of dimensions to use (could be less than size of coords).
 * @return some negative value if c1 is less than c2;
 *         some positive value if c2 is less than c1,
 *         0 oif both are equal.
 */
inline int64_t coordinatesCompare(Coordinates const& c1, Coordinates const& c2, size_t nDims = 0)
{
    if(nDims == 0)
    {
        nDims = c1.size();
    }
    assert(c1.size() == nDims && c2.size() == nDims);
    for(size_t i = 0; i < nDims; i++ )
    {
        int64_t res = c1[i] - c2[i];
        if (res != 0)
        {
            return res;
        }
    }
    return 0;
}


typedef std::set<Coordinates, CoordinatesLess> CoordinateSet;

/**
 * Array coordinate
 */
std::ostream& operator<<(std::ostream& stream,const Coordinates& ob);

//For some strange STL reason, operator<< does not work on Coordinates.
//So we create this wrapper class. This works:
// LOG4CXX_DEBUG(logger, "My coordinates are "<<CoordsToStr(coords))
struct CoordsToStr
{
public:
    Coordinates const& _co;
    CoordsToStr (const Coordinates& co):
        _co(co)
    {}
};

std::ostream& operator<<(std::ostream& stream, const CoordsToStr& w);

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
 *
 * Some newly introducd partitioning schemas need parameters (last parameter in redistribute(). They are:
 *   - psGroupBy: need a parameter of type (vector<bool>*). The vector has the same size as the full dimensions.
 *     Each element indicates whether the dimension is a groupby dimension.
 */
enum PartitioningSchema
{
    psReplication,
    psRoundRobin,  // WARNING: hashed, NOT actually round-robin anymore, needs name change
    psLocalInstance,
    psByRow,
    psByCol,
    psUndefined,
    psGroupby,
    psScaLAPACK
};

/**
 * The base class for optional data for certain PartitioningSchema.
 */
class PartitioningSchemaData {
public:
    virtual ~PartitioningSchemaData() {}
    virtual PartitioningSchema getID() = 0;
};

/**
 * The class for the optional data for psGroupby.
 */
class PartitioningSchemaDataGroupby: public PartitioningSchemaData {
public:
    /**
     * Whether each dimension is a groupby dim.
     */
    std::vector<bool> _arrIsGroupbyDim;

    virtual PartitioningSchema getID() {
        return psGroupby;
    }
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
     * Check if object has such name and alias (if given).
     *
     * @param name object name
     * @param alias alias name
     * @return true if has
     */
    bool hasNameAndAlias(const std::string &name, const std::string &alias = "") const;


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
        DETERIORATED = 0x08
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
     * @param flags array flags from ArrayDesc::ArrayFlags
     */
    ArrayDesc(const std::string &name, const Attributes& attributes, const Dimensions &dimensions,
              int32_t flags = 0);

    /**
     * Construct full descriptor (for returning metadata from catalog)
     *
     * @param arrId the unique array ID
     * @param uAId the unversioned array ID
     * @param vId the version number
     * @param name array name
     * @param attributes vector of attributes
     * @param dimensions vector of dimensions
     * @param flags array flags from ArrayDesc::ArrayFlags
     * @param documentation comment
     */
    ArrayDesc(ArrayID arrId, ArrayUAID uAId, VersionID vId, const std::string &name, const Attributes& attributes, const Dimensions &dimensions,
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
     * Get the unversioned array id (id of parent array)
     * @return unversioned array id
     */
    inline ArrayUAID getUAId() const
    {
        return _uAId;
    }

    /**
     * Get the unique versioned array id.
     * @return the versioned array id
     */
    inline ArrayID getId() const
    {
        return _arrId;
    }

    /**
     * Get the array version number.
     * @return the version number
     */
    inline VersionID getVersionId() const
    {
        return _versionId;
    }

    /**
     * Set array identifiers
     * @param [in] arrId the versioned array id
     * @param [in] uAId the unversioned array id
     * @param [in] vId the version number
     */
    inline void setIds(ArrayID arrId, ArrayUAID uAId, VersionID vId)
    {
        _arrId = arrId;
        _uAId = uAId;
        _versionId = vId;
    }

    /**
     * Mark array descriptor as deteriaorated (needed for maintain array descriptor's cache)
     */
    inline void invalidate()
    {
        _flags |= DETERIORATED;
    }

    inline bool isInvalidated() const
    {
        return _flags & DETERIORATED;
    }

    /**
     * Get name of array
     * @return array name
     */
    inline const std::string& getName() const
    {
        return _name;
    }

    /**
     * Set name of array
     * @param name array name
     */
    inline void setName(const std::string& name)
    {
        _name = name;
    }

    /**
     * Find out if an array name is for a versioned array.
     * In our current naming scheme, in order to be versioned, the name
     * must contain the "@" symbol, as in "myarray@3". However, NID
     * array names have the form "myarray@3:dimension1" and those arrays
     * are actually NOT versioned.
     * @param[in] name the name to check. A nonempty string.
     * @return true if name contains '@' at position 1 or greater and does not contain ':'.
     *         false otherwise.
     */
    inline static bool isNameVersioned(std::string const& name)
    {
        if (name.size()==0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "calling isNameVersioned on an empty string";
        }

        size_t const locationOfAt = name.find('@');
        size_t const locationOfColon = name.find(':');
        return locationOfAt > 0 && locationOfAt < name.size() && locationOfColon == std::string::npos;
    }

    /**
     * Find out if an array name is for an unversioned array - not a NID and not a version.
     * @param[in] the name to check. A nonempty string.
     * @return true if the name contains neither ':' nor '@'. False otherwise.
     */
    inline static bool isNameUnversioned(std::string const& name)
    {
        if (name.size()==0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "calling isNameUnversioned on an empty string";
        }

        size_t const locationOfAt = name.find('@');
        size_t const locationOfColon = name.find(':');
        return locationOfAt == std::string::npos && locationOfColon == std::string::npos;
    }

    /**
     * Given the versioned array name, extract the corresponing name for the unversioned array.
     * In other words, compute the name of the "parent" array. Or, simply put, given "foo@3" produce "foo".
     * @param[in] name
     * @return a substring of name up to and excluding '@', if isNameVersioned(name) is true.
     *         name otherwise.
     */
    inline static std::string makeUnversionedName(std::string const& name)
    {
        if(isNameVersioned(name))
        {
            size_t const locationOfAt = name.find('@');
            return name.substr(0, locationOfAt);
        }
        return name;
    }

    /**
    * Given the versioned array name, extract the version id.
    * Or, simply put, given "foo@3" produce 3.
    * @param[in] name
    * @return a substring of name after and excluding '@', converted to a VersionID, if
    *         isVersionedName(name) is true.
    *         0 otherwise.
    */
    inline static VersionID getVersionFromName(std::string const& name)
    {
        if(isNameVersioned(name))
        {
           size_t locationOfAt = name.find('@');
           return atol(&name[locationOfAt+1]);
        }
        return 0;
    }

    /**
     * Given an unversioned array name and a version ID, stitch the two together.
     * In other words, given "foo", 3 produce "foo@3".
     * @param[in] name must be a nonempty unversioned name
     * @param[in] version the version number
     * @return the concatenation of name, "@" and version
     */
    inline static std::string makeVersionedName(std::string const& name, VersionID const version)
    {
        assert(!isNameVersioned(name));
        std::stringstream ss;
        ss << name << "@" << version;
        return ss.str();
    }

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
    inline AttributeDesc const* getEmptyBitmapAttribute() const
    {
        return _bitmapAttr;
    }

    /**
     * Get vector of array attributes
     * @return array attributes
     */
    inline Attributes const& getAttributes(bool excludeEmptyBitmap = false) const
    {
        return excludeEmptyBitmap ? _attributesWithoutBitmap : _attributes;
    }

    /**
     * Get vector of array dimensions
     * @return array dimensions
     */
    inline Dimensions const& getDimensions() const
    {
        return _dimensions;
    }

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
      * Get boundaries of the chunk
      * @param chunkPosition - position of the chunk (should be aligned (for example, by getChunkPositionFor)
      * @param withOverlap - include or not include chunk overlap to result
      * @param lowerBound - lower bound of chunk area
      * @param upperBound - upper bound of chunk area
      */
    void getChunkBoundaries(Coordinates const& chunkPosition,
                            bool withOverlap,
                            Coordinates& lowerBound,
                            Coordinates& upperBound) const;
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
    inline int32_t getFlags() const
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
    inline const std::string& getComment() const
    {
        return _comment;
    }

    /**
     * Checks if arrya has non-zero overlap in any dimension
     */
    bool hasOverlap() const;

    /**
     * Check if array is updatable
     */
    inline bool isImmutable() const
    {
        bool res = _flags & IMMUTABLE;
        if(res)
        {
            SCIDB_ASSERT(_uAId == 0 || _uAId == _arrId);
        }
        return res;
    }

    /**
     * Check if array is local array
     */
    inline bool isLocal() const
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
    inline PartitioningSchema getPartitioningSchema() const
    {
        return _ps;
    }

    /**
     * Set partitioning schema
     */
    inline void setPartitioningSchema(PartitioningSchema ps)
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
        ar & _arrId;
        ar & _uAId;
        ar & _versionId;
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

    inline bool operator !=(ArrayDesc const& other) const {
        return !(*this == other);
    }

    void cutOverlap();
    Dimensions grabDimensions(VersionID version) const;

    bool coordsAreAtChunkStart(Coordinates const& coords) const;
    bool coordsAreAtChunkEnd(Coordinates const& coords) const;

    void addAttribute(AttributeDesc const& newAttribute);

    double getNumChunksAlongDimension(size_t dimension, Coordinate start = MAX_COORDINATE, Coordinate end = MIN_COORDINATE) const;

    size_t _accessCount;
    ~ArrayDesc();

private:
    void locateBitmapAttribute();
    void initializeDimensions();


    /**
     * The Versioned Array Identifier - unique ID for every different version of a named array.
     * This is the most important number, returned by ArrayDesc::getId(). It is used all over the system -
     * to map chunks to arrays, for transaction semantics, etc.
     */
    ArrayID _arrId;

    /**
     * The Unversioned Array Identifier - unique ID for every different named array.
     * Used to relate individual array versions to the "parent" array. Some arrays are
     * not versioned. Examples are IMMUTABLE arrays as well as NID arrays.
     * For those arrays, _arrId is is equal to _uAId (and _versionId is 0)
     */
    ArrayUAID _uAId;

    /**
     * The Array Version Number - simple, aka the number 3 in "myarray@3".
     */
    VersionID _versionId;

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

    /**
     * Construct full attribute descriptor
     *
     * @param id attribute identifier
     * @param name attribute name
     * @param type attribute type
     * @param flags attribute flags from AttributeDesc::AttributeFlags
     * @param defaultCompressionMethod default compression method for this attribute
     * @param aliases attribute aliases
     * @param defaultValue default attribute value (if NULL, then use predefined default value: zero for scalar types, empty for strings,...)
     * @param comment documentation comment
     * @param varSize size of variable size type
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
     * @param id attribute identifier
     * @param name attribute name
     * @param type attribute type
     * @param flags attribute flags from AttributeDesc::AttributeFlags
     * @param defaultCompressionMethod default compression method for this attribute
     * @param aliases attribute aliases
     * @param rereserve percent of chunk space reserved for future updates
     * @param defaultValue default attribute value (if NULL, then use predefined default value: zero for scalar types, empty for strings,...)
     * @param comment documentation comment
     * @param varSize size of variable size type
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
     * @param flags dimension flags from DimensionDesc::DimensionFlags
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
     * @param baseName name of dimension derived from catalog
     * @param names dimension names and/ aliases collected during query compilation
     * @param start dimension start
     * @param end dimension end
     * @param chunkInterval chunk size in this dimension
     * @param chunkOverlap chunk overlay in this dimension
     * @param type dimension type
     * @param flags dimension flags from DimensionDesc::DimensionFlags
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
     * @param name dimension name
     * @param startMin dimension minimum start
     * @param currStart dimension current start
     * @param currMax dimension current end
     * @param endMax dimension maximum end
     * @param chunkInterval chunk size in this dimension
     * @param chunkOverlap chunk overlay in this dimension
     * @param type dimension type
     * @param flags dimension flags from DimensionDesc::DimensionFlags
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
     * @param baseName dimension name derived from catalog
     * @param name dimension names and/ aliases collected during query compilation
     * @param startMin dimension minimum start
     * @param currStart dimension current start
     * @param currEnd dimension current end
     * @param endMax dimension maximum end
     * @param chunkInterval chunk size in this dimension
     * @param chunkOverlap chunk overlay in this dimension
     * @param type dimension type
     * @param flags dimension flags from DimensionDesc::DimensionFlags
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
     * @param path to the binary
     */
    InstanceDesc(const std::string &host, uint16_t port, const std::string &path);

    /**
     * Construct full instance descriptor
     *
     * @param instance_id instance identifier
     * @param host ip or hostname where instance running
     * @param port listening port
     * @param online instance status (online or offline)
     */
    InstanceDesc(uint64_t instance_id, const std::string &host,
                 uint16_t port,
                 uint64_t onlineTs,
                 const std::string &path);

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

    /**
     * Get instance binary path
     * @return path to the instance's binary
     */
    const std::string& getPath() const;

private:
    uint64_t _instance_id;
    std::string _host;
    uint16_t _port;
    uint64_t _online;
    std::string _path;
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
    time_t _timestamp;
};


/**
 * Helper function to add the empty tag attribute to Attributes,
 * if the empty tag attribute did not already exist.
 *
 * @param   attributes  the original Attributes
 * @return  the new Attributes
 */
inline Attributes addEmptyTagAttribute(const Attributes& attributes) {
    size_t size = attributes.size();
    assert(size>0);
    if (attributes[size-1].isEmptyIndicator()) {
        return attributes;
    }
    Attributes newAttributes = attributes;
    newAttributes.push_back(AttributeDesc((AttributeID)newAttributes.size(),
            DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,  TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0));
    return newAttributes;
}

/**
 * Helper function to add the empty tag attribute to ArrayDesc,
 * if the empty tag attribute did not already exist.
 *
 * @param   desc    the original ArrayDesc
 * @return  the new ArrayDesc
 */
inline ArrayDesc addEmptyTagAttribute(ArrayDesc const& desc)
{
    return ArrayDesc(desc.getName(), addEmptyTagAttribute(desc.getAttributes()), desc.getDimensions());
}

/**
 * Compute the first position of a chunk, given the chunk position and the dimensions info.
 * @param[in]  chunkPos      The chunk position (not including overlap)
 * @param[in]  dims          The dimensions.
 * @param[in]  withOverlap   Whether overlap is respected.
 * @return the first chunk position
 */
inline Coordinates computeFirstChunkPosition(Coordinates const& chunkPos, Dimensions const& dims, bool withOverlap = true)
{
    assert(chunkPos.size() == dims.size());
    if (!withOverlap) {
        return chunkPos;
    }

    Coordinates firstPos = chunkPos;
    for (size_t i=0; i<dims.size(); ++i) {
        assert(chunkPos[i]>=dims[i].getStart());
        assert(chunkPos[i]<=dims[i].getEndMax());

        firstPos[i] -= dims[i].getChunkOverlap();
        if (firstPos[i] < dims[i].getStart()) {
            firstPos[i] = dims[i].getStart();
        }
    }
    return firstPos;
}

/**
 * Compute the last position of a chunk, given the chunk position and the dimensions info.
 * @param[in]  chunkPos      The chunk position (not including overlap)
 * @param[in]  dims          The dimensions.
 * @param[in]  withOverlap   Whether overlap is respected.
 * @return the last chunk position
 */
inline Coordinates computeLastChunkPosition(Coordinates const& chunkPos, Dimensions const& dims, bool withOverlap = true)
{
    assert(chunkPos.size() == dims.size());

    Coordinates lastPos = chunkPos;
    for (size_t i=0; i<dims.size(); ++i) {
        assert(chunkPos[i]>=dims[i].getStart());
        assert(chunkPos[i]<=dims[i].getEndMax());

        lastPos[i] += dims[i].getChunkInterval()-1;
        if (withOverlap) {
            lastPos[i] += dims[i].getChunkOverlap();
        }
        if (lastPos[i] > dims[i].getEndMax()) {
            lastPos[i] = dims[i].getEndMax();
        }
    }
    return lastPos;
}

/**
 * Get the logical space size of a chunk.
 * @param[in]  low   the low position of the chunk
 * @param[in]  high  the high position of the chunk
 * @return     #cells in the space that the chunk covers
 * @throw      SYSTEM_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_LOGICAL_CHUNK_SIZE_TOO_LARGE)
 */
inline size_t getChunkNumberOfElements(Coordinates const& low, Coordinates const& high)
{
    size_t M = size_t(-1);
    size_t ret = 1;
    assert(low.size()==high.size());
    for (size_t i=0; i<low.size(); ++i) {
        assert(high[i] >= low[i]);
        size_t interval = high[i] - low[i] + 1;
        if (M/ret < interval) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_LOGICAL_CHUNK_SIZE_TOO_LARGE);
        }
        ret *= interval;
    }
    return ret;
}

/**
 * Get the logical space size of a chunk.
 * @param[in]  chunkPos      The chunk position (not including overlap)
 * @param[in]  dims          The dimensions.
 * @param[in]  withOverlap   Whether overlap is respected.
 * @return     #cells in the space the cell covers
 */
inline size_t getChunkNumberOfElements(Coordinates const& chunkPos, Dimensions const& dims, bool withOverlap = true)
{
    Coordinates low = computeFirstChunkPosition(chunkPos, dims, withOverlap);
    Coordinates high = computeLastChunkPosition(chunkPos, dims, withOverlap);
    return getChunkNumberOfElements(low, high);
}

} // namespace

#endif /* METADATA_H_ */
