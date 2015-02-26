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
 * @file Metadata.cpp
 *
 * @brief Structures for fetching and updating metadata of cluster.
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */
#include <sstream>
#include <boost/foreach.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem/path.hpp>

#ifndef SCIDB_CLIENT
#include "system/Config.h"
#endif
#include "system/SciDBConfigOptions.h"
#include "query/TypeSystem.h"
#include "array/Metadata.h"
#include "system/SystemCatalog.h"
#include "smgr/io/Storage.h"
#include "array/Compressor.h"

using namespace std;

namespace scidb
{
/*
 * For std::vector < uint64_t >
 */
std::ostream& operator<<(std::ostream& stream,const Coordinates& ob)
{
        stream << "{";
        for (size_t i=0,n=ob.size(); i<n; i++)
        {
                if (i)
                        stream << ", " << ob[i];
                else
                        stream << ob[i];
        }
        stream << "}";
        return stream;
}


std::ostream& operator<<(std::ostream& stream, const CoordsToStr& w)
{
    stream << w._co;
    return stream;
}

ObjectNames::ObjectNames()
{
}

ObjectNames::ObjectNames(const std::string &baseName):
    _baseName(baseName)
{
    addName(baseName);
}

ObjectNames::ObjectNames(const std::string &baseName, const NamesType &names):
        _names(names),
        _baseName(baseName)
{
}

void ObjectNames::addName(const std::string &name)
{
    string trimmedName = name;
    trim(trimmedName);
    assert(trimmedName != "");

    if (hasNameAndAlias(name))
        return;

    _names[name] = set<string>();
}

void ObjectNames::addAlias(const std::string &alias, const std::string &name)
{
    if (alias.size() != 0) {
        string trimmedAlias = alias;
        trim(trimmedAlias);
        assert(trimmedAlias != "");

        string trimmedName = name;
        trim(trimmedName);
        assert(trimmedName != "");

        _names[name].insert(alias);
    }
}

void ObjectNames::addAlias(const std::string &alias)
{
    if (alias.size() != 0) {
        string trimmedAlias = alias;
        trim(trimmedAlias);
        assert(trimmedAlias != "");

        BOOST_FOREACH(const NamesPairType &nameAlias, _names)
        {
            _names[nameAlias.first].insert(alias);
        }
    }
}

bool ObjectNames::hasNameAndAlias(const std::string &name, const std::string &alias) const
{
    NamesType::const_iterator nameIt = _names.find(name);

    if (nameIt != _names.end())
    {
        if (alias == "")
            return true;
        else
            return ( (*nameIt).second.find(alias) != (*nameIt).second.end() );
    }

    return false;
}

const ObjectNames::NamesType& ObjectNames::getNamesAndAliases() const
{
    return _names;
}

const std::string& ObjectNames::getBaseName() const
{
    return _baseName;
}

bool ObjectNames::operator==(const ObjectNames &o) const
{
    return (_names == o._names);
}

std::ostream& operator<<(std::ostream& stream, const ObjectNames::NamesType &ob)
{
    for (ObjectNames::NamesType::const_iterator nameIt = ob.begin(); nameIt != ob.end(); ++nameIt)
    {
        if (nameIt != ob.begin())
            stream << ", ";

        stream << (*nameIt).first;

        for (ObjectNames::AliasesType::const_iterator aliasIt = (*nameIt).second.begin(); aliasIt != (*nameIt).second.end(); ++aliasIt)
        {
            if (aliasIt != (*nameIt).second.begin())
                stream << ", ";

            stream << (*aliasIt);
        }
    }

    return stream;
}

void printNames (std::ostream& stream, const ObjectNames::NamesType &ob)
{
    for (ObjectNames::NamesType::const_iterator nameIt = ob.begin(); nameIt != ob.end(); ++nameIt)
    {
        if (nameIt != ob.begin()) {
            stream << ", ";
        }
        stream << (*nameIt).first;
    }
}

/*
 * Class ArrayDesc
 */
ArrayDesc::ArrayDesc() :
        _accessCount(0),
        _arrId(0),
        _uAId(0),
        _versionId(0),
        _name(""),
        _bitmapAttr(NULL),
        _flags(0),
        _ps(psUndefined)
{}

ArrayDesc::ArrayDesc(const std::string &name,
                     const Attributes& attributes,
                     const Dimensions &dimensions,
                     int32_t flags) :
    _accessCount(0),
    _arrId(0),
    _uAId(0),
    _versionId(0),
    _name(name),
    _attributes(attributes),
    _dimensions(dimensions),
    _flags(flags),
    _ps(psUndefined)
{
    locateBitmapAttribute();
    initializeDimensions();
}

ArrayDesc::ArrayDesc(ArrayID arrId, ArrayUAID uAId, VersionID vId,
                     const std::string &name,
                     const Attributes& attributes,
                     const Dimensions &dimensions,
                     int32_t flags, std::string const& comment) :
    _accessCount(0),
    _arrId(arrId),
    _uAId(uAId),
    _versionId(vId),
    _name(name),
    _attributes(attributes),
    _dimensions(dimensions),
    _flags(flags),
    _comment(comment),
    _ps(psUndefined)
{
    //either both 0 or not...
    assert(arrId == 0 || uAId != 0);

    locateBitmapAttribute();
    initializeDimensions();
}


ArrayDesc::ArrayDesc(ArrayDesc const& other)
{
    _arrId = other._arrId;
    _uAId = other._uAId;
    _versionId = other._versionId;
    _name = other._name;
    _attributes = other._attributes;
    _attributesWithoutBitmap = other._attributesWithoutBitmap;
    _dimensions = other._dimensions;
    _bitmapAttr = (other._bitmapAttr != NULL) ? &_attributes[other._bitmapAttr->getId()] : NULL;
    _flags = other._flags;
    _comment = other._comment;
    _accessCount = 0;
    initializeDimensions();
    _ps = other._ps;
}

bool ArrayDesc::operator ==(ArrayDesc const& other) const
{
    return
        _name == other._name &&
        _attributes == other._attributes &&
        _dimensions == other._dimensions &&
        _flags == other._flags;
}


ArrayDesc& ArrayDesc::operator = (ArrayDesc const& other)
{
    _arrId = other._arrId;
    _uAId = other._uAId;
    _versionId = other._versionId;
    _name = other._name;
    _attributes = other._attributes;
    _attributesWithoutBitmap = other._attributesWithoutBitmap;
    _dimensions = other._dimensions;
    _bitmapAttr = (other._bitmapAttr != NULL) ? &_attributes[other._bitmapAttr->getId()] : NULL;
    _flags = other._flags;
    _comment = other._comment;
    initializeDimensions();
    _ps = other._ps;
    return *this;
}

ArrayDesc::~ArrayDesc() 
{
    assert(_accessCount == 0);
}

void ArrayDesc::initializeDimensions()
{
    Coordinate logicalChunkSize = 1;
    for (size_t i = 0, n = _dimensions.size(); i < n; i++) {
        _dimensions[i]._array = this;
        // check that logical size of chunk is less than 2^64: detect overflow during calculation of logical chunk size
        if (_dimensions[i].getChunkInterval() != 0 && logicalChunkSize*_dimensions[i].getChunkInterval()/_dimensions[i].getChunkInterval() != logicalChunkSize) { 
            throw SYSTEM_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_LOGICAL_CHUNK_SIZE_TOO_LARGE);
        }
        logicalChunkSize *= _dimensions[i].getChunkInterval();
    }
}


void ArrayDesc::trim()
{
    for (size_t i = 0, n = _dimensions.size(); i < n; i++) {
        DimensionDesc& dim = _dimensions[i];
        if (dim._startMin == MIN_COORDINATE && dim._currStart != MAX_COORDINATE) {
            dim._startMin = dim._currStart;
        }
        if (dim._endMax == MAX_COORDINATE && dim._currEnd != MIN_COORDINATE) { 
            dim._endMax = (dim._startMin + (dim._currEnd - dim._startMin + dim._chunkInterval) / dim._chunkInterval * dim._chunkInterval + dim._chunkOverlap - 1);
        }
    }
}

bool ArrayDesc::containsOverlaps() const
{
    Dimensions const& dims = getDimensions();
    for (size_t i = 0, n = dims.size(); i < n; i++) {
        if (dims[i].getChunkOverlap() != 0) {
            return true;
        }
    }
    return false;
}

std::string const& ArrayDesc::getMappingArrayName(size_t dimension) const
{
    std::string const& mappingArrayName = _dimensions[dimension].getMappingArrayName();
    if (mappingArrayName.empty()) { 
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_MAPPING_ARRAY) << _dimensions[dimension].getBaseName() << _name;
    }
    return mappingArrayName;
}

string ArrayDesc::createMappingArrayName(size_t dimension, VersionID version) const
{
    stringstream ss;
    ss << "NID_" << _arrId << '@' << version << ':' << _dimensions[dimension].getBaseName();
    return ss.str();
}

Coordinate ArrayDesc::getOrdinalCoordinate(size_t dimension, Value const& value,
                                           CoordinateMappingMode mode,
                                           const boost::shared_ptr<Query>& query) const
{
#ifndef SCIDB_CLIENT
   return (_dimensions[dimension].isInteger())
   ? value.getInt64()
   : StorageManager::getInstance().mapCoordinate(getMappingArrayName(dimension),
                                                 _dimensions[dimension], value, mode, query);
#else
   return value.getInt64();
#endif
}

Value ArrayDesc::getOriginalCoordinate(size_t dimension, Coordinate pos,
                                       const boost::shared_ptr<Query>& query) const
{
#ifndef SCIDB_CLIENT
    if (!_dimensions[dimension].isInteger()) {
       return StorageManager::getInstance().reverseMapCoordinate(getMappingArrayName(dimension),
                                                                 _dimensions[dimension], pos, query);
    }
#endif
    Value value;
    value.setInt64(pos);
    return value;
}

void ArrayDesc::getMappingArrayDesc(size_t dimension, ArrayDesc& indexDesc) const
{
#ifndef SCIDB_CLIENT
    SystemCatalog::getInstance()->getArrayDesc(getMappingArrayName(dimension), indexDesc);
#endif
}

uint64_t ArrayDesc::getChunkNumber(Coordinates const& pos) const
{
    Dimensions const& dims = _dimensions;
    uint64_t no = 0;
    /// The goal here is to produce good hash function without using array dimension sizes (which can be changed in case of unboundary arrays)
    for (size_t i = 0, n = pos.size(); i < n; i++)
    {
        // 1013 is prime number close to 1024. 1024*1024 is assumed to be optimal chunk size for 2-d array.
        // For 1-d arrays value of this constant is not important, because we are multiplying it on 0.
        // 3-d arrays and arrays with more dimensions are less common and using prime number and XOR should provide 
        // well enough (uniform) mixing of bits.
        no = (no * 1013) ^ ((pos[i] - dims[i].getStart()) / dims[i].getChunkInterval());
    }
    return no;
}

bool ArrayDesc::contains(Coordinates const& pos) const
{
    Dimensions const& dims = _dimensions;
    for (size_t i = 0, n = pos.size(); i < n; i++) {
        if (pos[i] < dims[i].getStart() || pos[i] > dims[i].getEndMax()) {
            return false;
        }
    }
    return true;
}

void ArrayDesc::getChunkPositionFor(Coordinates& pos) const
{
    Dimensions const& dims = _dimensions;
    for (size_t i = 0, n = pos.size(); i < n; i++) {
        if ( dims[i].getChunkInterval() != 0) { 
            pos[i] -= (pos[i] - dims[i].getStart()) % dims[i].getChunkInterval();
        }
    }
}

void ArrayDesc::getChunkBoundaries(Coordinates const& chunkPosition,
                                   bool withOverlap,
                                   Coordinates& lowerBound,
                                   Coordinates& upperBound) const
{
#ifndef NDEBUG
    do
    {
        Coordinates alignedChunkPosition = chunkPosition;
        getChunkPositionFor(alignedChunkPosition);
        SCIDB_ASSERT(alignedChunkPosition == chunkPosition);
    }
    while(false);
#endif /* NDEBUG */
    Dimensions const& d = getDimensions();
    Dimensions::size_type const n = d.size();
    SCIDB_ASSERT(n == chunkPosition.size());
    lowerBound = chunkPosition;
    upperBound = chunkPosition;
    for (size_t i = 0; i < n; i++) {
        upperBound[i] += d[i].getChunkInterval() - 1;
    }
    if (withOverlap) {
        for (size_t i = 0; i < n; i++) {
            lowerBound[i] -= d[i].getChunkOverlap();
            upperBound[i] += d[i].getChunkOverlap();
        }
    }
    for (size_t i = 0; i < n; ++i) {
        lowerBound[i] = std::max(lowerBound[i], d[i].getStart());
        upperBound[i] = std::min(upperBound[i], d[i].getEndMax());
    }
}

void ArrayDesc::locateBitmapAttribute()
{
    _bitmapAttr = NULL;
    _attributesWithoutBitmap = _attributes;
    for (size_t i = 0, n = _attributes.size(); i < n; i++) {
        if (_attributes[i].getType() ==  TID_INDICATOR) {
            _bitmapAttr = &_attributes[i];
            _attributesWithoutBitmap.erase(_attributesWithoutBitmap.begin() + i);
        }
    }
}

uint64_t ArrayDesc::getSize() const
{
    uint64_t size = 1;
    uint64_t max = std::numeric_limits<uint64_t>::max();
    for (size_t i = 0, n = _dimensions.size(); i < n; i++)
    {
        uint64_t length = _dimensions[i].getLength();
        //check for uint64_t overflow
        if (length >= INFINITE_LENGTH || length > max / size)
        {
            return INFINITE_LENGTH;
        }
        size *= length;
    }
    return size;
}

uint64_t ArrayDesc::getCurrSize() const
{
    uint64_t size = 1;
    for (size_t i = 0, n = _dimensions.size(); i < n; i++) {
        uint64_t length = _dimensions[i].getCurrLength();
        if (length == INFINITE_LENGTH) {
            return INFINITE_LENGTH;
        }
        size *= length;
    }
    return size;
}

uint64_t ArrayDesc::getUsedSpace() const
{
    uint64_t nElems = getCurrSize();
    if (nElems == INFINITE_LENGTH) {
        return INFINITE_LENGTH;
    }
    size_t totalBitSize = 0;
    for (size_t i = 0, n = _attributes.size(); i < n; i++) {
        totalBitSize +=  TypeLibrary::getType(_attributes[i].getType()).bitSize();
        if (_attributes[i].isNullable()) {
            totalBitSize += 1;
        }
    }
    return (nElems*totalBitSize + 7)/8;
}

uint64_t ArrayDesc::getNumberOfChunks() const
{
    uint64_t nChunks = 1;
    for (size_t i = 0, n = _dimensions.size(); i < n; i++) {
        uint64_t length = _dimensions[i].getLength();
        if (length == INFINITE_LENGTH) {
            return INFINITE_LENGTH;
        }
        nChunks *= (length + _dimensions[i].getChunkInterval() - 1) / _dimensions[i].getChunkInterval();
    }
    return nChunks*_attributes.size();
}

void ArrayDesc::cutOverlap() 
{
    for (size_t i = 0, n = _dimensions.size(); i < n; i++) { 
        _dimensions[i]._chunkOverlap = 0;
    }
}

bool ArrayDesc::hasOverlap() const
{
    for (size_t i = 0, n = _dimensions.size(); i < n; i++) {
        if (_dimensions[i].getChunkOverlap() != 0) { 
            return true;
        }
    }
    return false;
}

Dimensions ArrayDesc::grabDimensions(VersionID version) const
{
    Dimensions dims(_dimensions.size());
    for (size_t i = 0; i < dims.size(); i++) {
        DimensionDesc const& dim = _dimensions[i];
        if (dim._mappingArrayName == createMappingArrayName(i, 0)) {
            dims[i] = DimensionDesc(dim.getBaseName(),
                                    dim.getNamesAndAliases(),
                                    dim.getStartMin(), dim.getCurrStart(),
                                    dim.getCurrEnd(), dim.getEndMax(), dim.getChunkInterval(),
                                    dim.getChunkOverlap(), dim.getType(), dim.getFlags(),
                                    createMappingArrayName(i, version),
                                    dim.getComment(),
                                    dim.getFuncMapOffset(),
                                    dim.getFuncMapScale()
                );

        } else { 
            dims[i] = dim;
        }
    }
    return dims;
}

void ArrayDesc::addAlias(const std::string &alias)
{
    BOOST_FOREACH(AttributeDesc &attr, _attributes)
    {
        attr.addAlias(alias);
    }

    BOOST_FOREACH(AttributeDesc &attr, _attributesWithoutBitmap)
    {
        attr.addAlias(alias);
    }

    BOOST_FOREACH(DimensionDesc &dim, _dimensions)
    {
        dim.addAlias(alias);
    }
}

bool ArrayDesc::coordsAreAtChunkStart(Coordinates const& coords) const
{
    if (coords.size() != _dimensions.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_DIMENSIONS_MISMATCH);

    for (size_t i = 0; i < coords.size(); i++ )
    {
       if ( coords[i] < _dimensions[i].getStartMin() ||
            coords[i] > _dimensions[i].getEndMax() ||
            (coords[i] - _dimensions[i].getStartMin()) % _dimensions[i].getChunkInterval() != 0 )
       {
           return false;
       }
    }
    return true;
}

bool ArrayDesc::coordsAreAtChunkEnd(Coordinates const& coords) const
{
    if (coords.size() != _dimensions.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_DIMENSIONS_MISMATCH);

    for (size_t i = 0; i < coords.size(); i++ )
    {
        if ( coords[i] != _dimensions[i].getEndMax() &&
             (coords[i] < _dimensions[i].getStartMin() ||
              coords[i] > _dimensions[i].getEndMax() ||
              (coords[i] + 1 - _dimensions[i].getStartMin()) % _dimensions[i].getChunkInterval() != 0 ))
        {
            return false;
        }
    }
    return true;
}

void ArrayDesc::addAttribute(AttributeDesc const& newAttribute)
{
    assert(newAttribute.getId() == _attributes.size());

    for (size_t i = 0; i< _dimensions.size(); i++)
    {
        if (_dimensions[i].getBaseName() == newAttribute.getName() || newAttribute.hasAlias(_dimensions[i].getBaseName()))
        {
            throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_DUPLICATE_ATTRIBUTE_NAME) << newAttribute.getName();
        }
    }

    for (size_t i = 0; i < _attributes.size(); i++)
    {
        if (_attributes[i].getName() == newAttribute.getName())
        {
            throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_DUPLICATE_ATTRIBUTE_NAME) << newAttribute.getName();
        }
    }

    _attributes.push_back(newAttribute);

    if (newAttribute.getType() == TID_INDICATOR)
    {
        assert(_bitmapAttr == NULL);
        _bitmapAttr = &_attributes[_attributes.size()-1];
    }
    else
    {
        _attributesWithoutBitmap.push_back(newAttribute);
    }
}

double ArrayDesc::getNumChunksAlongDimension(size_t dimension, Coordinate start, Coordinate end) const
{
    assert(dimension < _dimensions.size());
    if(start==MAX_COORDINATE && end ==MIN_COORDINATE)
    {
        start = _dimensions[dimension].getStartMin();
        end = _dimensions[dimension].getEndMax();
    }
    return ceil((end * 1.0 - start + 1.0) / _dimensions[dimension].getChunkInterval());
}


void printSchema(std::ostream& stream,const ArrayDesc& ob)
{
    if (!ob.getComment().empty()) {
        stream << "\n--- " << ob.getComment() << "\n";
    }
#ifndef SCIDB_CLIENT
    if (Config::getInstance()->getOption<bool>(CONFIG_ARRAY_EMPTYABLE_BY_DEFAULT)) { 
        if (ob.getEmptyBitmapAttribute() == NULL) {
            stream << "not empty ";
        }
    } else {
        if (ob.getEmptyBitmapAttribute() != NULL) {
            stream << "empty ";
        }
    }
#endif
    stream << ob.getName()
           << '<' << ob.getAttributes(true)
           << "> [";
    printSchema(stream, ob.getDimensions());
    stream << ']';
}
std::ostream& operator<<(std::ostream& stream,const ArrayDesc& ob)
{
    if (!ob.getComment().empty()) {
        stream << "\n--- " << ob.getComment() << "\n";
    }
#ifndef SCIDB_CLIENT
    if (Config::getInstance()->getOption<bool>(CONFIG_ARRAY_EMPTYABLE_BY_DEFAULT)) { 
        if (ob.getEmptyBitmapAttribute() == NULL) {
            stream << "not empty ";
        }
    } else {
        if (ob.getEmptyBitmapAttribute() != NULL) {
            stream << "empty ";
        }
    }
#endif
    stream << ob.getName()
           << '<' << ob.getAttributes(true)
           << "> [" << ob.getDimensions() << ']';
    return stream;
}

/*
 * Class AttributeDesc
 */
AttributeDesc::AttributeDesc() :
        _id(0),
        _name(""),
        _type( TypeId( TID_VOID)),
    _flags(0),
    _defaultCompressionMethod(0),
    _reserve(
#ifndef SCIDB_CLIENT
        Config::getInstance()->getOption<int>(CONFIG_CHUNK_RESERVE)
#else
        0
#endif
    )
{
}

AttributeDesc::AttributeDesc(AttributeID id, const std::string &name,  TypeId type, int16_t flags,
                             uint16_t defaultCompressionMethod,
                             const std::set<std::string> &aliases,
                             int16_t reserve, Value const* defaultValue,
                             const string &defaultValueExpr,
                             std::string const& comment,
                             size_t varSize):
    _id(id),
    _name(name),
    _aliases(aliases),
    _type(type),
    _flags(flags | (type ==  TID_INDICATOR ? IS_EMPTY_INDICATOR : 0)),
    _defaultCompressionMethod(defaultCompressionMethod),
    _reserve((flags & ArrayDesc::IMMUTABLE) ? 0 : reserve),
    _comment(comment),
    _varSize(varSize),
    _defaultValueExpr(defaultValueExpr)
{
    if (defaultValue != NULL) {
        _defaultValue = *defaultValue;
    } else {
        _defaultValue = Value(TypeLibrary::getType(type));
        if (flags & IS_NULLABLE) {
            _defaultValue.setNull();
        } else {
            setDefaultValue(_defaultValue, type);
        }
    }
}

AttributeDesc::AttributeDesc(AttributeID id, const std::string &name,  TypeId type, int16_t flags,
        uint16_t defaultCompressionMethod, const std::set<std::string> &aliases,
        Value const* defaultValue,
        const string &defaultValueExpr,
        std::string const& comment,
        size_t varSize) :
    _id(id),
    _name(name),
    _aliases(aliases),
    _type(type),
    _flags(flags | (type ==  TID_INDICATOR ? IS_EMPTY_INDICATOR : 0)),
    _defaultCompressionMethod(defaultCompressionMethod),
    _reserve(
#ifndef SCIDB_CLIENT
        (flags & ArrayDesc::IMMUTABLE) ? 0 : Config::getInstance()->getOption<int>(CONFIG_CHUNK_RESERVE)
#else
        0
#endif
    ),
    _comment(comment),
    _varSize(varSize),
    _defaultValueExpr(defaultValueExpr)
{
    if (defaultValue != NULL) {
        _defaultValue = *defaultValue;
    } else {
        _defaultValue = Value(TypeLibrary::getType(type));
        if (flags & IS_NULLABLE) {
            _defaultValue.setNull();
        } else {
            setDefaultValue(_defaultValue, type);
        }
    }
}

bool AttributeDesc::operator ==(AttributeDesc const& other) const
{
    return
        _id == other._id &&
        _name == other._name &&
        _aliases == other._aliases &&
        _type == other._type &&
        _flags == other._flags &&
        _defaultCompressionMethod == other._defaultCompressionMethod &&
        _reserve == other._reserve &&
        _defaultValue == other._defaultValue &&
        _varSize == other._varSize &&
        _defaultValueExpr == other._defaultValueExpr;
}

AttributeID AttributeDesc::getId() const
{
        return _id;
}

const std::string& AttributeDesc::getName() const
{
        return _name;
}
const std::set<std::string>& AttributeDesc::getAliases() const
{
        return _aliases;
}

void AttributeDesc::addAlias(const string& alias)
{
    string trimmedAlias = alias;
    trim(trimmedAlias);
    _aliases.insert(trimmedAlias);
}

bool AttributeDesc::hasAlias(const std::string& alias) const
{
    if (alias == "")
        return true;
    else
        return (_aliases.find(alias) != _aliases.end());
}

 TypeId AttributeDesc::getType() const
{
        return _type;
}

int AttributeDesc::getFlags() const
{
    return _flags;
}

bool AttributeDesc::isNullable() const
{
        return (_flags & IS_NULLABLE) != 0;
}

bool AttributeDesc::isEmptyIndicator() const
{
        return (_flags & IS_EMPTY_INDICATOR) != 0;
}

uint16_t AttributeDesc::getDefaultCompressionMethod() const
{
        return _defaultCompressionMethod;
}

Value const& AttributeDesc::getDefaultValue() const
{
    return _defaultValue;
}

int16_t AttributeDesc::getReserve() const
{
        return _reserve;
}

std::string const& AttributeDesc::getComment() const
{
        return _comment;
}

size_t AttributeDesc::getSize() const
{
    Type const& type = TypeLibrary::getType(_type);
    return type.byteSize() > 0 ? type.byteSize() : getVarSize();
}

size_t AttributeDesc::getVarSize() const
{
    return _varSize;
}

void AttributeDesc::setDefaultCompressionMethod(uint16_t method)
{
    _defaultCompressionMethod = method;
}

const std::string& AttributeDesc::getDefaultValueExpr() const
{
    return _defaultValueExpr;
}

std::ostream& operator<<(std::ostream& stream,const Attributes& atts)
{
  for (size_t i=0,n=atts.size(); i<n; i++)
  {
        stream << atts[i];
        if (i != n-1)
                stream << ',';
  }
  return stream;
}

std::ostream& operator<<(std::ostream& stream, const AttributeDesc& att)
{
    if (!att.getComment().empty()) {
        stream << "\n--- " << att.getComment() << "\n";
    }
    //don't print NOT NULL because it default behaviour
    stream << att.getName() << ':' << att.getType()
                 << (att.getFlags() & AttributeDesc::IS_NULLABLE ? " NULL" : "");
    if (!att.getDefaultValue().isZero()) {
        stream << " DEFAULT " << ValueToString(att.getType(), att.getDefaultValue());
    }
    if (att.getDefaultCompressionMethod() != CompressorFactory::NO_COMPRESSION) {
        stream << " COMPRESSION '" << CompressorFactory::getInstance().getCompressors()[att.getDefaultCompressionMethod()]->getName() << "'";
    }
    return stream;
}

/*
 * Class DimensionDesc
 */

DimensionDesc::DimensionDesc() :
        ObjectNames(),

        _startMin(0),
        _currStart(0),
        _currEnd(0),
        _endMax(0),
        
        _chunkInterval(0),
        _chunkOverlap(0),
        
        _funcMapOffset(0),
        _funcMapScale(1),

        _type(TID_INT64),     
        _flags(0),
        _isInteger(true)
{}

DimensionDesc::DimensionDesc(const std::string &name, Coordinate start, Coordinate end, uint32_t chunkInterval,
                             uint32_t chunkOverlap, TypeId type, int flags, std::string const& mappingArrayName, std::string const& comment) :
    ObjectNames(name),

    _startMin(start),
    _currStart(MAX_COORDINATE),
    _currEnd(MIN_COORDINATE),
    _endMax(end),

    _chunkInterval(chunkInterval),
    _chunkOverlap(chunkOverlap),
        
    _funcMapOffset(0),
    _funcMapScale(1),

    _type(type),
    _flags(flags),
    _mappingArrayName(mappingArrayName),
    _comment(comment),
    _isInteger(type == TID_INT64)
{
}

DimensionDesc::DimensionDesc(const std::string &baseName, const NamesType &names, Coordinate start, Coordinate end,
                             uint32_t chunkInterval, uint32_t chunkOverlap, TypeId type, int flags, std::string const& mappingArrayName, std::string const& comment) :
    ObjectNames(baseName, names),

    _startMin(start),
    _currStart(MAX_COORDINATE),
    _currEnd(MIN_COORDINATE),
    _endMax(end),

    _chunkInterval(chunkInterval),
    _chunkOverlap(chunkOverlap),
        
    _funcMapOffset(0),
    _funcMapScale(1),

    _type(type),
    _flags(flags),
    _mappingArrayName(mappingArrayName),
    _comment(comment),
    _isInteger(type == TID_INT64)
{
}

DimensionDesc::DimensionDesc(const std::string &name, Coordinate startMin, Coordinate currStart, Coordinate currEnd,
                             Coordinate endMax, uint32_t chunkInterval, uint32_t chunkOverlap, TypeId type, int flags, 
                             std::string const& mappingArrayName, std::string const& comment,
                             Coordinate funcMapOffset, Coordinate funcMapScale) :
    ObjectNames(name),

    _startMin(startMin),
    _currStart(currStart),
    _currEnd(currEnd),
    _endMax(endMax),
    _chunkInterval(chunkInterval),
    _chunkOverlap(chunkOverlap),        
    _funcMapOffset(funcMapOffset),
    _funcMapScale(funcMapScale),
    _type(type),
    _flags(flags),
    _mappingArrayName(mappingArrayName),
    _comment(comment),
    _isInteger(type == TID_INT64)
{
}

DimensionDesc::DimensionDesc(const std::string &baseName, const NamesType &names, Coordinate startMin,
                             Coordinate currStart, Coordinate currEnd, Coordinate endMax, uint32_t chunkInterval, uint32_t chunkOverlap,
                             TypeId type, int flags, std::string const& mappingArrayName, std::string const& comment,
                             Coordinate funcMapOffset, Coordinate funcMapScale) :
    ObjectNames(baseName, names),

    _startMin(startMin),
    _currStart(currStart),
    _currEnd(currEnd),
    _endMax(endMax),
    _chunkInterval(chunkInterval),
    _chunkOverlap(chunkOverlap),
    _funcMapOffset(funcMapOffset),
    _funcMapScale(funcMapScale),
    _type(type),
    _flags(flags),
    _mappingArrayName(mappingArrayName),
    _comment(comment),
    _isInteger(type == TID_INT64)
{
}

std::string const& DimensionDesc::getMappingArrayName() const
{
    return _mappingArrayName;
}

std::string const& DimensionDesc::getComment() const
{
    return _comment;
}



bool DimensionDesc::operator == (DimensionDesc const& other) const
{
    return
        _names == other._names &&
        _startMin == other._startMin &&
        _endMax == other._endMax &&
        _chunkInterval == other._chunkInterval &&
        _chunkOverlap == other._chunkOverlap &&
        _flags == other._flags;
        _type == other._type;
}

Coordinate DimensionDesc::getLowBoundary() const
{
#ifndef SCIDB_CLIENT
    if (_startMin == MIN_COORDINATE) {
        if (_array->getId() != 0) {
            size_t index = this - &_array->_dimensions[0];
            return SystemCatalog::getInstance()->getLowBoundary(_array->getId())[index];
        } else {
            return _currStart;
        }
    }
#endif
    return _startMin;
}

Coordinate DimensionDesc::getHighBoundary() const
{
#ifndef SCIDB_CLIENT
    if (_endMax == MAX_COORDINATE) {
        if (_array->getId() != 0) {
            size_t index = this - &_array->_dimensions[0];
            return SystemCatalog::getInstance()->getHighBoundary(_array->getId())[index];
        } else {
            return _currEnd;
        }
    }
#endif
    return _endMax;
}

Coordinate DimensionDesc::getStart() const
{
        return _startMin;
}

uint64_t DimensionDesc::getLength() const
{
        return _startMin == MIN_COORDINATE || _endMax == MAX_COORDINATE ? INFINITE_LENGTH : (_endMax - _startMin + 1);
}

uint64_t DimensionDesc::getCurrLength() const
{
    Coordinate low = _startMin;
    Coordinate high = _endMax;
#ifndef SCIDB_CLIENT
    if (_startMin == MIN_COORDINATE || _endMax == MAX_COORDINATE) {
        if (_array->getId() != 0) {
            size_t index = this - &_array->_dimensions[0];
            if (_startMin == MIN_COORDINATE) {
                low = SystemCatalog::getInstance()->getLowBoundary(_array->getId())[index];
            }
            if (_endMax == MAX_COORDINATE) {
                high = SystemCatalog::getInstance()->getHighBoundary(_array->getId())[index];
            }
        } else {
            low = _currStart;
            high = _currEnd;
        }
    }
#endif
    /*
     * check for empty array - according to informal agreement,
     * high boundary for empty array is MAX_COORDINATE
     */
    if (low == MAX_COORDINATE || high == MIN_COORDINATE) {
        return 0;
    } else {
        return high - low + 1;
    }
}

Coordinate DimensionDesc::getStartMin() const
{
        return _startMin;
}

Coordinate DimensionDesc::getCurrStart() const
{
        return _currStart;
}

Coordinate DimensionDesc::getCurrEnd() const
{
        return _currEnd;
}

Coordinate DimensionDesc::getEndMax() const
{
        return _endMax;
}

uint32_t DimensionDesc::getChunkInterval() const
{
        return _chunkInterval;
}

uint32_t DimensionDesc::getChunkOverlap() const
{
        return _chunkOverlap;
}

TypeId  DimensionDesc::getType() const
{
    return _type;
}

void printSchema (std::ostream& stream,const Dimensions& dims)
{
  for (size_t i=0,n=dims.size(); i<n; i++)
  {
      printSchema(stream, dims[i]);
      if (i != n-1) {
          stream << ',';
      }
  }
}

std::ostream& operator<<(std::ostream& stream,const Dimensions& dims)
{
  for (size_t i=0,n=dims.size(); i<n; i++)
  {
        stream << dims[i];
        if (i != n-1)
                stream << ',';
  }
  return stream;
}

std::ostream& operator<<(std::ostream& stream,const DimensionDesc& dim)
{
    if (!dim.getComment().empty()) {
        stream << "\n--- " << dim.getComment() << "\n";
    }
    if (dim.isInteger())
    {
        Coordinate start = dim.getStart();
        stringstream ssstart;
        ssstart << start;

        Coordinate end = dim.getEndMax();
        stringstream ssend;
        ssend << end;

        stream << dim.getNamesAndAliases() << '=' << (start == MIN_COORDINATE ? "*" : ssstart.str()) << ':'
            << (end == MAX_COORDINATE ? "*" : ssend.str()) << ","
            << dim.getChunkInterval() << "," << dim.getChunkOverlap();
    }
    else
    {
        stringstream bound;
        if (dim.getLength() == INFINITE_LENGTH)
        {
            bound <<"*";
        }
        else
        {
            bound <<dim.getLength();
        }

        stream << dim.getNamesAndAliases() << '(' << (dim.isDistinct() ? "" : "all ") << dim.getType() << ")=" << bound.str() << ','
               << dim.getChunkInterval() << ',' << dim.getChunkOverlap();
    }

        return stream;
}

void printSchema (std::ostream& stream,const DimensionDesc& dim)
{
    if (!dim.getComment().empty()) {
        stream << "\n--- " << dim.getComment() << "\n";
    }
    if (dim.isInteger())
    {
        Coordinate start = dim.getStart();
        stringstream ssstart;
        ssstart << start;

        Coordinate end = dim.getEndMax();
        stringstream ssend;
        ssend << end;

        printNames(stream, dim.getNamesAndAliases());
        stream << '=' << (start == MIN_COORDINATE ? "*" : ssstart.str()) << ':'
               << (end == MAX_COORDINATE ? "*" : ssend.str()) << ","
               << dim.getChunkInterval() << "," << dim.getChunkOverlap();
    }
    else
    {
        stringstream bound;
        if (dim.getLength() == INFINITE_LENGTH)
        {
            bound <<"*";
        }
        else
        {
            bound <<dim.getLength();
        }

        printNames(stream, dim.getNamesAndAliases());
        stream << '(' << (dim.isDistinct() ? "" : "all ") << dim.getType() << ")=" << bound.str() << ','
               << dim.getChunkInterval() << ',' << dim.getChunkOverlap();
    }
}


/*
 * Class InstanceDesc
 */
InstanceDesc::InstanceDesc() :
        _instance_id(0),
        _host(""),
        _port(0),
        _online(~0)
{
}

InstanceDesc::InstanceDesc(const std::string &host, uint16_t port, const std::string &path) :
        _host(host),
        _port(port),
        _online(~0)
{
     boost::filesystem::path p(path);
     _path = p.normalize().string();
}

InstanceDesc::InstanceDesc(uint64_t instance_id, const std::string &host,
                           uint16_t port, uint64_t online, const std::string &path) :
        _instance_id(instance_id),
        _host(host),
        _port(port),
        _online(online)
{
    boost::filesystem::path p(path);
    _path = p.normalize().string();
}

uint64_t InstanceDesc::getInstanceId() const
{
        return _instance_id;
}

const std::string& InstanceDesc::getHost() const
{
        return _host;
}

const std::string& InstanceDesc::getPath() const
{
        return _path;
}

uint16_t InstanceDesc::getPort() const
{
        return _port;
}

uint64_t InstanceDesc::getOnlineSince() const
{
        return _online;
}

std::ostream& operator<<(std::ostream& stream,const InstanceDesc& instance)
{

    stream << "instance { id = " << instance.getInstanceId()
           << ", host = " << instance.getHost() << ", port = " << instance.getPort()
           << ", has been on-line since " << instance.getOnlineSince()
           << ", path = " << instance.getPath();

  return stream;
}

} // namespace

