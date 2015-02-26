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
 * ListArrayBuilder.h
 *
 *  Created on: May 25, 2012
 *      Author: poliocough@gmail.com
 */

#ifndef LISTARRAYBUILDER_H_
#define LISTARRAYBUILDER_H_

#include <array/MemArray.h>
#include <smgr/io/InternalStorage.h>


namespace scidb
{

/**
 * Abstract class to build a per-instance MemArray that contains a list of arbitrary elements.
 * Every MemArray built with this class contains two dimensions:
 * [inst=0:numInstances-1,1,0, n=0:*,LIST_CHUNK_SIZE,0]
 * Where n is the zero-based number of the object at that particular instance (0,1,2...).
 * This allows us to create a list of an arbitrary number of objects, on every instance,
 * and present this list seamlessly as a single array.
 *
 * Ideally, every subclass just needs to provide two things:
 * - a getAttributes() function which returns the list of the K attributes for the resulting array.
 *   K must include the emtpy tag.
 * - an addToArray() function which takes an object and splits it into the K-1 attribute values.
 */
template <typename T>
class ListArrayBuilder
{
protected:
    static const uint64_t LIST_CHUNK_SIZE=1000000;
    static const size_t LIST_NUM_DIMS=2;

    bool _initialized;
    shared_ptr<Query> _query;
    boost::shared_ptr<MemArray> _array;
    Coordinates _currPos;
    Coordinates _nextChunkPos;
    vector<shared_ptr<ArrayIterator> > _outAIters;
    vector<shared_ptr<ChunkIterator> > _outCIters;
    size_t _nAttrs;

    /**
     * Add one element to the array
     * @param value the element to add.
     */
    virtual void addToArray(T const& value) =0;

    ListArrayBuilder(): _initialized(false) {}

    /**
     * Construct and return the dimensions of the array.
     * @param query the query context
     * @return dimensions as described above.
     */
    virtual Dimensions getDimensions(boost::shared_ptr<Query> const& query) const;

    /**
     * Construct and return the attributes of the array. The attributes must include the empty tag.
     * @return the attributes that the result array contains.
     */
    virtual Attributes getAttributes() const = 0;

public:
    virtual ~ListArrayBuilder() {}

    /**
     * Construct and return the schema of the array
     * @return the array named "list" using getDimensions and getAttributes
     */
    virtual ArrayDesc getSchema(boost::shared_ptr<Query> const& query) const;

    /**
     * Perform initialization and reset of internal fields. Must be called prior to calling listElement or getArray.
     * @param query the query context
     */
    virtual void initialize(boost::shared_ptr<Query> const& query);

    /**
     * Add information about one element to the array. Initialize must be called prior to this.
     * @param value the element to add
     */
    virtual void listElement(T const& value);

    /**
     * Get the result array. Initialize must be called prior to this.
     * @return a well-formed MemArray that contains information.
     */
    virtual boost::shared_ptr<MemArray> getArray();
};

/**
 * A ListArrayBuilder for listing ChunkDescriptor objects.
 * The second element in the pair is an indicator whether the Chunk Descriptor is "free" (true) or "occupied" (false).
 */
class ListChunkDescriptorsArrayBuilder : public ListArrayBuilder < pair<ChunkDescriptor, bool> >
{
private:
    /**
     * Add information abotu a ChunkDescriptor to the array.
     * @param value the first element is the descriptor, the second element is true if descriptor is free.
     */
    virtual void addToArray(pair<ChunkDescriptor, bool> const& value);

public:
    ListChunkDescriptorsArrayBuilder(): ListArrayBuilder < pair<ChunkDescriptor, bool> >() {}
    virtual ~ListChunkDescriptorsArrayBuilder() {}

    /**
     * Get the attributes of the array.
     * @return the attribute descriptors
     */
    virtual Attributes getAttributes() const;
};

struct ChunkMapEntry
{
    ArrayUAID _uaid;
    StorageAddress _addr;
    DBChunk const* _chunk;

    ChunkMapEntry(ArrayUAID const uaid, StorageAddress const& addr, DBChunk const* const chunk):
        _uaid(uaid),
        _addr(addr),
        _chunk(chunk)
    {}
};


/**
 * A ListArrayBuilder for listing DBChunk objects.
 * Technically, we could take the ArrayUAID from the DBChunk. That value should be the same as the ArrayUAID that
 * points to the node in the tree. But we are taking the value from the tree to be extra defensive.
 */
class ListChunkMapArrayBuilder : public ListArrayBuilder <ChunkMapEntry>
{
private:
    /**
     * Add information about a DBChunk to the array.
     * @param value - a pair of the Unversioned Array ID and DBChunk to list
     */
    virtual void addToArray(ChunkMapEntry const& value);

public:
    ListChunkMapArrayBuilder(): ListArrayBuilder <ChunkMapEntry>() {}
    virtual ~ListChunkMapArrayBuilder() {}

    /**
     * Get the attributes of the array
     * @return the attribute descriptors
     */
    virtual Attributes getAttributes() const;
};

}

#endif /* LISTARRAYBUILDER_H_ */
