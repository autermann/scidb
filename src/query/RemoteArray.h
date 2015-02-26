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
 * @file RemoteArray.h
 *
 * @author roman.simakov@gmail.com
 */

#ifndef REMOTEARRAY_H_
#define REMOTEARRAY_H_

#include <boost/enable_shared_from_this.hpp>

#include "util/Semaphore.h"
#include "array/Metadata.h"
#include "array/StreamArray.h"
#include "network/BaseConnection.h"


namespace scidb
{

class Statistics;

/**
 * Class implement fetching chunks from current result array of remote instance.
 */
class RemoteArray: public StreamArray
{
public:
    void handleChunkMsg(boost::shared_ptr< MessageDesc> chunkDesc);

    static boost::shared_ptr<RemoteArray> create(const ArrayDesc& arrayDesc, QueryID queryId, InstanceID instanceID);

private:
    bool proceedChunkMsg(AttributeID attId, MemChunk& chunk);
    void requestNextChunk(AttributeID attId);

    RemoteArray(const ArrayDesc& arrayDesc, QueryID queryId, InstanceID instanceID);

    QueryID _queryId;
    InstanceID _instanceID;
    std::vector<Semaphore> _received;
    std::vector<boost::shared_ptr<MessageDesc> > _messages;
    std::vector<bool> _requested;

    // overloaded method
    ConstChunk const* nextChunk(AttributeID attId, MemChunk& chunk);
};


class RemoteMergedArray: public MultiStreamArray
{
public:
    void handleChunkMsg(boost::shared_ptr< MessageDesc> chunkDesc);

    static boost::shared_ptr<RemoteMergedArray> create(const ArrayDesc& arrayDesc, QueryID queryId, Statistics& statistics);

private:
    bool proceedChunkMsg(size_t stream, AttributeID attId, MemChunk* chunk);
    void requestNextChunk(size_t stream, AttributeID attId, bool positionOnly);

    RemoteMergedArray(const ArrayDesc& arrayDesc, QueryID queryId, Statistics& statistics);

    QueryID _queryId;
    std::vector< std::vector< Semaphore > > _received;
    std::vector< std::vector< boost::shared_ptr<MessageDesc> > > _messages;
    std::vector< std::vector< Coordinates> > _nextPositions; // size() == 0 if there is no next chunk
    std::vector< std::vector<bool> > _hasPositions;    // true if position was requested and false if not (for the first time)
    boost::shared_ptr< Array> _localArray;

    /**
     * @param chunk a pointer to chunk with result. If NULL the position only is requested.
     */
    bool fetchChunk(size_t i, AttributeID attId, MemChunk* chunk = NULL);

    virtual ConstChunk const* nextChunkBody(size_t stream, AttributeID attId, MemChunk& chunk,
            Coordinates const& pos);
    virtual bool nextChunkPos(size_t stream, AttributeID attId, Coordinates& pos);
};


} // namespace

#endif /* REMOTEARRAY_H_ */
