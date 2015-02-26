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
 * @file
 *
 * @brief Database array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include "array/DBArray.h"
#include "smgr/io/InternalStorage.h"
#include "system/Exceptions.h"
#include "system/SystemCatalog.h"

namespace scidb
{
    //
    // DBArray
    //
    string DBArray::getRealName() const
    {
        return SystemCatalog::getInstance()->getArrayDesc(_desc.getId())->getName();
    }
            

    DBArray::DBArray(const DBArray& other)
    : _desc(other._desc),
      _query(other._query)
    {}

    DBArray::DBArray(ArrayID id, const boost::shared_ptr<Query>& query) 
    : _query(query)
    {
        SystemCatalog::getInstance()->getArrayDesc(id, _desc);
        if (query) { 
            query->sharedLock(getRealName());
        }
    }

    DBArray::DBArray(ArrayDesc const& desc, const boost::shared_ptr<Query>& query) 
    : 
    _desc(desc),
    _query(query)
    {
        _desc.setPartitioningSchema(SystemCatalog::getInstance()->getPartitioningSchema(desc.getId()));
        if (query) { 
            query->sharedLock(getRealName());
        }
    }

    DBArray::DBArray(std::string const& name, const boost::shared_ptr<Query>& query)
    : _query(query)
    {
        SystemCatalog::getInstance()->getArrayDesc(name, _desc);
        if (query) { 
            query->sharedLock(name);
        }
    }
    
    std::string const& DBArray::getName() const
    {
        return _desc.getName();
    }

    ArrayID DBArray::getHandle() const
    {
        return _desc.getId();
    }

    ArrayDesc const& DBArray::getArrayDesc() const
    {
        return _desc;
    }

    boost::shared_ptr<ArrayIterator> DBArray::getIterator(AttributeID attId)
    {
       boost::shared_ptr<Query> query(_query.lock());
       return StorageManager::getInstance().getArrayIterator(_desc, attId, query);
    }

    boost::shared_ptr<ConstArrayIterator> DBArray::getConstIterator(AttributeID attId) const
    {
        boost::shared_ptr<Query> query(_query.lock());
        return StorageManager::getInstance().getConstArrayIterator(_desc, attId, query);
    }

    boost::shared_ptr<CoordinateSet> DBArray::getChunkPositions() const
    {
        if( !hasChunkPositions() )
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << "calling getChunkPositions on an invalid array";
        }
        boost::shared_ptr<Query> query(_query.lock());
        boost::shared_ptr<CoordinateSet> result (new CoordinateSet());
        if(query)
        {
            StorageManager::getInstance().getChunkPositions(_desc, query, *(result.get()));
        }
        return result;
    }

    void DBArray::populateFrom(boost::shared_ptr<Array>& input)
    {
        bool vertical = (input->getSupportedAccess() == Array::RANDOM);
        set<Coordinates, CoordinatesLess> newChunkCoords;
        Array::append(input, vertical, &newChunkCoords);
        boost::shared_ptr<Query> query(_query.lock());
        StorageManager::getInstance().removeDeadChunks(_desc, newChunkCoords, query);
    }
}
