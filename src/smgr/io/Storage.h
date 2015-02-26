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
 * Storage.h
 *
 *  Created on: 06.01.2010
 *      Author: knizhnik@garret.ru
 *      Description: Storage manager interface
 */

#ifndef STORAGE_H_
#define STORAGE_H_

#include <stdlib.h>
#include <string>
#include <exception>

#include <system/Cluster.h>
#include "array/MemArray.h"
#include "array/Compressor.h"
#include <query/Query.h>

namespace scidb
{
    using namespace std;
    using namespace boost;

    /**
     * Descriptor of database segment.
     * It is used to describe partition or file used to store data
     */
    struct Segment
    {
        /**
         * Path to the file or raw partition
         */
        string path;
        /**
         * Size of raw partition or maximal size of the file
         */
        uint64_t size;

        /**
         * Segment constructor
         */
        Segment(string path, uint64_t size)
        {
            this->path = path;
            this->size = size;
        }

        /**
         * Default constructor
         */
        Segment() {}
    };

    /**
     * Interface of chunks scatter - determine which chunks belongs to this node.
     * This interface is using in Storage.setScatter method and also can be used
     * by scatter/gather component
     */
    class Scatter
    {
      public:
        virtual ~Scatter() {}
        /**
         * Check if local node contains chunk with specified address of first element
         * @return true if chunk with specified address belongs to the current domain
         */
        virtual bool contains(const Address& addr) = 0;
    };

    /**
     * Scatter implementation based on hash function
     */
    class HashScatter : public Scatter
    {
      public:
        virtual ~HashScatter() {}
        /**
         * Return true if nDomains == 0 || chunk.getAddress().hash() % nDomains == domainId-1
         */
        virtual bool contains(const Address& addr);

       /**
         * Construct scatter with specified domainId.
         * @param nDomains total number of domains (should be equal to the number of nodes at which query is planed to be executed)
         * @param domainId identifier of domain [1...nDomains]
         */
        HashScatter(size_t nDomains, size_t domainId);

        /**
         * Construct scatter with current nodeId used as domainId.
         * Use nodeId assigned to this node while node registration as domainId and
         * number of registed nodes as number of domains.
         * So this method is equivalent to HashScatter(SystemCatalog::getInstance()->getNumberOfNodes(),
         *                                             (size_t))StorageManager::getInstance().getNodeId());
         */
        HashScatter();
      private:
        size_t domainId;
        size_t nDomains;
    };


    /**
     * Storage manager interface
     */
    class Storage
    {
      public:
        virtual ~Storage() {}
        /**
         * Open storage manager at specified URL.
         * Format and sematic of storage URL depends in particular implementation of storage manager.
         * For local storage URL specifies path to the description file.
         * Description file has the following format:
         * ----------------------
         * <storage-header-path>
         * <segment1-size-Mb> <segment1-path>
         * <segment2-size-Mb> <segment2-path>
         * ...
         * ----------------------
         * @param url implementation dependent database url
         * @param cacheSize chunk cache size: amount of memory in bytes which can be used to cache most frequently used
         */
        virtual void open(const string& url, size_t cacheSize) = 0;

        /**
         * Get iterator through array chunks available in the storage.
         * Order of iteration is not specified.
         * @param arrDesc array descriptor
         * @param attId attribute identifier
         * @param query in the context of which the iterator is requeted
         * @return shared pointer to the array iterator
         */
        virtual boost::shared_ptr<ArrayIterator> getArrayIterator(const ArrayDesc& arrDesc,
                                                                  AttributeID attId,
                                                                  boost::shared_ptr<Query>& query) = 0;

        /**
         * Flush all changes to the physical device(s).
         * If power fault or system failure happens when there is some unflushed data, then these changes
         * can be lost
         */
        virtual void flush() = 0;

        /**
         * Close storage manager
         */
        virtual void close() = 0;

        /**
         * Set this node identifier
         */
        virtual void setNodeId(NodeID id) = 0;

        /**
         * Get this node identifier
         */
        virtual NodeID getNodeId() const = 0;

        /**
         * Check if storage is local: provide access to the chunks located only in the local file system
         */
        virtual bool isLocal() = 0;

        /**
         * This method allows to virtually partition data in case of DFS-based storage manager.
         * By default DFS storage manager array iterator traverse all array chunks.
         * But this method allows to restrict storage to visit only some fraction of chunks.
         * Scatter.contains() methos is used to determine which chunks belongs to this node.
         * @param pointer to the scatter implementation, if NULL then virtual partitioning is disabled
         */
        virtual void setScatter(Scatter* scatter) = 0;

        /**
         * Remove data of the particular array
         */
        virtual void remove(ArrayID id, bool allVersions = false) = 0;

        /**
         * Map value of this coordinate to the integer value
         * @param indexName name of coordinate index
         * @param value original coordinate value
         * @param query performing the mapping
         * @return ordinal number to which this value is mapped
         */
        virtual Coordinate mapCoordinate(string const& indexName, DimensionDesc const& dim,
                                         Value const& value, CoordinateMappingMode mode,
                                         const boost::shared_ptr<Query>& query) = 0;

        /**
         * Perform reverse mapping of integer dimension to the original dimension domain
         * @param indexName name of coordinate index
         * @param pos integer coordinate
         * @param query performing the mapping
         * @return original value for the coordinate
         */
        virtual Value reverseMapCoordinate(string const& indexName,
                                           DimensionDesc const& dim, Coordinate pos,
                                           const boost::shared_ptr<Query>& query) = 0;

        /**
         * Cleanup coordinates mapping cache
         */
        virtual void cleanupCache() = 0;

        /**
         * Rollback uncompleted updates
         * @param map of updated array which has to be rollbacked
         */
        virtual void rollback(std::map<ArrayID,VersionID> const& undoUpdates) = 0;

        /**
         * Recover node
         * @param recoveredNode ID of recovered node
         */
        virtual void recover(NodeID recoveredNode, boost::shared_ptr<Query>& query) = 0;

        struct DiskInfo 
        {             
            uint64_t used;
            uint64_t available;
            uint64_t clusterSize;
            uint64_t nFreeClusters;
            uint64_t nSegments;
        };
        
        virtual void getDiskInfo(DiskInfo& info) = 0;
    };

    /**
     * Storage factory.
     * By default it points to local storage manager implementation.
     * But it is possible to register any storae manager implementation using setInstance method
     */
    class StorageManager
    {
      public:
        /**
         * Set custom miplementaiton of storage manager
         */
        static void setInstance(Storage& storage) {
            instance = &storage;
        }
        /**
         * Get instance of the storage (it is assumed that there can be only one storage in the application)
         */
        static Storage& getInstance() {
            return *instance;
        }
      private:
        static Storage* instance;
    };
}

#endif
