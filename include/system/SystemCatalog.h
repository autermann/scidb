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
 *      @file
 *
 *      @brief API for fetching and updating system catalog metadata.
 *
 *      @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#ifndef SYSTEMCATALOG_H_
#define SYSTEMCATALOG_H_

#include <string>
#include <vector>
#include <map>
#include <list>
#include <assert.h>
#include <boost/shared_ptr.hpp>

#include "query/TypeSystem.h"
#include "array/Metadata.h"
#include "util/Singleton.h"
#include <system/Cluster.h>

namespace pqxx
{
// forward declaration of pqxx::connection
    class connect_direct;
    template<typename T> class basic_connection;
    typedef basic_connection<connect_direct> connection;
}

namespace scidb
{
    class Mutex;

/**
 * @brief Global object for accessing and manipulating cluster's metadata.
 *
 * On first access catalog object will be created and as result private constructor of SystemCatalog
 * will be called where connection to PostgreSQL will be created. After this
 * cluster can be initialized. Node must add itself to SC or mark itself as online,
 * and it ready to work (though we must wait other nodes online, this can be
 * implemented as PostgreSQL event which will be transparently transported to
 * NetworkManager through callback for example).
 */
    class SystemCatalog : public Singleton<SystemCatalog>
    {
      public:

        class LockDesc
        {
          public:
            typedef enum {INVALID_ROLE=0, COORD, WORKER} NodeRole;
            typedef enum {INVALID_MODE=0, RD, WR, CRT, RM, RNT, RNF} LockMode;

            LockDesc(const std::string& arrayName,
                     const QueryID&  queryId,
                     const NodeID&   nodeId,
                     const NodeRole& nodeRole,
                     const LockMode& lockMode) :
            _arrayName(arrayName),
            _arrayId(0),
            _queryId(queryId),
            _nodeId(nodeId),
            _arrayVersionId(0),
            _arrayVersion(0),
            _nodeRole(nodeRole),
            _lockMode(lockMode) {}

            virtual ~LockDesc() {}
            const std::string& getArrayName() const { return _arrayName; }
            const ArrayID& getArrayId() const { return _arrayId; }
            const QueryID& getQueryId() const { return _queryId; }
            const NodeID&  getNodeId() const { return _nodeId; }
            const VersionID& getArrayVersion() const { return _arrayVersion; }
            const ArrayID& getArrayVersionId() const { return _arrayVersionId; }
            const NodeRole& getNodeRole() const { return _nodeRole; }
            const LockMode& getLockMode() const { return _lockMode; }
            void setArrayId(const ArrayID& arrayId) { _arrayId = arrayId; }
            void setArrayVersionId(const ArrayID& versionId) { _arrayVersionId = versionId; }
            void setArrayVersion(const VersionID& version) { _arrayVersion = version; }
            void setLockMode(const LockMode& mode) { _lockMode = mode; }
            std::string toString()
            {
                std::ostringstream out;
                out << "Lock: arrayName="
                    << _arrayName
                    << ", arrayId="
                    << _arrayId
                    << ", queryId="
                    << _queryId
                    << ", nodeId="
                    << _nodeId
                    << ", nodeRole="
                    << (_nodeRole==COORD ? "COORD" : "WORKER")
                    << ", lockMode="
                    << _lockMode
                    << ", arrayVersion="
                    << _arrayVersion
                    << ", arrayVersionId="
                    << _arrayVersionId;
                return out.str();
            }
          private:
            LockDesc(const LockDesc&);
            LockDesc& operator=(const LockDesc&);
            bool operator== (const LockDesc&);
            bool operator!= (const LockDesc&);

            std::string _arrayName;
            ArrayID  _arrayId;
            QueryID  _queryId;
            NodeID   _nodeId;
            ArrayID  _arrayVersionId;
            VersionID _arrayVersion;
            NodeRole _nodeRole; // 1-coordinator, 2-worker
            LockMode  _lockMode; // {1=read, write, remove, renameto, renamefrom}
        };
        
/**
 * Rename old array (and all of its versions) to the new name
 * @param[in] old_array_name
 * @param[in] new array_name
 * @throws SystemException(SCIDB_LE_ARRAY_DOESNT_EXIST) if old_array_name does not exist
 * @throws SystemException(SCIDB_LE_ARRAY_ALREADY_EXISTS) if new_array_name already exists
 */
 void renameArray(const std::string &old_array_name, const std::string &new_array_name);

/**
 * @throws a scidb::Exception if necessary
 */
typedef boost::function<bool()> ErrorChecker;
 
/**
 * Acquire a lock in the catalog. On a coordinator the method will block until the lock can be acquired.
 * On a worker node, the lock will not be acquired unless a corresponding coordinator lock exists.
 * @param[in] lockDesc the lock descriptor
 * @param[in] errorChecker that is allowed to interrupt the lock acquisition
 * @return true if the lock was acquired, false otherwise
 */
 bool lockArray(const boost::shared_ptr<LockDesc>&  lockDesc, ErrorChecker& errorChecker);

 /**
 * Release a lock in the catalog.
 * @param[in] lockDesc the lock descriptor
 * @return true if the lock was released, false if it did not exist
 */
 bool unlockArray(const boost::shared_ptr<LockDesc>& lockDesc);

 /**
 * Update the lock with new fields. Array name, query ID, node ID, node role
 * cannot be updated after the lock acquisition.
 * @param[in] lockDesc the lock descriptor
 * @return true if the lock was released, false if it did not exist
 */
 bool updateArrayLock(const boost::shared_ptr<LockDesc>& lockDesc);

 /**
 * Get all arrays locks from the catalog for a given node.
 * @param[in] nodeId
 * @param[in] coordLocks locks acquired as in the coordinator role
 * @param[in] workerLocks locks acquired as in the worker role
 */
 void readArrayLocks(const NodeID nodeId,
                     std::list< boost::shared_ptr<LockDesc> >& coordLocks,
                     std::list< boost::shared_ptr<LockDesc> >& workerLocks);

 /**
  * Delete all arrays locks from the catalog on a given node.
  * @param[in] nodeId
  * @return number of locks deleted
  */
 uint32_t deleteArrayLocks(const NodeID& nodeId);

 /**
  * Delete all arrays locks from the catalog for a given query on a given node.
  * @param[in] nodeId
  * @param[in] queryId
  * @return number of locks deleted
  */
uint32_t deleteArrayLocks(const NodeID& nodeId, const QueryID& queryId);

 /**
  * Check if a coordinator lock for given array name and query ID exists in the catalog
  * @param[in] arrayName
  * @param[in] queryId
  * @return the lock found in the catalog possibly empty
  */
 boost::shared_ptr<LockDesc> checkForCoordinatorLock(const std::string& arrayName,
                                                     const QueryID& queryId);


        /**
         * Populate PostgreSQL database with metadata, generate cluster UUID and return
         * it as result
         *
         * @return Cluster UUID
         */
        const std::string& initializeCluster();

        /**
         * @return is cluster ready to work?
         */
        bool isInitialized() const;


        /**
         * @return UUID if cluster initialized else - void string
         */
        const std::string& getClusterUuid() const;

        /**
         * Add new array to catalog by descriptor
         * @param[in] array_desc Descriptor populated with array metadata
         * @param[in] ps Partitioning scheme for mapping array data to nodes
         * @return global ID assigned to the created array
         */
        ArrayID addArray(const ArrayDesc &array_desc, PartitioningSchema ps);

        /**
         * Update array descriptor. The value of the array_desc.getId()
         * identifies the array record to be updated to the new description,
         * which allows you to change an array's name.
         *
         * @param[in] array_desc Descriptor populated with array metadata
         */
        void updateArray(const ArrayDesc &array_desc);

        /**
         * Fills vector with array names from the persistent catalog manager.
         * @param arrays Vector of strings
         */
        void getArrays(std::vector<std::string> &arrays) const;

        /**
         * Checks if there is an array with the specified ID in the catalog. First
         * check the local instance's list of arrays. If the array is not present
         * in the local catalog management, check the persistent catalog manager.
         *
         * @param[in] array_id Array id
         * @return true if there is array with such ID, false otherwise
         */
        bool containsArray(const ArrayID array_id) const;

        /**
         * Checks if there is array with specified name in the storage. First
         * check the local instance's list of arrays. If the array is not present
         * in the local catalog management, check the persistent catalog manager.
         *
         * @param[in] array_name Array name
         * @return true if there is array with such name in the storage, false otherwise
         */
        bool containsArray(const std::string &array_name) const;

        /**
         * Get array ID by array name
         * @param[in] array_name Array name
         * @return array id or 0 if there is no array with such name
         */
        ArrayID findArrayByName(const std::string &array_name) const;

        /**
         * Returns array metadata using the array name.
         * @param[in] array_name Array name
         * @param[out] array_desc Array descriptor
         * @param[in] throwException throw exception if array with specified name is not found
         * @return true if array is found, false if array is not found and throwException is false
         */
        bool getArrayDesc(const std::string &array_name, ArrayDesc &array_desc, const bool throwException = true);

        /**
         * Returns array metadata using the array name.
         * @param[in] array_name Array name
         * @param[in] array_version version identifier or LAST_VERSION
         * @param[out] array_desc Array descriptor
         * @param[in] throwException throw exception if array with specified name is not found
         * @return true if array is found, false if array is not found and throwException is false
         */
        bool getArrayDesc(const std::string &array_name, VersionID version, ArrayDesc &array_desc, const bool throwException = true);

        /**
         * Returns array metadata by its ID
         * @param[in] id array identifier
         * @param[out] array_desc Array descriptor
         */
        void getArrayDesc(const ArrayID id, ArrayDesc &array_desc);

        /**
         * Returns array metadata by its ID
         * @param[in] id array identifier
         * @return Array descriptor
         */        
        boost::shared_ptr<ArrayDesc> getArrayDesc(const ArrayID id);

        /**
         * Returns array partitioning scheme by its ID
         * @param[in] id array identifier
         * @return Array partitionins scheme
         */
        PartitioningSchema getPartitioningSchema(const ArrayID arrayId);

        /**
         * Delete array from catalog by its name.
         * @param[in] array_name Array name
         * @return true if array was deleted, false if it did not exist
         */
        bool deleteArray(const std::string &array_name);
        
        /**
         * Delete array from persistent system catalog manager by its ID
         * @param[in] id array identifier
         */
        void deleteArray(const ArrayID id);

        /**
         * Remove array metadata from cache
         * @param[in] array_name Array name
         */
        void deleteArrayCache(const std::string &array_name);

        /**
         * Remove array metadata from cache
         * @param id array identifier
         */
        void deleteArrayCache(const ArrayID id);

        /**
         * Cleanup local system catalog cache
         */
        void cleanupCache();

        /**
         * Delete all array descriptors for a given base name from the cache.
         * This includes the version and index array descriptors
         * i.e. baseArrayName@* & baseArrayName:*
         * @param baseArrayName
         */
        void deleteAllArrayVersionsFromCacheByName(const std::string& baseArrayName);
        
        /**
         * Create new version of the array
         * @param[in] id array ID
         * @param[in] version_array_id version array ID
         * @return identifier of newly create version
         */
        VersionID createNewVersion(const ArrayID id, const ArrayID version_array_id);

        /**
         * Delete version of the array
         * @param[in] arrayID array ID
         * @param[in] versionID version ID
         */
        void deleteVersion(const ArrayID arrayID, const VersionID versionID);

        /**
         * Get last version of the array
         * @param[in] id array ID
         * @return identifier of last array version or 0 if this array has no versions
         */
        VersionID getLastVersion(const ArrayID id);

        /**
         * Get the latest version preceeding specified timestamp
         * @param[in] id array ID
         * @param[in] timestamp string with timestamp
         * @return identifier ofmost recent version of array before specified timestamp or 0 if there is no such version
         */
        VersionID lookupVersionByTimestamp(const ArrayID id, const uint64_t timestamp);

        /**
         * Get list of updatable array's versions
         * @param[in] arrayId array identifier
         * @return vector of VersionDesc
         */
        std::vector< VersionDesc> getArrayVersions(const ArrayID array_id) const;

        /**
         * Get array actual upper boundary
         * @param[in] id array ID
         * @return array of maximal coordinates of array elements
         */
        Coordinates getHighBoundary(const ArrayID array_id);

        /**
         * Get array actual low boundary
         * @param[in] id array ID
         * @return array of minimum coordinates of array elements
         */
        Coordinates getLowBoundary(const ArrayID array_id);

        /**
         * Update array high and low boundaries
         * @param[in] id array ID
         * @param[in] low low boundary
         * @param[in] high high boundary
         */
        void updateArrayBoundaries(ArrayID id, Coordinates const& low, Coordinates const& high);

        /**
         * Get number of registered nodes
         * return total number of nodes registered in catalog
         */
        uint32_t getNumberOfNodes() const;

        /**
         * Add new node to catalog
         * @param[in] node Node descriptor
         * @return Identifier of node (ordinal number actually)
         */
        uint64_t addNode(const NodeDesc &node) const;

        /**
         * Return all nodes registered in catalog.
         * @param[out] nodes Nodes vector
         */
        void getNodes(Nodes &nodes) const;

        /**
         * Get node metadata by its identifier
         * @param[in] node_id Node identifier
         * @param[out] node Node metadata
         */
        void getNode(NodeID node_id, NodeDesc &node) const;

        /**
         * Switch node to online and update its host and port
         * @param[in] node_id Node identifier
         * @param[in] host Node host
         * @param[in] port Node port
         */
        void markNodeOnline(const NodeID node_id, const std::string host, const uint16_t port) const;

        /**
         * Switch node to offline
         * @param[in] node_id Node identifier
         */
        void markNodeOffline(const NodeID node_id) const;

        /**
         * Set default compression method for the specified array attribute:
         * @param arrId array identifier
         * @param attId attribute identifier
         * @param compressionMethod default compression for this attribute
         */
        void setDefaultCompressionMethod(const ArrayID arrId, const AttributeID attId,
                                         const int16_t compressionMethod);

        /**
         * Temporary method for connecting to PostgreSQL database used as metadata
         * catalog
         *
         * @param[in] connectionString something like 'host=host_with_ph port=5432 dbname=catalog_db_name user=user_name password=users_password'
         */
        void connect(const std::string& connectionString);

        /**
         * Temporary method for checking connection to catalog's database.
         *
         * @return is connection established
         */
        bool isConnected() const;

        /**
         * Load library, and record loaded library in persistent system catalog
         * manager.
         *
         * @param[in] library name
         */
        void addLibrary(const std::string& libraryName) const;

        /**
         * Get info about loaded libraries from the persistent system catalog
         * manager.
         *
         * @param[out] vector of library names
         */
        void getLibraries(std::vector< std::string >& libraries) const;

        /**
         * Unload library.
         *
         * @param[in] library name
         */
        void removeLibrary(const std::string& libraryName) const;

      private:

    /**
     * Helper method to get an appropriate SQL string for a given lock
     */
    static std::string getLockInsertSql(const boost::shared_ptr<LockDesc>& lockDesc);

        /**
         * Default constructor for SystemCatalog()
         */
        SystemCatalog();
        ~SystemCatalog();

        boost::shared_ptr<ArrayDesc> reloadArrayDesc(const ArrayID array_id, bool throwException);

        bool _initialized;
        pqxx::connection *_connection;
        std::string _uuid;

        std::map<ArrayID, boost::shared_ptr<ArrayDesc> > arrayDescByID;

        //FIXME: libpq don't have ability of simultaneous access to one connection from
        // multiple threads even on read-only operatinos, so every operation must
        // be locked with this mutex while system catalog using PostgreSQL as storage.
        static Mutex _pgLock;

        friend class Singleton<SystemCatalog>;
    };

} // namespace catalog

#endif /* SYSTEMCATALOG_H_ */
