/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2014 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
* a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

/*
 * DataStore.h
 *
 *  Created on: 10/16/13
 *      Author: sfridella@pardigm4.com
 *      Description: Storage file interface
 */

#ifndef DATASTORE_H_
#define DATASTORE_H_

#include "Storage.h"
#include <dirent.h>
#include <map>
#include <set>
#include <util/FileIO.h>
#include <util/Mutex.h>

namespace scidb
{

class DataStores;

/**
 * @brief   Class which manages on-disk storage for an array.
 *
 * @details DataStore manages the on-disk storage for an array.
 *          To write data into a datastore, space must first be
 *          allocated using the allocateSpace() interface.  The
 *          resulting chunk (offset, len) can then be safely written
 *          to using writeData().  readData() is used to read data
 *          from the data store.  To ensure data and metadata
 *          associated with the datastore is stable on disk,
 *          flush must be called.
 */
class DataStore
{
public:

    typedef uint64_t Guid;

    /**
     * Find space for the chunk of indicated size in the DataStore.
     * @param requestedSize minimum required size
     * @param allocatedSize actual allocated size
     * @throws SystemException on error
     */
    off_t allocateSpace(size_t requestedSize, size_t& allocatedSize);

    /**
     * Write bytes to the DataStore, to a location that is already
     * allocated
     * @param off Location to write, must be allocated
     * @param buffer Data to write
     * @param len Number of bytes to write
     * @param allocatedSize Size of allocated region
     * @throws SystemException on error
     */
    void writeData(off_t off,
                   void const* buffer,
                   size_t len,
                   size_t allocatedSize);

    /**
     * Read a chunk from the DataStore
     * @param off Location of chunk to read
     * @param buffer Place to put data read
     * @param len Size of chunk to read
     * @throws SystemException on error
     */
    void readData(off_t off, void* buffer, size_t len);

    /**
     * Flush dirty data and metadata for the DataStore
     * @throws SystemException on error
     */
    void flush();

    /**
     * Mark chunk as free both in the free lists and on
     * disk
     * @param off Location of chunk to free
     * @param allocated Allocated size of chunk in file
     * @throws SystemException on error
     */
    void freeChunk(off_t off, size_t allocated);

    /**
     * Return the size of the data store
     * @param size Out param size of the store in bytes
     * @param blocks Out param # of 512b blocks used
     */
    void getSizes(off_t& size, blkcnt_t& blocks);

    /**
     * Destroy a DataStore object
     */
    ~DataStore();

    /**
     * Construct a new DataStore object
     */
    DataStore(char const* filename, Guid guid);

private:

    /* Round up size_t value to next power of two
     */
    static size_t roundUpPowerOf2(size_t size);

    /* Persist free lists to disk
       @pre caller has locked the DataStore
       @throws system exception on error
     */
    void persistFreelists();

    /* Read free lists from disk file
       @returns 0 on success
     */
    int readFreelistFromFile();

    /* Invalidate the free-list file on disk
       @pre caller has locked the DataStore
     */
    void invalidateFreelistFile();

    /* Remove the free-list file from disk
       @pre caller has locked the DataStore
     */
    void removeFreelistFile();

    /* Set the data store to be removed from disk on close
     */
    void removeOnClose()
        { _file->removeOnClose(); }

    /* Iterate the free lists and find a free chunk of the requested size
       @pre caller has locked the DataStore
     */
    off_t searchFreelist(size_t request);

    /* Add block to free list and try to consolidate buddy blocks
     */
    void addToFreelist(size_t bucket, off_t off);

    /* Allocate more space into the data store to handle the requested chunk
     */
    void makeMoreSpace(size_t request);

    /* Update the largest free chunk member
     */
    void calcLargestFreeChunk();

    /* Free lists for data store
       power-of-two ---->  set of offsets
     */
    typedef std::map< size_t, std::set<off_t> > DataStoreFreelists;

    /* Header that prepends all chunks on disk
     */
    class DiskChunkHeader
    {
    public:
        static const size_t usedValue;    // special value to mark headers in use
        static const size_t freeValue;    // special value to mark free header
        size_t magic;                
        size_t size;
        DiskChunkHeader(bool free, size_t sz) : 
            magic(free ? freeValue : usedValue),
            size(sz)
            {}
        DiskChunkHeader() :
            magic(freeValue),
            size(0)
            {}
        bool isValid() { return (magic == usedValue) || (magic == freeValue); }
        bool isFree() { return magic == freeValue; }
    };

    /* Serialized free list bucket
     */
    class FreelistBucket
    {
        boost::scoped_array<char> _buf; // serialized data

        /* Pointers into _buf
         */
        size_t* _size;        // total size of the serialized data
        size_t* _key;         // size of blocks in bucket
        size_t* _nelements;   // number of free elements in bucket
        off_t* _offsets;      // offsets of free elements
        uint32_t* _crc;       // crc for entire serialized bucket

    public:
        /* Construct an flb from a free list bucket
         */
        FreelistBucket(size_t key, std::set<off_t>& bucket);
        /* Construct an flb by reading it from a file
         */
        FreelistBucket(File::FilePtr& f, off_t offset);

        /* Serialize the flb to a file
         */
        void write(File::FilePtr& f, off_t offset);
        
        /* Unserialize the flb into the freelist
         */
        void unload(DataStoreFreelists& fl);

        size_t size() { return *_size + sizeof(size_t); }
    };

    DataStores*        _dsm;              // DataStores manager
    Mutex              _dslock;           // lock protects local state
    Guid               _guid;             // unique id for this store
    File::FilePtr      _file;             // handle for data file
    DataStoreFreelists _freelists;        // free blocks in the data file
    size_t             _largestFreeChunk; // size of the biggest chunk in free list
    size_t             _allocatedSize;    // size of the store including free blks
    bool               _dirty;            // unflushed data is present
    bool               _fldirty;          // fl data differs from fl data on-disk

    friend class DataStores;
};


/**
 * @brief   Class which manages on-disk storage for an array.
 *
 * @details DataStores is a singleton class which manages and
 *          keeps track of the currently open DataStore objects.
 *          Callers obtain/close a DataStore by going through
 *          the single DataStores instance.
 */
class DataStores : public Singleton<DataStores>
{
public:

    /**
     * Initialize the global DataStore state
     * @param basepath path to the root of the storage heirarchy
     */
    void initDataStores(char const* basepath);

    /**
     * Get a reference to a specific DataStore
     * @param guid unique identifier for desired datastore
     */
    boost::shared_ptr<DataStore> getDataStore(DataStore::Guid guid);

    /**
     * Remove a data store from memory and (if remove is true) from disk
     * @param guid unique identifier for target datastore
     */
    void closeDataStore(DataStore::Guid guid, bool remove);

    /**
     * Flush all DataStore objects
     * @throws user exception on error
     */
    void flushAllDataStores();

    /**
     * Accessor, return the min allocation size
     */
    size_t getMinAllocSize()
        { return _minAllocSize; }

    /**
     * Accessor, return a ref to the error listener
     */
    InjectedErrorListener<DataStoreInjectedError>&
    getErrorListener()
        { return _listener; }

    /**
     * Constructor
     */
    DataStores() :
        _theDataStores(NULL),
        _basePath(""),
        _minAllocSize(0)
        {}

    /**
     * Destructor
     */
    ~DataStores()
        { _listener.stop(); }

private:
    
    typedef std::map< DataStore::Guid, boost::shared_ptr<DataStore> > DataStoreMap;

    /* Global map of all DataStores
     */
    DataStoreMap* _theDataStores;
    Mutex         _dataStoreLock;

    std::string _basePath;        // base path of data directory
    size_t      _minAllocSize;    // smallest allowed allocation

    /* Error listener for invalidate path
     */
    InjectedErrorListener<DataStoreInjectedError> _listener;
};

/**
 * @brief   Periodically flushes dirty datastores in the background
 *
 * @details Runs a separate thread that wakes up at a configured interval
 *          and flushes any data stores that have been added to its list.
 */
class DataStoreFlusher : public Singleton<DataStoreFlusher>
{
private:
    boost::shared_ptr<JobQueue> _queue;
    boost::shared_ptr<ThreadPool> _threadPool;
    bool _running;
    std::set <DataStore::Guid> _datastores;
    Mutex _lock;

    class DataStoreFlushJob : public Job
    {
    private:
        int64_t _timeIntervalNanos;
        DataStoreFlusher *_flusher;
        
    public:
        DataStoreFlushJob(int timeIntervalMSecs,
                          DataStoreFlusher* flusher):
            Job(boost::shared_ptr<Query>()),
            _timeIntervalNanos( (int64_t) timeIntervalMSecs * 1000000 ),
            _flusher(flusher)
            {}
        
        virtual void run();
    };

    boost::shared_ptr<DataStoreFlushJob> _myJob;
    
public:
    DataStoreFlusher():
        _queue(boost::shared_ptr<JobQueue>(new JobQueue())),
        _threadPool(boost::shared_ptr<ThreadPool>(new ThreadPool(1, _queue))),
        _running(false),
        _datastores(),
        _lock(),
        _myJob()
        {}

    void start(int timeIntervalMSecs);
    void add(DataStore::Guid dsguid);
    void stop();
    
private:
    ~DataStoreFlusher()
        {
            stop();
        }
    
    friend class Singleton<DataStoreFlusher>;
};
    
}

#endif // DATASTORE_H_
