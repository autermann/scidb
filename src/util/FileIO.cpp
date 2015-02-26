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
 * @file FileArray.cpp
 *
 * @brief Temporary on-disk array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include <inttypes.h>
#include "util/FileIO.h"
#include "log4cxx/logger.h"
#include "system/Exceptions.h"
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"

using namespace std;

namespace scidb
{
    // Logger for operator. static to prevent visibility of variable outside of file
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc"));

    const size_t MAX_READ_RETRIES = 3;
    const size_t MAX_WRITE_RETRIES = 10;


    void File::writeAll(int fd, const void* data, size_t size, uint64_t offs) 
    { 
        const char* src = (const char*)data;
        size_t nRetries = 0;
        while (size != 0) { 
            ssize_t rc = pwrite(fd, src, size, offs);
            if (rc <= 0) {
                if (rc < 0 && errno == EINTR) { 
                    continue;
                }
#ifdef NDEBUG
                if ((rc == 0 || errno == EAGAIN) && ++nRetries < MAX_WRITE_RETRIES) 
#else
                if (rc == 0 || errno == EAGAIN)                      
#endif
                { 
                    LOG4CXX_DEBUG(logger, "pwrite writes nothing fd " << fd << " src " << size_t(src) <<" size "<<size<<" offs "<<offs<<" rc "<<rc<<" errno "<<errno<<" retries "<<nRetries);
                    sleep(1);
                    continue;
                }
                LOG4CXX_DEBUG(logger, "Failed pwrite call fd " << fd << " src " << size_t(src) <<" size "<<size<<" offs "<<offs<<" rc "<<rc<<" errno "<<errno);
                throw SYSTEM_EXCEPTION(SCIDB_SE_IO, SCIDB_LE_PWRITE_ERROR) << size << offs << errno;
            } else { 
                nRetries = 0;
            }
            src += rc;
            size -= rc;
            offs += rc;
        }
        currentStatistics->writtenSize += size;
        currentStatistics->writtenChunks++;
    }
    
    void File::readAll(int fd, void* data, size_t size, uint64_t offs) 
    { 
        char* dst = (char*)data;
        size_t nRetries = 0;
        while (size != 0) { 
            ssize_t rc = pread(fd, dst, size, offs);
            if (rc <= 0)
            {
                if (rc < 0 && errno == EINTR) { 
                    continue;
                }
#ifdef NDEBUG
                if (rc < 0 && errno == EAGAIN && ++nRetries < MAX_READ_RETRIES) 
#else
                if (rc == 0 || errno == EAGAIN)                      
#endif
                { 
                    LOG4CXX_DEBUG(logger, "pread returns nothing fd " << fd << " dst " << size_t(dst) <<" size "<<size<<" offs "<<offs<<" rc "<<rc<<" errno "<<errno<<" retries "<<nRetries);
                    sleep(1);
                    continue;
                } 
                LOG4CXX_DEBUG(logger, "Failed pread call fd " << fd << " dst " << size_t(dst) <<" size "<<size<<" offs "<<offs<<" rc "<<rc<<" errno "<<errno);
                throw SYSTEM_EXCEPTION(SCIDB_SE_IO, SCIDB_LE_PREAD_ERROR) << size << offs << errno;
            } else {
                nRetries = 0;
            }

            dst += rc;
            size -= rc;
            offs += rc;
        }
        currentStatistics->readSize += size;
        currentStatistics->readChunks++;
    }

    int File::createTemporary(std::string const& arrName, char const* filePath)  {
        std::string dir;
        int fd;
        if (filePath == NULL) { 
#ifndef SCIDB_CLIENT
            dir = Config::getInstance()->getOption<std::string>(CONFIG_TMP_DIR);
#endif
            if (dir.length() != 0 && dir[dir.length()-1] != '/') { 
                dir += '/';
            }
            dir += arrName;
            dir += ".XXXXXX";
            filePath = dir.c_str();
            fd = ::mkstemp((char*)filePath);
        } else { 
            fd = ::open(filePath, O_RDWR|O_TRUNC|O_EXCL|O_CREAT|O_LARGEFILE, 0666);
        }
        if (fd < 0)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_OPEN_FILE) << filePath << errno;
        unlink(filePath); // remove array when it will be closed
        return fd;
    }

#ifndef SCIDB_CLIENT
    inline int64_t getTimeNanos()
    {
        struct timeval tv;
        gettimeofday(&tv,0);
        return ((int64_t) tv.tv_sec) * 1000000000 + ((int64_t) tv.tv_usec) * 1000;
    }
    void BackgroundFileFlusher::FsyncJob::run()
    {
        while (true)
        {
            int64_t totalSyncTime= 0;
            {
                set<int> fds;
                {
                    ScopedMutexLock cs(_flusher->_lock);
                    if ( (_flusher->_running) == false)
                    {
                        return;
                    }
                    fds = _flusher->_fileDescriptors;
                }
                set<int>::iterator it;
                for (it= fds.begin(); it != fds.end(); it++)
                {
                    int64_t t0 = getTimeNanos();
                    if(::fdatasync(*it))
                    {
                        LOG4CXX_DEBUG(logger, "BFF: fdatasync fail on fd "<<(*it)<<" errno "<<errno);
                    }
                    int64_t t1 = getTimeNanos();
                    if(_logThresholdNanos >= 0 && (int64_t)(t1 - t0) > _logThresholdNanos )
                    {
                        double syncTime = ((double)(t1-t0)) / 1000000000.0;
                        LOG4CXX_DEBUG(logger, "BFF: fdatasync fd "<<(*it)<<" time "<<syncTime);
                    }
                    totalSyncTime = totalSyncTime + t1 - t0;
                }
            }
            if ( totalSyncTime < _timeIntervalNanos )
            {
                uint64_t sleepTime = _timeIntervalNanos - totalSyncTime;
                struct timespec req;
                req.tv_sec= sleepTime / 1000000000;
                req.tv_nsec = sleepTime % 1000000000;
                while (::nanosleep(&req, &req) != 0)
                {
                    if (errno != EINTR)
                    {
                        LOG4CXX_DEBUG(logger, "BFF: nanosleep fail errno "<<errno);
                    }
                }
            }
        }
    }
    void BackgroundFileFlusher::start(int timeIntervalMSecs, int logThresholdMSecs, vector<int> const& fileDescriptors)
    {
        ScopedMutexLock cs(_lock);
        if (_running)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_OPERATION_FAILED) << "BFF: error on start; already running";
        }
        _running = true;
        for(size_t i=0; i<fileDescriptors.size(); i++)
        {
            _fileDescriptors.insert(fileDescriptors[i]);
        }
        if (!_threadPool->isStarted())
        {
            _threadPool->start();
        }
        _myJob.reset(new FsyncJob(timeIntervalMSecs, logThresholdMSecs, this));
        _queue->pushJob(_myJob);
    }
    void BackgroundFileFlusher::stop()
    {
        {
            ScopedMutexLock cs(_lock);
            if (_running)
            {
                _running = false;
            }
            else
            {
                return;
            }
        }
        if(!_myJob->wait())
        {
            LOG4CXX_ERROR(logger, "BFF: error on stop.");
        }
    }
    void BackgroundFileFlusher::addDescriptors(vector<int> const& fileDescriptors)
    {
        ScopedMutexLock cs(_lock);
        if(_running)
        {
            for(size_t i=0; i<fileDescriptors.size(); i++)
            {
                _fileDescriptors.insert(fileDescriptors[i]);
            }
        }
    }
    void BackgroundFileFlusher::dropDescriptors(vector<int> const& fileDescriptors)
    {
        ScopedMutexLock cs(_lock);
        if (_running)
        {
            for(size_t i=0; i<fileDescriptors.size(); i++)
            {
                _fileDescriptors.erase(fileDescriptors[i]);
            }
        }
    }
    void BackgroundFileFlusher::addDescriptor (int const& fileDesc)
    {
        ScopedMutexLock cs(_lock);
        if (_running)
        {
            _fileDescriptors.insert(fileDesc);
        }
    }
    void BackgroundFileFlusher::dropDescriptor (int const& fileDesc)
    {
        ScopedMutexLock cs(_lock);
        if(_running)
        {
            _fileDescriptors.erase(fileDesc);
        }
    }
#endif
}
