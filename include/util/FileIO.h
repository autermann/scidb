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
 * @brief Storage implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#ifndef __FILE_IO__
#define __FILE_IO__

//
// The macro defintions below are used two switch on 64-bit IO mode
//
#define __EXTENSIONS__
#define _EXTENSIONS
#define _FILE_OFFSET_BITS 64
#if ! defined(HPUX11_NOT_ITANIUM) && ! defined(L64)
#define _LARGEFILE64_SOURCE 1 // access to files greater than 2Gb in Solaris
#define _LARGE_FILE_API     1 // access to files greater than 2Gb in AIX
#endif

#if !defined(O_LARGEFILE) && !defined(aix64) && (!defined(SOLARIS) || defined(SOLARIS64))
#define O_LARGEFILE 0
#endif

#include <string>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <set>
#include <list>
#include <sys/types.h>
#include <dirent.h>

#include "query/Statistics.h"

#ifndef SCIDB_CLIENT
#include "util/Singleton.h"
#include "util/JobQueue.h"
#include "util/ThreadPool.h"
#endif

namespace scidb
{
    class File
    {
      private:

        /**
         * Close directory
         * @param dirName directory name
         * @param dirp open directory handle
         * @param raise exception on error (true by default)
         * @return errno on error, 0 otherwise
         * @throws SystemException if the underlying system call fails
         */
        static int closeDir(const char* dirName, DIR *dirp, bool raise=true);

    public:
        static void writeAll(int fd, const void* data, size_t size, uint64_t offs);
        static void readAll(int fd, void* data, size_t size, uint64_t offs);

        static int createTemporary(std::string const& arrName, char const* filePath = NULL);
        /**
         * Remove (unlink) file
         * @param file to remove
         * @param raise exception on error (true by default)
         * @return errno on error, 0 otherwise
         * @throws SystemException if the underlying system call fails
         */
        static int remove(char const* filePath, bool raise=true);

        /**
         * Read the contents of a directory
         * @param dirName directory name
         * @param entries directory entries
         * @return errno on error, 0 otherwise
         * @throws SystemException if the underlying system call fails
         */
        static void readDir(const char* dirName, std::list<std::string>& entries);

        /**
         * Create a directory
         * @return true if directory is created or has already existed
         * @param dirPath directory path
         */
        static bool createDir(const std::string& dirPath);

        /**
         * Close file descriptor (restarting after signal interrupt if necessary)
         * @return 0 on success, or -1 otherwise
         */
        static int closeFd(int fd);

        /**
         * Open a file (restarting after signal interrupt if necessary)
         * @return file descriptor or -1
         * @param filePath file path
         * @param flags open mode flags to pass to ::open()
         *        such as O_APPEND, O_CREAT, O_EXCL, etc.
         */
        static int openFile(const std::string& fileName, int flags);
    };

#ifndef SCIDB_CLIENT
    class BackgroundFileFlusher: public Singleton<BackgroundFileFlusher>
    {
    private:
        boost::shared_ptr<JobQueue> _queue;
        boost::shared_ptr<ThreadPool> _threadPool;
        bool _running;
        std::set <int> _fileDescriptors;
        Mutex _lock;

        class FsyncJob : public Job
        {
        private:
            int64_t _timeIntervalNanos;
            int64_t _logThresholdNanos;
            BackgroundFileFlusher *_flusher;

        public:
            FsyncJob(int timeIntervalMSecs,
                     int logThresholdMSecs,
                     BackgroundFileFlusher* flusher):
                 Job(boost::shared_ptr<Query>()),
                 _timeIntervalNanos( (int64_t) timeIntervalMSecs * 1000000 ),
                 _logThresholdNanos( (int64_t) logThresholdMSecs * 1000000 ),
                 _flusher(flusher)
            {}

            virtual void run();
        };

        boost::shared_ptr<FsyncJob> _myJob;

    public:
        BackgroundFileFlusher():
            _queue(boost::shared_ptr<JobQueue>(new JobQueue())),
            _threadPool(boost::shared_ptr<ThreadPool>(new ThreadPool(1, _queue))),
            _running(false),
            _fileDescriptors(),
            _lock(),
            _myJob()
        {}

        void start(int timeIntervalMSecs, int logThresholdMSecs, std::vector<int> const& fileDescriptors);
        void addDescriptors(std::vector<int> const& fileDescriptors);
        void dropDescriptors(std::vector<int> const& fileDescriptors);
        void addDescriptor (int const& fileDesc);
        void dropDescriptor (int const& fileDesc);
        void stop();

    private:
        ~BackgroundFileFlusher()
        {
            stop();
        }

        friend class Singleton<BackgroundFileFlusher>;
    };

#endif
}

#endif
