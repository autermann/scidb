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
 * @file Thread.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The Thread class
 */

#include "log4cxx/logger.h"

#include "util/Thread.h"
#include "util/JobQueue.h"

namespace {
class PAttrEraser
{
    public:
    PAttrEraser(pthread_attr_t *attrPtr) : _attrPtr(attrPtr)
    {
        assert(_attrPtr!=NULL);
    }
    ~PAttrEraser()
    {
        pthread_attr_destroy(_attrPtr);
    }
    private:
    pthread_attr_t *_attrPtr;
};
}

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.common.thread"));

Thread::Thread(ThreadPool& threadPool, size_t index): _threadPool(threadPool), _index(index)
{
    _isStarted = false;
}

void Thread::start()
{
    ScopedMutexLock lock(_threadPool._mutex);

    assert(!_isStarted);
    if (_isStarted) {
        return;
    }

    pthread_attr_t attr;
    int rc = pthread_attr_init(&attr);

    if (rc!=0) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_THREAD, SCIDB_LE_UNKNOWN_ERROR) << rc;
    }
    PAttrEraser onStack(&attr);

    const size_t stackSize = 1024*1024;
    assert(stackSize > PTHREAD_STACK_MIN);
    rc = pthread_attr_setstacksize(&attr, stackSize);
    if (rc != 0) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_THREAD, SCIDB_LE_UNKNOWN_ERROR) << rc;
    }

    rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    if (rc != 0) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_THREAD, SCIDB_LE_UNKNOWN_ERROR) << rc;
    }

    rc = pthread_create(&_handle, &attr, &Thread::threadFunction, this);
    if (rc != 0) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_THREAD, SCIDB_LE_UNKNOWN_ERROR) << rc;
    }
    _isStarted = true;
}

Thread::~Thread()
{
}

void* Thread::threadFunction(void* arg)	// static member
{
    assert(arg);
    scidb::Thread* thread = reinterpret_cast<scidb::Thread*>(arg);
    thread->_threadFunction();
    return (void*)NULL;
}

void Thread::_threadFunction()
{
    LOG4CXX_TRACE(logger, "Thread::threadFunction: begin tid =" << pthread_self());
    while (true)
    {
        try
        {
            boost::shared_ptr<Job> job = _threadPool.getQueue()->popJob();
            {
                ScopedMutexLock lock(_threadPool._mutex);
                if (_threadPool._shutdown) { 
                    break;
                }
                _threadPool._currentJobs[_index] = job;
            }
            job->execute();
            {
                ScopedMutexLock lock(_threadPool._mutex);
                _threadPool._currentJobs[_index].reset();
            }
        }
        // Need to check error type. If fatal stops execution. If no to continue.
        catch (const std::exception& e)
        {
            try {  // This try catch block must prevent crash if there is no space on disk where log file is located.
                LOG4CXX_ERROR(logger, "Thread::threadFunction: unhandled exception: " << e.what())
            } catch (...) {}
            throw;
        }
    }
    _threadPool._terminatedThreads.release();
    LOG4CXX_TRACE(logger, "Thread::threadFunction: end tid =" << pthread_self());
}


} //namespace
