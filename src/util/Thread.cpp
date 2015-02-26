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
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
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
#include <sstream>
#include <time.h>
#include <log4cxx/logger.h>

#include <util/Thread.h>
#include <util/JobQueue.h>

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

Thread::Thread(ThreadPool& threadPool, size_t index):
_threadPool(threadPool),
_index(index),
_isStarted(false)
{
}

bool Thread::isStarted()
{
    ScopedMutexLock lock(_threadPool._mutex);
    return _isStarted;
}
void Thread::start()
{
    ScopedMutexLock lock(_threadPool._mutex);

    assert(!_isStarted);
    if (_isStarted) {
        assert(false);
        return;
    }

    pthread_attr_t attr;
    int rc = pthread_attr_init(&attr);

    if (rc!=0) {
        std::stringstream ss; ss<<"pthread_attr_init: "<<rc;
        throw std::runtime_error(ss.str());
    }
    PAttrEraser onStack(&attr);

    const size_t stackSize = 1024*1024;
    assert(stackSize > PTHREAD_STACK_MIN);
    rc = pthread_attr_setstacksize(&attr, stackSize);
    if (rc != 0) {
        std::stringstream ss; ss<<"pthread_attr_setstacksize: "<<rc;
        throw std::runtime_error(ss.str());
    }

    rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    if (rc != 0) {
        std::stringstream ss; ss<<"pthread_attr_setdetachstate: "<<rc;
        throw std::runtime_error(ss.str());
    }

    rc = pthread_create(&_handle, &attr, &Thread::threadFunction, this);
    if (rc != 0) {
        std::stringstream ss; ss<<"pthread_attr_create: "<<rc;
        throw std::runtime_error(ss.str());
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
    // pin the semaphore
    boost::shared_ptr<Semaphore> sem(_threadPool._terminatedThreads);
    ThreadPool* tp = &_threadPool;

    LOG4CXX_TRACE(logger, "Thread::threadFunction: begin tid = "
                  << pthread_self()
                  << ", pool = " << tp);
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
                LOG4CXX_ERROR(logger, "Thread::threadFunction: unhandled exception: "
                              << e.what()
                              << ", tid = " << pthread_self()
                              << ", pool = " << tp);
            } catch (...) {}
            throw;
        }
    }

    sem->release();
    LOG4CXX_TRACE(logger, "Thread::threadFunction: end tid = "
                  << pthread_self()
                  << ", pool = " << tp);
}

void Thread::nanoSleep(uint64_t nanoSec)
{
  struct timespec ts;
  if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
      assert(false);
      throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_GET_SYSTEM_TIME);
  }
  const uint64_t tenTo9 = 1000000000;
  ts.tv_sec = ts.tv_sec + static_cast<time_t>(nanoSec / tenTo9);
  const uint64_t tmp = ts.tv_nsec + (nanoSec % tenTo9);
  if (tmp < tenTo9) {
      ts.tv_nsec = static_cast<long>(tmp);
  } else {
      ts.tv_sec = ts.tv_sec + static_cast<time_t>(tmp / tenTo9);
      ts.tv_nsec = static_cast<long>(tmp % tenTo9);
  }
  assert(ts.tv_sec>=0);
  assert(ts.tv_nsec < static_cast<long>(tenTo9));

  while (true) {
      int rc = ::clock_nanosleep(CLOCK_REALTIME,
                                 TIMER_ABSTIME,
                                 &ts, NULL);
      if (rc==0) { return; }

      if (rc==EINTR) { continue; }

      assert(false);
      throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "clock_nanosleep";
  }
}

} //namespace
