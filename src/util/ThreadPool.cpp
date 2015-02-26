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
 * @file ThreadPool.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The ThreadPool class
 */

#include "util/Thread.h"
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"


namespace scidb
{

ThreadPool::ThreadPool(size_t threadCount, boost::shared_ptr<JobQueue> queue)
: _queue(queue), _currentJobs(threadCount), _threadCount(threadCount)
{
    _shutdown = false;
    if (_threadCount <= 0) {
        throw InvalidArgumentException(REL_FILE, __FUNCTION__, __LINE__)
        << "thread count";
    }
}

void ThreadPool::start()
{
    ScopedMutexLock lock(_mutex);

    if (_shutdown) {
        throw AlreadyStoppedException(REL_FILE, __FUNCTION__, __LINE__)
        << "thread pool cannot be started after being stopped";
    }
    if (_threads.size() > 0) {
        throw AlreadyStartedException(REL_FILE, __FUNCTION__, __LINE__)
        << "thread pool can be started only once";
    }
    assert(_threadCount>0);

    _threads.reserve(_threadCount);
    for (size_t i = 0; i < _threadCount; i++)
    {
        boost::shared_ptr<Thread> thread(new Thread(*this, i));
        _threads.push_back(thread);
        thread->start();
    }
}

bool ThreadPool::isStarted()
{
    ScopedMutexLock lock(_mutex);
    return _threads.size() > 0;
}

class FakeJob : public Job
{
public:
    FakeJob(): Job(boost::shared_ptr<Query>()) {
    }

    virtual void run()
    {
    }
};

void ThreadPool::stop()
{
    std::vector<boost::shared_ptr<Thread> > threads;
    { // scope
        ScopedMutexLock lock(_mutex);
        if (_shutdown) {
            return;
        }
        _shutdown = true;
        threads.swap(_threads);
    }
    int nThreads = (int)threads.size();
    for (int i = 0; i < nThreads; i++) {
        _queue->pushJob(boost::shared_ptr<Job>(new FakeJob()));
    }
    _terminatedThreads.enter(nThreads);
}

void ThreadPool::waitCurrentJobs()
{
    std::vector< boost::shared_ptr<Job> > jobs;
    { // scope
        ScopedMutexLock lock(_mutex);
        jobs = _currentJobs;
    }
    for (size_t i = 0; i < jobs.size(); i++) {
        if (jobs[i])
            jobs[i]->wait();
    }
}

} //namespace
