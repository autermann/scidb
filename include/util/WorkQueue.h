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
 * @file WorkQueue.h
 *
 * @brief The WorkQueue class
 *
 * A queue of work items that limits the max number of simulteneously dispatched items. 
 * It uses an external thread-pool (i.e. JobQueue). The intent is that a collection
 * of cooperating WorkQueues can use a single thread-pool (which is easy to control),
 * but will not starve each other if the total max of outstanding items is no greater
 * than the number of threads in the pool.
 */

#ifndef WORKQUEUE_H_
#define WORKQUEUE_H_

#include <deque>
#include <boost/enable_shared_from_this.hpp>
#include <boost/make_shared.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>
#include <util/JobQueue.h>
#include <util/Mutex.h>

namespace scidb
{

class WorkQueue : public boost::enable_shared_from_this<WorkQueue>
{
 public:

   typedef boost::function<void()> WorkItem;

   class OverflowException: public SystemException
   {
   public:
      OverflowException(const char* file, const char* function, int32_t line)
      : SystemException(file, function, line, "scidb", SCIDB_SE_NO_MEMORY, SCIDB_LE_WORK_QUEUE_FULL,
          "SCIDB_E_NO_MEMORY", "SCIDB_E_WORK_QUEUE_FULL", uint64_t(0))
      {
      }

      ~OverflowException() throw () {}
      void raise() const { throw *this; }
   };

   WorkQueue(const boost::shared_ptr<JobQueue>& jobQueue)
   : _jobQueue(jobQueue), _maxOutstanding(DEFAULT_MAX_OUTSTANDING), _maxSize(DEFAULT_MAX_SIZE),  _outstanding(0), _isStarted(true)
   {
       if (!jobQueue) {
           throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "NULL job queue");
       }
   }

   WorkQueue(const boost::shared_ptr<JobQueue>& jobQueue,
             uint32_t maxOutstanding, uint32_t maxSize)
   : _jobQueue(jobQueue), _maxOutstanding(maxOutstanding), _maxSize(maxSize),  _outstanding(0), _isStarted(true)
   {
       if (!jobQueue) {
           throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "NULL job queue");
       }
   }

   virtual ~WorkQueue()
   {
   }

   void enqueue(WorkItem& work)
   {
      {
         ScopedMutexLock lock(_mutex);
         if ((_size()+1)>_maxSize) {
            throw OverflowException(REL_FILE, __FUNCTION__, __LINE__);
         }
         _workQueue.push_back(work);
      }
      spawn();
   }

   void start()
   {
      {
         ScopedMutexLock lock(_mutex);
         _isStarted = true;
      }
      spawn();
   }

   void stop()
   {
      {
         ScopedMutexLock lock(_mutex);
         _isStarted = false;
      }
   }

   uint32_t size()
   {
       ScopedMutexLock lock(_mutex);
       return _size();
   }

 private:

   uint32_t _size()
   {
       // mutex must be locked
       return (_outstanding + _workQueue.size());
   }

   void done()
   {
      {
         ScopedMutexLock lock(_mutex);
         assert(_outstanding>0);
         --_outstanding;
         assert(_outstanding < _maxOutstanding);
      }
      spawn();
   }

   void spawn()
   {
      std::deque<WorkItem> q;
      {
         ScopedMutexLock lock(_mutex);

         assert(_outstanding <= _maxOutstanding);

         if (!_isStarted) {
             return;
         }

         while ((_outstanding < _maxOutstanding) && !_workQueue.empty()) {
            q.push_back(WorkItem());
            q.back().swap(_workQueue.front());
            _workQueue.pop_front();
            ++_outstanding;
         }
      }
      while (!q.empty())
      {
         WorkItem& item = q.front();
         boost::shared_ptr<Job> jobPtr(new WorkQueueJob(item, shared_from_this()));
         _jobQueue->pushJob(jobPtr);
         q.pop_front();
      }
   }

   class WorkQueueJob : public Job
   {
   public:
     virtual ~WorkQueueJob() {}

     WorkQueueJob(WorkItem& work, boost::shared_ptr<WorkQueue> workQ)
     : Job(boost::shared_ptr<Query>()), _workQueue(workQ)
      {
         /*
          * the swap saves memory allocation/copy
          * but it also dangerous because it clears the
          * passed in WorkItem.
          * This class is private, so it should be OK
          */
         _workItem.swap(work);
      }
   private: 
      WorkQueueJob();
      WorkQueueJob(const WorkQueueJob&);
      WorkQueueJob& operator=(const WorkQueueJob&);

      virtual void run()
      {
          assert(!_workItem.empty());
          try {
              _workItem();
          } catch(const scidb::Exception& ) {
              releaseWorkItem();
              throw;
          }
          releaseWorkItem();
      }
      void releaseWorkItem()
      {
         WorkItem().swap(_workItem); //destroy
         if (boost::shared_ptr<WorkQueue> wq = _workQueue.lock()) {
            wq->done();
         }
      }
      WorkItem _workItem;
      boost::weak_ptr<WorkQueue> _workQueue;
   };
   friend class WorkQueueJob;

   WorkQueue();
   WorkQueue(const WorkQueue&);
   WorkQueue& operator=(const WorkQueue&);

   const static uint32_t DEFAULT_MAX_OUTSTANDING = 1;
   const static uint32_t DEFAULT_MAX_SIZE = 1000000;

   boost::shared_ptr<JobQueue> _jobQueue;
   std::deque<WorkItem> _workQueue;
   uint32_t _maxOutstanding;
   uint32_t _maxSize;
   uint32_t _outstanding;
   Mutex _mutex;
   bool _isStarted;
 };

} //namespace scidb

#endif /* WORKQUEUE_H_ */
