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
 * @file Event.h
 *
 * @author knizhnik@garret.ru, roman.simakov@gmail.com
 *
 * @brief POSIX conditional variable
 */

#ifndef EVENT_H_
#define EVENT_H_

#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include <errno.h>

#include "util/Mutex.h"
#include "system/Exceptions.h"
#include "boost/function.hpp"

namespace scidb
{

class Event
{
private:
    pthread_cond_t  _cond;
    bool signaled;

public:
    /**
     * @throws a scidb::Exception if necessary
     */
    typedef boost::function<bool()> ErrorChecker;

    Event()
    {
        if (pthread_cond_init(&_cond, NULL)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "pthread_cond_init";
        }
        signaled = false;
    }

    ~Event()
    {
        if (pthread_cond_destroy(&_cond)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "pthread_cond_destroy";
        }
    }

    bool wait(Mutex& cs, ErrorChecker& errorChecker)
    {
       if (errorChecker) {
            if (!errorChecker()) {
               return false;
            }
           
            signaled = false;
            do {
                struct timeval tv;
                struct timespec ts;
                gettimeofday(&tv, NULL);
                ts.tv_sec = tv.tv_sec + 10;
                ts.tv_nsec = tv.tv_usec*1000;
                const int e = pthread_cond_timedwait(&_cond, &cs._mutex, &ts);
                if (e == 0) {
                    return true;
                }
                if (e != ETIMEDOUT) 
                {
                   throw SYSTEM_EXCEPTION(SCIDB_SE_THREAD, SCIDB_LE_THREAD_EVENT_ERROR) << e;
                }
                if (!errorChecker()) {
                   return false;
                }
            } while (!signaled);
        }
        else
        {
            if (pthread_cond_wait(&_cond, &cs._mutex))
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "pthread_cond_wait";
        }
        return true;
    }

    void signal()
    { 
        signaled = true;
        if (pthread_cond_broadcast(&_cond))
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "pthread_cond_broadcast";
    }
};

}

#endif
