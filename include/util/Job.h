/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2013 SciDB, Inc.
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
 * @file Job.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The base class for jobs of thread pool
 */

#ifndef JOB_H_
#define JOB_H_

#include <string>
#include <vector>

#include "Atomic.h"
#include "Semaphore.h"

namespace scidb
{


/**
 * Base virtual class for job executed by ThreadPool.
 */
class Query;

class Job
{
private:
    Semaphore _done;
    Atomic<bool> _removed;
protected:
    boost::shared_ptr<Exception> _error; 
    boost::shared_ptr<Query> _query;

    // This method must be implemented in children
    virtual void run() = 0;

public:
    Job(boost::shared_ptr<Query> query): _removed(false), _query(query)
    {
    }
    virtual ~Job()
    {
    }
    boost::shared_ptr<Query> getQuery()
    {
        return _query;
    }
    void execute();

    // Waits until job is done
    bool wait(bool propagateException = false, bool allowMultipleWaits = true);

    // Force to skip job execution
    void skip()
    {
        _removed = true;
    }
    
    void rethrow();
};


} //namespace

#endif /* JOB_H_ */
