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
 * @file Job.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The Job class
 */

#include "util/Job.h"
#include "query/Query.h"
#include "log4cxx/logger.h"

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.common.thread"));

namespace scidb
{
    void Job::execute()
    {
        if (_query) {
            Query::setCurrentQueryID(_query->getQueryID());
        } else {
            Query::setCurrentQueryID(0);
        }

        if (!_removed) {
            const char *err_msg = "Job::execute: unhandled exception in job";
            try {
                run();
            } catch (Exception const& x) {
                _error = x.copy();
                LOG4CXX_ERROR(logger, err_msg << ": " << x.what());
            } catch (const std::exception& e) {
                _error = SYSTEM_EXCEPTION_SPTR(SCIDB_SE_EXECUTION, SCIDB_LE_UNKNOWN_ERROR) << e.what();
                LOG4CXX_ERROR(logger, err_msg << ": " << e.what());
            } catch (...) {
                _error = SYSTEM_EXCEPTION_SPTR(SCIDB_SE_EXECUTION, SCIDB_LE_UNKNOWN_ERROR) << err_msg;
                LOG4CXX_ERROR(logger, err_msg);
            }
        }
        _query.reset();
        _done.release();
        Query::setCurrentQueryID(0);
    }

    // Waits until job is done
    bool Job::wait(bool propagateException, bool allowMultipleWaits)
    {
        _done.enter();
        if (allowMultipleWaits) { 
            _done.release(); // allow multiple waits
        }
        if (_error && _error->getShortErrorCode() != SCIDB_E_NO_ERROR) {
            if (propagateException)
            {
                _error->raise();
            }
            return false;
        }
        return true;
    }

    void Job::rethrow() 
    {
        _error->raise();
    }

} //namespace
