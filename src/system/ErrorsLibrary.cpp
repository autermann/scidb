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
 * @author: Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief Class for controlling built-in and user defined error codes and messages.
 */

#include "util/Mutex.h"

#include "system/ErrorsLibrary.h"
#include "system/ErrorCodes.h"
#include "system/Exceptions.h"

namespace scidb
{

#define ERRMSG(code, message) __errorMap__[code] = message;

ErrorsLibrary::ErrorsLibrary()
{
    #define __errorMap__ _builtinShortErrorsMsg
    #include "system/ShortErrorsList.h"

    #undef __errorMap__
    #define __errorMap__ _builtinLongErrorsMsg
    #include "system/LongErrorsList.h"

    #undef __errorMap__

    registerErrors("scidb", &_builtinLongErrorsMsg);
}

void ErrorsLibrary::registerErrors(const std::string &errorsNamespace, ErrorsMessages* errrosMessages)
{
    ScopedMutexLock lock(_lock);

    if (_errorNamespaces.find(errorsNamespace) != _errorNamespaces.end())
        throw SYSTEM_EXCEPTION(SCIDB_SE_ERRORS_MGR, SCIDB_LE_ERRNS_ALREADY_REGISTERED) << errorsNamespace;

    //Check error codes consistency. All system codes must be equal or less than 0xFF and user codes must be
    //greater than 0xFF
    if ("scidb" == errorsNamespace)
    {
        for (ErrorsMessages::const_iterator it = errrosMessages->begin(); it != errrosMessages->end(); ++it)
        {
            assert(it->first <= SCIDB_MAX_SYSTEM_ERROR);
        }
    }
    else
    {
        for (ErrorsMessages::const_iterator it = errrosMessages->begin(); it != errrosMessages->end(); ++it)
        {
            if (it->first <= SCIDB_MAX_SYSTEM_ERROR)
            {
                throw USER_EXCEPTION(SCIDB_SE_ERRORS_MGR, SCIDB_LE_INVALID_USER_ERROR_CODE)
                    << errorsNamespace << it->first << SCIDB_MAX_SYSTEM_ERROR;
            }
        }
    }


    _errorNamespaces[errorsNamespace] = errrosMessages;
}

void ErrorsLibrary::unregisterErrors(const std::string &errorsNamespace)
{
    ScopedMutexLock lock(_lock);

    if ("scidb" == errorsNamespace)
        throw SYSTEM_EXCEPTION(SCIDB_SE_ERRORS_MGR, SCIDB_LE_ERRNS_CAN_NOT_BE_UNREGISTERED) << errorsNamespace;

    _errorNamespaces.erase(errorsNamespace);
}

const std::string ErrorsLibrary::getShortErrorMessage(int32_t shortError)
{
    ScopedMutexLock lock(_lock);

    if (_builtinShortErrorsMsg.end() == _builtinShortErrorsMsg.find(shortError))
    {
        return boost::str(boost::format("!!!Can not obtain short error message for short error code"
            " '%1%' because it does not registered!!!") % shortError);
    }

    return _builtinShortErrorsMsg[shortError];
}

const std::string ErrorsLibrary::getLongErrorMessage(const std::string &errorsNamespace, int32_t longError)
{
    ScopedMutexLock lock(_lock);

    ErrorsNamespaces::const_iterator nsIt = _errorNamespaces.find(errorsNamespace);
    if (_errorNamespaces.end() == nsIt)
    {
        return boost::str(boost::format("!!!Can not obtain long error message for long error code '%1%' because "
            "errors namespace '%2%' does not registered!!!") % longError % errorsNamespace);
    }

    ErrorsMessages::const_iterator errIt = nsIt->second->find(longError);
    if (nsIt->second->end() == errIt)
    {
        return boost::str(boost::format("!!!Can not obtain error message for error code '%1%'"
            " from errors namespace '%2%' because error code '%1%' does not registered!!!")
            % longError % errorsNamespace);
    }

    return errIt->second;
}

}
