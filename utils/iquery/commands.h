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
 * @brief Iquery commands classes
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */
 
#ifndef IQUERY_COMMANDS
#define IQUERY_COMMANDS

class IqueryCmd
{
public:
    enum Type
    {
        SET,
        LANG,
        VERBOSE,
        TIMER,
        FETCH,
        HELP,
        QUIT
    };

    IqueryCmd(Type cmdtype):
        _cmdtype(cmdtype)
    {
    }

    Type getCmdType() const
    {
        return _cmdtype;
    }

    virtual ~IqueryCmd()
    {
    }
private:
    Type _cmdtype;
};

class SimpleIqueryCmd: public IqueryCmd
{
public:
    SimpleIqueryCmd(Type cmdtype, int value):
        IqueryCmd(cmdtype),
        _value(value)
    {
    }

    int getValue() const
    {
        return _value;
    }

private:
    int _value;
};

#endif //IQUERY_COMMANDS
