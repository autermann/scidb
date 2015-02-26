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
 * @file
 *
 * @brief Wrapper around AQL parser and lexer to workaround conflicting symbols and macroses
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#ifndef AQLPARSERWRAPPER_H_
#define AQLPARSERWRAPPER_H_

#include <string>

namespace scidb
{

class QueryParser;

class AQLParserWrapper
{
public:
    AQLParserWrapper(QueryParser &glue, bool trace = false);

    int parse(const std::string &input);

private:
    QueryParser &_glue;

    bool _trace;
};

} //namespace

#endif /* AQLPARSERWRAPPER_H_ */
