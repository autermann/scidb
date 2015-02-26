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
 * @brief Wrapper around AFL parser and lexer to workaround conflicting symbols and macroses
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#include <sstream>

#include "query/parser/AFLParserWrapper.h"
#include "query/parser/AFLScanner.h"
#include "query/parser/QueryParser.h"
#include "query/parser/AST.h"

namespace scidb
{

AFLParserWrapper::AFLParserWrapper(QueryParser &glue, bool trace):
    _glue(glue),
    _trace(trace)
{
}

int AFLParserWrapper::parse(const std::string &input)
{
    std::istringstream iss(input);
    AFLScanner scanner(_glue, &iss);
    scanner.set_debug(_trace);
    _glue._aflScanner = &scanner;

    AFLParser parser(_glue);
    parser.set_debug_level(_trace);

    int result = parser.parse();

    if (result == 0 && _glue._ast)
        _glue._ast->setQueryString(input);

    return result;
}

}
