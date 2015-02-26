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
 * @brief Wrapper around AQL parser and lexer to workaround conflicting symbols and macroses
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#include <sstream>

#include "query/parser/AQLParserWrapper.h"
#include "query/parser/AQLScanner.h"
#include "query/parser/QueryParser.h"
#include "query/parser/AST.h"

namespace scidb
{

AQLParserWrapper::AQLParserWrapper(QueryParser &glue, bool trace):
    _glue(glue),
    _trace(trace)
{
}

int AQLParserWrapper::parse(const std::string &input)
{
    std::istringstream iss(input);
    AQLScanner scanner(_glue, &iss);
    scanner.set_debug(_trace);
    _glue._aqlScanner = &scanner;

    AQLParser parser(_glue);
    parser.set_debug_level(_trace);

    int result = parser.parse();

    if (result == 0 && _glue._ast)
        _glue._ast->setQueryString(input);

    return result;
}

}
