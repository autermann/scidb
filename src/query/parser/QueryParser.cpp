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
 * @brief Glue class between lexer and parser.
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#include <boost/format.hpp>
#include <boost/make_shared.hpp>

#include <sstream>

#include "query/parser/QueryParser.h"
#include "query/parser/AST.h"
#include "query/parser/ParsingContext.h"
#include "system/Exceptions.h"

#include "query/parser/AQLParserWrapper.h"
#include "query/parser/AFLParserWrapper.h"

#include "location.hh"

using namespace boost;

namespace scidb
{

QueryParser::QueryParser(bool trace) :
    _trace(trace),
    _aqlScanner(NULL),
    _aflScanner(NULL),
    _ast(NULL)
{}


boost::shared_ptr<AstNode> QueryParser::parse(const std::string& input, bool aql)
{
    _parsingContext = boost::shared_ptr<ParsingContext>(new ParsingContext(input));

    if (aql)
    {
        AQLParserWrapper parser(*this, _trace);

        if (parser.parse(input) != 0)
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_PARSER, SCIDB_LE_QUERY_PARSING_ERROR, _errorContext) << _errorString;
        }
    }
    else
    {
        AFLParserWrapper parser(*this, _trace);

        if (parser.parse(input) != 0)
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_PARSER, SCIDB_LE_QUERY_PARSING_ERROR, _errorContext) << _errorString;
        }
    }


    return boost::shared_ptr<AstNode>(_ast);
}

void QueryParser::error(const class location& loc, const std::string& msg)
{
    _errorContext = boost::shared_ptr<ParsingContext>(
            new ParsingContext(_parsingContext->getQueryString(), loc.begin.line, loc.begin.column, loc.end.line, loc.end.column));
    _errorString = msg;
}

void QueryParser::error2(const class location& loc, const std::string& msg)
{
    throw USER_QUERY_EXCEPTION(
        SCIDB_SE_PARSER,
        SCIDB_LE_QUERY_PARSING_ERROR,
        boost::make_shared<ParsingContext>(_parsingContext->getQueryString(), loc.begin.line, loc.begin.column,
            loc.end.line, loc.end.column)) << msg;
}

} // namespace scidb
