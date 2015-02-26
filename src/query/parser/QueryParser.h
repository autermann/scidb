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

#ifndef QUERYPARSER_H_
#define QUERYPARSER_H_

#include <sstream>
#include <boost/shared_ptr.hpp>

namespace scidb
{

class AstNode;
class ParsingContext;

/**
 * Class which combining string lexer and grammar parser objects into query parser.
 * 
 * @note Not reentrant! Instance holding root of AST and context during parsing.
 */
class QueryParser
{
public:
    /**
     * Constructor
     * @param traceScanning Tracing scanning process
     * @param traceParsing Tracing parser process
     * @return
     */
    QueryParser(bool trace = false);

    /**
     * Invoke lexer and parser for input string, returning AST
     * @param input Query string
     * @return Pointer to first node in AST
     */
    boost::shared_ptr<AstNode> parse(const std::string& input, bool aql = true);

    void setComment(std::string const& comment) { 
        _docComment = comment;
    }

    void error(const class location& l, const std::string& m);

private:
    bool _trace;
    class AQLScanner* _aqlScanner;
    class AFLScanner* _aflScanner;
    AstNode* _ast;
    boost::shared_ptr<ParsingContext> _parsingContext;

    //_errorString and _errorContext will be initialized when parser failed to parse input query.
    std::string _errorString;
    boost::shared_ptr<ParsingContext> _errorContext;
    std::string _docComment;

    friend class AQLParser;
    friend class AFLParser;
    friend class AFLParserWrapper;
    friend class AQLParserWrapper;
};

} // namespace scidb

#endif /* QUERYPARSER_H_ */
