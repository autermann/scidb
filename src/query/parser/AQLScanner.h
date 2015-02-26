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
 * @brief Derived scanner from base FlexLexer
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#ifndef AQL_SCANNER_H_
#define AQL_SCANNER_H_

#undef yyFlexLexer
#define yyFlexLexer AQLBaseFlexLexer
#include "FlexLexer.h"

#include "AQLParser.hpp"

namespace scidb
{

class QueryParser;

class AQLScanner : public AQLBaseFlexLexer
{
public:
    AQLScanner(QueryParser& glue, std::istream* arg_yyin = 0,    std::ostream* arg_yyout = 0);

    virtual ~AQLScanner();

    virtual AQLParser::token_type lex(AQLParser::semantic_type* yylval, AQLParser::location_type* yylloc);

    void set_debug(bool b);

    void error(const std::string &msg, const  AQLParser::location_type* location);

private:
    QueryParser &_glue;
};

}

#endif /* AQL_SCANNER_H_ */
