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

#ifndef  AQL_SCANNER_H_
#define  AQL_SCANNER_H_

#include <util/arena/ScopedArena.h>
#include "AQLParser.hpp"
#undef   yyFlexLexer
#define  yyFlexLexer AQLBaseFlexLexer
#include "FlexLexer.h"

namespace scidb
{

class QueryParser;

class AQLScanner : public AQLBaseFlexLexer
{
public:
    AQLScanner(QueryParser& glue,std::istream* = 0,std::ostream* = 0);

    virtual ~AQLScanner() {}

    virtual short lex(AQLParser::semantic_type*,AQLParser::location_type*);

    void set_debug(bool);

private:
    char* copyLexeme(size_t,const char*);

private:
    QueryParser&       _glue;
    arena::ScopedArena _arena;
};

}

#endif
