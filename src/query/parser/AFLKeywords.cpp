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

#include <string.h>                                      // For strcasecmp()
#include <boost/tuple/tuple.hpp>                         // For tie()
#include <util/Platform.h>                               // For SCISB_SIZE
#include "AFLParser.hpp"                                 // For token numbers
#include "ALKeywords.h"                                  // For our interface

/****************************************************************************/
namespace scidb {
/****************************************************************************/

namespace {
struct byName
{
    bool operator()(const ALKeyword& k,const char* s) const {return strcasecmp(k.name,s) < 0;}
    bool operator()(const char* s,const ALKeyword& k) const {return strcasecmp(s,k.name) < 0;}
};}

const ALKeyword* getAFLKeyword(const char* nm)
{
    static const ALKeyword map[] =                       // Must remain sorted
    {
        {"and",        AFLParser::token::AND,         true },
        {"array",      AFLParser::token::ARRAY,       false},
        {"as",         AFLParser::token::AS,          false},
        {"asc",        AFLParser::token::ASC,         false},
        {"between",    AFLParser::token::BETWEEN,     false},
        {"case",       AFLParser::token::CASE,        true },
        {"compression",AFLParser::token::COMPRESSION, false},
        {"create",     AFLParser::token::CREATE,      false},
        {"default",    AFLParser::token::DEFAULT,     false},
        {"desc",       AFLParser::token::DESC,        false},
        {"else",       AFLParser::token::ELSE,        true },
        {"empty",      AFLParser::token::EMPTY,       true },
        {"end",        AFLParser::token::END,         false},
        {"false",      AFLParser::token::FALSE,       true },
        {"if",         AFLParser::token::IF,          true },
        {"is",         AFLParser::token::IS,          false},
        {"not",        AFLParser::token::NOT,         true },
        {"null",       AFLParser::token::NULL_VALUE,  true },
        {"or",         AFLParser::token::OR,          true },
        {"reserve",    AFLParser::token::RESERVE,     false},
        {"then",       AFLParser::token::THEN,        true },
        {"true",       AFLParser::token::TRUE,        true },
        {"when",       AFLParser::token::WHEN,        true }
    };

    const ALKeyword *f,*l;

    boost::tie(f,l) = std::equal_range(map,map+SCIDB_SIZE(map),nm,byName());

    return f == l ? 0 : f;
}

/****************************************************************************/
}
/****************************************************************************/
