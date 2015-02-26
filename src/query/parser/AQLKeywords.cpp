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
#include "AQLParser.hpp"                                 // For token numbers
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

const ALKeyword* getAQLKeyword(const char* nm)
{
    static const ALKeyword map[] =                       // Must remain sorted
    {
        {"all",         AQLParser::token::ALL,          false},
        {"and",         AQLParser::token::AND,          true },
        {"array",       AQLParser::token::ARRAY,        false},
        {"as",          AQLParser::token::AS,           false},
        {"asc",         AQLParser::token::ASC,          false},
        {"between",     AQLParser::token::BETWEEN,      false},
        {"by",          AQLParser::token::BY,           false},
        {"cancel",      AQLParser::token::CANCEL,       true },
        {"case",        AQLParser::token::CASE,         true },
        {"compression", AQLParser::token::COMPRESSION,  false},
        {"create",      AQLParser::token::CREATE,       false},
        {"cross",       AQLParser::token::CROSS,        true },
        {"current",     AQLParser::token::CURRENT,      false},
        {"default",     AQLParser::token::DEFAULT,      false},
        {"desc",        AQLParser::token::DESC,         false},
        {"drop",        AQLParser::token::DROP,         false},
        {"else",        AQLParser::token::ELSE,         true },
        {"empty",       AQLParser::token::EMPTY,        true },
        {"end",         AQLParser::token::END,          false},
        {"errors",      AQLParser::token::ERRORS,       false},
        {"false",       AQLParser::token::FALSE,        true },
        {"fixed",       AQLParser::token::FIXED,        true },
        {"following",   AQLParser::token::FOLLOWING,    false},
        {"from",        AQLParser::token::FROM,         true },
        {"group",       AQLParser::token::GROUP,        true },
        {"if",          AQLParser::token::IF,           true },
        {"insert",      AQLParser::token::INSERT,       true },
        {"instance",    AQLParser::token::INSTANCE,     false},
        {"instances",   AQLParser::token::INSTANCES,    false},
        {"into",        AQLParser::token::INTO,         true },
        {"is",          AQLParser::token::IS,           false},
        {"join",        AQLParser::token::JOIN,         true },
        {"library",     AQLParser::token::LIBRARY,      false},
        {"load",        AQLParser::token::LOAD,         false},
        {"not",         AQLParser::token::NOT,          true },
        {"null",        AQLParser::token::NULL_VALUE,   true },
        {"on",          AQLParser::token::ON,           true },
        {"or",          AQLParser::token::OR,           true },
        {"order",       AQLParser::token::ORDER,        true },
        {"over",        AQLParser::token::OVER,         false},
        {"partition",   AQLParser::token::PARTITION,    false},
        {"preceding",   AQLParser::token::PRECEDING,    false},
        {"query",       AQLParser::token::QUERY,        false},
        {"redimension", AQLParser::token::REDIMENSION,  true },
        {"regrid",      AQLParser::token::REGRID,       true },
        {"rename",      AQLParser::token::RENAME,       true },
        {"reserve",     AQLParser::token::RESERVE,      false},
        {"save",        AQLParser::token::SAVE,         false},
        {"select",      AQLParser::token::SELECT,       true },
        {"set",         AQLParser::token::SET,          true },
        {"shadow",      AQLParser::token::SHADOW,       false},
        {"start",       AQLParser::token::START,        false},
        {"step",        AQLParser::token::STEP,         false},
        {"then",        AQLParser::token::THEN,         true },
        {"thin",        AQLParser::token::THIN,         false},
        {"to",          AQLParser::token::TO,           false},
        {"true",        AQLParser::token::TRUE,         true },
        {"unbound",     AQLParser::token::UNBOUND,      false},
        {"unload",      AQLParser::token::UNLOAD,       true },
        {"update",      AQLParser::token::UPDATE,       true },
        {"values",      AQLParser::token::VALUES,       false},
        {"variable",    AQLParser::token::VARIABLE,     true },
        {"when",        AQLParser::token::WHEN,         true },
        {"where",       AQLParser::token::WHERE,        true },
        {"window",      AQLParser::token::WINDOW,       true }
    };

    const ALKeyword *f,*l;

    boost::tie(f,l) = std::equal_range(map,map+SCIDB_SIZE(map),nm,byName());

    return f == l ? 0 : f;
}

/****************************************************************************/
}
/****************************************************************************/
