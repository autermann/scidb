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
 * @brief Routines for manipulating registered keywords of AFL parser
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */
#include <string.h>

#include "query/parser/AFLKeywords.h"

using namespace std;

namespace scidb
{

const AFLKeyword AFLKeywords[] =
{
    #include "query/parser/AFLKeywordsList.h"
    {NULL,  AFLParser::token::EOQ} // just array end marker for loop
};

const AFLKeyword* FindAFLKeyword(const char *keyword)
{
    const AFLKeyword *kw = &AFLKeywords[0];

    while(kw->name)
    {
        if (!strcasecmp(kw->name, keyword))
            return kw;
        ++kw;
    }

    return NULL;
}

}
