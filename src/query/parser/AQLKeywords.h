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
 * @brief Routines for manipulating registered keywords of AQL parser
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#ifndef AQL_KEYWORDS_H_
#define AQL_KEYWORDS_H_

#include <stdint.h>
#include <string>

#include "AQLParser.hpp"

namespace scidb
{

/**
 * Macroses for easy defining keywords and collating them to tokens
 */
#define AQLKW(name, tok, reserved) {name,  AQLParser::token::tok, reserved},

/**
 * Structure for defining keywords.
 */
struct AQLKeyword
{
	const char* name;
	AQLParser::token::yytokentype tok;
	bool reserved;
};

/**
 * Seaching keyword in array of possible keywords
 * @param keyword Keyword for searching
 * @return If found - Keyword structure, else - NULL.
 */
const AQLKeyword* FindAQLKeyword(const char *keyword);


} // namespace scidb

#endif /* AQL_KEYWORDS_H_ */
