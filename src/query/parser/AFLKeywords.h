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
 * @brief Routines for manipulating registered keywords of AFL parser
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#ifndef AFL_KEYWORDS_H_
#define AFL_KEYWORDS_H_

#include <stdint.h>
#include <string>

#include "AFLParser.hpp"

namespace scidb
{

/**
 * Macroses for easy defining keywords and collating them to tokens
 */
#define AFLKW(name, tok, reserved) {name,  AFLParser::token::tok, reserved},

/**
 * Structure for defining keywords.
 */
struct AFLKeyword
{
	const char* name;
	AFLParser::token::yytokentype tok;
    bool reserved;
};

/**
 * Seaching keyword in array of possible keywords
 * @param keyword Keyword for searching
 * @return If found - Keyword structure, else - NULL.
 */
const AFLKeyword* FindAFLKeyword(const char *keyword);


} // namespace scidb

#endif /* AFL_KEYWORDS_H_ */
