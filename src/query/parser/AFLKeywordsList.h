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
 * @brief All registered keywords used in AFL queries
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

AFLKW("and", token::AND)
AFLKW("or", token::OR)
AFLKW("array", token::ARRAY)
AFLKW("as", token::AS)
AFLKW("compression", token::COMPRESSION)
AFLKW("create", token::CREATE)
AFLKW("default", token::DEFAULT)
AFLKW("empty", token::EMPTY)
AFLKW("not", token::NOT)
AFLKW("null", token::NULL_VALUE)
AFLKW("immutable", token::IMMUTABLE)
AFLKW("if", token::IF)
AFLKW("then", token::THEN)
AFLKW("else", token::ELSE)
AFLKW("case", token::CASE)
AFLKW("when", token::WHEN)
AFLKW("end", token::END)
AFLKW("agg", token::AGG)
AFLKW("true", token::TRUE)
AFLKW("false", token::FALSE)
AFLKW("NA", token::NA)
AFLKW("is", token::IS)
AFLKW("reserve", token::RESERVE)
AFLKW("asc", token::ASC)
AFLKW("desc", token::DESC)
AFLKW("all", token::ALL)
AFLKW("distinct", token::DISTINCT)
