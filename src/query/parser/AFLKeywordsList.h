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
 * @brief All registered keywords used in AFL queries
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

AFLKW("and", token::AND, true)
AFLKW("or", token::OR, true)
AFLKW("array", token::ARRAY, false)
AFLKW("as", token::AS, false)
AFLKW("compression", token::COMPRESSION, false)
AFLKW("create", token::CREATE, false)
AFLKW("default", token::DEFAULT, false)
AFLKW("empty", token::EMPTY, true)
AFLKW("not", token::NOT, true)
AFLKW("null", token::NULL_VALUE, true)
AFLKW("if", token::IF, true)
AFLKW("then", token::THEN, true)
AFLKW("else", token::ELSE, true)
AFLKW("case", token::CASE, true)
AFLKW("when", token::WHEN, true)
AFLKW("end", token::END, false)
AFLKW("agg", token::AGG, true)
AFLKW("true", token::TRUE, true)
AFLKW("false", token::FALSE, true)
AFLKW("NA", token::NA, true)
AFLKW("is", token::IS, false)
AFLKW("reserve", token::RESERVE, false)
AFLKW("asc", token::ASC, false)
AFLKW("desc", token::DESC, false)
AFLKW("between", token::BETWEEN, false)
