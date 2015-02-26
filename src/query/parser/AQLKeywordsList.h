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
 * @brief All registered keywords used in AQL
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

AQLKW("and", token::AND)
AQLKW("or", token::OR)
AQLKW("array", token::ARRAY)
AQLKW("as", token::AS)
AQLKW("compression", token::COMPRESSION)
AQLKW("create", token::CREATE)
AQLKW("default", token::DEFAULT)
AQLKW("empty", token::EMPTY)
AQLKW("not", token::NOT)
AQLKW("null", token::NULL_VALUE)
AQLKW("immutable", token::IMMUTABLE)
AQLKW("if", token::IF)
AQLKW("then", token::THEN)
AQLKW("else", token::ELSE)
AQLKW("case", token::CASE)
AQLKW("when", token::WHEN)
AQLKW("end", token::END)
AQLKW("agg", token::AGG)
AQLKW("select", token::SELECT)
AQLKW("from", token::FROM)
AQLKW("where", token::WHERE)
AQLKW("group", token::GROUP)
AQLKW("by", token::BY)
AQLKW("join", token::JOIN)
AQLKW("on", token::ON)
AQLKW("regrid", token::REGRID)
AQLKW("between", token::BETWEEN)
AQLKW("load", token::LOAD)
AQLKW("into", token::INTO)
AQLKW("values", token::VALUES)
AQLKW("update", token::UPDATE)
AQLKW("set", token::SET)
AQLKW("library", token::LIBRARY)
AQLKW("unload", token::UNLOAD)
AQLKW("true", token::TRUE)
AQLKW("false", token::FALSE)
AQLKW("drop", token::DROP)
AQLKW("is", token::IS)
AQLKW("reserve", token::RESERVE)
AQLKW("cross", token::CROSS)
AQLKW("window", token::WINDOW)
AQLKW("asc", token::ASC)
AQLKW("desc", token::DESC)
AQLKW("redimension", token::REDIMENSION)
