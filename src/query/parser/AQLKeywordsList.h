/*
**
BEGIN_COPYRIGHT
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

AQLKW("and", token::AND, true)
AQLKW("or", token::OR, true)
AQLKW("array", token::ARRAY, false)
AQLKW("as", token::AS, false)
AQLKW("compression", token::COMPRESSION, false)
AQLKW("create", token::CREATE, false)
AQLKW("default", token::DEFAULT, false)
AQLKW("empty", token::EMPTY, true)
AQLKW("not", token::NOT, true)
AQLKW("null", token::NULL_VALUE, true)
AQLKW("immutable", token::IMMUTABLE, false)
AQLKW("if", token::IF, true)
AQLKW("then", token::THEN, true)
AQLKW("else", token::ELSE, true)
AQLKW("case", token::CASE, true)
AQLKW("when", token::WHEN, true)
AQLKW("end", token::END, false)
AQLKW("select", token::SELECT, true)
AQLKW("from", token::FROM, true)
AQLKW("where", token::WHERE, true)
AQLKW("group", token::GROUP, true)
AQLKW("by", token::BY, false)
AQLKW("join", token::JOIN, true)
AQLKW("on", token::ON, true)
AQLKW("regrid", token::REGRID, true)
AQLKW("between", token::BETWEEN, false)
AQLKW("load", token::LOAD, false)
AQLKW("into", token::INTO, true)
AQLKW("values", token::VALUES, false)
AQLKW("update", token::UPDATE, true)
AQLKW("set", token::SET, true)
AQLKW("library", token::LIBRARY, false)
AQLKW("unload", token::UNLOAD, true)
AQLKW("true", token::TRUE, true)
AQLKW("false", token::FALSE, true)
AQLKW("drop", token::DROP, false)
AQLKW("is", token::IS, false)
AQLKW("reserve", token::RESERVE, false)
AQLKW("cross", token::CROSS, true)
AQLKW("window", token::WINDOW, true)
AQLKW("asc", token::ASC, false)
AQLKW("desc", token::DESC, false)
AQLKW("redimension", token::REDIMENSION, true)
AQLKW("all", token::ALL, false)
AQLKW("distinct", token::DISTINCT, false)
AQLKW("current", token::CURRENT, false)
AQLKW("instance", token::INSTANCE, false)
AQLKW("instances", token::INSTANCES, false)
AQLKW("save", token::SAVE, false)
AQLKW("errors", token::ERRORS, false)
AQLKW("shadow", token::SHADOW, false)
AQLKW("partition", token::PARTITION, false)
AQLKW("preceding", token::PRECEDING, false)
AQLKW("following", token::FOLLOWING, false)
AQLKW("unbound", token::UNBOUND, false)
AQLKW("step", token::STEP, false)
AQLKW("over", token::OVER, false)
AQLKW("start", token::START, false)
AQLKW("thin", token::THIN, false)
AQLKW("rename", token::RENAME, true)
AQLKW("to", token::TO, false)
AQLKW("cancel", token::CANCEL, true)
AQLKW("query", token::QUERY, false)
AQLKW("fixed", token::FIXED, true)
AQLKW("variable", token::VARIABLE, true)
AQLKW("order", token::ORDER, true)
AQLKW("insert", token::INSERT, true)

