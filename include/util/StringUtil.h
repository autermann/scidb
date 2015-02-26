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


/*
 * @file
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief Misc string utilities
 */

#ifndef STRINGUTIL_H_
#define STRINGUTIL_H_

#ifndef PROJECT_ROOT
#error #############################################################################################
#error # You must define PROJECT_ROOT with full path to your source directory before including
#error # Exceptions.h. PROJECT_ROOT used for reducing absolute __FILE__ path to relative in SciDB
#error # exceptions.
#error #############################################################################################
#endif

#include <strings.h>

/**
 * Structure for creating case insensitive maps
 */
struct __lesscasecmp
{
	bool operator()(const std::string& a, const std::string& b) const
	{
		return strcasecmp(a.c_str(), b.c_str()) < 0;
	}
};

/**
 * Not all pathes from __FILE__ macros are absolute. Pathes from *.yy and *.ll seems to be relative
 * so check needed or bad things happens.
 */
#define REL_FILE \
    (__FILE__[0] == '/' ? std::string(__FILE__).substr(strlen(PROJECT_ROOT)).c_str() : __FILE__)

#endif /* STRINGUTIL_H_ */
