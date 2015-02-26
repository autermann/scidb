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
 * @file Sysinfo.h
 *
 * @brief Information about host system and hardware
 *
 * @author knizhnik@garret.ru
 */

#ifndef SYSINFO_H
#define SYSINFO_H

namespace scidb
{

class Sysinfo
{
public:
	enum CacheLevel
	{
		CPU_CACHE_L1 = 1,
		CPU_CACHE_L2 = 2,
		CPU_CACHE_L3 = 4
	};

    static int getNumberOfCPUs();
    static int getCPUCacheSize(int level);
};    

} //namespace

#endif
