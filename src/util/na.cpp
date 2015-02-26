
/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2011 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation version 3 of the License, or
* (at your option) any later version.
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
 * @file na.cpp
 *
 * @brief NA - a NaN of a specific value used to represent an _intended_ missing value
 *
 * @author James McQueston <jmcqueston@paradigm4.com>
 */

#include "util/na.h"

namespace scidb {

    void testNa()
    {

    // template<> inline bool isNA<float>(float val) { return NA::ISNA(val); }
    }

} // namespace scidb
