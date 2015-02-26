#ifndef NA__H_
#define NA__H_

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
 * @file na.h
 *
 * @brief NA - a NaN of a specific value used to represent an _intended_ missing value
 *
 * @author James McQueston <jmcqueston@paradigm4.com>
 */

#include "naInternal.h"

namespace scidb {

    template<class T> bool isNA(T val)     { return val == NA::NAInfo<T>::value ; }
    template<class T> bool isNAonly(T val) { return val == NA::NAInfo<T>::value ; } // works for all non-floating point types

    // floating-point NA's are NaN's so floating-point comparison won't work
    // have to check if they're nan, and if so decode the payload

    // during a computation that is supposed to ignore missing values, use this one
    // any kind of nan counts as NA during computations that emulate R
    // Matches the behavior of R's isna() function and its internals C-call isNA()
    template<> inline bool isNA<double>(double val) { return std::isnan(val); }
    template<> inline bool isNA<float>(float val)   { return std::isnan(val); }

    // when saving to a file, use this, so only nan("1954") will be saved as "NA"
    // after that, test for other nans using <cmath>'s isnan(), if the tool that
    // converts to string does not convert it to "nan" on a particular platform.
    // so it can be restored exactly and a round-trip to R will produce the same
    // binary value as if it stayed in R.
    // Matche the behavior of R's ISNA internal macro
    template<> inline bool isNAonly<double>(double val) { return NA::ISNA(val); }
    template<> inline bool isNAonly<float>(float val)   { return NA::ISNA(val); }

} // namespace scidb

#endif // NA__H_
