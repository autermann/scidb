
#ifndef NA_INTERNAL__H_
#define NA_INTERNAL__H_
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
 * @file naInternal.h
 *
 * @brief NA - a NaN of a specific value used to represent an _intended_ missing value
 *             these are the NaN-payload mainpulation functions and definitions
 *
 * @author James McQueston <jmcqueston@paradigm4.com>
 */
#include <stdint.h>

#include <cmath>
#include <string>
#include <limits>

namespace scidb {
namespace NA {

    // if these functions exist in some more standard form, eliminate these
    uint32_t            nanPayloadf(float val);
    uint64_t            nanPayload(double val);

    typedef struct {
        uint64_t low ;       // least significant bits
        uint64_t high ;      // most significant bits
    } nanPayloadLong_t ;

    nanPayloadLong_t    nanPayloadl(long double val); 

    const uint32_t      NA_NANPAYLOAD = 1954 ;
    extern const char*  NA_NANPAYLOAD_STR; // "1954"


    // equivalent of the ISNA(x) macro, true for NA _only_
    inline bool ISNA(float val)  { return std::isnan(val) && nanPayloadf(val) == NA_NANPAYLOAD ; }
    inline bool ISNA(double val) { return std::isnan(val) && nanPayload (val) == NA_NANPAYLOAD ; }
    inline bool ISNA(long double val) {
        if (std::isnan(val)) {
            nanPayloadLong_t payload = nanPayloadl(val);
            return payload.low == NA_NANPAYLOAD && payload.high == 0;
        } else {
            return false ;
        }
    }

    // equivalent of the ISNAN(x) macro, true for NAN or NA
    inline bool ISNAN(long double val) { return std::isnan(val); }
    inline bool ISNAN(double val) { return std::isnan(val); }
    inline bool ISNAN(float val) { return std::isnan(val); }

    template<class T> class NAInfo {          // primary template
    public:
        // this is the generic implementation for any integer/unsigned type
        static T        value() { return std::numeric_limits<int>::min() ; } 
    private:
                        NAInfo() { ; }
    };

    // these are the specializations for float, double, long double, and string
    template<>
    class NAInfo<double> {
    public:
        // TODO JHM ; see whether a static value is faster (guess: no)
        static double value() { return nan(NA_NANPAYLOAD_STR); } 
    };

    template<>
    class NAInfo<float> {
    public:
        static float value() { return nanf(NA_NANPAYLOAD_STR) ; } 
    };

    template<>
    class NAInfo<long double> {
    public:
        static long double value() { return static_cast<long double>(nan(NA_NANPAYLOAD_STR)) ; } 
    };

    template<>
    class NAInfo<std::string> {
    public:
        // TODO JHM ; see if returning a static value is faster.  (guess: yes)
        static const std::string value() { return std::string("NA") ; } 
    };



    void nanPayloadsUnitTest();             // to invoke unit testing on this functionality
} // namespace NA
} // namespace scidb

#endif // NA_INTERNAL__H_
