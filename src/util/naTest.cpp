
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
 * @file naTest.cpp
 *
 * @brief NA - a NaN of a specific value used to represent an _intended_ missing value
 *             this file contains unit tests
 *
 * @author James McQueston <jmcqueston@paradigm4.com>
 */

#include "util/na.h"
#include <math.h>
#include <stdio.h>
#include <sys/types.h>

template<class T>
int precision()
{
    register T one = 1 ;
    register T divided = 1 ;
    for(int i = -1; i < 100; i++) {
       if (one == one - divided) {
           // first loss of precision
           return i;
       }
       divided = divided / 2 ;
    }
    return -1 ; // failure
}

template<class T>
void range(T& nearMinVal, T& nearMaxVal)
{
    register T nearMax = 1 ;
    for(int i = 1; i < 17000 ; i++) {
        register T nearMaxNew = nearMax * 2 ;
        if (isinf(nearMaxNew)) break;
        nearMax = nearMaxNew ;
    }
    nearMaxVal = nearMax ;

    register T nearMin = 1 ;
    for(int i = 1; i < 17000 ; i++) {
        register T nearMinNew = nearMin / 2 ;
        if (nearMinNew == 0) break;
        nearMin = nearMinNew ;
    }
    nearMinVal = nearMin ; // result
}



int main()
{
    // basic info
    printf("8*sizeof(float) %ld\n", 8*sizeof(float));
    printf("8*sizeof(double) %ld\n", 8*sizeof(double));
    printf("8*sizeof(long double) %ld\n", 8*sizeof(long double));
    printf("\n");

    printf("precision<float> = %d\n", precision<float>());
    printf("precision<double> = %d\n", precision<double>());
    printf("precision<long double> = %d\n", precision<long double>());
    printf("\n");

    float       nearMinf, nearMaxf ; range(nearMinf, nearMaxf);
    double      nearMin, nearMax ;   range(nearMin,  nearMax);
    long double nearMinl, nearMaxl ; range(nearMinl, nearMaxl);
    printf("range<float> = %a to %a\n", nearMinf, nearMaxf);
    //printf("range<float> = %f to %f\n", nearMinf, nearMaxf);

    printf("range<double> = %a to %a\n", nearMin, nearMax);
    //printf("range<double> = %f to %f\n", nearMin, nearMax);

    printf("range<long double> = %La to %La\n", nearMinl, nearMaxl);
    //printf("range<long double> = %Lf to %Lf\n", nearMinl, nearMaxl);
    printf("\n");

    // can we construct nans?
    float nanF = nanf("3");
    double nanD = nan("3");
    long double nanL = nanl("3");

    static const char* isNotIs[] = { "is not", "is" };

    printf("nanF %s NaN\n", isNotIs[isnan(nanF)]);
    printf("nanD %s NaN\n", isNotIs[isnan(nanD)]);

    scidb::testNanPayloads();

    // long double stuff
    //printf("nanL %s NaN\n", isNotIs[isnan(nanL)]);
    printf("isnan(nanL) -> %d\n", isnan(nanL));
    printf("sizeof(nanL) -> %lu\n", sizeof(nanL));

    long double numL = 3 ;
    printf("numL*numL = %Lf\n", numL * numL);
    printf("(long double)(0) = %La\n", (long double)(0));
    printf("(long double)(1) = %La\n", (long double)(1));
    printf("(long double)(2) = %La\n", (long double)(2));
    printf("(long double)(3) = %La\n", (long double)(3));
    int i;
    for(i=1; i <=256; i=i*2) {
        printf("...(long double)(%d) = %La\n", i, (long double)(i));
    }
    printf("long double(nanD) = %La\n", (long double)(nanD));

    return 0; // success
}
