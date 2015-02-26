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

// std C++
#include <iostream>

// std C
#include <stdlib.h>

// locals
#include "slaveTools.h"

namespace scidb
{
    
///
/// getSlaveBLACSInfo()
///
/// for a given context ICTXT, return the parameters of the ScaLAPACK
/// 
/// This is slated to be re-worked during Cheshire m4.  It will probably
/// become a method on ScaLAPACK operator.
///
///
void getSlaveBLACSInfo(const sl_int_t ICTXT, sl_int_t& NPROW, sl_int_t& NPCOL, sl_int_t& MYPROW, sl_int_t& MYPCOL, sl_int_t& MYPNUM)
{
    // TODO JHM ; disable cerr debugs before checkin
    std::cerr << "blacs_gridinfo_" << std::endl;

    NPROW=-1 ; NPCOL=-1 ; MYPROW=-1 ; MYPCOL=-1 ;
    blacs_gridinfo_(ICTXT, NPROW, NPCOL, MYPROW, MYPCOL);
    std::cerr << "blacs_gridinfo_ -- MYPROW,MYPCOL=" << MYPROW << "," << MYPCOL << std::endl;

    if(NPROW < 0 || NPCOL < 0) {
        std::cerr << "blacs_gridinfo_ error -- aborting" << std::endl;
        ::exit(99); // something that does not look like a signal
    }   

    if(MYPROW < 0 || MYPCOL < 0) {
        std::cerr << "blacs_gridinfo_ error -- aborting" << std::endl;
        ::exit(99); // something that does not look like a signal
    }   

    std::cerr << "blacs_pnum_ " << std::endl;
    MYPNUM = blacs_pnum_(ICTXT, MYPROW, MYPCOL);
    std::cerr << "blacs_pnum_ -- MYPNUM:" << MYPNUM <<std::endl;
}

} // namespace scidb

