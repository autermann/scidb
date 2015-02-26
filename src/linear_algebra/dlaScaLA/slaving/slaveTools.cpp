/*
**
* BEGIN_COPYRIGHT
*
* PARADIGM4 INC.
* This file is part of the Paradigm4 Enterprise SciDB distribution kit
* and may only be used with a valid Paradigm4 contract and in accord
* with the terms and conditions specified by that contract.
*
* Copyright © 2010 - 2012 Paradigm4 Inc.
* All Rights Reserved.
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

