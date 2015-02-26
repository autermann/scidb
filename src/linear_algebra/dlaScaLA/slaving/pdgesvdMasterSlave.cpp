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
#include <string>
#include <iostream>

// local
#include "pdgesvdSlave.hpp" // for argument structure

using namespace std;

namespace scidb
{

void pdgesvdMarshallArgs(void*  argsBuf,
                         const sl_int_t& NPROW, const sl_int_t& NPCOL,
                         const sl_int_t& MYPROW, const sl_int_t& MYPCOL, const sl_int_t& MYPNUM,
                         const char &jobU, const char &jobVT,
                         const sl_int_t& M, const sl_int_t &N,
                         double* A, const sl_int_t& IA, const sl_int_t& JA, const sl_desc_t& DESC_A,
                         double* S,
                         double* U,  const sl_int_t& IU,  const sl_int_t& JU,  const sl_desc_t& DESC_U,
                         double* VT, const sl_int_t& IVT, const sl_int_t& JVT, const sl_desc_t& DESC_VT)
{
    enum dummy { DBG=0 };

    if(DBG) {
        std::cerr << "argsBuf:" << argsBuf << std::endl;
        std::cerr << "A:" << (void*)(A) << std::endl;
        std::cerr << "S:" << (void*)(S) << std::endl;
        std::cerr << "U:" << (void*)(U) << std::endl;
        std::cerr << "VT:" << (void*)(VT) << std::endl;
    }

    // marshall all arguments except the buffers A,S,U,&VT into a struct:
    scidb::PdgesvdArgs * args = reinterpret_cast<scidb::PdgesvdArgs*>(argsBuf) ;
    args->NPROW = NPROW;
    args->NPCOL = NPCOL;
    args->MYPROW = MYPROW;
    args->MYPCOL = MYPCOL;
    args->MYPNUM = MYPNUM;

    args->jobU = jobU ;
    args->jobVT = jobVT ;
    args->M = M ;
    args->N = N ;

    args->A.I = IA ;
    args->A.J = JA ;
    args->A.DESC = DESC_A ;

    args->U.I = IU ;
    args->U.J = JU ;
    args->U.DESC = DESC_U ;

    args->VT.I = IVT ;
    args->VT.J = JVT ;
    args->VT.DESC = DESC_VT ;

    if(DBG) {
        std::cerr << "argsBuf:  ----------------------------" << std::endl ;
        std::cerr << *args << std::endl;
        std::cerr << "argsBuf:  end-------------------------" << std::endl ;
    }
}


} // namespace scidb
