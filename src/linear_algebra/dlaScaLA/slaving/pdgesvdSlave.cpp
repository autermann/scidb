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

// std C
#include <stdlib.h>

// de-facto standards
#include <boost/scoped_array.hpp>

// SciDB
#include <scalapackUtil/reformat.hpp>

// local
#include "pdgesvdSlave.hpp"
#include "slaveTools.h"

namespace scidb
{
#if !defined(NDEBUG) && defined(SCALAPACK_DEBUG)
enum dbgdummy { DBG=1 };
#else
enum dbgdummy { DBG=0 };
#endif
    
/// TODO: I think there is a version of this in scalapackTools.h to use instead
///
///
/// for a given context ICTXT, return the parameters of the ScaLAPACK
/// 
/// This is slated to be re-worked during Cheshire m4.  It will probably
/// become a method on ScaLAPACK operator.
///
///
static void getSlInfo(const sl_int_t ICTXT, sl_int_t& NPROW, sl_int_t& NPCOL, sl_int_t& MYPROW, sl_int_t& MYPCOL, sl_int_t& MYPNUM)
{
    if(DBG) std::cerr << "blacs_gridinfo_" << std::endl;

    NPROW=-1 ; NPCOL=-1 ; MYPROW=-1 ; MYPCOL=-1 ;
    blacs_gridinfo_(ICTXT, NPROW, NPCOL, MYPROW, MYPCOL);
    if(DBG) std::cerr << "blacs_gridinfo_ -- MYPROW,MYPCOL=" << MYPROW << "," << MYPCOL << std::endl;

    if(NPROW < 0 || NPCOL < 0) {
        std::cerr << "blacs_gridinfo_ error -- aborting" << std::endl;
        ::exit(99); // something that does not look like a signal
    }

    if(MYPROW < 0 || MYPCOL < 0) {
        std::cerr << "blacs_gridinfo_ error -- aborting" << std::endl;
        ::exit(99); // something that does not look like a signal
    }

    if(DBG) std::cerr << "blacs_pnum_ " << std::endl;
    MYPNUM = blacs_pnum_(ICTXT, MYPROW, MYPCOL);
    if(DBG) std::cerr << "blacs_pnum_ -- MYPNUM:" << MYPNUM <<std::endl;
}

///
/// @return INFO = the status of the psgesvd_()
///
sl_int_t pdgesvdSlave(void* bufs[], size_t sizes[], unsigned count)
{
    enum dummy {BUF_ARGS=0, BUF_A, BUF_S, BUF_U, BUF_VT, NUM_BUFS };


    for(size_t i=0; i < count; i++) {
        if(DBG) {
            std::cerr << "doPdgesvd: buffer at:"<< bufs[i] << std::endl;
            std::cerr << "doPdgesvd: bufsize =" << sizes[i] << std::endl;
        }
    }

    if(count < NUM_BUFS) {
        std::cerr << "pdgesvdSlave: master sent " << count << " buffers, but " << NUM_BUFS << " are required." << std::endl;
        ::exit(99); // something that does not look like a signal
    }

    // take a COPY of args (because we will have to patch DESC.CTXT)
    scidb::PdgesvdArgs args = *reinterpret_cast<PdgesvdArgs*>(bufs[BUF_ARGS]) ;
    if(DBG) {
        std::cerr << "pdgesvdSlave: args --------------------------" << std::endl ;
        std::cerr << args << std::endl;
        std::cerr << "pdgesvdSlave: args end ----------------------" << std::endl ;
    }

    // set up the scalapack grid
    if(DBG) std::cerr << "##### sl_init() NPROW:"<<args.NPROW<<" NPCOL:"<<args.NPCOL<<std::endl;
    slpp::int_t ICTXT=-1; // will be overwritten by sl_init
    sl_init_(ICTXT/*out*/, args.NPROW/*in*/, args.NPCOL/*in*/); 
    sl_int_t NPROW=args.NPROW;
    sl_int_t NPCOL=args.NPCOL;

    // find out where I am in the scalapack grid
    sl_int_t MYPROW=-1, MYPCOL=-1, MYPNUM=-1; // will be overwritten by getSlInfo
    getSlInfo(ICTXT/*in*/, NPROW/*in*/, NPCOL/*in*/, MYPROW/*out*/, MYPCOL/*out*/, MYPNUM/*out*/);

    if(NPROW != args.NPROW || NPCOL != args.NPCOL ||
       MYPROW != args.MYPROW || MYPCOL != args.MYPCOL || MYPNUM != args.MYPNUM){
        if(DBG) {
            std::cerr << "scalapack general parameter mismatch" << std::endl;
            std::cerr << "args NPROW:"<<args.NPROW<<" NPCOL:"<<args.NPCOL
                      << "MYPROW:"<<args.MYPROW<<" MYPCOL:"<<args.MYPCOL<<"MYPNUM:"<<MYPNUM
                      << std::endl;
            std::cerr << "ScaLAPACK NPROW:"<<NPROW<<" NPCOL:"<<NPCOL
                      << "MYPROW:"<<MYPROW<<" MYPCOL:"<<MYPCOL<<"MYPNUM:"<<MYPNUM
                      << std::endl;
        }
    }


      // setup MB,NB
    const sl_int_t& M = args.A.DESC.M ;
    const sl_int_t& N = args.A.DESC.N ;
    const sl_int_t& MB = args.A.DESC.MB ;
    const sl_int_t& NB = args.A.DESC.NB ;

    const sl_int_t& LLD_A = args.A.DESC.LLD ;
    const sl_int_t one = 1 ;
    const sl_int_t  LTD_A = std::max(one, numroc_( N, NB, MYPCOL, /*CSRC_A*/0, NPCOL )); 

    const sl_int_t& MP = LLD_A ;
    const sl_int_t& NQ = LTD_A ;

    // size check args
    SLAVE_ASSERT_ALWAYS( sizes[BUF_ARGS] >= sizeof(PdgesvdArgs));

    // size check A, S, U, VT
    sl_int_t SIZE_A = MP * NQ ;
    sl_int_t SIZE_S = std::min(M, N);
    sl_int_t size_p = std::max(one, numroc_( SIZE_S, MB, MYPROW, /*RSRC_A*/0, NPROW ));
    sl_int_t size_q = std::max(one, numroc_( SIZE_S, NB, MYPCOL, /*RSRC_A*/0, NPCOL ));
    sl_int_t SIZE_U = MP * size_q;
    sl_int_t SIZE_VT= size_p * NQ;

    if(DBG) {
        std::cerr << "##################################################" << std::endl;
        std::cerr << "####pdgesvdSlave##################################" << std::endl;
        std::cerr << "one:" << one << std::endl;
        std::cerr << "SIZE_S:" << SIZE_S << std::endl;
        std::cerr << "MB:" << MB << std::endl;
        std::cerr << "MYPROW:" << MYPROW << std::endl;
        std::cerr << "NPROW:" << NPROW << std::endl;
    }

    // TODO: >= because master is permitted to use a larger buffer
    //          to allow to see if rounding up to chunksize eliminates some errors
    //          before we put the roundUp formula everywhere
    SLAVE_ASSERT_ALWAYS(sizes[BUF_A] >= SIZE_A * sizeof(double));
    SLAVE_ASSERT_ALWAYS(sizes[BUF_S] >= SIZE_S * sizeof(double));
    SLAVE_ASSERT_ALWAYS( sizes[BUF_U] >= SIZE_U *sizeof(double));
    SLAVE_ASSERT_ALWAYS( sizes[BUF_VT] >= SIZE_VT *sizeof(double));

    // sizes are correct, give the pointers their names
    double* A = reinterpret_cast<double*>(bufs[BUF_A]) ;
    double* S = reinterpret_cast<double*>(bufs[BUF_S]) ;
    double* U = reinterpret_cast<double*>(bufs[BUF_U]) ;
    double* VT = reinterpret_cast<double*>(bufs[BUF_VT]) ;

    // debug that the input is readable and show its contents
    if(DBG) {
        for(int ii=0; ii < SIZE_A; ii++) {
            std::cerr << "("<< MYPROW << "," << MYPCOL << ") A["<<ii<<"] = " << A[ii] << std::endl;  
        }
    }

    if(false) {
        // debug that outputs are writeable:
        for(int ii=0; ii < SIZE_S; ii++) {
            S[ii] = -9999.0 ;
        }
        for(int ii=0; ii < SIZE_U; ii++) {
            U[ii] = -9999.0 ;
        }
        for(int ii=0; ii < SIZE_VT; ii++) {
            VT[ii] = -9999.0 ;
        }
    }

    // ScaLAPACK: the DESCS are complete except for the correct context
    args.A.DESC.CTXT= ICTXT ;
    // (no DESC for S)
    args.U.DESC.CTXT= ICTXT ;
    args.VT.DESC.CTXT= ICTXT ;

    if(DBG) std::cerr << "pdgesvdSlave calling PDGESVD to get work size" << std:: endl;
    sl_int_t INFO ;
    double LWORK_DOUBLE;
    pdgesvd_(args.jobU, args.jobVT, args.M, args.N,
             A,  args.A.I,  args.A.J,  args.A.DESC, S,
             U,  args.U.I,  args.U.J,  args.U.DESC,
             VT, args.VT.I, args.VT.J, args.VT.DESC,
             &LWORK_DOUBLE, -1, INFO);

    sl_int_t LWORK = int(LWORK_DOUBLE); // get the cast from SVDPhysical.cpp
    // ALLOCATE an array WORK size LWORK
    boost::scoped_array<double> WORKtmp(new double[LWORK]);
    double* WORK = WORKtmp.get();

    if(true || DBG) {    // we'll leave this on in Cheshire.0 and re-evaluate later
        std::cerr << "pdgesvdSlave: argsBuf is: {" << std::endl;
        std::cerr << args << std::endl;
        std::cerr << "}" << std::endl << std::endl;

        std::cerr << "pdgesvdSlave: calling pdgesvd_ for computation, with args:" << std::endl ;
        std::cerr << "jobU: " << args.jobU
                  << ", jobVT: " << args.jobVT
                  << ", M: " << args.M
                  << ", N: " << args.N << std::endl;

        std::cerr << "A: " <<  (void*)(A)
                  << ", A.I: " << args.A.I
                  << ", A.J: " << args.A.J << std::endl;
        std::cerr << ", A.DESC: " << args.A.DESC << std::endl;

        std::cerr << "S: " << (void*)(S) << std::endl;

        std::cerr << "U: " <<  (void*)(U)
                  << ", U.I: " << args.U.I
                  << ", U.J: " << args.U.J << std::endl;
        std::cerr << ", U.DESC: " << args.U.DESC << std::endl;

        std::cerr << "VT: " <<  (void*)(VT)
                  << ", VT.I: " << args.VT.I
                  << ", VT.J: " << args.VT.J << std::endl;
        std::cerr << ", VT.DESC: " << args.VT.DESC << std::endl;

        std::cerr << "WORK: " << (void*)(WORK)
                  << " LWORK: " << LWORK << std::endl;
    }

    //////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////
    pdgesvd_(args.jobU, args.jobVT, args.M, args.N,
             A,  args.A.I,  args.A.J,  args.A.DESC, S,
             U,  args.U.I,  args.U.J,  args.U.DESC,
             VT, args.VT.I, args.VT.J, args.VT.DESC,
             WORK, LWORK, INFO);

    if(true || DBG) {    // we'll leave this on in Cheshire.0 and re-evaluate later
        std::cerr << "pdgesvdSlave: result INFO: " << INFO << std::endl;
    }

    if (DBG) {
        std::cerr << "pdgesvdSlave ------ outputs:" << std::endl;
        // debug prints of the outputs:
        for(int ii=0; ii < SIZE_S; ii++) {
            std::cerr << "S["<<ii<<"] = " << S[ii] << std::endl;  
        }
        for(int ii=0; ii < SIZE_U; ii++) {
            std::cerr << "U["<<ii<<"] = " << U[ii] << std::endl;  
        }
        for(int ii=0; ii < SIZE_VT; ii++) {
            std::cerr << "VT["<<ii<<"] = " << VT[ii] << std::endl;  
        }
    }

    return INFO ;
}

} // namespace scidb
