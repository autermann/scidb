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
#include <cmath>

// std C
#include <stdlib.h>

// de-facto standards
#include <boost/scoped_array.hpp>

// SciDB
#include <scalapackUtil/reformat.hpp>

// local
#include "pdgesvdSlave.hpp"
#include "pdgesvdMasterSlave.hpp"
#include "slaveTools.h"

namespace scidb
{
#if !defined(NDEBUG) && defined(SCALAPACK_DEBUG)
enum dbgdummy { DBG=1 };
#else
enum dbgdummy { DBG=0 };
#endif

//
// forward decls
//
sl_int_t            pdgesvdSlave2(const sl_int_t ICTXT,  PdgesvdArgs args,
                                  void* bufs[], size_t sizes[], unsigned count);
scidb::PdgesvdArgs  pdgesvdGenTestArgs(sl_int_t ICTXT, sl_int_t NPROW, sl_int_t NPCOL,
                                                       sl_int_t MYPROW, sl_int_t MYPCOL, sl_int_t MYPNUM,
                                      sl_int_t order);


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
    if(DBG) std::cerr << "getSlInfo: ICTXT: " << ICTXT << std::endl;

    NPROW=-1 ; NPCOL=-1 ; MYPROW=-1 ; MYPCOL=-1 ; MYPNUM= -1;
    blacs_gridinfo_(ICTXT, NPROW, NPCOL, MYPROW, MYPCOL);
    if(DBG) std::cerr << "getSlInfo: blacs_gridinfo_(ICTXT: "<<ICTXT<<") -> "
                      << "NPROW: " << NPROW << ", NPCOL: " << NPCOL
                      << ", MYPROW: " << MYPROW << ", MYPCOL: " << MYPCOL << std::endl;

    if(NPROW < 1 || NPCOL < 1) {
        std::cerr << "getSlInfo: blacs_gridinfo_ error -- aborting" << std::endl;
        ::blacs_abort_(ICTXT, 99); // something that does not look like a signal
    }

    if(MYPROW < 0 || MYPCOL < 0) {
        std::cerr << "getSlInfo: blacs_gridinfo_ error -- aborting" << std::endl;
        ::blacs_abort_(ICTXT, 99); // something that does not look like a signal
    }

    MYPNUM = blacs_pnum_(ICTXT, MYPROW, MYPCOL);
    if(DBG) std::cerr << "getSlInfo: blacs_pnum() -> MYPNUM: " << MYPNUM <<std::endl;
}


///
/// @return INFO = the status of the psgesvd_()
///
enum dummy {BUF_A=1, BUF_S, BUF_U, BUF_VT, NUM_BUFS };  // used by both pdgesvdSlave and ...Slave2

sl_int_t pdgesvdSlave(void* bufs[], size_t sizes[], unsigned count, bool debugOverwriteArgs)
{
    // TODO:  exit()S and SLAVE_ASSERT()s need to use MPI_abort() / blacs_abort() instead

    for(size_t i=0; i < count; i++) {
        if(DBG) {
            std::cerr << "doPdgesvd: buffer at:"<< bufs[i] << std::endl;
            std::cerr << "doPdgesvd: bufsize =" << sizes[i] << std::endl;
        }
    }

    if(count < NUM_BUFS) {
        std::cerr << "pdgesvdSlave: master sent " << count << " buffers, but " << NUM_BUFS << " are required." << std::endl;
        ::abort();
    }
    // size check and get reference to args
    enum dummy {BUF_ARGS=0};  // NOTE bufs[BUF_ARGS] should not be referenced by pdgesvdSlave2
    SLAVE_ASSERT_ALWAYS( sizes[BUF_ARGS] >= sizeof(PdgesvdArgs));
    scidb::PdgesvdArgs args = *reinterpret_cast<PdgesvdArgs*>(bufs[BUF_ARGS]) ;

    // set up the scalapack grid, this has to be done before we generate the fake
    // problem
    slpp::int_t ICTXT=-1; // will be overwritten by sl_init
    sl_init_(ICTXT/*out*/, args.NPROW/*in*/, args.NPCOL/*in*/);  // sl_init calls blacs_grid_init
    // NOCHECKIN next line
    if(DBG) std::cerr << "pdgesvdSlave: sl_init(NPROW: "<<args.NPROW<<", NPCOL:"<<args.NPCOL<<") -> ICTXT: " << ICTXT << std::endl;

    // blacs_grid_info is legal after this

    // take a COPY of args, because we may have to overwrite it (for debug) when overwriteArgs is set
    // NOTE bufs[BUF_ARGS] should not be referenced after this point
    if(debugOverwriteArgs) {
        sl_int_t NPROW, NPCOL, MYPROW, MYPCOL, MYPNUM;
        getSlInfo(ICTXT/*in*/, NPROW/*in*/, NPCOL/*in*/, MYPROW/*out*/, MYPCOL/*out*/, MYPNUM/*out*/);

        size_t matrixCells = sizes[1]/sizeof(double);
        size_t matrixOrder = floor(sqrt(matrixCells));  // TODO: should be multiplied by NPROW*NPCOL
        args = pdgesvdGenTestArgs(ICTXT, NPROW, NPCOL, MYPROW, MYPCOL, MYPNUM, matrixOrder);
    }

    return pdgesvdSlave2(ICTXT, args, bufs, sizes, count);
}

// NOTE: args are modified, so it should not be made pointer or const-ref

///
/// This is the new standard style for implementing a slave routine for a ScaLAPACK operator,
/// in this case, pdgesvd_().  The difference from the old style is that the new style
/// requires that the ScaLAPACK context, ICTXT, be provided.  Until that requirement can be pushed
/// up into the "mpi_slave_xxx" files, the existing pdgesvdSlave() routine will create the context
/// and then call this routine.
///
sl_int_t pdgesvdSlave2(const sl_int_t ICTXT, PdgesvdArgs args, void* bufs[], size_t sizes[], unsigned count)
{
    // find out where I am in the scalapack grid
    sl_int_t NPROW, NPCOL, MYPROW, MYPCOL, MYPNUM;
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
    args.A.DESC.CTXT= ICTXT ;  // note: no DESC for S, it is not distributed, all have a copy
    args.U.DESC.CTXT= ICTXT ;
    args.VT.DESC.CTXT= ICTXT ;

    if(DBG) {
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
    }


    if(DBG) std::cerr << "pdgesvdSlave calling PDGESVD to get work size" << std:: endl;
    sl_int_t INFO = 0;
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

    //////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////
    if(DBG) std::cerr << "pdgesvdSlave: calling pdgesvd_ for computation." << std::endl ;
    INFO=0;
    pdgesvd_(args.jobU, args.jobVT, args.M, args.N,
             A,  args.A.I,  args.A.J,  args.A.DESC, S,
             U,  args.U.I,  args.U.J,  args.U.DESC,
             VT, args.VT.I, args.VT.J, args.VT.DESC,
             WORK, LWORK, INFO);


    sl_int_t numToPrintAtStart=4 ;
    if (MYPNUM==0 && DBG) {
        int ii; // used in 2nd loop
        for(ii=0; ii < std::min(SIZE_S, numToPrintAtStart); ii++) {
            std::cerr << "pdgesvdSlave: S["<<ii<<"] = " << S[ii] << std::endl;
        }
        // now skip to numToPrintAtStart before the end, (without repeating) and print to the end
        // to see if the test cases are producing zero eigenvalues (don't want that)
        sl_int_t numToPrintAtEnd=4;
        for(int ii=std::max(ii, SIZE_S-numToPrintAtEnd); ii < SIZE_S; ii++) {
            std::cerr << "pdgesvdSlave: S["<<ii<<"] = " << S[ii] << std::endl;
        }
    }
    if (DBG) {
        for(int ii=0; ii < std::min(SIZE_U, numToPrintAtStart); ii++) {
            std::cerr << "pdgesvdSlave: U["<<ii<<"] = " << U[ii] << std::endl;
        }
        for(int ii=0; ii < std::min(SIZE_VT, numToPrintAtStart); ii++) {
            std::cerr << "pdgesvdSlave: VT["<<ii<<"] = " << VT[ii] << std::endl;
        }
    }

    if(MYPNUM == 0) {
        if(INFO < 0) {
            // argument error
            std::cerr << "pdgesvdSlave(r:"<<MYPNUM<<"): "
                      << "ERROR: argument error, argument # " << -INFO << std::endl;
        } else if(INFO == (std::min(args.M, args.N)+1)) {
            // heterogeneity detected (eigenvalues did not match on all nodes)
            std::cerr << "pdgesvdSlave(r:"<<MYPNUM<<"): "
                      << "WARNING: eigenvalues did not match across all instances" << std::endl;
        } else if (INFO > 0) { // other + value of INFO
            // DBDSQR did not converge
            std::cerr << "pdgesvdSlave(r:"<<MYPNUM<<"): "
                      << "ERROR: DBDSQR did not converge: " << INFO << std::endl;
        }
    }
    return INFO ;
}

///
/// This code generates a PdgesvdArgs parameter block that can be used to drive
/// pdgesvdSlave2() when there is no SciDB application to provide the info.
/// It makes up parameters for a pdgesvd call that are appropriate to the
/// processor grid and order of matrix being decomposed
///
scidb::PdgesvdArgs pdgesvdGenTestArgs(sl_int_t ICTXT, sl_int_t NPROW, sl_int_t NPCOL,
                                                      sl_int_t MYPROW, sl_int_t MYPCOL, sl_int_t MYPNUM,
                                      sl_int_t order)
{
    scidb::PdgesvdArgs result;

    // hard-code a problem based on order and fixed block size
    const slpp::int_t M=order;
    const slpp::int_t N=order;
    const slpp::int_t MIN_MN=order;
    const slpp::int_t BLKSZ=32;   // NOCHECKIN
    const slpp::int_t one = 1 ;
    const char jobU = 'V';
    const char jobVT = 'V';

    // create ScaLAPACK array descriptors
    const slpp::int_t RSRC = 0 ;
    // LLD(A)
    slpp::int_t LLD_A = std::max(one, numroc_( order, BLKSZ, MYPROW, RSRC, NPROW ));
    // LLD(VT)
    slpp::int_t LLD_VT = std::max(one, numroc_( order, BLKSZ, MYPROW, RSRC, NPROW ));

    // WARNING -- note I never checked INFO from descinits !!
    sl_int_t INFO = 0;

    slpp::desc_t DESC_A;
    descinit_(DESC_A, order, order, BLKSZ, BLKSZ, 0, 0, ICTXT, LLD_A, INFO);
    slpp::desc_t DESC_U;
    descinit_(DESC_U, order, order, BLKSZ, BLKSZ, 0, 0, ICTXT, LLD_A, INFO);
    slpp::desc_t DESC_VT;
    descinit_(DESC_VT, order, order, BLKSZ, BLKSZ, 0, 0, ICTXT, LLD_VT, INFO);
    // S is different: global, not distributed, so its LLD(S) == LEN(S)
    slpp::desc_t DESC_S;
    descinit_(DESC_S, MIN_MN, 1, BLKSZ, BLKSZ, 0, 0, ICTXT, MIN_MN, INFO);

    pdgesvdMarshallArgs(&result, NPROW,  NPCOL, MYPROW, MYPCOL, MYPNUM,
                           jobU, jobVT, M, N,
                           NULL /*A*/,  one, one, DESC_A,
                           NULL /*S*/,
                           NULL /*U*/,  one, one, DESC_U,
                           NULL /*VT*/, one, one, DESC_VT);
    return result;
}


} // namespace scidb
