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
#include <sstream>
#include <string>

// std C
#include <time.h>

// de-facto standards
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_array.hpp>
#include <log4cxx/logger.h>

// SciDB
#include <array/MemArray.h>
#include <query/Network.h>
#include <query/Query.h>
#include <system/BlockCyclic.h>
#include <system/Cluster.h>
#include <system/Exceptions.h>
#include <system/Utils.h>


// more SciDB
#include <array/OpArray.h>
#include <scalapackUtil/reformat.hpp>
#include <scalapackUtil/ScaLAPACKPhysical.hpp>
#include <scalapackUtil/scalapackFromCpp.hpp>
#include <dlaScaLA/scalapackEmulation/scalapackEmulation.hpp>
#include <dlaScaLA/slaving/pdgesvdMaster.hpp>
#include <dlaScaLA/slaving/pdgesvdSlave.hpp>

// more Scidb
#include <mpi/MPISlaveProxy.h>
#include <mpi/MPILauncher.h>
#include <mpi/MPIManager.h>
#include "DLAErrors.h"
#include <util/shm/SharedMemoryIpc.h>


//
// NOTE: code sections marked REFACTOR are being identified as
//       candidates to be moved into MPIOperator and ScaLAPACKOperator
//       base classes.  This is one of the scheduled items for
//       DLA/ScaLAPACK milestone D (Cheshire milestone 4 timeframe)
//

namespace scidb
{
using namespace scidb;
using namespace boost;


static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.libmath.ops.gesvd"));


static const bool DBG = false;

///
/// A Physical SVD operator implemented using ScaLAPACK
/// The interesting work is done in invokeMPISvd(), above
///
class SVDPhysical : public ScaLAPACKPhysical
{
public:
    SVDPhysical(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema)
    :
        ScaLAPACKPhysical(logicalName, physicalName, parameters, schema)
    {
    }
    void invokeMPISvd(std::vector< shared_ptr<Array> >* inputArrays,
                      shared_ptr<Query>& query,
                      std::string& whichMatrix,
                      ArrayDesc& outSchema,
                      shared_ptr<Array>* result,
                      slpp::int_t* INFO);

    virtual shared_ptr<Array> execute(std::vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query);
};


///
/// Everything about the execute() method concerning the MPI execution of the arrays
/// is factored into this method.  This does not include the re-distribution of data
/// chunks into the ScaLAPACK distribution scheme, as the supplied inputArrays
/// must already be in that scheme.
///
/// + determine ScaLAPACK parameters regarding the shape of the process grid
/// + start and connect to an MPI slave process
/// + create ScaLAPACK descriptors for the input arrays
/// + convert the inputArrays into in-memory ScaLAPACK layout in shared memory
/// + call a "master" routine that passes the ScaLAPACK operator name, parameters,
///   and shared memory descriptors to the ScaLAPACK MPI process that will do the
///   actual computation.
/// + wait for successful completion
/// + construct an "OpArray" that make and Array API view of the output memory.
/// + return that output array.
///


/// TODO: fix GEMMPhysical.cpp as well and factor this nicely
slpp::int_t upToMultiple(slpp::int_t size, slpp::int_t blocksize)
{
    return (size+blocksize-1)/blocksize * blocksize;
}

void SVDPhysical::invokeMPISvd(std::vector< shared_ptr<Array> >* inputArrays,
                              shared_ptr<Query>& query,
                              std::string& whichMatrix,
                              ArrayDesc& outSchema,
                              shared_ptr<Array>* result,
                              slpp::int_t* INFO)
{
    if(DBG) std::cerr << "invokeMPISvd() reached" << std::endl ;
    LOG4CXX_DEBUG(logger, "invokeMPISVD()");

    // MPI_Init(); -- now done in slave processes
    // in SciDB we use query->getInstancesCount() & getInstanceID()
    // and we use a fake scalapack gridinit to set up the grid and where
    // we are in it.  so the blacs_getinfo calls below are fakes to help
    // keep the code more similar

    //!
    //!.... Get the (emulated) BLACS info .............................................
    //!
    slpp::int_t ICTXT=-1;
    slpp::int_t NPROW=-1, NPCOL=-1, MYPROW=-1 , MYPCOL=-1 ;
    blacs_gridinfo_(ICTXT, NPROW, NPCOL, MYPROW, MYPCOL);
    checkBlacsInfo(query, ICTXT, NPROW, NPCOL, MYPROW, MYPCOL);

    slpp::int_t instanceID = query->getInstanceID();
    slpp::int_t MYPE = instanceID ;  // if checkBlacsInfo returns, this assumption is true

    launchMPISlaves(query, NPROW*NPCOL);

    // REFACTOR: this is a pattern in DLAs
    //
    // get dimension information about the input arrays
    //
    if(DBG) std::cerr << "invokeMPISvd get dim info" << std::endl ;
    boost::shared_ptr<Array> Ain = (*inputArrays)[0];

    std::ostringstream tmp;
    Ain->getArrayDesc().getDimensions()[0].toString(tmp) ;
    if(DBG) std::cerr << tmp.str() << std::endl;

    std::ostringstream tmp2;
    Ain->getArrayDesc().getDimensions()[1].toString(tmp2) ;
    if(DBG) std::cerr << tmp2.str() << std::endl;

    // find M,N from input array
    slpp::int_t M = nrow(Ain.get());
    slpp::int_t N = ncol(Ain.get());
    if(DBG) std::cerr << "M " << M << " N " << N << std::endl;

    // find MB,NB from input array, which is the chunk size

    checkInputArray(Ain.get());
    //!
    //!.... Set up ScaLAPACK array descriptors ........................................
    //!

    // these formulas for LLD (loacal leading dimension) and LTD (local trailing dimension)
    // are found in the headers of the scalapack functions such as pdgesvd_()
    const slpp::int_t MB= brow(Ain.get());
    const slpp::int_t NB= bcol(Ain.get());
    const slpp::int_t one = 1 ;
    slpp::int_t MIN_MN = std::min(M,N);

    const slpp::int_t RSRC = 0 ;
    const slpp::int_t CSRC = 0 ;
    // LLD(A)
    slpp::int_t LLD_A = std::max(one, numroc_( M, MB, MYPROW, RSRC, NPROW ));
    if(DBG) std::cerr << "M:"<<M <<" MB:"<<MB << " MYPROW:"<<MYPROW << " NPROW:"<< NPROW << std::endl;
    if(DBG) std::cerr << "--> LLD_A = " << LLD_A << std::endl;

    // LTD(A)
    slpp::int_t LTD_A = std::max(one, numroc_( N, NB, MYPCOL, CSRC, NPCOL ));
    if(DBG) std::cerr << "N:"<<N <<" NB:"<<NB << " MYPCOL:"<<MYPCOL << " NPCOL:"<<NPCOL<< std::endl;
    if(DBG) std::cerr << "-->LTD_A = " << LTD_A << std::endl;

    // LLD(VT)
    slpp::int_t LLD_VT = std::max(one, numroc_( MIN_MN, MB, MYPROW, RSRC, NPROW ));
    if(DBG) std::cerr << "MIN_MN:"<<MIN_MN <<" MB:"<<MB << " MYPROW:"<<MYPROW << " NPROW:"<< NPROW << std::endl;
    if(DBG) std::cerr << "-->LLD_VT = " << LLD_VT << std::endl;

    // LTD(U)
    slpp::int_t LTD_U = std::max(one, numroc_( MIN_MN, NB, MYPCOL, CSRC, NPCOL ));
    if(DBG) std::cerr << "MIN_MN:"<<MIN_MN <<" NB:"<<NB << " MYPCOL:"<<MYPCOL << " NPCOL:"<<NPCOL<< std::endl;
    if(DBG) std::cerr << "-->LTD_U = " << LTD_U << std::endl;


    // create ScaLAPACK array descriptors
    if(DBG) std::cerr << "descinit_ DESC_A" << std::endl;
    slpp::desc_t DESC_A;
    descinit_(DESC_A, M, N, MB, NB, 0, 0, ICTXT, LLD_A, *INFO);
    if(DBG) std::cerr << "DESC_A=" << DESC_A << std::endl;

    if(DBG) std::cerr << "descinit_ DESC_U" << std::endl;
    slpp::desc_t DESC_U;
    descinit_(DESC_U, M, MIN_MN, MB, NB, 0, 0, ICTXT, LLD_A, *INFO);

    if(DBG) std::cerr << "descinit_ DESC_VT" << std::endl;
    slpp::desc_t DESC_VT;
    descinit_(DESC_VT, MIN_MN, N, MB, NB, 0, 0, ICTXT, LLD_VT, *INFO);

    // S is different: global, not distributed, so its LLD(S) == LEN(S)
    if(DBG) std::cerr << "descinit_ DESC_S" << std::endl;
    slpp::desc_t DESC_S;
    descinit_(DESC_S, MIN_MN, 1, MB, NB, 0, 0, ICTXT, MIN_MN, *INFO);

    //REFACTOR
    if(DBG) {
        std::cerr << "#### pdgesvdMaster #########################################" << std::endl;
        std::cerr << "MB:" << MB << std::endl;
        std::cerr << "NB:" << NB << std::endl;
        std::cerr << "MYPROW:" << MYPROW << std::endl;
        std::cerr << "NPROW:" << NPROW << std::endl;
    }


    if(DBG) {
        std::cerr << "LOCAL SIZES:@@@@@@@@@@@@@@@@@@@" << std::endl ;
        std::cerr << "XX MIN_MN = " << MIN_MN << std::endl;
        std::cerr << "XX LLD_A   = " << LLD_A << std::endl;
        std::cerr << "XX LTD_U   = " << LTD_U << std::endl;
        std::cerr << "XX LLD_VT  = " << LLD_VT << std::endl;
        std::cerr << "XX LTD_A   = " << LTD_A << std::endl;
    }

    // local sizes
    const slpp::int_t SIZE_A  = LLD_A  * LTD_A ;
    const slpp::int_t SIZE_U  = LLD_A  * LTD_U ;
    const slpp::int_t SIZE_VT = LLD_VT * LTD_A ;

    //
    // Create IPC buffers
    //
    enum dummy {BUF_ARGS=0, BUF_MAT_A, BUF_MAT_S, BUF_MAT_U, BUF_MAT_VT, NUM_BUFS };
    size_t elemBytes[NUM_BUFS];
    size_t nElem[NUM_BUFS];
    std::string dbgNames[NUM_BUFS];

    elemBytes[BUF_ARGS] = 1 ; nElem[BUF_ARGS] = sizeof(scidb::PdgesvdArgs) ; dbgNames[BUF_ARGS] = "PdgesvdArgs";
    typedef scidb::SharedMemoryPtr<double> shmSharedPtr_t ;

    const slpp::int_t ALLOC_A = upToMultiple(SIZE_A, MB*NB);
    const slpp::int_t ALLOC_S = upToMultiple(MIN_MN, MB*NB);  // NOCHECKIN, different case needs examining
    const slpp::int_t ALLOC_U = upToMultiple(SIZE_U, MB*NB);
    const slpp::int_t ALLOC_VT = upToMultiple(SIZE_VT, MB*NB);

    elemBytes[BUF_MAT_A] = sizeof(double) ; nElem[BUF_MAT_A] = ALLOC_A ; dbgNames[BUF_MAT_A] = "A" ;
    elemBytes[BUF_MAT_S] = sizeof(double) ; nElem[BUF_MAT_S] = ALLOC_S ; dbgNames[BUF_MAT_S] = "S" ;
    elemBytes[BUF_MAT_U] = sizeof(double) ; nElem[BUF_MAT_U] = ALLOC_U ; dbgNames[BUF_MAT_U] = "U" ;
    elemBytes[BUF_MAT_VT]= sizeof(double) ; nElem[BUF_MAT_VT]= ALLOC_VT; dbgNames[BUF_MAT_VT]= "VT" ;

    std::vector<MPIPhysical::SMIptr_t> shmIpc = allocateMPISharedMemory(NUM_BUFS, elemBytes, nElem, dbgNames);

    //
    // Zero inputs, to emulate a sparse matrix implementation (but slower)
    // and then extract the non-missing info onto that.
    // Set outputs to NaN, to catch invalid cells being returned
    void *argsBuf = shmIpc[0]->get();
    double* A = reinterpret_cast<double*>(shmIpc[1]->get());
    double* S = reinterpret_cast<double*>(shmIpc[2]->get()); shmSharedPtr_t Sx(shmIpc[2]);         
    double* U = reinterpret_cast<double*>(shmIpc[3]->get()); shmSharedPtr_t Ux(shmIpc[3]);
    double* VT = reinterpret_cast<double*>(shmIpc[4]->get());shmSharedPtr_t VTx(shmIpc[4]);  

    setInputMatrixToAlgebraDefault(A, nElem[BUF_MAT_A]);
    extractArrayToScaLAPACK(Ain, A, DESC_A);
    setOutputMatrixToAlgebraDefault(S, nElem[BUF_MAT_S]);
    setOutputMatrixToAlgebraDefault(U, nElem[BUF_MAT_U]);
    setOutputMatrixToAlgebraDefault(VT, nElem[BUF_MAT_VT]);

    // debug that the reformat worked correctly:
    if(DBG) {
        for(int ii=0; ii < SIZE_A; ii++) {
            std::cerr << "("<< MYPROW << "," << MYPCOL << ") A["<<ii<<"] = " << A[ii] << std::endl;
        }
    }

    //!
    //!.... Call PDGESVD to compute the SVD of A .............................
    //!
    if(DBG) std::cerr << "SVD: calling pdgesvd_ M,N:" << M  << "," << N
                                  << "MB,NB:" << MB << "," << NB << std::endl;
    LOG4CXX_DEBUG(logger, "MPISVD: calling pdgesvd_ M,N:" << M <<","<< "MB,NB:" << MB << "," << NB);

    if(DBG) std::cerr << "SVDPhysical calling PDGESVD to compute" << std:: endl;
    boost::shared_ptr<MpiSlaveProxy> slave = _ctx->getSlave(_launchId);
    pdgesvdMaster(query.get(), _ctx, slave, _ipcName, argsBuf,
                  NPROW, NPCOL, MYPROW, MYPCOL, MYPE,
                  'V', 'V', M, N,
                  A,  one, one,  DESC_A, S,
                  U,  one, one,  DESC_U,
                  VT, one, one, DESC_VT,
                  *INFO);

    LOG4CXX_DEBUG(logger, "SVD: pdgesvdMaster finished");
    if(DBG) std::cerr << "SVD: calling pdgesvdMaster finished" << std::endl;
    if(DBG) std::cerr << "SVD: pdgesvdMaster returned INFO:" << *INFO << std::endl;

    Dimensions const& dims = Ain->getArrayDesc().getDimensions();

    boost::shared_array<char> resPtrDummy(reinterpret_cast<char*>(NULL));

    typedef scidb::ReformatFromScalapack<shmSharedPtr_t> reformatOp_t ;

    if (whichMatrix == "S" || whichMatrix == "SIGMA" || whichMatrix == "values")
    {
        if(DBG) std::cerr << "sequential values from 'value/S' memory" << std::endl;
        for(int ii=0; ii < MIN_MN; ii++) {
            if(DBG) std::cerr << "S["<<ii<<"] = " << S[ii] << std::endl;
        }

        if(DBG) std::cerr << "using pdelgetOp to reformat svd vals from memory to scidb array , start" << std::endl ;
        // TODO JHM ;
        // Check that we have any singluar values to output
        // this means checking for non-zeros in S or some other
        // indicator from the pdgesvd
        // if (myLen == 0)
        //     return shared_ptr<Array>(new MemArray(outSchema));
        //

        // The S output vector is not a distributed array like A, U, &VT are
        // The entire S vector is returned on every instance, so we have to make
        // sure that only one instance does the work of converting any particular
        // chunk.
        // To manage this we introduced the "global" flag to the pdelgetOp operator
        // which is also then able to avoid actually the significant overhead of
        // the ScaLAPACK SPMD pdelget() and instead simply subscripts the array.
        //
        // We treat the S vector, as mathematicians do, as a column vector, as this
        // stays consistent with ScaLAPACK.


        // first coordinate for the first chunk that my instance handles
        uint64_t chunkSize = dims[0].getChunkInterval();
        Coordinates first(1);
        first[0] = dims[0].getStart() + MYPROW * chunkSize ;
        if(DBG) std::cerr << "SVD('left'): first:" << first[0] << " = start+ MYPROW:"<<MYPROW<< " * chunksz:"<< chunkSize << std::endl ;

        Coordinates last(1);
        last[0] = dims[0].getStart() + MIN_MN - 1;
        if(DBG) std::cerr << "SVD('left'): last:" << last[0] << " = start+ MIN_MN - 1"<<MIN_MN<< std::endl;

        Coordinates iterDelta(1);
        iterDelta[0] = NPROW * chunkSize ;
        if(DBG) std::cerr << "SVD('left'): inter:" << iterDelta[0] << " = NPROW:"<<NPROW<<" * chunksz:"<<chunkSize << std::endl;

        reformatOp_t      pdelgetOp(Sx, DESC_S, dims[0].getStart(), 0, true);

        if(MYPCOL==0) {
            // return results only from the first column of instances
            if(DBG) std::cerr << "SVD('left') instance @ MYPROW,MYPCOL: "<<MYPROW<<","<<MYPCOL
                      << " returns chunks of S" << std::endl;
            *result = shared_ptr<Array>(new OpArray<reformatOp_t>(outSchema, resPtrDummy, pdelgetOp,
                                                                first, last, iterDelta));
        } else {
            // vector has been output by first column or first row, whichever is shorter
            // so return an empty array.  (We still had to do our part to generate
            // the global vector, so we can't return the empty array at the top of execute
            // like we do for completely non-participatory instances.)
            if(DBG) std::cerr << "MYPROW,MYPCOL: "<<MYPROW<<","<<MYPCOL
                      << " does not return chunks of S" << std::endl;
            *result = shared_ptr<Array>(new MemArray(outSchema)); // empty array
        }
    }

    else if (whichMatrix == "U" || whichMatrix == "left")
    {
        if(DBG) std::cerr << "--------------------------------------" << std::endl;
        if(DBG) std::cerr << "sequential values from 'left/U' memory" << std::endl;
        for(int ii=0; ii < SIZE_U; ii++) {
            if(DBG) std::cerr << "U["<<ii<<"] = " << U[ii] << std::endl;
        }
        if(DBG) std::cerr << "--------------------------------------" << std::endl;
        if(DBG) std::cerr << "using pdelgetOp to reformat svd left from memory to scidb array , start" << std::endl ;
        //
        // an OpArray is a SplitArray that is filled on-the-fly by calling the operator
        // so all we have to do is create one with an upper-left corner equal to the
        // global position of the first local block we have.  so we need to map
        // our "processor" coordinate into that position, which we do by multiplying
        // by the chunkSize
        //
        Coordinates first(2);
        first[0] = dims[0].getStart() + MYPROW * dims[0].getChunkInterval();
        first[1] = dims[1].getStart() + MYPCOL * dims[1].getChunkInterval();

        Coordinates last(2);
        last[0] = dims[0].getStart() + dims[0].getLength() - 1;
        last[1] = dims[1].getStart() + dims[1].getLength() - 1;

        Coordinates iterDelta(2);
        iterDelta[0] = NPROW * dims[0].getChunkInterval();
        iterDelta[1] = NPCOL * dims[1].getChunkInterval();

        if(DBG) std::cerr << "SVD(left) SplitArray from ("<<first[0]<<","<<first[1]<<") to (" << last[0] <<"," <<last[1]<<") delta:"<<iterDelta[0]<<","<<iterDelta[1]<< std::endl;
        LOG4CXX_DEBUG(logger, "Creating array ("<<first[0]<<","<<first[1]<<"), (" << last[0] <<"," <<last[1]<<")");

        reformatOp_t      pdelgetOp(Ux, DESC_U, dims[0].getStart(), dims[1].getStart());
        *result = shared_ptr<Array>(new OpArray<reformatOp_t>(outSchema, resPtrDummy, pdelgetOp,
                                                            first, last, iterDelta));
    }

    else if (whichMatrix == "VT" || whichMatrix == "right")
    {
        if(DBG) std::cerr << "sequential values from 'right/VT' memory" << std::endl;
        for(int ii=0; ii < SIZE_VT; ii++) {
            if(DBG) std::cerr << "VT["<<ii<<"] = " << VT[ii] << std::endl;
        }

        if(DBG) std::cerr << "reformat ScaLAPACK svd right to scidb array , start" << std::endl ;
        Coordinates first(2);
        first[0] = dims[0].getStart() + MYPROW * dims[0].getChunkInterval();
        first[1] = dims[1].getStart() + MYPCOL * dims[1].getChunkInterval();

        // TODO JHM ; clean up use of last
        Coordinates last(2);
        last[0] = dims[0].getStart() + dims[0].getLength() - 1;
        last[1] = dims[1].getStart() + dims[1].getLength() - 1;

        Coordinates iterDelta(2);
        iterDelta[0] = NPROW * dims[0].getChunkInterval();
        iterDelta[1] = NPCOL * dims[1].getChunkInterval();

        if(DBG) std::cerr << "SVD(right) SplitArray: "<<first[0]<<","<<first[1]<<" to " << last[0] <<"," <<last[1]<< std::endl;
        LOG4CXX_DEBUG(logger, "Creating array ("<<first[0]<<","<<first[1]<<"), (" << last[0] <<"," <<last[1]<<")");

        reformatOp_t    pdelgetOp(VTx, DESC_VT, dims[0].getStart(), dims[1].getStart());
        *result = shared_ptr<Array>(new OpArray<reformatOp_t>(outSchema, resPtrDummy, pdelgetOp,
                                                              first, last, iterDelta));
    }

    // REFACTOR to MPIOperator
    //--------------------- Cleanup for objects no longer in use
    // after the for loop the shared memory is still intact
    for(size_t i=0; i<shmIpc.size(); i++) {
        if (!shmIpc[i]) {
            continue;
        }
        shmIpc[i]->close();
        if (!shmIpc[i]->remove()) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "shared_memory_remove");
        }
    }

    unlaunchMPISlaves();
    resetMPI();


}


///
/// + converts inputArrays to psScaLAPACK distribution
/// + intersects the array chunkGrid with the maximum process grid
/// + sets up the ScaLAPACK grid accordingly and if not participating, return early
/// + calls invokeMPISvd()
/// + returns the output OpArray.
/// 
shared_ptr<Array> SVDPhysical::execute(std::vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query)
{
    const bool DBG = false ;
    if(DBG) std::cerr << "SVDPhysical::execute() begin ---------------------------------------" << std::endl;

    // redistribute input arrays to ScaLAPACK block-cyclic
    std::vector<shared_ptr<Array> > redistInputs = redistributeInputArrays(inputArrays, query);

    // REFACTOR make a checkArgs(inputArrays, query); + assert just one input
    shared_ptr<Array> input = redistInputs[0];  // just one input

    //!
    //!.... Initialize the (imitation)BLACS used by the instances to calculate sizes
    //!     AS IF they are MPI processes (which they are not)
    //!
    bool doesParticipate = doBlacsInit(redistInputs, query);
    if (!doesParticipate) {
        return shared_ptr<Array>(new MemArray(_schema));
    }

    //
    // invokeMPISVD()
    //
    string whichMatrix = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getString();
    shared_ptr<Array> result;
    slpp::int_t INFO = DEFAULT_BAD_INFO ;

    invokeMPISvd(&redistInputs, query, whichMatrix, _schema, &result, &INFO);

    if (INFO != 0) {
        std::stringstream ss; ss << "svd error: pdgesvd_ INFO is: " << INFO << std::endl ;
        if(DBG) std::cerr << ss.str() << std::endl ;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << ss.str());
    }

    // return the scidb array
    if(DBG) std::cerr << "SVDPhysical::execute success" << std::endl ;
    LOG4CXX_DEBUG(logger, "SVDPhysical::execute success");

    Dimensions const& rdims = result->getArrayDesc().getDimensions();
    if (whichMatrix == "values") {
        if(DBG) std::cerr << "returning result array size: " << rdims[0].getLength() << std::endl ;
    } else if ( whichMatrix == "left" || whichMatrix == "right" ) {
        if(DBG) std::cerr << "returning result array size: " << rdims[1].getLength() <<
                     "," << rdims[0].getLength() << std::endl ;
    }

    if(DBG) std::cerr << "SVDPhysical::execute end ---------------------------------------" << std::endl;
    return result;
}

REGISTER_PHYSICAL_OPERATOR_FACTORY(SVDPhysical, "gesvd", "SVDPhysical");

} // namespace
