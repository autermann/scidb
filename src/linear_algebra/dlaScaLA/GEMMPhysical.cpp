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
#include <tr1/array>

// std C
#include <time.h>

// de-facto standards
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_array.hpp>
#include <log4cxx/logger.h>

// SciDB
#include <array/MemArray.h>
#include <mpi/MPISlaveProxy.h>
#include <mpi/MPILauncher.h>
#include <mpi/MPIManager.h>
#include <query/Network.h>
#include <query/Query.h>
#include <system/BlockCyclic.h>
#include <system/Cluster.h>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <util/shm/SharedMemoryIpc.h>


// more SciDb
#include <array/OpArray.h>
#include <scalapackUtil/reformat.hpp>
#include <scalapackUtil/ScaLAPACKPhysical.hpp>
#include <scalapackUtil/scalapackFromCpp.hpp>
#include <dlaScaLA/scalapackEmulation/scalapackEmulation.hpp>
#include <dlaScaLA/slaving/pdgemmMaster.hpp>
#include <dlaScaLA/slaving/pdgemmSlave.hpp>

// locals
#include "DLAErrors.h"


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

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.libdense_linear_algebra.ops.gemm"));

static const bool DBG = false;

///
/// A Physical multiply operator implemented using ScaLAPACK
/// The interesting work is done in invokeMPIGemm(), above
///
class GEMMPhysical : public ScaLAPACKPhysical
{
public:

    GEMMPhysical(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema)
    :
        ScaLAPACKPhysical(logicalName, physicalName, parameters, schema)
    {
    }
    void invokeMPIGemm(std::vector< shared_ptr<Array> >* inputArrays,
                       bool transA, bool transB,
                       double ALPHA, double BETA,
                       shared_ptr<Query>& query,
                       ArrayDesc& outSchema,
                       shared_ptr<Array>* result,
                       slpp::int_t* INFO);

    virtual shared_ptr<Array> execute(std::vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query);
private:
    typedef std::tr1::array<slpp::int_t, 2 > matSize_t; // handling size/chunkSize/offsets as pairs makes code more readable

    /// get matrix size as vector
    matSize_t getMatSize(boost::shared_ptr<Array>& array);
    /// get matrix chunk size as vector
    matSize_t getBlockSize(boost::shared_ptr<Array>& array);
};


// TODO: factor to class ScaLAPACKPhysical ?
inline GEMMPhysical::matSize_t GEMMPhysical::getMatSize(boost::shared_ptr<Array>& array) {
    matSize_t result;
    result[0] = nrow(array.get());
    result[1] = ncol(array.get());
    return result;
}

// TODO: factor to class ScaLAPACKPhysical ?
inline GEMMPhysical::matSize_t GEMMPhysical::getBlockSize(boost::shared_ptr<Array>& array) {
    matSize_t result;
    result[0] = brow(array.get());
    result[1] = bcol(array.get());
    return result;
}


// TODO: REFACTORING: continue use of matSize_t in more places
// TODO: REFACTORING: make a "super array" that encapsulates the SciDB::Array and the ScaLAPACK DESC
//                    so we can pass fewer arguments

char getTransposeCode(bool transpose) {
    return transpose ? 'T' : 'N' ;
}

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
void GEMMPhysical::invokeMPIGemm(std::vector< shared_ptr<Array> >* inputArrays,
                                 bool transA, bool transB,
                                 double ALPHA, double BETA,
                                 shared_ptr<Query>& query,
                                 ArrayDesc& outSchema,
                                 shared_ptr<Array>* result,
                                 slpp::int_t* INFO)
{
    enum dummy  {R=0, C=1};              // row column
    enum dummy2 {AA=0, BB, CC, NUM_MATRICES};  // which matrix: alpha AA * BB + beta CC -> result

    if(DBG) std::cerr << "invokeMPIGemm() reached" << std::endl ;
    LOG4CXX_DEBUG(logger, "invokeMPIGemm()");

    size_t numArray = NUM_MATRICES; // for now ... may make CC optional when beta is 0

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
    if(DBG) std::cerr << "invokeMPIGemm get dim info" << std::endl ;

    boost::shared_ptr<Array> inArray[NUM_MATRICES];
    for(size_t i=0; i < numArray; i++ ) {
        inArray[i] = (*inputArrays)[i] ;
    }

    // TODO JHM : checkArray/Inputs must make sure we are only using 2D arrays
    //            and/or convert 1d arrays to nrows x 1

    // matrix sizes from arrays A,B,C
    matSize_t size[NUM_MATRICES];
    matSize_t blockSize[NUM_MATRICES];
    for(size_t i=0; i < numArray; i++ ) {
        size[i] = getMatSize(inArray[i]);
        if(DBG) std::cerr << "size["<<i<<"] " << size[i][R] << "," << size[i][C] << std::endl;

        blockSize[i] = getBlockSize(inArray[i]);
        if(DBG) std::cerr << "blockSize["<<i<<"] " << blockSize[i][R] << "," << blockSize[i][C] << std::endl;
    }

    for(size_t i=0; i < numArray; i++ ) {
        checkInputArray(inArray[i].get());  // check block size constraints, etc
    }
    //!
    //!.... Set up ScaLAPACK array descriptors ........................................
    //!

    // these formulas for LLD (local leading dimension) and LTD (local trailing dimension)
    // are found in the headers of the ScaLAPACK functions such as pdgemm_()
    const slpp::int_t one = 1 ;

    // TODO: turn these pairs into matSize_t matrixLocalSize[NUM_MATRICES];
    slpp::int_t LLD[NUM_MATRICES]; // local leading dimension
    slpp::int_t LTD[NUM_MATRICES]; // local trailing dimension
    for(size_t i=0; i < numArray; i++ ) {
        slpp::int_t RSRC = 0 ;
        LLD[i] = std::max(one, numroc_( size[i][R], blockSize[i][R], MYPROW, RSRC, NPROW ));
        if(DBG) std::cerr << "M:"<<size[i][R] <<" blockSize[i][R]:"<<blockSize[i][R] << " MYPROW:"<<MYPROW << " NPROW:"<< NPROW << std::endl;
        if(DBG) std::cerr << "--> LLD[i] = " << LLD[i] << std::endl;
        LTD[i] = std::max(one, numroc_( size[i][C], blockSize[i][C], MYPCOL, RSRC, NPCOL ));
    }

    // create ScaLAPACK array descriptors
    slpp::desc_t DESC[NUM_MATRICES];
    for(size_t i=0; i < numArray; i++ ) {
        if(DBG) std::cerr << "descinit_ DESC["<<i<<"]"<< std::endl;
        descinit_(DESC[i], size[i][R], size[i][C], blockSize[i][R], blockSize[i][C], 0, 0, ICTXT, LLD[i], *INFO);
        if(DBG) std::cerr << "DESC[i]=" << DESC[i] << std::endl;
        // debugging for #1986 ... when #instances is prime, process grid is a row.  When small chunk sizes are used,
        // desc.LLD is being set to a number larger than the chunk size ... I don't understand or expect this.
        bool doDebugTicket1986=true;  // remains on until fixed, can't ship with this not understood.
        if(doDebugTicket1986) {
            if (DESC[i].LLD > DESC[i].MB) {
                std::cerr << "DEBUG, ticket 1986, DESC[i].LLD: " << DESC[i].LLD << " > DESC[i].MB: " << DESC[i].MB << std::endl;
            }
        }
    }

    //REFACTOR
    if(DBG) {
        std::cerr << "#### pdgemmMaster #########################################" << std::endl;
        std::cerr << "MYPROW:" << MYPROW << std::endl;
        std::cerr << "NPROW:" << NPROW << std::endl;
    }

    // matrix allocations are of local size, not global size
    slpp::int_t matrixLocalSize[NUM_MATRICES];
    for(size_t i=0; i < numArray; i++ ) {
        matrixLocalSize[i]  = LLD[i] * LTD[i] ;
    }

    //
    // Create IPC buffers
    //
    enum dummy3 {BUF_ARGS=0, BUF_MAT_AA, BUF_MAT_BB, BUF_MAT_CC, NUM_BUFS };
    size_t nElem[NUM_BUFS];
    size_t elemBytes[NUM_BUFS];
    std::string dbgNames[NUM_BUFS];

    elemBytes[BUF_ARGS]= 1 ;           nElem[BUF_ARGS]= sizeof(scidb::PdgemmArgs) ; dbgNames[BUF_ARGS] = "PdgemmArgs";

    elemBytes[BUF_MAT_AA]= sizeof(double) ; nElem[BUF_MAT_AA]= matrixLocalSize[AA]; dbgNames[BUF_MAT_AA] = "A" ;
    elemBytes[BUF_MAT_BB]= sizeof(double) ; nElem[BUF_MAT_BB]= matrixLocalSize[BB]; dbgNames[BUF_MAT_BB] = "B" ;
    elemBytes[BUF_MAT_CC]= sizeof(double) ; nElem[BUF_MAT_CC]= matrixLocalSize[CC]; dbgNames[BUF_MAT_CC] = "C" ;
    typedef scidb::SharedMemoryPtr<double> shmSharedPtr_t ;

    std::vector<MPIPhysical::SMIptr_t> shmIpc = allocateMPISharedMemory(NUM_BUFS, elemBytes, nElem, dbgNames);

    //
    // Zero inputs, to emulate a sparse matrix implementation (but slower)
    // and then extract the non-missing info onto that.
    //
    double* asDoubles[NUM_MATRICES];
    for(size_t mat=0; mat < numArray; mat++ ) {
        asDoubles[mat] = reinterpret_cast<double*>(shmIpc[mat+1]->get());
        setInputMatrixToAlgebraDefault(asDoubles[mat], nElem[mat]);  // C is both Input & Output, but Input takes precedence
        extractArrayToScaLAPACK(inArray[mat], asDoubles[mat], DESC[mat]);

        if(DBG) { // that the reformat worked correctly
            for(int ii=0; ii < matrixLocalSize[mat]; ii++) {
                std::cerr << "("<< MYPROW << "," << MYPCOL << ") array["<<mat<<"]["<<ii<<"] = " << asDoubles[mat][ii] << std::endl;
            }
        }
    }
    shmSharedPtr_t Cx(shmIpc[CC+1]);

    //!
    //!.... Call pdgemm to compute the product of A and B .............................
    //!
    LOG4CXX_DEBUG(logger, "MPIGemm: calling pdgemm_ M,N,K:" << size[AA][R]  << ","
                                                            << size[BB][R] << ","
                                                            << size[CC][C]
                           << " MB,NB:" << blockSize[AA][R] << "," << blockSize[AA][C]);

    if(DBG) std::cerr << "GEMMPhysical calling pdgemm to compute" << std:: endl;
    boost::shared_ptr<MpiSlaveProxy> slave = _ctx->getSlave(_launchId);

    // note on the value of K, from docs on PDGEMM:
    // If transa = 'T' or 'C'(true), it is the number of rows in submatrix A."
    // If transa = 'N'(false), it is the number of columns in submatrix A."
    slpp::int_t K = size[AA][ transA ? R : C ];

    pdgemmMaster(query.get(), _ctx, slave, _ipcName, shmIpc[BUF_ARGS]->get(),
                  NPROW, NPCOL, MYPROW, MYPCOL, MYPE,
                  getTransposeCode(transA), getTransposeCode(transB),
                  size[CC][R], size[CC][C], K,
                  &ALPHA,
                  asDoubles[AA], one, one, DESC[AA],
                  asDoubles[BB], one, one, DESC[BB],
                  &BETA,
                  asDoubles[CC], one, one, DESC[CC],
                  *INFO);

    LOG4CXX_DEBUG(logger, "Gemm: pdgemmMaster finished");
    if(DBG) std::cerr << "Gemm: pdgemmMaster finished with INFO " << *INFO << std::endl;


    boost::shared_array<char> resPtrDummy(reinterpret_cast<char*>(NULL));


    typedef scidb::ReformatFromScalapack<shmSharedPtr_t> reformatOp_t ;

    if(DBG) std::cerr << "--------------------------------------" << std::endl;
    if(DBG) std::cerr << "sequential values from 'C' memory" << std::endl;
    if(DBG) {
        for(int ii=0; ii < matrixLocalSize[CC]; ii++) {
            std::cerr << "("<< MYPROW << "," << MYPCOL << ") C["<<ii<<"] = " << asDoubles[CC][ii] << std::endl;
        }
    }

    if(DBG) std::cerr << "--------------------------------------" << std::endl;
    if(DBG) std::cerr << "using pdelgetOp to reformat Gemm left from memory to scidb array , start" << std::endl ;

    //
    // an OpArray is a SplitArray that is filled on-the-fly by calling the operator
    // so all we have to do is create one with an upper-left corner equal to the
    // global position of the first local block we have.  so we need to map
    // our "processor" coordinate into that position, which we do by multiplying
    // by the chunkSize
    //
    Dimensions const& dims = inArray[CC]->getArrayDesc().getDimensions();
    Coordinates first(2);
    first[R] = dims[R].getStart() + MYPROW * blockSize[CC][R];
    first[C] = dims[C].getStart() + MYPCOL * blockSize[CC][C];

    Coordinates last(2);
    last[R] = dims[R].getStart() + size[CC][R] - 1;
    last[C] = dims[C].getStart() + size[CC][C] - 1;

    Coordinates iterDelta(2);
    iterDelta[0] = NPROW * blockSize[CC][R];
    iterDelta[1] = NPCOL * blockSize[CC][C];

    if(DBG) std::cerr << "Gemm(left) SplitArray from ("<<first[R]<<","<<first[C]<<") to (" << last[R] <<"," <<last[C]<<") delta:"<<iterDelta[R]<<","<<iterDelta[C]<< std::endl;
    LOG4CXX_DEBUG(logger, "Creating array ("<<first[R]<<","<<first[C]<<"), (" << last[R] <<"," <<last[C]<<")");

    reformatOp_t      pdelgetOp(Cx, DESC[CC], dims[R].getStart(), dims[C].getStart());
    *result = shared_ptr<Array>(new OpArray<reformatOp_t>(outSchema, resPtrDummy, pdelgetOp,
                                                          first, last, iterDelta));


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

    if(DBG) std::cerr << "invoke: returning from invokeMPIGemm with INFO:" << *INFO << std::endl ;
}


///
/// + converts inputArrays to psScaLAPACK distribution
/// + intersects the array chunkGrid with the maximum process grid
/// + sets up the ScaLAPACK grid accordingly and if not participating, return early
/// + calls invokeMPIGemm()
/// + returns the output OpArray.
///
shared_ptr<Array> GEMMPhysical::execute(std::vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query)
{
    const bool DBG = false ;
    if(DBG) std::cerr << "GEMMPhysical::execute() begin ---------------------------------------" << std::endl;

    // TODO: make a GEMMLogical checkArgs(inputArrays, query); which asserts two or three arrays

    // TODO: extend the user API to allow setting the following 4 parameters
    bool TRANSA = false ;
    bool TRANSB = false ;
    double ALPHA = 1.0 ;
    double BETA = 1.0 ;

    // redistribute input arrays to ScaLAPACK block-cyclic
    std::vector<shared_ptr<Array> > redistInputs = redistributeInputArrays(inputArrays, query);


    //!
    //!.... Initialize the (imitation)BLACS used by the instances to calculate sizes
    //!     AS IF they are MPI processes (which they are not)
    //!
    bool doesParticipate = doBlacsInit(redistInputs, query);
    if (!doesParticipate) {
        return shared_ptr<Array>(new MemArray(_schema));
    }

    //
    // invokeMPIGemm()
    //
    shared_ptr<Array> result;
    slpp::int_t INFO = DEFAULT_BAD_INFO ;
    invokeMPIGemm(&redistInputs, TRANSA, TRANSB, ALPHA, BETA, query, _schema, &result, &INFO);
    if (INFO != 0) {
        std::stringstream ss; ss << "Gemm error: pdgemm_ INFO is: " << INFO << std::endl ;
        if(DBG) std::cerr << ss.str() << std::endl ;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << ss.str());
    }

    // return the scidb array
    if(DBG) std::cerr << "GEMMPhysical::execute success" << std::endl ;
    LOG4CXX_DEBUG(logger, "GEMMPhysical::execute success");

    if(DBG) std::cerr << "GEMMPhysical::execute end ---------------------------------------" << std::endl;
    return result;
}

REGISTER_PHYSICAL_OPERATOR_FACTORY(GEMMPhysical, "gemm", "GEMMPhysical");

} // namespace
