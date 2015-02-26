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
};


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

    LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPIGemm(): begin");

    size_t numArray = inputArrays->size();
    if (numArray != NUM_MATRICES) {  // for now ... may make CC optional when beta is 0, later
        LOG4CXX_ERROR(logger, "GEMMPhysical::invokeMPIGemm(): " << numArray << " != NUM_MATRICES " << size_t(NUM_MATRICES));
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                   << "GEMMPhysical::invokeMPIGemm(): requires 3 input Arrays/matrices.");
    }

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
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPIGemm():"
                               << " size["<<i<<"] " << size[i][R] << "," << size[i][C]);

        blockSize[i] = getMatChunkSize(inArray[i]);
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPIGemm():"
                              << " blockSize["<<i<<"] " << blockSize[i][R] << "," << blockSize[i][C]);
    }

    for(size_t i=0; i < numArray; i++ ) {
        checkInputArray(inArray[i]);  // check block size constraints, etc
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
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPIGemm():"
                              << " M["<<i<<"][R]"<<size[i][R] <<" MB["<<i<<"][R]:"<<blockSize[i][R]
                              << " N["<<i<<"][R]"<<size[i][C] <<" NB["<<i<<"][R]:"<<blockSize[i][C]
                              << " MYPROW:"<<MYPROW << " NPROW:"<< NPROW);
        LLD[i] = std::max(one, numroc_( size[i][R], blockSize[i][R], MYPROW, RSRC, NPROW ));
        LTD[i] = std::max(one, numroc_( size[i][C], blockSize[i][C], MYPCOL, RSRC, NPCOL ));
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPIGemm():"
                              << " LLD["<<i<<"] = " << LLD[i]
                              << " LTD["<<i<<"] = " << LTD[i]);
    }

    // create ScaLAPACK array descriptors
    slpp::desc_t DESC[NUM_MATRICES];
    for(size_t i=0; i < numArray; i++ ) {
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPIGemm():"
                              << " descinit_(DESC["<<i<<"], M=" << size[i][R] << ", N=" << size[i][C]
                              << ", MB=" << blockSize[i][R] << ", NB=" << blockSize[i][R]
                              << ", IRSRC=" << 0 << ", ICSRC=" << 0 << ", ICTXT=" << ICTXT
                              << ", LLD=" << LLD[i] << ", INFO");

        slpp::int_t descinitINFO = 0; // an output implemented as non-const ref (due to Fortran calling conventions)
        descinit_(DESC[i], size[i][R], size[i][C], blockSize[i][R], blockSize[i][C], 0, 0, ICTXT, LLD[i], descinitINFO);
        if (descinitINFO != 0) {
            LOG4CXX_ERROR(logger, "GEMMPhysical::invokeMPIGemm(): descinit(DESC) failed, INFO " << descinitINFO
                                                                                    << " DESC " << DESC);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                       << "GEMMPhysical::invokeMPIGemm(): descinit(DESC) failed");
        }

        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPIGemm():"
                              << " descinit_() returned DESC["<<i<<"] " << DESC[i]);

        // debugging for #1986 ... when #instances is prime, process grid is a row.  When small chunk sizes are used,
        // desc.LLD is being set to a number larger than the chunk size ... I don't understand or expect this.
        bool doDebugTicket1986=true;  // remains on until fixed, can't ship with this not understood.
        if(doDebugTicket1986) {
            if (DESC[i].LLD > DESC[i].MB) {
                LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPIGemm(): ticket 1986 issue"
                                      <<  ", DESC["<<i<<"].LLD " << DESC[i].LLD
                                      << " > DESC["<<i<<"].MB: " << DESC[i].MB);
            }
        }
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
    assert(numArray < NUM_BUFS);

    size_t bufElemBytes[NUM_BUFS];
    size_t bufNumElem[NUM_BUFS];
    std::string bufDbgNames[NUM_BUFS];

    bufElemBytes[BUF_ARGS]= 1 ;           bufNumElem[BUF_ARGS]= sizeof(scidb::PdgemmArgs) ; bufDbgNames[BUF_ARGS] = "PdgemmArgs";
    bufElemBytes[BUF_MAT_AA]= sizeof(double) ; bufNumElem[BUF_MAT_AA]= matrixLocalSize[AA]; bufDbgNames[BUF_MAT_AA] = "A" ;
    bufElemBytes[BUF_MAT_BB]= sizeof(double) ; bufNumElem[BUF_MAT_BB]= matrixLocalSize[BB]; bufDbgNames[BUF_MAT_BB] = "B" ;
    bufElemBytes[BUF_MAT_CC]= sizeof(double) ; bufNumElem[BUF_MAT_CC]= matrixLocalSize[CC]; bufDbgNames[BUF_MAT_CC] = "C" ;
    typedef scidb::SharedMemoryPtr<double> shmSharedPtr_t ;

    std::vector<MPIPhysical::SMIptr_t> shmIpc = allocateMPISharedMemory(NUM_BUFS, bufElemBytes, bufNumElem, bufDbgNames);

    //
    // Zero inputs, to emulate a sparse matrix implementation (but slower)
    // and then extract the non-missing info onto that.
    //
    double* asDoubles[NUM_MATRICES];
    for(size_t mat=0; mat < numArray; mat++ ) {
        asDoubles[mat] = reinterpret_cast<double*>(shmIpc[mat+1]->get());
        size_t buf= mat+1; // TODO: this would be less error prone if I were to put the arg buf after the data bufs
        assert(buf < NUM_BUFS);
        setInputMatrixToAlgebraDefault(asDoubles[mat], bufNumElem[buf]);  // note mat C is also an output
        extractArrayToScaLAPACK(inArray[mat], asDoubles[mat], DESC[mat]);

        if(DBG) { // that the reformat worked correctly
            for(int ii=0; ii < matrixLocalSize[mat]; ii++) {
                LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPIGemm():"
                                      << " @myPPos("<< MYPROW << "," << MYPCOL << ")"
                                      << " array["<<mat<<"]["<<ii<<"] = " << asDoubles[mat][ii]);
            }
        }
    }
    size_t resultShmIpcIndx = BUF_MAT_CC;
    shmSharedPtr_t Cx(shmIpc[resultShmIpcIndx]);

    //!
    //!.... Call pdgemm to compute the product of A and B .............................
    //!
    LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPIGemm(): calling pdgemm_ M,N,K:" << size[AA][R]  << ","
                                                                  << size[BB][R] << ","
                                                                  << size[CC][C]
                           << " MB,NB:" << blockSize[AA][R] << "," << blockSize[AA][C]);

    if(DBG) std::cerr << "GEMMPhysical::invokeMPIGemm(): calling pdgemm to compute" << std:: endl;
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

    LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPIGemm(): pdgemmMaster finished");
    if(DBG) std::cerr << "GEMMPhysical::invokeMPIGemm(): pdgemmMaster finished with INFO " << *INFO << std::endl;


    boost::shared_array<char> resPtrDummy(reinterpret_cast<char*>(NULL));


    typedef scidb::ReformatFromScalapack<shmSharedPtr_t> reformatOp_t ;

    if(DBG) std::cerr << "GEMMPhysical::invokeMPIGemm():--------------------------------------" << std::endl;
    if(DBG) std::cerr << "GEMMPhysical::invokeMPIGemm(): sequential values from 'C' memory" << std::endl;
    if(DBG) {
        for(int ii=0; ii < matrixLocalSize[CC]; ii++) {
            std::cerr << "GEMMPhysical::invokeMPIGemm(): ("<< MYPROW << "," << MYPCOL << ") C["<<ii<<"] = " << asDoubles[CC][ii] << std::endl;
        }
    }

    if(DBG) std::cerr << "GEMMPhysical::invokeMPIGemm(): --------------------------------------" << std::endl;
    if(DBG) std::cerr << "GEMMPhysical::invokeMPIGemm(): using pdelgetOp to reformat Gemm left from memory to scidb array , start" << std::endl ;

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

    // the process grid may be larger than the size of output in chunks... e.g multiplying A(1x100) * B(100x1) -> C(1x1)
    bool doesOutput = first[R] <= last[R] && first[C] <= last[C] ;
    if (doesOutput) {
        // there is in fact some output in our shared memory... hook it up to an OpArray
        Coordinates iterDelta(2);
        iterDelta[0] = NPROW * blockSize[CC][R];
        iterDelta[1] = NPCOL * blockSize[CC][C];

        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPIGemm(): participated, creating output array: first ("<<first[R]<<","<<first[C]<<"), last(" << last[R] <<"," <<last[C]<<")");
        reformatOp_t      pdelgetOp(Cx, DESC[CC], dims[R].getStart(), dims[C].getStart());
        *result = shared_ptr<Array>(new OpArray<reformatOp_t>(outSchema, resPtrDummy, pdelgetOp,
                                                              first, last, iterDelta, query));
        releaseMPISharedMemoryInputs(shmIpc, resultShmIpcIndx);
    } else {
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPIGemm(): participated, but not in output array, creating empty output array: first ("<<first[R]<<","<<first[C]<<"), last(" << last[R] <<"," <<last[C]<<")");
        *result = shared_ptr<Array>(new MemArray(_schema));  // same as when we don't participate at all
        releaseMPISharedMemoryInputs(shmIpc, size_t(-1));    // we don't have a result, so set the one to not release to one it doesn't have
    }

    unlaunchMPISlaves();
    resetMPI();

    LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPIGemm() end with INFO " << *INFO );
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
    LOG4CXX_DEBUG(logger, "GEMMPhysical::execute(): begin.");

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
    if (doesParticipate) {
        LOG4CXX_DEBUG(logger, "GEMMPhysical::execute(): participating");
    } else {
        LOG4CXX_DEBUG(logger, "GEMMPhysical::execute(): not participating at all, immediate return of empty array");
        return shared_ptr<Array>(new MemArray(_schema));
    }

    //
    // invokeMPIGemm()
    //
    shared_ptr<Array> result;
    slpp::int_t INFO = DEFAULT_BAD_INFO ;
    invokeMPIGemm(&redistInputs, TRANSA, TRANSB, ALPHA, BETA, query, _schema, &result, &INFO);
    if (INFO != 0) {
        LOG4CXX_ERROR(logger, "GEMMPhysical::execute(): invokeMPIGemm() failed, INFO " << INFO);
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "invokeMPIGemm() failed");
    }

    // return the scidb array
    LOG4CXX_DEBUG(logger, "GEMMPhysical::execute(): (successful) end");
    return result;
}

REGISTER_PHYSICAL_OPERATOR_FACTORY(GEMMPhysical, "gemm", "GEMMPhysical");

} // namespace
