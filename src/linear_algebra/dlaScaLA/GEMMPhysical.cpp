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
#include <cmath>
#include <sstream>
#include <string>

// std C
#include <time.h>

// de-facto standards
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_array.hpp>

// SciDB
#include <array/MemArray.h>
#include <array/OpArray.h>
#include <log4cxx/logger.h>
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

// MPI/ScaLAPACK
#include <scalapackUtil/reformat.hpp>
#include <scalapackUtil/scalapackFromCpp.hpp>
#include <scalapackUtil/ScaLAPACKPhysical.hpp>
#include <dlaScaLA/scalapackEmulation/scalapackEmulation.hpp>
#include <dlaScaLA/slaving/pdgemmMaster.hpp>
#include <dlaScaLA/slaving/pdgemmSlave.hpp>

// locals
#include "GEMMOptions.hpp"
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

/**
 *  A Physical multiply operator implemented using ScaLAPACK
 *  The interesting work is done in invokeMPI(), above
 *
 */
class GEMMPhysical : public ScaLAPACKPhysical
{
public:

    GEMMPhysical(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema)
    :
        ScaLAPACKPhysical(logicalName, physicalName, parameters, schema)
    {
    }
    shared_ptr<Array> invokeMPI(std::vector< shared_ptr<Array> >& redistributedInputs,
                                const GEMMOptions options, shared_ptr<Query>& query,
                                ArrayDesc& outSchema);

    virtual shared_ptr<Array> execute(std::vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query);
private:
};


char getTransposeCode(bool transpose) {
    return transpose ? 'T' : 'N' ;
}

shared_ptr<Array> GEMMPhysical::invokeMPI(std::vector< shared_ptr<Array> >& redistributedInputs,
                                          const GEMMOptions options, shared_ptr<Query>& query,
                                          ArrayDesc& outSchema)
{
    //
    // Everything about the execute() method concerning the MPI execution of the arrays
    // is factored into this method.  This does not include the re-distribution of data
    // chunks into the ScaLAPACK distribution scheme, as the supplied inputArrays
    // must already be in that scheme.
    //
    // + intersects the array chunkGrids with the maximum process grid
    // + sets up the ScaLAPACK grid accordingly and if not participating, return early
    // + start and connect to an MPI slave process
    // + create ScaLAPACK descriptors for the input arrays
    // + convert the inputArrays into in-memory ScaLAPACK layout in shared memory
    // + call a "master" routine that passes the ScaLAPACK operator name, parameters,
    //   and shared memory descriptors to the ScaLAPACK MPI process that will do the
    //   actual computation.
    // + wait for successful completion
    // + construct an "OpArray" that make and Array API view of the output memory.
    // + return that output array.
    //
    enum dummy  {R=0, C=1};              // row column
    enum dummy2 {AA=0, BB, CC, NUM_MATRICES};  // which matrix: alpha AA * BB + beta CC -> result

    LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI(): begin");

    size_t numArray = redistributedInputs.size();
    if (numArray != NUM_MATRICES) {  // for now ... may make CC optional when beta is 0, later
        LOG4CXX_ERROR(logger, "GEMMPhysical::invokeMPI(): " << numArray << " != NUM_MATRICES " << size_t(NUM_MATRICES));
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                   << "GEMMPhysical::invokeMPI(): requires 3 input Arrays/matrices.");
    }

    //
    // Initialize the (emulated) BLACS and get the proces grid info
    //
    bool isParticipatingInScaLAPACK = doBlacsInit(redistributedInputs, query, "GEMMPhysical");
    slpp::int_t ICTXT=-1, NPROW=-1, NPCOL=-1, MYPROW=-1 , MYPCOL=-1 ;
    if (isParticipatingInScaLAPACK) {
        blacs_gridinfo_(ICTXT, NPROW, NPCOL, MYPROW, MYPCOL);
        checkBlacsInfo(query, ICTXT, NPROW, NPCOL, MYPROW, MYPCOL, "GEMMPhysical");
    }

    //
    // launch MPISlave if we participate
    // TODO: move this down into the ScaLAPACK code ... something that does
    //       the doBlacsInit, launchMPISlaves, and the check that they agree
    //
    bool isParticipatingInMPI = launchMPISlaves(query, NPROW*NPCOL);
    if (isParticipatingInScaLAPACK != isParticipatingInMPI) {
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI():"
                              << " isParticipatingInScaLAPACK " << isParticipatingInScaLAPACK
                              << " isParticipatingInMPI " << isParticipatingInMPI);
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                   << "GEMMPhysical::invokeMPI(): internal inconsistency in MPI slave launch.");
    }

    if (isParticipatingInMPI) {
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI(): participating in MPI");
    } else {
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI(): not participating in MPI");
        return shared_ptr<Array>(new MemArray(_schema,query));
    }

    // REFACTOR: this is a pattern in DLAs
    //
    // get dimension information about the input arrays
    //
    boost::shared_ptr<Array> inArray[NUM_MATRICES];
    for(size_t i=0; i < numArray; i++ ) {
        inArray[i] = redistributedInputs[i] ;
    }

    // TODO JHM : checkArray/Inputs must make sure we are only using 2D arrays
    //            and/or convert 1d arrays to nrows x 1

    // matrix sizes from arrays A,B,C
    matSize_t size[NUM_MATRICES];
    matSize_t blockSize[NUM_MATRICES];
    for(size_t i=0; i < numArray; i++ ) {
        size[i] = getMatSize(inArray[i]);
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI():"
                               << " size["<<i<<"] " << size[i][R] << "," << size[i][C]);

        blockSize[i] = getMatChunkSize(inArray[i]);
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI():"
                              << " blockSize["<<i<<"] " << blockSize[i][R] << "," << blockSize[i][C]);
    }

    for(size_t i=0; i < numArray; i++ ) {
        checkInputArray(inArray[i]);  // check block size constraints, etc
    }
    //
    //.... Set up ScaLAPACK array descriptors ........................................
    //

    // these formulas for LLD (local leading dimension) and LTD (local trailing dimension)
    // are found in the headers of the ScaLAPACK functions such as pdgemm_()
    const slpp::int_t one = 1 ;

    // TODO: turn these pairs into matSize_t matrixLocalSize[NUM_MATRICES];
    slpp::int_t LLD[NUM_MATRICES]; // local leading dimension
    slpp::int_t LTD[NUM_MATRICES]; // local trailing dimension
    for(size_t i=0; i < numArray; i++ ) {
        slpp::int_t RSRC = 0 ;
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI():"
                              << " M["<<i<<"][R]"<<size[i][R] <<" MB["<<i<<"][R]:"<<blockSize[i][R]
                              << " N["<<i<<"][R]"<<size[i][C] <<" NB["<<i<<"][R]:"<<blockSize[i][C]
                              << " MYPROW:"<<MYPROW << " NPROW:"<< NPROW);
        LLD[i] = std::max(one, numroc_( size[i][R], blockSize[i][R], MYPROW, RSRC, NPROW ));
        LTD[i] = std::max(one, numroc_( size[i][C], blockSize[i][C], MYPCOL, RSRC, NPCOL ));
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI():"
                              << " LLD["<<i<<"] = " << LLD[i]
                              << " LTD["<<i<<"] = " << LTD[i]);
    }

    // create ScaLAPACK array descriptors
    // TODO: lets factor this to a method on ScaLAPACKPhysical
    slpp::desc_t DESC[NUM_MATRICES];
    for(size_t i=0; i < numArray; i++ ) {
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI():"
                              << " descinit_(DESC["<<i<<"], M=" << size[i][R] << ", N=" << size[i][C]
                              << ", MB=" << blockSize[i][R] << ", NB=" << blockSize[i][R]
                              << ", IRSRC=" << 0 << ", ICSRC=" << 0 << ", ICTXT=" << ICTXT
                              << ", LLD=" << LLD[i]);

        slpp::int_t descinitINFO = 0; // an output implemented as non-const ref (due to Fortran calling conventions)
        descinit_(DESC[i], size[i][R], size[i][C], blockSize[i][R], blockSize[i][C], 0, 0, ICTXT, LLD[i], descinitINFO);
        if (descinitINFO != 0) {
            LOG4CXX_ERROR(logger, "GEMMPhysical::invokeMPI(): descinit(DESC) failed, INFO " << descinitINFO
                                                                                    << " DESC " << DESC);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                       << "GEMMPhysical::invokeMPI(): descinit(DESC) failed");
        }

        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI():"
                              << " descinit_() returned DESC["<<i<<"] " << DESC[i]);

        // debugging for #1986 ... when #instances is prime, process grid is a row.  When small chunk sizes are used,
        // desc.LLD is being set to a number larger than the chunk size ... I don't understand or expect this.
        bool doDebugTicket1986=true;  // remains on until fixed, can't ship with this not understood.
        if(doDebugTicket1986) {
            if (DESC[i].LLD > DESC[i].MB) {
                LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI(): ticket 1986 issue"
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
                LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI():"
                                      << " @myPPos("<< MYPROW << "," << MYPCOL << ")"
                                      << " array["<<mat<<"]["<<ii<<"] = " << asDoubles[mat][ii]);
            }
        }
    }
    size_t resultShmIpcIndx = BUF_MAT_CC;           // by default, GEMM assumes it will return something for C
                                                    // but this will change if find we don't particpate in the output
    shmSharedPtr_t Cx(shmIpc[resultShmIpcIndx]);

    //
    //.... Call pdgemm to compute the product of A and B .............................
    //
    LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI(): calling pdgemm_ M,N,K:" << size[AA][R]  << ","
                                                                  << size[BB][R] << ","
                                                                  << size[CC][C]
                           << " MB,NB:" << blockSize[AA][R] << "," << blockSize[AA][C]);

    if(DBG) std::cerr << "GEMMPhysical::invokeMPI(): calling pdgemm to compute" << std:: endl;
    boost::shared_ptr<MpiSlaveProxy> slave = _ctx->getSlave(_launchId);

    // note on the value of K, from docs on PDGEMM:
    // If transa = 'T' or 'C'(true), it is the number of rows in submatrix A."
    // If transa = 'N'(false), it is the number of columns in submatrix A."
    slpp::int_t K = nCol(inArray[AA], options.transposeA);
    slpp::int_t MYPE = query->getInstanceID() ;  // we map 1-to-1 between instanceID and MPI rank
    slpp::int_t INFO = DEFAULT_BAD_INFO ;
    pdgemmMaster(query.get(), _ctx, slave, _ipcName, shmIpc[BUF_ARGS]->get(),
                  NPROW, NPCOL, MYPROW, MYPCOL, MYPE,
                  getTransposeCode(options.transposeA), getTransposeCode(options.transposeB),
                  size[CC][R], size[CC][C], K,
                  &options.alpha,
                  asDoubles[AA], one, one, DESC[AA],
                  asDoubles[BB], one, one, DESC[BB],
                  &options.beta,
                  asDoubles[CC], one, one, DESC[CC],
                  INFO);
    raiseIfBadResultInfo(INFO, "pdgemm");

    boost::shared_array<char> resPtrDummy(reinterpret_cast<char*>(NULL));
    typedef scidb::ReformatFromScalapack<shmSharedPtr_t> reformatOp_t ;

    if(logger->isTraceEnabled()) {
        LOG4CXX_TRACE(logger, "GEMMPhysical::invokeMPI():--------------------------------------");
        LOG4CXX_TRACE(logger, "GEMMPhysical::invokeMPI(): sequential values from 'C' memory");
        for(int ii=0; ii < matrixLocalSize[CC]; ii++) {
            LOG4CXX_TRACE(logger, "GEMMPhysical::invokeMPI(): ("<< MYPROW << "," << MYPCOL << ") C["<<ii<<"] = " << asDoubles[CC][ii]);
        }
        LOG4CXX_TRACE(logger, "GEMMPhysical::invokeMPI(): --------------------------------------");
        LOG4CXX_TRACE(logger, "GEMMPhysical::invokeMPI(): using pdelgetOp to reformat Gemm left from memory to scidb array , start");
    }


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

    shared_ptr<Array> result;
    // the process grid may be larger than the size of output in chunks... e.g multiplying A(1x100) * B(100x1) -> C(1x1)
    bool isParticipatingInOutput = first[R] <= last[R] && first[C] <= last[C] ;
    if (isParticipatingInOutput) {
        // there is in fact some output in our shared memory... hook it up to an OpArray
        Coordinates iterDelta(2);
        iterDelta[0] = NPROW * blockSize[CC][R];
        iterDelta[1] = NPCOL * blockSize[CC][C];

        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI(): participated, creating output array: first ("<<first[R]<<","<<first[C]<<"), last(" << last[R] <<"," <<last[C]<<")");
        reformatOp_t      pdelgetOp(Cx, DESC[CC], dims[R].getStart(), dims[C].getStart());
        result = shared_ptr<Array>(new OpArray<reformatOp_t>(outSchema, resPtrDummy, pdelgetOp,
                                                             first, last, iterDelta, query));
        assert(resultShmIpcIndx == BUF_MAT_CC);
    } else {
        LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI(): participated, but not in output array, creating empty output array: first ("<<first[R]<<","<<first[C]<<"), last(" << last[R] <<"," <<last[C]<<")");
        result = shared_ptr<Array>(new MemArray(_schema,query));  // same as when we don't participate at all
        resultShmIpcIndx = shmIpc.size();                   // indicate we don't want to hold on to buffer BUF_MAT_CC after all
    }

    // TODO: common pattern in ScaLAPACK operators: factor to base class
    releaseMPISharedMemoryInputs(shmIpc, resultShmIpcIndx);
    unlaunchMPISlaves();
    resetMPI();

    LOG4CXX_DEBUG(logger, "GEMMPhysical::invokeMPI() end");
    return result;
}


shared_ptr<Array> GEMMPhysical::execute(std::vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query)
{
    //
    // + converts inputArrays to psScaLAPACK distribution
    // + calls invokeMPI()
    // + returns the output OpArray.
    //
    LOG4CXX_DEBUG(logger, "GEMMPhysical::execute(): begin.");

    // TODO: make a GEMMLogical checkArgs(inputArrays, query); which asserts two or three arrays

    // get string of parameters from the optional 4th argument:
    // (TRANSA, TRANSB, ALPHA, BETA)
    std::string namedOptionStr;
    if (_parameters.size() >= 1) {
        assert(_parameters[0]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
        typedef boost::shared_ptr<OperatorParamPhysicalExpression> ParamType_t ;
        ParamType_t& paramExpr = reinterpret_cast<ParamType_t&>(_parameters[0]);
        assert(paramExpr->isConstant());
        namedOptionStr = paramExpr->getExpression()->evaluate().getString();
    }

    GEMMOptions options(namedOptionStr);

    // redistribute input arrays to ScaLAPACK block-cyclic
    std::vector<shared_ptr<Array> > redistributedInputs = redistributeInputArrays(inputArrays, query, "GEMMPhysical");

    //
    // invokeMPI()
    //
    shared_ptr<Array> result;
    result = invokeMPI(redistributedInputs, options, query, _schema);

    // return the scidb array
    LOG4CXX_DEBUG(logger, "GEMMPhysical::execute(): (successful) end");
    return result;
}

REGISTER_PHYSICAL_OPERATOR_FACTORY(GEMMPhysical, "gemm", "GEMMPhysical");

} // namespace
