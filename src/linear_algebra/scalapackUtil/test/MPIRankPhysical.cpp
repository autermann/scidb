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
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_array.hpp>

// SciDB
#include <array/MemArray.h>
#include <array/Metadata.h>
#include <log4cxx/logger.h>
#include <query/Network.h>
#include <query/Operator.h>
#include <system/BlockCyclic.h>

//SciDB
#include <log4cxx/logger.h>
#include <query/Query.h>
#include <system/Exceptions.h>

// more SciDB
#include <array/ArrayExtractOp.hpp>
#include <array/OpArray.h>
#include <scalapackUtil/reformat.hpp>
#include <scalapackUtil/ScaLAPACKPhysical.hpp>
#include <scalapackUtil/scalapackFromCpp.hpp>
#include <dlaScaLA/scalapackEmulation/scalapackEmulation.hpp>
#include <scalapackUtil/test/slaving/mpiRankMaster.hpp>
#include <scalapackUtil/test/slaving/mpiRankSlave.hpp>

// more SciDB
#include <mpi/MPIUtils.h>
#include <mpi/MPISlaveProxy.h>
#include <mpi/MPILauncher.h>
#include <mpi/MPIManager.h>
#include "DLAErrors.h"
#include <util/shm/SharedMemoryIpc.h>


using namespace scidb;
using namespace boost;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.svd"));

// REFACTOR:
inline int64_t nrow(boost::shared_ptr<Array>& a) { return a->getArrayDesc().getDimensions()[0].getLength(); }
// REFACTOR:
inline int64_t ncol(boost::shared_ptr<Array>& a) { return a->getArrayDesc().getDimensions()[1].getLength(); }

// REFACTOR:
inline int64_t brow(boost::shared_ptr<Array>& a) { return a->getArrayDesc().getDimensions()[0].getChunkInterval(); }
// REFACTOR:
inline int64_t bcol(boost::shared_ptr<Array>& a) { return a->getArrayDesc().getDimensions()[1].getChunkInterval(); }

// REFACTOR:
inline Coordinates getStart(boost::shared_ptr<Array>& a) {
    Coordinates result(2);
    result[0] = a->getArrayDesc().getDimensions()[0].getStart();
    result[1] = a->getArrayDesc().getDimensions()[1].getStart();
    std::cerr << "getStart(array) returns (" << result[0] << "," << result[1] << ")" << std::endl;
    return result;
}
// REFACTOR:
inline Coordinates getEndMax(boost::shared_ptr<Array>& a) {
    Coordinates result(2);
    result[0] = a->getArrayDesc().getDimensions()[0].getEndMax();
    result[1] = a->getArrayDesc().getDimensions()[1].getEndMax();
    std::cerr << "getEndMax(array) returns (" << result[0] << "," << result[1] << ")" << std::endl;
    return result;
}

// REFACTOR:
static void launchMPIJob(boost::shared_ptr<MpiLauncher>& launcher,
                  const boost::shared_ptr<const InstanceMembership>& membership,
                  const shared_ptr<Query>& query,
                  const int maxSlaves)
{
    std::vector<std::string> args;
    launcher->launch(args, membership, maxSlaves);

    if (logger->isDebugEnabled()) {
        vector<pid_t> pids;
        launcher->getPids(pids);
        for (vector<pid_t>::const_iterator i=pids.begin(); i != pids.end(); ++i) {
            LOG4CXX_DEBUG(logger,"DLA launched PID= "<<(*i));
        }
    }
}

void invokeMPIRank(std::vector< shared_ptr<Array> >* inputArrays,
                  shared_ptr<Query>& query,
                  shared_ptr<MpiOperatorContext>& ctx,
                  ArrayDesc& outSchema,
                  shared_ptr<Array>* result,
                  slpp::int_t* INFO)
{
    const bool DBG = false;

    std::cerr << "invokeMPIRank reached" << std::endl ;

    size_t nInstances = query->getInstancesCount();
    slpp::int_t instanceID = query->getInstanceID();

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
    if(DBG) {
        std::cerr << "(invoke) blacs_gridinfo_(ctx:" << ICTXT << ")" << std::endl;
        std::cerr << "-> NPROW: " << NPROW  << ", NPCOL: " << NPCOL << std::endl;
        std::cerr << "-> MYPROW:" << MYPROW << ", MYPCOL:" << MYPCOL << std::endl;
    }

    // REFACTOR these checks
    if(MYPROW < 0 || MYPCOL < 0) {
        std::cerr << "MPIRank operator error: MYPROW:"<< MYPROW << " MYPCOL:"<<MYPCOL << std::endl ;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << "MPIRank operator error: MYPROW:"<< MYPROW << " MYPCOL:"<<MYPCOL);
    }

    if(MYPROW >= NPROW) {
        std::cerr << "MPIRank operator error: MYPROW:"<< MYPROW << " NPROW:"<<NPROW << std::endl ;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << "MPIRank operator error: MYPROW:"<< MYPROW << " NPROW:"<<NPROW);
    }

    if(MYPCOL >= NPCOL) {
        std::cerr << "MPIRank operator error: MYPCOL:"<< MYPCOL << " NPCOL:"<<NPCOL << std::endl ;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << "MPIRank operator error: MYPCOL:"<< MYPCOL << " NPCOL:"<<NPCOL);
    }

    // check that mpi_commsize(NPE, MYPE) values
    // which are managed in the slave as:
    //     NPE = MpiManager::getInstance()->getWorldSize();
    //     MYPE = MpiManager::getInstance()->getRank();
    // and here can be derived from the blacs_getinfo
    // 
    // lets check them against the instanceCount and instanceID to make sure
    // everything is consistent

    // NPE <= instanceCount
    size_t NPE = NPROW*NPCOL; // from blacs
    if(NPE > nInstances) {
        std::stringstream msg; msg << "MPIRank operator error: NPE:"<<NPE<< " nInstances:"<< nInstances;
        std::cerr << msg.str() << std::endl;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << msg.str()) ;
    }

    // MYPE == instanceID
    slpp::int_t MYPE = MYPROW*NPCOL + MYPCOL ; // row-major
    if(MYPE != instanceID) {
        std::stringstream msg; msg << "MPIRank operator error: MYPE:"<<MYPE<< " instanceID:"<< instanceID;
        std::cerr << msg.str() << std::endl;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << msg.str()) ;
    }

    if(DBG) std::cerr << "NPE/nInstances: " << NPE << std::endl;
    if(DBG) std::cerr << "MYPE/instanceID: " << MYPE << std::endl;

    // REFACTOR  -- not only in DLA operators but also perhaps PhysicalMpiTest
    //
    // taken from PhysicalMpiTest operator code, pre-loop
    //
    const boost::shared_ptr<const InstanceMembership> membership =
        Cluster::getInstance()->getInstanceMembership();

    if ((membership->getViewId() != query->getCoordinatorLiveness()->getViewId()) ||
        (membership->getInstances().size() != query->getInstancesCount())) {
        // because we can't yet handle the extra data from
        // replicas that we would be fed in "degraded mode"

        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM2);
    }

    // REFACTOR
    //
    // taken from PhysicalMpiTest operator code, in-loop
    //
    if(DBG) std::cerr << "invokeMPIRank slave creation" << std::endl ;

    const string& installPath = MpiManager::getInstallPath(membership);
    uint64_t launchId =ctx->getNextLaunchId();

    boost::shared_ptr<MpiSlaveProxy> slave(new MpiSlaveProxy(launchId, query, installPath));
    ctx->setSlave(launchId, slave);

    bool mustLaunch = (query->getCoordinatorID() == COORDINATOR_INSTANCE);
    boost::shared_ptr<MpiLauncher> launcher;
    if (mustLaunch) {
        launcher = boost::shared_ptr<MpiLauncher>(new MpiLauncher(launchId, query));
        ctx->setLauncher(launchId, launcher);
        launchMPIJob(launcher, membership, query, NPROW*NPCOL);
    }

    //-------------------- Get the handshake
    if(DBG) std::cerr << "invokeMPIRank slave waitForHandshake 1" << std::endl ;
    if(DBG) std::cerr << "-------------------------------------" << std::endl ;
    slave->waitForHandshake(ctx);
    if(DBG) std::cerr << "invokeMPIRank slave waitForHandshake 1 done" << std::endl ;

    // After the handshake the old slave must be gone
    boost::shared_ptr<MpiSlaveProxy> oldSlave = ctx->getSlave(launchId-1);
    if (oldSlave) {
        if(DBG) std::cerr << "invokeMPIRank oldSlave->destroy()" << std::endl ;
        oldSlave->destroy();
        oldSlave.reset();
    }
    ctx->complete(launchId-1);

    // REFACTOR: this is a pattern in DLAs
    //
    // get dimension information about the input arrays
    //
    if(DBG) std::cerr << "invokeMPIRank get dim info" << std::endl ;
    boost::shared_ptr<Array> Ain = (*inputArrays)[0];

    std::ostringstream tmp;
    Ain->getArrayDesc().getDimensions()[0].toString(tmp) ;
    if(DBG) std::cerr << tmp.str() << std::endl;

    std::ostringstream tmp2;
    Ain->getArrayDesc().getDimensions()[1].toString(tmp2) ;
    std::cerr << tmp2.str() << std::endl;

    // find M,N from input array
    slpp::int_t M = nrow(Ain);
    slpp::int_t N = ncol(Ain);
    if(DBG) std::cerr << "M " << M << " N " << N << std::endl;

    // find MB,NB from input array, which is the chunk size
    // note that the best chunksizes for ScaLAPACK are 32x32 and 64x64
    // for Intel {Sandy,Ivy}Bridge processors. (Core 2 2xxx, 3xxx cpus)
    // small matrices will often have equally small chunk size, so shold
    // handle those.
    const slpp::int_t SL_BLOCK_SIZE=64; // scalapack block size
    // TODO JHM: need to test and fix what happens if the chunk
    //           size is not a legitimate one at this point
    if (brow(Ain) > SL_BLOCK_SIZE ||
        bcol(Ain) > SL_BLOCK_SIZE) {
        std::cerr << "MPIRank operator error: chunksize " << brow(Ain) << " x "<< bcol(Ain) << " is too large" << std::endl ;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << "chunksize " << brow(Ain) << " x "<< bcol(Ain) << " is too large");
    }
    const slpp::int_t MB= brow(Ain);
    const slpp::int_t NB= bcol(Ain);

    if (MB != NB) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << "chunksizes are MB=" << MB << " and NB=" << NB << "must be the same."
               << " 64 or 128 is suggested.");
    }

    //!
    //!.... Set up ScaLAPACK array descriptors ........................................
    //!

    // these formulas for LD (leading dimension) and TD (trailing dimension)
    // are found in the headers of the scalapack functions such as pdgesvd_()
    const slpp::int_t one = 1 ;
    slpp::int_t LD_IN = std::max(one, numroc_( M, MB, MYPROW, /*RSRC_IN*/0, NPROW ));
    if(DBG) std::cerr << "M:"<<M <<" MB:"<<MB << " MYPROW:"<<MYPROW << " NPROW:"<<NPROW<< std::endl;
    if(DBG) std::cerr << "--> LD_IN = " << LD_IN << std::endl;

    slpp::int_t TD_IN = std::max(one, numroc_( N, NB, MYPCOL, /*CSRC_IN*/0, NPCOL ));
    if(DBG) std::cerr << "N:"<<N <<" NB:"<<NB  << " MYPCOL:"<<MYPCOL << " NPCOL:"<<NPCOL<< std::endl;
    if(DBG) std::cerr << "-->TD_IN = " << TD_IN << std::endl;

    // create ScaLAPACK array descriptors
    std::cerr << "descinit_ DESC_IN" << std::endl;
    slpp::desc_t DESC_IN;
    descinit_(DESC_IN, M, N, MB, NB, 0, 0, ICTXT, LD_IN, *INFO);

    std::cerr << "descinit_ DESC_OUT" << std::endl;
    slpp::desc_t DESC_OUT;
    descinit_(DESC_OUT, M, N, MB, NB, 0, 0, ICTXT, LD_IN, *INFO);

    slpp::int_t MP = LD_IN;
    slpp::int_t NQ = TD_IN;

    //REFACTOR
    if(DBG) {
        std::cerr << "##################################################" << std::endl;
        std::cerr << "####master#########################################" << std::endl;
        std::cerr << "one:" << one << std::endl;
        std::cerr << "MB:" << MB << std::endl;
        std::cerr << "MYPROW:" << MYPROW << std::endl;
        std::cerr << "NPROW:" << NPROW << std::endl;
    }

    std::cerr << "LOCAL SIZES:@@@@@@@@@@@@@@@@@@@" << std::endl ;
    std::cerr << "XX MP   = " << MP << std::endl;
    std::cerr << "XX NQ   = " << NQ << std::endl;

    // sizes
    const slpp::int_t SIZE_IN = MP * NQ ;
    const slpp::int_t SIZE_OUT = SIZE_IN ;

    const size_t NUM_BUFS=3 ;
    int sizes[NUM_BUFS];
    sizes[0] =          sizeof (scidb::MPIRankArgs) ;
    sizes[1] = SIZE_IN * sizeof(double) ;
    sizes[2] = SIZE_OUT* sizeof(double) ;

    if(DBG) {
        std::cerr << "SHM ALLOCATIONS:@@@@@@@@@@@@@@@@@@@" << std::endl ;
        std::cerr << "sizes[0] (args) = " << sizes[0] << std::endl;
        std::cerr << "sizes[1] (IN) = " << sizes[1] << std::endl;
        std::cerr << "sizes[2] (OUT) = "<< sizes[2] << std::endl;
    }


    //-------------------- Create IPC
    string clusterUuid = Cluster::getInstance()->getUuid();
    InstanceID instanceId = Cluster::getInstance()->getLocalInstanceId();
    std::string ipcName = mpi::getIpcName(installPath, clusterUuid, query->getQueryID(), instanceId, launchId);

    typedef boost::shared_ptr<SharedMemoryIpc> SMIptr_t ;
    std::vector<SMIptr_t> shmIpc(NUM_BUFS);
    for(size_t ii=0; ii<NUM_BUFS; ii++) {
        std::stringstream suffix;
        suffix << "." << ii ;
        LOG4CXX_TRACE(logger, "IPC name = " << (ipcName + suffix.str()));
        shmIpc[ii] = SMIptr_t(mpi::newSharedMemoryIpc(ipcName+suffix.str()));
        ctx->addSharedMemoryIpc(launchId, shmIpc[ii]);

        try {
            shmIpc[ii]->create(SharedMemoryIpc::RDWR);
            shmIpc[ii]->truncate(sizes[ii]);
        } catch(SharedMemoryIpc::SystemErrorException& e) {
            std::stringstream ss; ss << "shared_memory_mmap " << e.what();
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << ss.str());
        } catch(SharedMemoryIpc::InvalidStateException& e) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << e.what());
        }

        if(DBG) {
            std::cerr << "----------------------------------------------" << std::endl ;
            std::cerr << "WARNING: prefill with NaN enabled" << std::endl ;
            std::cerr << "----------------------------------------------" << std::endl ;
        }
        double* buf = reinterpret_cast<double*>(shmIpc[ii]->get());
        const double nanVal = ::nan(""); // any non-signalling NaN, don't care which
        for (size_t jj=0; jj < (sizes[ii]/sizeof(double)); jj++) {
            buf[jj] = nanVal;
        }
    }

    typedef scidb::SharedMemoryPtr<double> shmSharedPtr_t ;
    void *argsBuf = shmIpc[0]->get();
    double* IN = reinterpret_cast<double*>(shmIpc[1]->get());
    double* OUT= reinterpret_cast<double*>(shmIpc[2]->get()); shmSharedPtr_t OUTx(shmIpc[2]);

    // use extractDataToOp() and the reformatToScalapack() operator
    // to reformat the data according to ScaLAPACK requirements.
    Coordinates coordFirst = getStart(Ain);
    Coordinates coordLast = getEndMax(Ain);

    scidb::ReformatToScalapack pdelsetOp(IN, DESC_IN, coordFirst[0], coordFirst[1]);

    if(DBG) std::cerr << "extract data from SciDB Ain to ScaLAPACK double* IN" << std::endl ;
    LOG4CXX_DEBUG(logger, "extract data from SciDB Ain to ScaLAPACK double* IN");
    extractDataToOp(Ain, /*attrID*/0, coordFirst, coordLast, pdelsetOp);
    LOG4CXX_DEBUG(logger, "extraction done");
    if(DBG) std::cerr << "extraction done" << std::endl ;

    // debug that the reformat worked correctly:
    if(DBG) {
        for(int ii=0; ii < SIZE_IN; ii++) {
            std::cerr << "("<< MYPROW << "," << MYPCOL << ") IN["<<ii<<"] = " << IN[ii] << std::endl;
        }
    }

	// SPECIAL TO MPIRank, does not factor out
    for(int ii=0; ii < SIZE_IN; ii++) {
        IN[ii] = MYPE ; // change it to what we think the rank will be when it really gets to MPI
    }      

    //!
    //!.... Call the master wrapper
    //!
    if(DBG) std::cerr << "MPIRankPhysical: calling mpiRankMaster M,N:" << M  << "," << N
                                  << "MB,NB:" << MB << "," << NB << std::endl;
    LOG4CXX_DEBUG(logger, "MPIRankPhysical: calling mpiRankMaster M,N:" << M <<","<< "MB,NB:" << MB << "," << NB);

    if(DBG) std::cerr << "MPIRankPhysical calling PDGESVD to compute" << std:: endl;
    mpirankMaster(query.get(), ctx, slave, ipcName, argsBuf,
                  NPROW, NPCOL, MYPROW, MYPCOL, MYPE,
                  IN,  DESC_IN, OUT, DESC_OUT, *INFO);

    LOG4CXX_DEBUG(logger, "MPIRank: mpiRankMaster finished");
    std::cerr << "MPIRank: calling mpiRankMaster finished" << std::endl;
    std::cerr << "MPIRank: mpiRankMaster returned INFO:" << *INFO << std::endl;

    Dimensions const& dims = Ain->getArrayDesc().getDimensions();

    boost::shared_array<char> resPtrDummy(reinterpret_cast<char*>(NULL));

    typedef scidb::ReformatFromScalapack<shmSharedPtr_t> reformatOp_t ;

    // Only in MPIRank
    std::cerr << "--------------------------------------" << std::endl;
    std::cerr << "sequential values of 'OUT' ScaLAPACK memory" << std::endl;
    for(int ii=0; ii < SIZE_OUT; ii++) {
        std::cerr << "OUT["<<ii<<"] = " << OUT[ii] << std::endl;
    }
    std::cerr << "--------------------------------------" << std::endl;
    std::cerr << "using pdelgetOp to redist mpiRank OUT from memory to scidb array , start" << std::endl ;

    //
    // an OpArray is a SplitArray that is filled on-the-fly by calling the operator
    // so all we have to do is create one with an upper-left corner equal the the
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

    if(DBG) std::cerr << "MPIRank OUT SplitArray from ("<<first[0]<<","<<first[1]<<") to (" << last[0] <<"," <<last[1]<<") delta:"<<iterDelta[0]<<","<<iterDelta[1]<< std::endl;
    LOG4CXX_DEBUG(logger, "Creating array ("<<first[0]<<","<<first[1]<<"), (" << last[0] <<"," <<last[1]<<")");

    reformatOp_t    pdelgetOp(OUTx, DESC_OUT, dims[0].getStart(), dims[1].getStart());
    *result = shared_ptr<Array>(new OpArray<reformatOp_t>(outSchema, resPtrDummy, pdelgetOp,
                                                            first, last, iterDelta));

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
    if (mustLaunch) {
        launcher->destroy();
    }
    ctx.reset();

    if(DBG) std::cerr << "invoke: returning from invokeMPIRank with INFO:" << *INFO << std::endl ;
}

///
/// An operator that returuns the ScaLAPACK rank that is repsonsible for that
/// cell, computed by actually receiving data from the ScaLAPACK slave process,
/// in order that this mapping can be compared to the functions we use in SciDB to
/// compute the same thing without using ScaLAPACK.
///
class MPIRankPhysical : public ScaLAPACKPhysical
{
public:
    MPIRankPhysical(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema)
    :
    	ScaLAPACKPhysical(logicalName, physicalName, parameters, schema)
    {
    }

    virtual bool changesDistribution(const std::vector<ArrayDesc> & inputSchemas) const
    {
        return true;
    }

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                 const std::vector< ArrayDesc> & inputSchemas) const
    {
        return ArrayDistribution(psScaLAPACK);
    }
    // if outputting partial blocks that need to be merged, one needs to override
    // another operator called ???

    virtual shared_ptr<Array> execute(std::vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query);

};

///
/// SVDPhysical::execute()
/// + converts inputArrays to psScaLAPACK distribution
/// + intersects the array chunkGrid with the maximum process grid
/// + sets up the ScaLAPACK grid accordingly and if not participating, return early
/// + calls invokeMPIRank()
/// + returns the output OpArray.
///
shared_ptr<Array> MPIRankPhysical::execute(std::vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query)
{
    const bool DBG = false ;

    std::cerr << "MPIRankPhysical::execute() begin ---------------------------------------" << std::endl;

    //
    // repartition and redistribution from SciDB chunks and arbitrary distribution
    // to
    // ScaLAPACK tiles (efficient cache-aware size) and psScaLAPACK, which is
    //   true 2D block-cyclic.  Ideally, we will be able to repart() to from
    //   arbitrary chunkSize to one that is efficient for ScaLAPACK
    //
    size_t nInstances = query->getInstancesCount();
    slpp::int_t instanceID = query->getInstanceID();

    // redistribute input arrays to ScaLAPACK block-cyclic
    std::vector<shared_ptr<Array> > redistInputs = redistributeInputArrays(inputArrays, query);

    shared_ptr<Array> input = redistInputs[0];
    Dimensions const& dims = input->getArrayDesc().getDimensions();
    size_t nRows = dims[0].getLength();
    size_t nCols = dims[1].getLength();
    if (!nRows || !nCols ) {
        return shared_ptr<Array>(new MemArray(_schema));
    }

    //!
    //!.... Initialize the (imitation)BLACS used by the instances to calculate sizes
    //!     AS IF they are MPI processes (which they are not)
    //!
    const ProcGrid* procGrid = query->getProcGrid();

    procRowCol_t MN = { nRows, nCols};
    procRowCol_t MNB = { procNum_t(dims[0].getChunkInterval()),
                         procNum_t(dims[1].getChunkInterval()) };

    procRowCol_t blacsGridSize = procGrid->useableGridSize(MN, MNB);
    procRowCol_t myGridPos = procGrid->gridPos(instanceID, blacsGridSize);

    if(DBG) std::cerr << "*** myGridPos.row:" << myGridPos.row << " myGridPos.col:" << myGridPos.col << std::endl;
    if (myGridPos.row >= blacsGridSize.row || myGridPos.col >= blacsGridSize.col) {
        if(DBG) {
            std::cerr << "instID:" << instanceID << " myGridPos.row:" << myGridPos.row
                                                 << " myGridPos.col:" << myGridPos.col << std::endl;
            std::cerr << "NOT in grid: " << blacsGridSize.row << " x " << blacsGridSize.col << std::endl;
            std::cerr << "should not invoke a slave" << std::endl ;
        }
        //
        // We are an "extra" instance that must return an empty array
        // we will not start mpi slaves for such instances
        return shared_ptr<Array>(new MemArray(_schema));
    } else {
        if(DBG) {
            std::cerr << "instID:" << instanceID << " myGridPos.row:" << myGridPos.row
                                                 << " myGridPos.col:" << myGridPos.col << std::endl;
            std::cerr << "IN GRID: " << blacsGridSize.row << " x " << blacsGridSize.col << std::endl;
        }
    }

    slpp::int_t ICTXT=-1;
    slpp::int_t IC = query->getInstancesCount();
    slpp::int_t NP = blacsGridSize.row * blacsGridSize.col ;
    std::cerr << "(execute) NP:"<<NP << " IC:" <<IC << std::endl;
    std::cerr << "(execute) set_fake_blacs_gridinfo_(ctx:"<< ICTXT
                                   << ", nprow:"<<blacsGridSize.row
                                   << ", npcol:"<<blacsGridSize.col << "," << std::endl;
    std::cerr << "                         myRow:"<<myGridPos.row
                                     << ", myCol:" << myGridPos.col << ")" << std::endl;
    set_fake_blacs_gridinfo_(ICTXT, blacsGridSize.row, blacsGridSize.col, myGridPos.row, myGridPos.col);

    // check that it worked
    slpp::int_t NPROW=-1, NPCOL=-1, MYPROW=-1 , MYPCOL=-1 ;
    blacs_gridinfo_(ICTXT, NPROW, NPCOL, MYPROW, MYPCOL);
    if(DBG) {
        std::cerr << "blacs_gridinfo_(ctx:" << ICTXT << ")" << std::endl;
        std::cerr << "   -> gridsiz:(" << NPROW  << ", " << NPCOL << ")" << std::endl;
        std::cerr << "   -> gridPos:(" << MYPROW << ", " << MYPCOL << ")" << std::endl;
    }

    int minLen = std::min(nRows, nCols);
    std::cerr << "-------------------------------------" << std::endl ;
    if(DBG) {
        std::cerr << "MPIRankPhysical::execute(): nInstances=" << nInstances << std::endl ;
        std::cerr << "MPIRankPhysical::execute(): nCols=" << nCols << std::endl ;
        std::cerr << "MPIRankPhysical::execute(): nRows=" << nRows << std::endl ;
        // std::cerr << "MPIRankPhysical::execute(): blockSize=" << blockSize << std::endl ;
        // std::cerr << "MPIRankPhysical::execute(): usedInstances=" << usedInstances << std::endl ;
        std::cerr << "MPIRankPhysical::execute(): minLen=" << minLen << std::endl ;
        std::cerr << "MPIRankPhysical::execute(): dims[0].getChunkInterval()=" << dims[0].getChunkInterval() << std::endl ;
        std::cerr << "MPIRankPhysical::execute(): dims[1].getChunkInterval()=" << dims[1].getChunkInterval() << std::endl ;
        std::cerr << "-------------------------------------" << std::endl ;

        std::cerr << "MPIRank: preparing to extractData, nRows=" << nRows << ", nCols = " << nCols << std::endl ;
    }
    LOG4CXX_DEBUG(logger, "MPIRank: preparing to extractData, nRows=" << nRows << ", nCols = " << nCols);

    Coordinates first(2);
    first[0] = dims[0].getStart() + MYPROW * dims[0].getChunkInterval();
    first[1] = dims[1].getStart() + MYPCOL * dims[1].getChunkInterval();

    Coordinates last(2);
    last[0] = dims[0].getStart() + dims[0].getLength() - 1;
    last[1] = dims[1].getStart() + dims[1].getLength() - 1;

    if(DBG) std::cerr << "@@@ calling invokeMPIRank()" << std::endl ;
    LOG4CXX_DEBUG(logger, "*@@@ calling invokeMPIRank()");

    const slpp::int_t DEFAULT_BAD_RESULT = -99;  // scalapack negative errors are the position of the bad argument
    slpp::int_t INFO = DEFAULT_BAD_RESULT ;
    shared_ptr<Array> result;

    invokeMPIRank(&redistInputs, query, _ctx, _schema, &result, &INFO);

    if(DBG) std::cerr << "@@@ execute: post invokeMPIRank, INFO:" << INFO << std::endl ;

    if (INFO != 0) {
        if(DBG) std::cerr << "ERROR: INFO is " << INFO << std::endl ;
        if(INFO < 0) {
            throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR35);
        } else {
            throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR36);
        }
        // some now leftover errors:
        //    throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR37);
        //    throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_UDO, DLA_ERROR33);

    }

    // return the scidb array
    if(DBG) std::cerr << "invokeMPIRank returning result" << std::endl ;
    LOG4CXX_DEBUG(logger, "invokeMPIRank returning result");

    Dimensions const& rdims = result->getArrayDesc().getDimensions();
    std::cerr << "returning result array size: " << rdims[0].getLength() <<
                                             "," << rdims[1].getLength() << std::endl ;

    if(DBG) std::cerr << "MPIRankPhysical::execute() end ---------------------------------------" << std::endl;
    return result;
}

REGISTER_PHYSICAL_OPERATOR_FACTORY(MPIRankPhysical, "mpirank", "MPIRankPhysical");

} //namespace
