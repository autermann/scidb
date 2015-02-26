///
/// ScaLAPACKPhysical.cpp
///
///
// std C++
#include <sstream>
#include <string>

// std C

// de-facto standards
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_array.hpp>

// SciDB
#include <system/BlockCyclic.h>
#include <system/Exceptions.h>
#include <system/Utils.h>


// more SciDB
#include <array/ArrayExtractOp.hpp>
#include <array/OpArray.h>
#include <mpi/MPIPhysical.hpp>
#include <scalapackUtil/reformat.hpp>
#include <scalapackUtil/scalapackFromCpp.hpp>
#include <dlaScaLA/scalapackEmulation/scalapackEmulation.hpp>
#include <dlaScaLA/slaving/pdgesvdMaster.hpp>
#include <dlaScaLA/slaving/pdgesvdSlave.hpp>
#include <scalapackUtil/reformat.hpp>


#include <scalapackUtil/ScaLAPACKPhysical.hpp>
#include <system/Cluster.h>


namespace scidb {
static const bool DBG = false;
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.scalapack"));


void checkBlacsInfo(shared_ptr<Query>& query, slpp::int_t ICTXT, slpp::int_t NPROW, slpp::int_t NPCOL, slpp::int_t MYPROW, slpp::int_t MYPCOL)
{
    size_t nInstances = query->getInstancesCount();
    slpp::int_t instanceID = query->getInstanceID();

    if(DBG) {
        std::cerr << "(invoke) blacs_gridinfo_(ctx:" << ICTXT << ")" << std::endl;
        std::cerr << "-> NPROW: " << NPROW  << ", NPCOL: " << NPCOL << std::endl;
        std::cerr << "-> MYPROW:" << MYPROW << ", MYPCOL:" << MYPCOL << std::endl;
    }

    // REFACTOR these checks
    if(MYPROW < 0 || MYPCOL < 0) {
        std::cerr << "MPISVD operator error: MYPROW:"<< MYPROW << " MYPCOL:"<<MYPCOL << std::endl ;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << "MPIRank operator error: MYPROW:"<< MYPROW << " MYPCOL:"<<MYPCOL);
    }

    if(MYPROW >= NPROW) {
        std::cerr << "MPISVD operator error: MYPROW:"<< MYPROW << " NPROW:"<<NPROW << std::endl ;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << "MPIRank operator error: MYPROW:"<< MYPROW << " NPROW:"<<NPROW);
    }

    if(MYPCOL >= NPCOL) {
        std::cerr << "MPISVD operator error: MYPCOL:"<< MYPCOL << " NPCOL:"<<NPCOL << std::endl ;
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
        std::stringstream msg; msg << "MPISVD operator error: NPE:"<<NPE<< " nInstances:"<< nInstances;
        std::cerr << msg.str() << std::endl;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << msg.str()) ;
    }

    // MYPE == instanceID
    slpp::int_t MYPE = MYPROW*NPCOL + MYPCOL ; // row-major
    if(MYPE != instanceID) {
        std::stringstream msg; msg << "MPISVD operator error: MYPE:"<<MYPE<< " instanceID:"<< instanceID;
        std::cerr << msg.str() << std::endl;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << msg.str()) ;
    }

    if(DBG) std::cerr << "NPE/nInstances: " << NPE << std::endl;
    if(DBG) std::cerr << "MYPE/instanceID: " << MYPE << std::endl;

}

///
/// ScaLAPACK computation routines are only efficient for a certain
/// range of sizes and are generally only implemented for
/// square block sizes.  Check these constraints
///
// TODO JHM : rename Ain -> array
void checkInputArray(Array* Ain)
{
    // note that the best chunksizes for ScaLAPACK are 32x32 and 64x64
    // for Intel {Sandy,Ivy}Bridge processors. (Core 2 2xxx, 3xxx cpus)
    // small matrices will often have equally small chunk size, so shold
    // handle those.
    // However, the SciDB per-chunk overhead is overly high for 64x64 chunks.
    // 128x128 doesn't appear to slow down on a 6-core SandyBridge-E, we'll
    // have to do further testing / adapation if the small caches of
    // a Xeon cause problems.
    const slpp::int_t SL_MAX_BLOCK_SIZE=128;

    const slpp::int_t MB= brow(Ain);
    const slpp::int_t NB= bcol(Ain);

    // TODO JHM: add test case for illegitimate block size
    // TODO JHM test early, add separate auto repart in execute if not efficient size, then retest
    if (MB > SL_MAX_BLOCK_SIZE ||
        NB > SL_MAX_BLOCK_SIZE) {
        std::stringstream ss; ss << "SVD operator error: chunksize " << brow(Ain) << " x "<< bcol(Ain) << " is too large, 64 to 128 is recommended" ;
        if(DBG) std::cerr << ss << std::endl;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << ss.str());
    }

    if (MB != NB) {
        std::stringstream ss; ss << "SVD operator error: chunksizes " << brow(Ain) << " x "<< bcol(Ain)
                                 << " are not the same, which is a current restriction for SVD." ;
        if(DBG) std::cerr << ss << std::endl;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << ss.str());
    }
}


void extractArrayToScaLAPACK(boost::shared_ptr<Array>& array, double* dst, slpp::desc_t& desc)
{
    // use extractDataToOp() and the reformatToScalapack() operator
    // to reformat the data according to ScaLAPACK requirements.
    Coordinates coordFirst = getStart(array.get());
    Coordinates coordLast = getEndMax(array.get());
    scidb::ReformatToScalapack pdelsetOp(dst, desc, coordFirst[0], coordFirst[1]);

    Timing reformatTimer;
    LOG4CXX_DEBUG(logger, "extractArrayToScaLAPACK start");
        extractDataToOp(array, /*attrID*/0, coordFirst, coordLast, pdelsetOp);
    LOG4CXX_DEBUG(logger, "extractArrayToScaLAPACK end");
    if(doCerrTiming()) std::cerr << "extractArrayToScaLAPACK took " << reformatTimer.stop() << std::endl;
}

bool ScaLAPACKPhysical::requiresRepart(ArrayDesc const& inputSchema) const
{
    return false ; // see #2032 ... have to disable as it will core dump otherwise

    Dimensions const& inDims = inputSchema.getDimensions();

    for(size_t dimIdx=0; dimIdx < inDims.size(); dimIdx++) {
        if(inDims[dimIdx].getChunkInterval() != slpp::SCALAPACK_EFFICIENT_BLOCK_SIZE) {
            return true;
        }
    }

    // #2032
    // (A)     the optimizer won't insert on all inputs, rumour is, so we raise exceptions in the
    //     logical::inferSchemas() until this is fixed.  If I try it, it faults anyway,
    //     so waiting on an answer to #2032 before moving forward
    // (B)     The test above returns true more than we want to, because we can't compare and
    //     analyze the differing chunksizes to determine which we will change and which one
    //     will be the common one we change all the others to.
    //         Since we can't analyze them, we have to insist they are a specific size, when
    //     the user may know well what they are doing and using one in the acceptable range.
    //         Proposal ... requiresRepart() passes in the inputSchemas for all inputs
    //     and returns a vector of bool, or specifies which it is asking about, and we answer with that
    //     single bool.
    return false;
}

ArrayDesc ScaLAPACKPhysical::getRepartSchema(ArrayDesc const& inputSchema) const
{
    Dimensions const& inDims = inputSchema.getDimensions();

    Dimensions resultDims;
    for (size_t dimIdx =0; dimIdx < inDims.size(); dimIdx++)
    {
        DimensionDesc inDim = inDims[dimIdx];
        resultDims.push_back(DimensionDesc(inDim.getBaseName(),
                              inDim.getNamesAndAliases(),
                              inDim.getStartMin(),
                              inDim.getCurrStart(),
                              inDim.getCurrEnd(),
                              inDim.getEndMax(),
                              slpp::SCALAPACK_EFFICIENT_BLOCK_SIZE,  // no way to generate a consensus size.
                              false,
                              inDim.getType(),
                              inDim.getFlags(),
                              inDim.getMappingArrayName(),
                              inDim.getComment(),
                              inDim.getFuncMapOffset(),
                              inDim.getFuncMapScale()));
    }

    Attributes inAttrs = inputSchema.getAttributes();
    return ArrayDesc(inputSchema.getName(), inAttrs, resultDims);
}


///
/// + converts inputArrays to psScaLAPACK distribution
std::vector<shared_ptr<Array> > ScaLAPACKPhysical::redistributeInputArrays(std::vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query)
{
    const bool DBG = false ;

    if(DBG) std::cerr << "ScaLAPACKPhysical::redistributeInputArrays() begin ---------------------------------------" << std::endl;
    //
    // repartition and redistribution from SciDB chunks and arbitrary distribution
    // to
    // ScaLAPACK tiles (efficient cache-aware size) and psScaLAPACK, which is
    //   true 2D block-cyclic.  Ideally, we will be able to repart() to from
    //   arbitrary chunkSize to one that is efficient for ScaLAPACK
    //
    std::vector<shared_ptr<Array> > redistInputs = inputArrays ; // copy by default
    size_t nInstances = query->getInstancesCount();
    bool requiresRedistribute = true ;  // TODO: when all tests functioning, enable this and re-test
                                        //       (we don't want a different code path during bring-up)
    if (nInstances>1 || requiresRedistribute) {
#if 0
        // TODO: listed in ticket #1962, we do not yet handle chunksizes above some fixed limit by introducing a repart
        if (chunking is not 64 x 64 square) {
            // rechunk to 64 x 64 and redistribute, because
            // then its very very close to being scalapack
            // block cyclic layout... except that the resulting chunks are
            // the transpose of the blocks, even though they are now on the
            // right hosts
            input=redistribute(repartArray(input), query, psScaLAPACK);
        } else
#endif
        // redistribute to psScaLAPACK
        for(size_t ii=0; ii < inputArrays.size(); ii++) {
            if (redistInputs[ii]->getArrayDesc().getPartitioningSchema() != psScaLAPACK) {
                Timing redistTime;
                redistInputs[ii]=redistribute(inputArrays[ii], query, psScaLAPACK);
                if(doCerrTiming()) std::cerr << "ScaLAPACKPhysical: redist["<<ii<<"] took " << redistTime.stop() << std::endl;
            }
        }
    } else {
        if(DBG) std::cerr << "single instance, no redistribution." << std::endl;
    }
    return redistInputs;
}

/// + intersects the array chunkGrid with the maximum process grid
/// + sets up the ScaLAPACK grid accordingly and if not participating, return early
/// + calls invokeMPISvd()
/// + returns the output OpArray.
///
bool ScaLAPACKPhysical::doBlacsInit(std::vector< shared_ptr<Array> >& redistInputs, shared_ptr<Query> query)
{
    //!
    //!.... Initialize the (imitation)BLACS used by the instances to calculate sizes
    //!     AS IF they are MPI processes (which they are not)
    //!
    shared_ptr<Array> input = redistInputs[0];
    Dimensions const& dims = input->getArrayDesc().getDimensions();
    size_t nRows = dims[0].getLength();
    size_t nCols = dims[1].getLength();
    if (!nRows || !nCols ) {
        return false; // don't try to handle this case
    }

    slpp::int_t instanceID = query->getInstanceID();
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
        // we won't start mpi slaves for such instances

        // XXX Make sure that the coordinator always participates
        // to work around the apparent mpirun bug in dealing with --prefix,
        // where the --prefix specified for the first instance overrides all the following ones.
        // As long as the coordinator=0, the condition should be true.
        // XXX TODO: fix it for any coordinator
        assert(query->getCoordinatorID() != COORDINATOR_INSTANCE);
        return false ;
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

    if(DBG) {
        std::cerr << "(execute) NP:"<<NP << " IC:" <<IC << std::endl;
        std::cerr << "(execute) set_fake_blacs_gridinfo_(ctx:"<< ICTXT
                                   << ", nprow:"<<blacsGridSize.row
                                   << ", npcol:"<<blacsGridSize.col << "," << std::endl;
        std::cerr << "(execute)                           myRow:" << myGridPos.row
                                   << ", myCol:" << myGridPos.col << ")" << std::endl;
    }
    set_fake_blacs_gridinfo_(ICTXT, blacsGridSize.row, blacsGridSize.col, myGridPos.row, myGridPos.col);

    // check that it worked
    slpp::int_t NPROW=-1, NPCOL=-1, MYPROW=-1 , MYPCOL=-1 ;
    blacs_gridinfo_(ICTXT, NPROW, NPCOL, MYPROW, MYPCOL);
    if(DBG) {
        std::cerr << "blacs_gridinfo_(ctx:" << ICTXT << ")" << std::endl;
        std::cerr << "   -> gridsiz:(" << NPROW  << ", " << NPCOL << ")" << std::endl;
        std::cerr << "   -> gridPos:(" << MYPROW << ", " << MYPCOL << ")" << std::endl;
    }
    return true;
}


} // namespace



