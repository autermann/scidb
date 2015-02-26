///
/// ScaLAPACKPhysical.cpp
///
///
// std C++
#include <sstream>
#include <string>

// std C

// de-facto standards
#include <boost/foreach.hpp>
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
#include <DLAErrors.h>
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
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.scalapack.physicalOp"));


// TODO: REFACTORING: continue use of matSize_t in more places
// TODO: REFACTORING: make a "super array" that encapsulates the SciDB::Array and the ScaLAPACK DESC
//                    so we can pass fewer arguments


void checkBlacsInfo(shared_ptr<Query>& query, slpp::int_t ICTXT, slpp::int_t NPROW, slpp::int_t NPCOL, slpp::int_t MYPROW, slpp::int_t MYPCOL)
{
    size_t nInstances = query->getInstancesCount();
    slpp::int_t instanceID = query->getInstanceID();

    LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::doBlacsInit(): checkBlacsInfo "
                           << "(invoke) blacs_gridinfo_(ctx " << ICTXT << ")"
                           << " = NPROC (" << NPROW  << ", " << NPCOL << ")"
                           << " ; MYPROC (" << MYPROW << ", " << MYPCOL << ")");

    // REFACTOR these checks
    if(MYPROW < 0 || MYPCOL < 0) {
        LOG4CXX_ERROR(logger, "ScaLAPACKPhysical::checkBlacsInfo(): zero size mpi process grid: MYPROW " << MYPROW << " MYPCOL " << MYPCOL);
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << "ScaLAPACKPhysical::checkBlacsInfo(): zero size mpi process grid");
    }

    if(MYPROW >= NPROW) {
        LOG4CXX_ERROR(logger, "ScaLAPACKPhysical::checkBlacsInfo(): MYPROW " << MYPROW << " >= NPROW " << NPROW);
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << "ScaLAPACKPhysical::checkBlacsInfo(): illegal position in mpi process grid");
    }

    if(MYPCOL >= NPCOL) {
        LOG4CXX_ERROR(logger, "ScaLAPACKPhysical::checkBlacsInfo(): MYPCOL " << MYPCOL << " >= NPCOL " << NPCOL);
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << "ScaLAPACKPhysical::checkBlacsInfo(): illegal position in mpi process grid");
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
        std::stringstream msg; msg << "Scalapack operator error: NPE "<<NPE<< " nInstances "<< nInstances;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << msg.str()) ;
    }

    // MYPE == instanceID
    slpp::int_t MYPE = MYPROW*NPCOL + MYPCOL ; // row-major
    if(MYPE != instanceID) {
        std::stringstream msg; msg << "Scalapack operator error: MYPE "<<MYPE<< " instanceID "<< instanceID;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << msg.str()) ;
    }

    LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::doBlacsInit(): checkBlacsInfo"
                           << " NPE/nInstances " << NPE
                           << " MYPE/instanceID " << MYPE);
}

///
/// ScaLAPACK computation routines are only efficient for a certain
/// range of sizes and are generally only implemented for
/// square block sizes.  Check these constraints
///
// TODO JHM : rename Ain -> array
void ScaLAPACKPhysical::checkInputArray(boost::shared_ptr<Array>& Ain) const
{
    // chunksize was already checked in ScaLAPACKLogical.cpp, but since this code
    // was already here, we'll just fix it to check the same limit, rather than
    // remove it this late in the 12.11 release.
    // TODO: resolve better


    const slpp::int_t MB= chunkRow(Ain);
    const slpp::int_t NB= chunkCol(Ain);

    // TODO JHM: add test case for illegitimate block size
    // TODO JHM test early, add separate auto repart in execute if not efficient size, then retest
    if (MB > slpp::SCALAPACK_MAX_BLOCK_SIZE ||
        NB > slpp::SCALAPACK_MAX_BLOCK_SIZE) {
        std::stringstream ss; ss << "ScaLAPACK operator error:"
                                 << " chunksize "    << chunkRow(Ain)
                                 << " or chunksize " << chunkCol(Ain)
                                 << " is too large."
                                 << " Must be " << slpp::SCALAPACK_MIN_BLOCK_SIZE
                                 << " to "      << slpp::SCALAPACK_MAX_BLOCK_SIZE ;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << ss.str());
    }

    if (MB != NB) {
        std::stringstream ss; ss << "ScaLAPACK operator error: row chunksize " << chunkRow(Ain)
                                                    << " != column chunksize "<< chunkCol(Ain)
                                                    << " which is required." ;
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
    LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::redistributeInputArrays(): begin");
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
                LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::redistributeInputArrays():"
                                       << " redistributed input " << ii
                                       << " chunksize (" << redistInputs[ii]->getArrayDesc().getDimensions()[0].getChunkInterval()
                                       << ", "           << redistInputs[ii]->getArrayDesc().getDimensions()[1].getChunkInterval()
                                       << ")");
            } else {
                LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::redistributeInputArrays():"
                                       << " keeping input " << ii
                                       << " chunksize (" << inputArrays[ii]->getArrayDesc().getDimensions()[0].getChunkInterval()
                                       << ", "           << inputArrays[ii]->getArrayDesc().getDimensions()[1].getChunkInterval()
                                       << ")");
            }
        }
    } else {
        LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::redistributeInputArrays(): single instance, no redistribution.");
    }

    LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::redistributeInputArrays(): end");
    return redistInputs;
}

///
///.... Initialize the (imitation)BLACS used by the instances to calculate sizes
///     AS IF they are MPI processes (which they are not)
///
/// + intersects the array chunkGrid with the maximum process grid
/// + sets up the ScaLAPACK grid accordingly and if not participating, return early
/// + calls invokeMPISvd()
/// + returns the output OpArray.
///
bool ScaLAPACKPhysical::doBlacsInit(std::vector< shared_ptr<Array> >& redistInputs, shared_ptr<Query> query)
{

    // find max (union) size of all array/matrices.
    size_t maxSize[2];
    maxSize[0] = 0;
    maxSize[1] = 0;
    BOOST_FOREACH( shared_ptr<Array> input, redistInputs ) {
        matSize_t inputSize = getMatSize(input);
        maxSize[0] = std::max(maxSize[0], inputSize[0]);
        maxSize[1] = std::max(maxSize[1], inputSize[1]);
    }
    if (!maxSize[0] || !maxSize[1] ) {
        throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_OPERATOR, DLA_ERROR7);
    }

    slpp::int_t instanceID = query->getInstanceID();
    const ProcGrid* procGrid = query->getProcGrid();

    procRowCol_t MN = { maxSize[0], maxSize[1]};
    procRowCol_t MNB = { chunkRow(redistInputs[0]), chunkCol(redistInputs[0]) };

    procRowCol_t blacsGridSize = procGrid->useableGridSize(MN, MNB);
    procRowCol_t myGridPos = procGrid->gridPos(instanceID, blacsGridSize);

    LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::doBlacsInit():"
                              << " gridPos (" << myGridPos.row << ", " << myGridPos.col << ")"
                              << " gridSize (" << blacsGridSize.row << ", " << blacsGridSize.col << ")");

    if (myGridPos.row >= blacsGridSize.row || myGridPos.col >= blacsGridSize.col) {
        LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::doBlacsInit(): instance " << instanceID << " NOT in grid"
                                  << " gridPos (" << myGridPos.row << ", " << myGridPos.col << ")"
                                  << " gridSize (" << blacsGridSize.row << ", " << blacsGridSize.col << ")");
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
        LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::doBlacsInit(): instID " << instanceID << "is in grid.");
    }

    slpp::int_t ICTXT=-1;

    LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::doBlacsInit():"
                              << " calling set_fake_blacs_gridinfo_(ctx " << ICTXT
                              << ", nProw " << blacsGridSize.row << ", nPcol "<< blacsGridSize.col
                              << ", myPRow " << myGridPos.row << ", myPCol " << myGridPos.col << ")");
    set_fake_blacs_gridinfo_(ICTXT, blacsGridSize.row, blacsGridSize.col, myGridPos.row, myGridPos.col);

    // check that it worked
    slpp::int_t NPROW=-1, NPCOL=-1, MYPROW=-1 , MYPCOL=-1 ;
    blacs_gridinfo_(ICTXT, NPROW, NPCOL, MYPROW, MYPCOL);
    LOG4CXX_DEBUG(logger, "ScaLAPACKPhysical::doBlacsInit(): blacs_gridinfo(" << ICTXT << ") returns "
                              << " gridsiz (" << NPROW  << ", " << NPCOL << ")"
                              << " gridPos (" << MYPROW << ", " << MYPCOL << ")");

    return true;
}

} // namespace



