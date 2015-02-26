///
/// ScaLAPACKLogical.cpp
///
///
#include <utility>
#include "scalapackUtil/ScaLAPACKLogical.hpp"

namespace scidb {

inline bool hasSingleAttribute(ArrayDesc const& desc)
{
    return desc.getAttributes().size() == 1 || (desc.getAttributes().size() == 2 && desc.getAttributes()[1].isEmptyIndicator());
}

void checkScaLAPACKInputs(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query,
                          size_t nMatsMin, size_t nMatsMax)
{
    enum dummy  {ROW=0, COL=1};
    enum dummy2 { ATTR0=0 };

    const size_t NUM_MATRICES = schemas.size();


    if(schemas.size() < nMatsMin ||
       schemas.size() > nMatsMax) {
        throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR2);
    }

    // Check the properties first by argument, then by order property is determined in AFL statement:
    // size, chunkSize, overlap.
    // Check individual properties in the loop, and any inter-matrix properties after the loop
    // TODO: in all of these, name the argument # at fault
    for(size_t iArray=0; iArray < NUM_MATRICES; iArray++) {

        // check: attribute count == 1
        if (!hasSingleAttribute(schemas[iArray])) {
            throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR2);
            // TODO: offending matrix is iArray
        }

        // check: attribute type is double
        if (schemas[iArray].getAttributes()[ATTR0].getType() != TID_DOUBLE) {
            throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR5);
            // TODO: offending matrix is iArray
        }

        // check: nDim == 2 (a matrix)
        // TODO: relax nDim to be 1 and have it imply NCOL=1  (column vector)
        //       if you want a row vector, we could make transpose accept the column vector and output a 1 x N matrix
        //       and call that a "row vector"  The other way could never be acceptable.
        //
        const size_t SCALAPACK_IS_2D = 2 ;
        if (schemas[iArray].getDimensions().size() != SCALAPACK_IS_2D) {
            throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR3);
            // TODO: offending matrix is iArray
        }

        // check: size is bounded
        const Dimensions& dims = schemas[iArray].getDimensions();
        if (dims[ROW].getLength() == INFINITE_LENGTH ||
            dims[COL].getLength() == INFINITE_LENGTH) {
            throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR9);
        }
        // TODO: check: sizes are not larger than largest ScaLAPACK fortran INTEGER

        // TODO: check: chunk interval not too small
        assert(dims[ROW].getChunkInterval() > 0); // earlier system code already checks this
        assert(dims[COL].getChunkInterval() > 0);


        // check: chunk interval not too large
        if (dims[ROW].getChunkInterval() > slpp::SCALAPACK_MAX_BLOCK_SIZE ||
            dims[COL].getChunkInterval() > slpp::SCALAPACK_MAX_BLOCK_SIZE ) {
            // the cache will thrash and performance will be unexplicably horrible to the user
            throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR41); // too large
        }

        // TODO: make two variables slpp::SCLAPACK_EFFICIENT_BLOCK_SIZE_MINIMUM and _MAXIMUM instead of single number
        // TODO: the following does not work correctly.  postWarning() itself uses SCIDB_WARNING
        //       need an example to follow
        if (false) {  // broken code inside postWarning(SCIDB_WARNING()) faults. Not sure what the argument is supposed to be
            for(size_t d = ROW; d <= COL; d++) {
                if(dims[d].getChunkInterval() < slpp::SCALAPACK_EFFICIENT_BLOCK_SIZE ||
                   dims[d].getChunkInterval() < slpp::SCALAPACK_EFFICIENT_BLOCK_SIZE) {
                    query->postWarning(SCIDB_WARNING(DLA_WARNING4) << slpp::SCALAPACK_EFFICIENT_BLOCK_SIZE
                                                                   << slpp::SCALAPACK_EFFICIENT_BLOCK_SIZE);
                }
            }
        }

        // check: no overlap allowed
        //        TODO: improvement? if there's overlap, we may be able to ignore it,
        //              else invoke a common piece of code to remove it
        //              and in both cases emit a warning about non-optimality
        if (dims[ROW].getChunkOverlap()!=0 ||
            dims[COL].getChunkOverlap()!=0) {
            stringstream ss;
            ss<<"in matrix "<<iArray;
            throw (PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR40)
                   << ss.str());
        }
    }

    // check: the chunkSizes from the user must be identical (until auto-repart is working)
    const bool AUTO_REPART_WORKING = false ;  // #2032
    if( ! AUTO_REPART_WORKING ) {
        uint32_t commonChunkSize = schemas[0].getDimensions()[ROW].getChunkInterval();
        // TODO: remove these checks if #2023 is fixed and requiresRepart() is functioning correctly
        for(size_t iArray=0; iArray < NUM_MATRICES; iArray++) {
            const Dimensions& dims = schemas[iArray].getDimensions();
            // arbitrarily take first mentioned chunksize as the one for all to share
            if (dims[ROW].getChunkInterval() != commonChunkSize ||
                dims[COL].getChunkInterval() != commonChunkSize ) {
                throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR10);
                // TODO: name the matrix
            }
        }
    }

    // Chunksize matching critique
    //    This is not what we want it to be, but has to be until #2023 is fixed, which
    //    will allow the query planner and optimizer to repartition automatically, instead
    //    of putting the burden on the user.
    //
    //    (1) The required restriction to make ScaLAPACK work is that they are equal
    //    in both dimensions (square chunks) and equal for all matrices.
    //    (2) Next, we need a chunk of A,B, and C to fit into L1 cache at the same time.
    //    this is required to make any BLAS run well, while the next chunk is being
    //    statged into L2.  Different BLASes want the chunk size to be all in L1 or partly
    //    in L1 and L2.  We don't know this right now, but we do know that all
    //    modern procesors have tiny, unshared, L2 caches.  Assuming the multiply is really
    //    large, we disregard the O(M^2) communication time because the multiply will be
    //    O(M^3).  So knowing an Intel Sandybridge has L1 data cache size 32KiB we find that
    //    for doubles, the answer is to use less than sqrt(32*1024/3/8) which is 36.9  So 32
    //    square is the conservative number.  If we allow it to run in L2 (256KiB) then
    //    sqrt(256*1024/3/8) = 104.5.  This suggest that if L2/L1 cache movement is hidden
    //    then 64x64 is tolerable only if the L2/L1 movement is schedule well, as it was in
    //    the GOTO blas.  Right now, 32x32 is a much better starting point.
    //    (3) So what do we do if the chunksize is not optimal?  How do we compute the answer
    //    but give the warning (good approach if matrix size is small or medium).
    //    (4) If the user gives inputs that match, and don't need a repart, we can proceed.
    //    But if any of them need a repart, we can only do all of them to the optimal size.
    //    Due to the api of LogicalOperator::requiresRepart() we can't tell which situation
    //    it is ... all matching so don't insert, or it isn't the optimal size, so insert.
    //    We'll write this part to allow it if the inputs all match, even if not optimal,
    //    AND have the optimizer insert reparts.  That will "train" the user to what we
    //    think the minimal requirements will be here, and blow up below where we run into
    //    problems with the optimizer that need to be fixed soon.
    //
    // TODO: after #2032 is fixed, have James reduce/remove the above comment to the current truth
    //
}

// PGB: the requirement on names is that until such a time as we have syntax to disambiguate them by dimesion index
//      or other means, they must be distinct, else if stored, we will lose access to any but the first.
// JHM: in math, its annoying to have the names keep getting longer for the same thing.  So we only want to do
//      the appending of _? when required.

std::pair<string, string> ScaLAPACKDistinctDimensionNames(const string& a, const string& b)
{
    typedef std::pair<string, string> result_t ;
    result_t result;

    if (a != b) {
        // for algebra, avoid the renames when possible
        return result_t(a,b);
    } else {
        // fallback to appending _1 or _2 to both... would rather do it to just one,
        // but this is the only convention we have for conflicting in general.
        return result_t(a + "_1", b + "_2");
    }
}

} // namespace
