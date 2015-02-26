#ifndef SCALAPACK_EMULATION__HPP
#define SCALAPACK_EMULATION__HPP

#include <sys/types.h>
#include <linear_algebra/scalapackUtil/scalapackFromCpp.hpp>

///
/// These methods are temporary scaffolding to allow SciDB to make calls that mimick
/// the original SciDB prototype where ScaLAPACK was called directly from SciDB.
/// The only remaining use of ScaLAPACK calls in SciDB is to the methods that concern
/// setting up ScaLAPACK array descriptors [descinit_()], reading and writing individual
/// subscripts form ScaLAPACK-formatted matrix/vector memory [pdelset_(), pdelget_()]
/// calculating a bound on the number of rows or columns of a local pieces of distributed
/// array memory[numroc_()], and getting information about the process grid
/// [blacs_gridinfo_()]
///
/// The goal is to replace and repackage this functionality as native C++ functionality
/// to reduce or eliminate the need to have SciDB link ScaLAPACK code, and all (or most)
/// of this code will be eliminated.
///
/// Therefore, we're not going to document how to use these calls at this time, we'll
/// wait until the ScaLAPACK emulation in SciDB is refined.  This is scheduled during
/// Aug-Sept/2012.
///

extern "C" {
    // these declarations are for routines that are work-alikes to to real ScaLAPACK
    // calls, (with the exception of those with "fake" in the name, which are additional)
    // but allow those calls to work in a non-mpi process.
    // For the moment, these routines are implemented in FORTRAN (mostly copies of the
    // originals, with slight mods sometimes) and that is why they are
    // a) extern "C" (to defeat C++ name-mangling)
    // b) end in "_" because fortran adds that
    // c) specify arguments as <type>&, because this forms an automatic conversion to
    //    the right type, followed by delivering its address to FORTRAN, and all
    //    variables in FORTRAN are passed by such references.

    // just to shorten some otherwise long lines below:
    typedef slpp::int_t sl_int_t;
    typedef slpp::desc_t sl_desc_t;

    // Utilities (all copies of the real FORTRAN ones)
    void descinit_(sl_desc_t& desc,
                   const sl_int_t& m, const sl_int_t& n,
                   const sl_int_t& mb, const sl_int_t& nb,
                   const sl_int_t& irSrc, const sl_int_t& icSrc, const sl_int_t& icTxt,
                   const sl_int_t& lld, sl_int_t& info);
   
    void pdelset_(double* data, const sl_int_t& row, const sl_int_t& col,
                  const sl_desc_t& desc, const double& val);
    void pdelget_(const char& SCOPE, const char& TOP, double& ALPHA, const double* A,
                  const sl_int_t& IA, const sl_int_t& JA, const sl_desc_t& DESCA);

    sl_int_t numroc_(const sl_int_t&, const sl_int_t&, const sl_int_t&, const sl_int_t&, const sl_int_t&);


    // BLACS: (slightly modified form the FORTRAN ones)
    typedef blacs::int_t bl_int_t;
    void blacs_gridinfo_(const bl_int_t&, const bl_int_t&,
                         const bl_int_t&, bl_int_t&, bl_int_t&);
    bl_int_t blacs_pnum_(const bl_int_t& cntxt, const bl_int_t& myPRow, const bl_int_t& myPCol);
    void blacs_gridexit_(const bl_int_t&);
    void blacs_exit_(const bl_int_t&);

    // This one does not exist in FORTRAN, caues the ones immediate above to return these values
    void set_fake_blacs_gridinfo_(const bl_int_t& ictxt,
                                  const bl_int_t& nprow, const bl_int_t& npcol,
                                  const bl_int_t& myprow, const bl_int_t& mypcol);

}

#endif // SCALAPACK_EMULATION__HPP
