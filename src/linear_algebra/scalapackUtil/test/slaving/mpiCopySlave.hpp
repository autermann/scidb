#ifndef MPI_COPY_SLAVE__H
#define MPI_COPY_SLAVE__H

//
// this file contains routines that are rpc-like "wrappers" for ScaLAPACK calls,
// which allow them to run in a separate process from SciDB.  This is because ScaLAPACK
// runs on MPI, which is not tolerant of node failures.  On node failure, it will either
// kill all MPI processes in the "communcatior" group, or, if it is set not to do that,
// the "communicator" group becomes unuseable until the process is restarted.  Neither of
// these MPI behaviors is compatible with a database server that needs to run 24/7.
//
// Note that the ScaLAPACK routines are actually written in FORTRAN and do not have a
// specific C or C++ API, therfore the types of the arguents in the prototypes are designed
// to permit calling FORTRAN from C++, and it is those types that are required in the
// corresponding wrappers.
//
// This is the "slave-side" wrappers.  For the master-side wrappers, see pdgesvdMaster.hpp
// 
// ScaLAPACK    (SLAVE-SIDE) WRAPPER
// rank         rankSlave
//

// std C++
#include <ostream>

// std C

// defacto
#include "../../scalapackFromCpp.hpp"

// SciDB
#include "../../../dlaScaLA/slaving/scalapackSlave.hpp" // common argument support

namespace scidb {

//
// MPICopyArgs contains the marshaled arguments for a
// pdgesvdMaster to pdgesvdSlave remote execution
// + two flags, U & V,
// + the size of the input sub-array
// + the global arrays A, U, VT
// + the locallly-replicated array S
//
class MPICopyArgs {
public:
    friend std::ostream& operator<<(std::ostream& os, const MPICopyArgs& a);
    sl_int_t    NPROW;  // all ops will need these two to set up scalapack via blacs_init
    sl_int_t    NPCOL;
    sl_int_t    MYPROW; // all ops will need these to check the that the fake blacs_get_info
    sl_int_t    MYPCOL; // and the real one returned identical values
    sl_int_t    MYPNUM; // and the same for blacs_pnum()

    ScalapackArrayArgs IN;
    ScalapackArrayArgs OUT;
};

inline std::ostream& operator<<(std::ostream& os, const MPICopyArgs& a)
{
    os << "IN{" << a.IN << "}"                        << std::endl;
    os << "OUT{" << a.OUT << "}"                      << std::endl;
    return os;
}

sl_int_t mpiCopySlave(void* bufs[], size_t sizes[], unsigned count);

} // namespace scidb

#endif // MPI_COPY_SLAVE__H

