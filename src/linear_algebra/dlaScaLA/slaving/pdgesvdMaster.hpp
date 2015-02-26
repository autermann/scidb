
#ifndef PDGESVD_MASTER__H
#define PDGESVD_MASTER__H

// SciDB
#include <scalapackUtil/scalapackFromCpp.hpp>

// locals
#include <mpi/MPIManager.h>

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
// The followin ScaLAPACK "driver routines" are supported at this time:
// 
// ScaLAPACK    (MASTER-SIDE) WRAPPER
// pdgesvd_     pdgesvdMaster
//

namespace scidb {

///
/// This method causes a ScaLAPACK pdgesvd_ to be run on data A with outputs in
/// S (sigma or singular values)
/// U (U or left singular vectors)
/// V* (V congugate-transpose or right singular vectors congugate transpose)
/// 
/// The difference is that the "Master" version sends the command to a program
/// called "scidb_mpi_slave" and that process mmap/shmem's the buffer into its address
/// space, calls pdgesvd_() and returns.  The slave process is used so that if
/// MPI experiences a failure, it will not cause the typical MPI problems that
/// MPI failures cause for their processes, such as termination and/or inability
/// to communicate to other MPI processes after the error, with no recourse
/// except restart.
///
void pdgesvdMaster(Query* query,  // or do I need only the ctx?
                   boost::shared_ptr<MpiOperatorContext>& ctx,
                   boost::shared_ptr<MpiSlaveProxy>& slave,  // need ctx->getSlave();
                   const string& ipcName, // can this be in the ctx too?
                   void * argsBuf,
                   // the following args are common to all scalapack slave operators:
                   const sl_int_t& NPROW, const sl_int_t& NPCOL,
                   const sl_int_t& MYPROW, const sl_int_t& MYPCOL, const sl_int_t& MYPNUM,
                   // the follow argument types match the ScaLAPACK FORTRAN-compatible ones:
                   const char &jobU, const char &jobVT,
                   const sl_int_t& M, const sl_int_t &N, 
                   double *A, const sl_int_t &IA, const sl_int_t &JA, const sl_desc_t& DESC_A,
                   double *S, 
                   double *U,  const sl_int_t &IU,  const sl_int_t &JU,  const sl_desc_t& DESC_U,
                   double *VT, const sl_int_t &IVT, const sl_int_t &JVT, const sl_desc_t& DESC_VT,
                   sl_int_t &INFO);

} // namespace scidb
#endif // PDGESVD_MASTER__H

