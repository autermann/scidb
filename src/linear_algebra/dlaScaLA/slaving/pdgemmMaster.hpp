
#ifndef PDGEMM_MASTER__H
#define PDGEMM_MASTER__H

// SciDB
#include <mpi/MPIManager.h>
#include <scalapackUtil/scalapackFromCpp.hpp>

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
// pdgemm_     pdgemmMaster
//

namespace scidb {

///
/// This method causes a ScaLAPACK pdgemm_ to be run on data A with outputs in
/// S (sigma or singular values)
/// U (U or left singular vectors)
/// V* (V congugate-transpose or right singular vectors congugate transpose)
///
/// The difference is that the "Master" version sends the command to a program
/// called "scidb_mpi_slave" and that process mmap/shmem's the buffer into its address
/// space, calls pdgemm_() and returns.  The slave process is used so that if
/// MPI experiences a failure, it will not cause the typical MPI problems that
/// MPI failures cause for their processes, such as termination and/or inability
/// to communicate to other MPI processes after the error, with no recourse
/// except restart.
///
void pdgemmMaster(Query* query,  // or do I need only the ctx?
                   boost::shared_ptr<MpiOperatorContext>& ctx,
                   boost::shared_ptr<MpiSlaveProxy>& slave,  // need ctx->getSlave();
                   const string& ipcName, // can this be in the ctx too?
                   void * argsBuf,
                   // the following args are common to all scalapack slave operators:
                   const sl_int_t& NPROW, const sl_int_t& NPCOL,
                   const sl_int_t& MYPROW, const sl_int_t& MYPCOL, const sl_int_t& MYPNUM,
                   // the follow argument types match the ScaLAPACK FORTRAN-compatible ones:
                   const char &TRANSA, const char &TRANSB,
                   const sl_int_t& M, const sl_int_t &N, const sl_int_t &K,
                   double *ALPHA,
                   double *A, const sl_int_t &IA, const sl_int_t &JA, const sl_desc_t& DESC_A,
                   double *B,  const sl_int_t &IB,  const sl_int_t &JB,  const sl_desc_t& DESC_B,
                   double *BETA,
                   double *C, const sl_int_t &IC, const sl_int_t &JC, const sl_desc_t& DESC_C,
                   sl_int_t &INFO);

} // namespace scidb
#endif // PDGEMM_MASTER__H

