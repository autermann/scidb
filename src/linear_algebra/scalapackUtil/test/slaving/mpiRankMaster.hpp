
#ifndef MPI_RANK_MASTER__H
#define MPI_RANK_MASTER__H

// SciDB import
#include <scalapackUtil/scalapackFromCpp.hpp>
#include <mpi/MPIManager.h>

//
// this file contains routines that are rpc-like "wrappers" for ScaLAPACK
// (and sometimes MPI) calls,
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

namespace scidb {

void mpirankMaster(Query* query,  // or do I need only the ctx?
                   boost::shared_ptr<MpiOperatorContext>& ctx, // query->getOperatorCtxt returns superclass
                   boost::shared_ptr<MpiSlaveProxy>& slave,  // need ctx->getSlave();
                   const string& ipcName, // can this be in the ctx too?
                   void * argsBuf,
                   // the following args are common to all scalapack slave operators:
                   const sl_int_t& NPROW, const sl_int_t& NPCOL,
                   const sl_int_t& MYPROW, const sl_int_t& MYPCOL, const sl_int_t& MYPNUM,
                   // the follow argument types are just for the rank operator
                   double *IN, const sl_desc_t& DESC_IN,
                   double *OUT, const sl_desc_t& DESC_OUT,
                   sl_int_t &INFO);

} // namespace scidb
#endif // MPI_RANK_MASTER__H

