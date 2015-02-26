
#ifndef PDGESVD_MASTER_SLAVE__H
#define PDGESVD_MASTER_SLAVE__H

// SciDB
#include <scalapackUtil/scalapackFromCpp.hpp>
#include <mpi/MPIManager.h>

//
// this file contains routines that are use by both xxxMaster and xxxSlave routines
//

namespace scidb {

///
/// A "constructor" for the pdgesvdSlave() argument buffer.
/// We use a function, rather than a constructor, because it depends on runtime state,
/// in a way that a constructor should probably not depend on, at this time.
///
void pdgesvdMarshallArgs(void*  argsBuf,
                         const sl_int_t& NPROW, const sl_int_t& NPCOL,
                         const sl_int_t& MYPROW, const sl_int_t& MYPCOL, const sl_int_t& MYPNUM,
                         const char &jobU, const char &jobVT,
                         const sl_int_t& M, const sl_int_t &N,
                         double* A, const sl_int_t& IA, const sl_int_t& JA, const sl_desc_t& DESC_A,
                         double* S,
                         double* U,  const sl_int_t& IU,  const sl_int_t& JU,  const sl_desc_t& DESC_U,
                         double* VT, const sl_int_t& IVT, const sl_int_t& JVT, const sl_desc_t& DESC_VT);


} // namespace scidb
#endif // PDGESVD_MASTER_SLAVE__H

