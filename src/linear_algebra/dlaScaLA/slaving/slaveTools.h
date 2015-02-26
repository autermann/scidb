
#ifndef SLAVE_TOOLS__H
#define SLAVE_TOOLS__H

//
// this file contains routines for supporting the scalapack slaves.
// anything that is done in common between the slaves in terms of using ScaLAPACK
// should be factored to this file.
//

#include <scalapackUtil/scalapackFromCpp.hpp>

namespace scidb {

///
/// a method that returns, for the given ScaLAPACK context (ICTXT), the 5 basic parameters
/// of that context:
/// @param NPROW  number of processes in a row of the process grid
/// @param NPCOL  number of processes in a column of the the process grid
/// @param MYPROW row of this process in the process grid
/// @param MYPCOL column of this process in the process grid
/// @param MYPNUM index of the process in the process grid
///
void getSlaveBLACSInfo(const sl_int_t ICTXT, sl_int_t& NPROW, sl_int_t& NPCOL, sl_int_t& MYPROW, sl_int_t& MYPCOL, sl_int_t& MYPNUM);


} // namespace scidb

#define SLAVE_ASSERT_ALWAYS(expr) \
    {                             \
        if (!(expr)) {            \
            std::cerr << #expr << "false at: " <<  __FILE__ << " : " << __LINE__  << std::endl; \
            ::abort();            \
        }                         \
    }


#endif // SLAVE_TOOLS__H

