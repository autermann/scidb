#ifndef SCALAPACK_SLAVE__H
#define SCALAPACK_SLAVE__H

// std C++
#include <ostream>

// SciDB
#include "../../scalapackUtil/scalapackFromCpp.hpp" // home-brew c++ interface to FORTRAN ScaLAPACK

namespace scidb {

/// Scalapack arrays are described by a 9-value descriptor of type sl_desc_t.
/// most routines also accept an (I,J) offset for the starting point into that
/// array in order to describe a sub-array.
/// ScalapackArrayArgs is a "smart struct" that captures those 11 values 
class ScalapackArrayArgs {
public:
    friend std::ostream& operator<<(std::ostream& os, const ScalapackArrayArgs& a);
    sl_int_t    I;
    sl_int_t    J;
    sl_desc_t   DESC;
};

inline std::ostream& operator<<(std::ostream& os, const ScalapackArrayArgs& a)
{
    os << "I:" << a.I << " J:" << a.J << std::endl ;
    os << "DESC:" << std::endl;
    os <<   a.DESC << std::endl ;
    return os;
}

} // namespace scidb

#endif // SCALAPACK_SLAVE__H

