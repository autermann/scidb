#ifndef REFORMAT__HPP
#define REFORMAT__HPP

// std C++
#include <iostream>

// std C

// defacto std
#include <boost/shared_array.hpp>
#include <boost/shared_ptr.hpp>

// SciDB
#include <system/ErrorCodes.h>
#include <system/Exceptions.h>

// local
#include "scalapackFromCpp.hpp"

// local

namespace scidb
{

template<typename int_tt >
inline int_tt ceilScaled(const int_tt& val, const int_tt& s) {
    return (val + s - 1) / s * s ;
} 

template<typename int_tt >
inline int_tt floorScaled(const int_tt& val, const int_tt& s) {
    return val / s * s ;
} 

slpp::desc_t    scidbDistrib(const slpp::desc_t& DESCA);
void            redistScidbToScaLAPACK(double* A, const slpp::desc_t& DESCA,
                                       double* B, const slpp::desc_t& DESCB);

///
/// template argument for the extractToOp<> function
/// 
/// This operator is used as the template arg to extractToOp<Op_tt>(Array)
/// extractToOp passes over every cell of every chunk in the Array
/// at that node and calls Op_tt::operator()(val, row, col).  This operator
/// subtracts ctor arguments {minrow, mincol} from {row,col} and
/// stores the result in "data" which is the local instance's share
/// of a ScaLAPACK-format ScaLAPACK matrix.
///
/// SciDB chunks in psScaLAPACK distribution are written as ScaLAPACK blocks.
/// It is an error to use ReformatToScalapack on SciDB arrays that are not
/// in psScaLAPACK distribution.
/// This is why this class name is "Reformat..." instead of "Redistribute..."
/// 
/// Ctor args: 
/// + data: pointer to the ScaLAPACK array of doubles
/// + desc: the ScaLAPACK descriptor of "data"
/// + (minrow, mincol): the minimum value of the SciDB dimensions, such
/// that the SciDB array value at [minrow,mincol] can be stored at ScaLAPACK
/// location [0,0] (in the global description of both)
/// 
class ReformatToScalapack {
public:
    ReformatToScalapack(double* data, const slpp::desc_t& desc,
                        int64_t minrow, int64_t mincol)
    :
        _data(data),
        _desc(desc),
        _desc_1d(scidbDistrib(desc)),
        _minrow(minrow),
        _mincol(mincol)
    {}

    inline void operator()(double val, size_t row, size_t col)
    {
#if !defined(NDEBUG) && defined(SCALAPACK_DEBUG)
        std::cerr << "ReformatToScalapack::operator()(" << static_cast<void*>(_data)
                  << ", " << row
                  << ", " << col
                  << ", " << val
                  << ");" << std::endl;
#endif
        slpp::int_t R = row-_minrow ;
        slpp::int_t C = col-_mincol ;

#if !defined(NDEBUG) && defined(SCALAPACK_DEBUG)
        std::cerr << "    pdelset_(" << static_cast<void*>(_data)
                  << ", R+1=" << R+1
                  << ", C+1=" << C+1
                  << "_desc , val=" << val << ");" << std::endl ;
#endif
        pdelset_(_data, R+1, C+1, _desc, val);
    }

private:
    double*             _data ;
    slpp::desc_t        _desc ;
    slpp::desc_t        _desc_1d ;
    int64_t             _minrow ;
    int64_t             _mincol ;
};

///
/// template argument for the OpArray<> class
/// 
/// This operator is used to create an array from ScaLAPACK-format memory,
/// by constructing an OpArray<ReformatFromScalapack>.
///
/// Each time the OpArray<Op_tt> must produce a chunk, the chunk is filled
/// by calling Op_tt::operator()(row, col), which returns a double which
/// is the value of the array at SciDB integer dimensions (row,col)
/// 
/// SciDB chunks in psScaLAPACK distribution are read from ScaLAPACK blocks.
/// It is an error to use ReformatFromScalapack to produce a SciDB array that is
/// in psScaLAPACK distribution.
/// This is why this class name is "Reformat..." instead of "Redistribute..."
///
/// Ctor args: 
/// + data: pointer to the ScaLAPACK array of doubles
/// + desc: the ScaLAPACK descriptor of "data"
/// + (minrow, mincol): the minimum value of the SciDB dimensions, such
/// that the ScaLAPACK value [0,0] will be returned as ScidB array [minrow,mincol]
/// 
template<class Data_tt>
class ReformatFromScalapack {
public:
    ReformatFromScalapack(const Data_tt& data, const slpp::desc_t desc,
                              int64_t minrow, int64_t mincol, bool global=false)
    :
        _data(data),
        _desc(desc),
        _minrow(minrow),
        _mincol(mincol),
        _global(global)
    {
        #if !defined(NDEBUG) && defined(SCALAPACK_DEBUG)
            std::cerr << "ReformatFrom _desc.DTYPE = " << _desc.DTYPE << std::endl; 
            std::cerr << "ReformatFrom _desc.CTXT = " << _desc.CTXT << std::endl; 
            std::cerr << "ReformatFrom _desc.M,N = " << _desc.M << "," << _desc.N << std::endl; 
            std::cerr << "ReformatFrom _desc.MB,NB = " << _desc.MB << "," << _desc.NB << std::endl; 
            std::cerr << "ReformatFrom _desc.RSRC,CSRC = " << _desc.RSRC << "," << _desc.CSRC << std::endl; 
            std::cerr << "ReformatFrom _desc.LLD = " << _desc.LLD << std::endl; 
        #endif
    }

    inline double operator()(int64_t row, int64_t col) const
    {
        // I make it work in the local-only case by using a space for the
        // first two parameters.  This only permits it to work in the local process,
        // and not in SPMD style. Otherwise it might have been more like
        // the line that preceds it.  But we are going to let SciDB do any post-operator
        // redistribution to other instances, since it uses a scheme that differs from
        // ScaLAPACK

        // SPMD: pdelget_('A', ' ', val, _data.get(), row-_minrow+1, col-_mincol+1, _desc);
        double val = static_cast<double>(0xbadbeef); // not strictly necessary

        // note: haven't seen a global matrix yet, so that's only handled for the
        //       1D case operator()(row), below
        pdelget_(' ', ' ', val, _data.get(), row-_minrow+1, col-_mincol+1, _desc);

        #if !defined(NDEBUG) && defined(SCALAPACK_DEBUG)
            std::cerr << "ReformatFrom::operator()("<<row<<","<<col<<")" << std::endl ;
            std::cerr << "    minrow,mincol:"<<_minrow<<","<<_mincol<<")" << std::endl ;
            std::cerr << "    pdelget_(" << static_cast<void*>(_data.get())
                      << ", " << row - _minrow + 1
                      << ", " << col - _mincol + 1
                      << ", " << val
                      << ");" << std::endl;
        #endif
        // TODO JHM ; check info here and in ReformatToScalapack
        return val;
    }

    // single-dimension version such as the 'values' of an SVD
    inline double operator()(int64_t row) const {
        using namespace scidb;

        #if !defined(NDEBUG) && defined(SCALAPACK_DEBUG)
            std::cerr << "ReformatFrom::operator()("<<row<<");" << std::endl;
        #endif

        double val = static_cast<double>(0xbadbeef); // not strictly necessary

        slpp::int_t R = row-_minrow ;
        if(_global) {
            // like the S vector output by pdgesvd() ... available at every host
            // so we can just take the value directly from the array
            val = _data.get()[R] ;
            if(R >= _desc.M) {
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << "R >= _desc.M");
            }

            #if !defined(NDEBUG) && defined(SCALAPACK_DEBUG)
                std::cerr << "    _data.get()[R]="<<R<<"]" << "; val <- " << val << std::endl; 
            #endif
        } else {
            pdelget_(' ', ' ', val, _data.get(), row-_minrow+1, 1,  _desc);

            #if !defined(NDEBUG) && defined(SCALAPACK_DEBUG)
                std::cerr << "    pdelget_(' ', ' ', val, _data.get(), R+1=" << R+1
                          << ", C==1" << ", _desc={M=" << _desc.M 
                          << ", N=" << _desc.N << "})" << "; val <- " << val << std::endl; 
            #endif
        }
        return val;
    }

private:
    Data_tt             _data ;
    slpp::desc_t        _desc ;
    int64_t             _minrow ;
    int64_t             _mincol ;
    bool                _global ;
};

} // end namespace scidb

#endif // REFORMAT__HPP
