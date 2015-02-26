#ifndef SLPP__HPP
#define SLPP__HPP
//
/// these declarations allow C++ to make the following scalapack execute correctly
/// though ScaLAPACK is written in FORTRAN
///

// TODO JHM ; need revised naming scheme and organization of ScaLAPACK "helper" code in general

#include <ostream>

#include <sys/types.h>

namespace slpp {

    enum dummy { SCALAPACK_EFFICIENT_BLOCK_SIZE=32, SCALAPACK_MAX_BLOCK_SIZE=128 };

    // change type of integer_t to match how scalapack is compiled!!
    typedef int32_t int_t ;  // has to change depending on whether ScaLAPACK fortran 'INTEGER' is 4-byte or 8-byte ints
                             // have to find a way to reduce this impact ... we'll get compiler warnings one way or the
                             // the other as the width of this changes.  Will have to isolate better.
                             // DARN to ScaLAPACK and LAPACK for not defining the interfaces as INTEGER*8 instead.
    class desc_t {
    public:
        friend  std::ostream& operator<<(std::ostream& os, const desc_t& a);
        int_t   DTYPE ;
        int_t   CTXT ;
        int_t   M, N ;
        int_t   MB, NB ;
        int_t   RSRC, CSRC ;
        int_t   LLD ;     // 9 fields total
    };


    inline std::ostream& operator<<(std::ostream& os, const desc_t& a)
    {
        // indent of one space helps set it off from the array info it is typically
        //   nested inside of, when printed
        // an indent argument would be better, but breaks the use of operator<<()
        // a better solution is surely possible.
        os << " DTYPE:" << a.DTYPE << " CTXT:" << a.CTXT << std::endl;
        os << " M:" << a.M << " N:" << a.N << std::endl;
        os << " MB:" << a.MB << " NB:" << a.NB << std::endl;
        os << " RSRC:" << a.RSRC << " CSRC:" << a.CSRC << std::endl;
        os << " LLD:" << a.LLD ;
        return os;
    }
}

namespace blacs {
    typedef int32_t int_t ;
}
namespace mpi {
    typedef int32_t int_t ;
}


extern "C" {
    // these declarations let one call the FORTRAN-interface-only ScaLAPACK FORTRAN code
    // correctly from non-FORTRAN code.  The "extern "C"" prevents name-mangling which
    // FORTRAN doesn't do, unfortunately there is no "extern "FORTRAN"" to add the trailing _
    // 


    // scalapack
    typedef slpp::int_t sl_int_t;
    typedef slpp::desc_t sl_desc_t;

    void sl_init_(sl_int_t&, sl_int_t&, sl_int_t&);

    // scalapack tools
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


    // scalapack redist
    void pdgemr2d_(const sl_int_t& M, const sl_int_t& N,
                   void* A, const sl_int_t& IA, const sl_int_t& JA, const sl_desc_t& DESC_A,
                   void* B, const sl_int_t& IB, const sl_int_t& JB, const sl_desc_t& DESC_B,
                   const sl_int_t& GCONTEXT);

    // scalapack
        // matrix multiply
    void pdgemm_(const char &TRANSA, const char &TRANSB,
                 const sl_int_t& M, const sl_int_t &N, const sl_int_t &K,
                 double *ALPHA,
                 double *A, const sl_int_t &IA, const sl_int_t &JA, const sl_desc_t& DESC_A,
                 double *B, const sl_int_t &IB, const sl_int_t &JB, const sl_desc_t& DESC_B,
                 double *BETA,
                 double *C, const sl_int_t &IC, const sl_int_t &JC, const sl_desc_t& DESC_C);
        // SVD
    void pdgesvd_(const char &jobU, const char &jobVT,
                  const sl_int_t& M, const sl_int_t &N,
                  double *A, const sl_int_t &IA, const sl_int_t &JA, const sl_desc_t& DESC_A,
                  double *S,
                  double *U,  const sl_int_t &IU,  const sl_int_t &JU,  const sl_desc_t& DESC_U,
                  double *VT, const sl_int_t &IVT, const sl_int_t &JVT, const sl_desc_t& DESC_VT,
                  double *WORK, const sl_int_t &LWORK, sl_int_t &INFO);

    // blacs
    typedef blacs::int_t bl_int_t;
    void blacs_pinfo_(bl_int_t& mypnum, bl_int_t& nprocs);
    void blacs_get_(const bl_int_t& ICTXT, const bl_int_t& WHAT, bl_int_t& VAL);
    void blacs_gridinit_(const bl_int_t& ICTXT, const char&,
                         const bl_int_t& NPROW, const bl_int_t& NPCOL);
    void blacs_gridinfo_(const bl_int_t&, const bl_int_t&,
                         const bl_int_t&, bl_int_t&, bl_int_t&);
    bl_int_t blacs_pnum_(const bl_int_t& cntxt, const bl_int_t& myPRow, const bl_int_t& myPCol);
    void blacs_gridexit_(const bl_int_t&);
    void blacs_exit_(const bl_int_t&);
}

#endif // SLPP__HPP
