///
/// ScaLAPACKPhysical.hpp
///
///

#ifndef SCALAPACKPHYSICAL_HPP_
#define SCALAPACKPHYSICAL_HPP_

// std C++

// std C

// de-facto standards

// SciDB
#include <query/Query.h>

// more SciDB
#include <mpi/MPIPhysical.hpp>                    // NOTE: there are many handy helpers in this lower level, worth perusing

// local
#include "scalapackFromCpp.hpp"   // TODO JHM : rename slpp::int_t



namespace scidb {

// handy inline, divide, but if there is a remainder, go to the next higher number
// e.g. the number of blocks/groups of size divisor required to hold val units total.
template<typename int_tt>
inline int_tt divCeil(int_tt val, int_tt divisor) {
    return (val + divisor - 1) / divisor ;
}

// handy inline, round up to a multiple of factor
template<typename int_tt>
inline int_tt roundUp(int_tt val, int_tt factor) {
    return divCeil(val, factor) * factor ;
}

/// call with twice the length for complex or complex-double
/// rather than setting real_tt to a struct
/// we'll worry about specializing to memset() or bzero() for integer
/// types at a later time.
template<class val_tt>
void valsSet(val_tt* dst, val_tt val, size_t numVal) {
    // trivially unrollable
    for (size_t jj=0; jj < numVal; jj++) {
        dst[jj] = val;
    }
}

template<class float_tt>
void setInputMatrixToAlgebraDefault(float_tt* dst, size_t numVal) {
    valsSet(dst, float_tt(0), numVal); // empty cells imply zero

    enum dummy {DBG_DENSE_ALGEBRA_WITH_NAN_FILL=0};  // won't be correct if empty cells present
    if(DBG_DENSE_ALGEBRA_WITH_NAN_FILL) {
        valsSet(dst, ::nan(""), numVal); // any non-signalling nan will do
        std::cerr << "@@@@@@@@@@@@@ WARNING: prefill matrix memory with NaN for debug" << std::endl ;
    }
}

template<class float_tt>
void setOutputMatrixToAlgebraDefault(float_tt* dst, size_t numVal) {
    valsSet(dst, ::nan(""), numVal); // ScaLAPACK algorithm should provide all entries in matrix
}

void checkBlacsInfo(shared_ptr<Query>& query, slpp::int_t ICTXT, slpp::int_t NPROW, slpp::int_t NPCOL,
                                                                 slpp::int_t MYPROW, slpp::int_t MYPCOL) ;

///
/// ScaLAPACK computation routines are only efficient for a certain
/// range of sizes and are generally only implemented for
/// square block sizes.  Check these constraints
///
void checkInputArray(Array* Ain);
void extractArrayToScaLAPACK(boost::shared_ptr<Array>& array, double* dst, slpp::desc_t& desc);

class ScaLAPACKPhysical : public MPIPhysical
{
public:
    static const slpp::int_t DEFAULT_BAD_INFO = -99;                 // scalapack negative errors are the position of the bad argument


ScaLAPACKPhysical(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema)
    :
        MPIPhysical(logicalName, physicalName, parameters, schema)
    {
    }

    // standard API
    virtual bool                    changesDistribution(const std::vector<ArrayDesc> & inputSchemas) const
    { return true; }

    virtual ArrayDistribution       getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                          const std::vector< ArrayDesc> & inputSchemas) const
    { return ArrayDistribution(psScaLAPACK); }

    virtual bool                    requiresRepart(ArrayDesc const& inputSchema) const;
    virtual ArrayDesc               getRepartSchema(ArrayDesc const& inputSchema) const;

    // extending API
    std::vector<shared_ptr<Array> > redistributeInputArrays(std::vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query);
    bool                            doBlacsInit(std::vector< shared_ptr<Array> >& redistInputs, shared_ptr<Query> query);

};

} // namespace

#endif /* SCALAPACKPHYSICAL_HPP_ */
