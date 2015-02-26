/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2013 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/
///
/// ScaLAPACKPhysical.hpp
///
///

#ifndef SCALAPACKPHYSICAL_HPP_
#define SCALAPACKPHYSICAL_HPP_

// std C++
#include <tr1/array>

// std C

// de-facto standards

// SciDB
#include <query/Query.h>

// MPI/ScaLAPACK
#include <mpi/MPIPhysical.hpp>                    // NOTE: there are many handy helpers in this lower level, worth perusing
#include <scalapackUtil/dimUtil.hpp>
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
                                                                 slpp::int_t MYPROW, slpp::int_t MYPCOL, const std::string& callerLabel) ;

///
/// ScaLAPACK computation routines are only efficient for a certain
/// range of sizes and are generally only implemented for
/// square block sizes.  Check these constraints
///
void extractArrayToScaLAPACK(boost::shared_ptr<Array>& array, double* dst, slpp::desc_t& desc);

class ScaLAPACKPhysical : public MPIPhysical
{
public:
    static const slpp::int_t DEFAULT_BAD_INFO = -99;                 // scalapack negative errors are the position of the bad argument

    /**
     * @see     MPIPhysical::MPIPhysical
     * 
     * @param rule                  certain operators have constraints on the shape of their processor grid
     */
    enum GridSizeRule_e             {RuleInputUnion=0, RuleNotHigherThanWide};
    ScaLAPACKPhysical(const std::string& logicalName, const std::string& physicalName,
                      const Parameters& parameters, const ArrayDesc& schema,
                      GridSizeRule_e gridRule=RuleInputUnion)
    :
        MPIPhysical(logicalName, physicalName, parameters, schema),
        _gridRule(gridRule)
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
    std::vector<shared_ptr<Array> > redistributeInputArrays(std::vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query>& query, const std::string& callerLabel);
    /**
     * Initialize the ScaLAPACK BLACS (Basic Linear Algebra Communications Systems).
     * @param redistInputs  The final inputs to the operator (already repartitioned and redistributed
     * @param query         Current query
     * @return              Whether the instance participates in the ScaLAPACK computation or may instead
     */
    bool                            doBlacsInit(std::vector< shared_ptr<Array> >& redistributedInputs, shared_ptr<Query>& query, const std::string& callerLabel);

    /**
     * compute the correct ScaLAPACK BLACS process grid size for a particular set of input Arrays (Matrices)
     * @param redistributedInputs   the matrices
     * @param query                 current query 
     * @param callerLabel           identify the context of the call, essential for nested operator debugging
     * @return                      the BLACS grid size
     */
    virtual procRowCol_t            getBlacsGridSize(std::vector< shared_ptr<Array> >& redistributedInputs, shared_ptr<Query>& query, const std::string& callerLabel);

    /**
     * a standard way to test INFO returned by a slave,
     * logging and raising any error in a standardized way.
     * @param INFO          the ScaLAPACK INFO value as returned by MPISlaveProxy::waitForStatus()
     * @param operatorName  the ScaLAPACK operator name, e.g "pdgemm" or "pdgesvd" (do not include "Master" suffix)
     */
    void                            raiseIfBadResultInfo(sl_int_t INFO, const std::string& operatorName) const ;

protected:
    /// routines that make dealing with matrix parameters
    /// more readable and less error prone

    /// a structure to retrieve matrix parameters as a short vector -> 1/2 as many LOC as above
    /// very handy for the operators
    typedef std::tr1::array<size_t, 2 > matSize_t;
    /// get matrix size as vector
    matSize_t getMatSize(boost::shared_ptr<Array>& array) const;
    /// get matrix chunk size as vector
    matSize_t getMatChunkSize(boost::shared_ptr<Array>& array) const;

    void checkInputArray(boost::shared_ptr<Array>& Ain) const ;

private:
    GridSizeRule_e          _gridRule;  // some operators need special rules for determining the
                                        // best way to map their matrices to the processor grid
};


inline ScaLAPACKPhysical::matSize_t ScaLAPACKPhysical::getMatSize(boost::shared_ptr<Array>& array) const
{
    assert(array->getArrayDesc().getDimensions().size() == 2);

    matSize_t result;
    result.at(0) = array->getArrayDesc().getDimensions()[0].getLength();
    result.at(1) = array->getArrayDesc().getDimensions()[1].getLength();
    return result;
}


inline ScaLAPACKPhysical::matSize_t ScaLAPACKPhysical::getMatChunkSize(boost::shared_ptr<Array>& array) const
{
    assert(array->getArrayDesc().getDimensions().size() == 2);

    matSize_t result;
    result.at(0) = array->getArrayDesc().getDimensions()[0].getChunkInterval();
    result.at(1) = array->getArrayDesc().getDimensions()[1].getChunkInterval();
    return result;
}

} // namespace

#endif /* SCALAPACKPHYSICAL_HPP_ */
