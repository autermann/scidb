///
/// ScaLAPACKLogical.hpp
///
///

#ifndef SCALAPACKPHYSICAL_HPP_
#define SCALAPACKPHYSICAL_HPP_


// std C
// std C++
// de-facto standards

// SciDB
#include <query/Query.h>
#include <query/Operator.h>
#include <system/Exceptions.h>
#include <system/BlockCyclic.h>

// locals
#include "DLAErrors.h"
#include "scalapackFromCpp.hpp"   // TODO JHM : rename slpp::int_t


namespace scidb {

/// no actual ScaLAPACKLogical class yet,
/// just helpers for the Logicals are all that are needed so far

/// returns or throws an exception if the input matrices are not suitable for ScaLAPACK
/// processing.
void                      checkScaLAPACKInputs(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query,
                                               size_t nMatsMin, size_t nMatsMax);

/// constructs distinct dimension names, from names that may or may not be distinct
std::pair<string, string> ScaLAPACKDistinctDimensionNames(const string& a, const string& b);


} // namespace

#endif /* SCALAPACKPHYSICAL_HPP_ */
