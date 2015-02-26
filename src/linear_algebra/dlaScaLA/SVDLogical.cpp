/*
**
* BEGIN_COPYRIGHT
*
* PARADIGM4 INC.
* This file is part of the Paradigm4 Enterprise SciDB distribution kit
* and may only be used with a valid Paradigm4 contract and in accord
* with the terms and conditions specified by that contract.
*
* Copyright © 2010 - 2012 Paradigm4 Inc.
* All Rights Reserved.
*
* END_COPYRIGHT
*/

#include <boost/numeric/conversion/cast.hpp>

#include <query/Operator.h>
#include <query/OperatorLibrary.h>
#include <system/Exceptions.h>
#include <system/SystemCatalog.h>
#include <system/BlockCyclic.h>

#include "DLAErrors.h"
#include "scalapackUtil/ScaLAPACKLogical.hpp"



using namespace scidb;

namespace scidb
{

inline bool hasSingleAttribute(ArrayDesc const& desc)
{
    return desc.getAttributes().size() == 1 || (desc.getAttributes().size() == 2 && desc.getAttributes()[1].isEmptyIndicator());
}

///
/// handy inline, rounds up instead of down like int division does
/// good for, e.g. calculating block sizes
template<typename int_tt>
inline int_tt divCeil(int_tt val, int_tt divisor) {
    return (val + divisor - 1) / divisor ;
}

class SVDLogical: public LogicalOperator
{
public:
    SVDLogical(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
        ADD_PARAM_CONSTANT("string")
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query);
};

ArrayDesc SVDLogical::inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
{
    enum dumm { SINGLE_MATRIX = 1 };
    assert(schemas.size() == SINGLE_MATRIX);

    if(schemas.size() < 1)
        throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR2);

    //
    // per-array checks
    //
    checkScaLAPACKInputs(schemas, query, SINGLE_MATRIX, SINGLE_MATRIX);

    // TODO: check: ROWS * COLS is not larger than largest ScaLAPACK fortran INTEGER

    // TODO: check: total size of "work" to scalapack is not larger than largest fortran INTEGER
    //       hint: have Cmake adjust the type of slpp::int_t
    //       hint: maximum ScaLAPACK WORK array is usually determined by the function and its argument sizes

    const string& whichMatrix = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[0])->getExpression(), query, TID_STRING).getString();

    const Dimensions& dims = schemas[0].getDimensions();
    size_t minRowCol = std::min(dims[0].getLength(),
                                dims[1].getLength());

    const size_t ZERO_OUTPUT_OVERLAP = 0;
    // TODO: Question: Should these be case-insensitive matches?
    if (whichMatrix == "U" || whichMatrix == "left") // most frequent, and less-frequent names
    {
        Dimensions outDims(2);
        std::pair<string, string> distinctNames = ScaLAPACKDistinctDimensionNames(dims[0].getBaseName(),
                                                                                  "i"); // conventional subscript for sigma
        // nRow out is in the same space as nRow in
        outDims[0] = DimensionDesc(distinctNames.first,
                                   dims[0].getStart(),
                                   dims[0].getCurrStart(),
                                   dims[0].getCurrEnd(),
                                   dims[0].getEndMax(),
                                   dims[0].getChunkInterval(),
                                   ZERO_OUTPUT_OVERLAP,
                                   dims[0].getType(),
                                   dims[0].getFlags(),
                                   dims[0].getMappingArrayName(),
                                   dims[0].getComment(),
                                   dims[0].getFuncMapOffset(),
                                   dims[0].getFuncMapScale());

       // nCol out has size min(nRow,nCol).  It takes us to the subspace of the diagonal matrix "SIGMA"
       // note that it in a different basis than the original, so it cannot actually
       // share any meaningful integer or non-integer array dimensions with them.
       // therefore it uses just the interval 0 to minRowCol-1
       outDims[1] = DimensionDesc(distinctNames.second,
                                  Coordinate(0),                // start
                                  Coordinate(0),                // curStart
                                  Coordinate(minRowCol - 1),    // end
                                  Coordinate(minRowCol - 1),    // curEnd
                                  dims[1].getChunkInterval(),   // inherit
                                  ZERO_OUTPUT_OVERLAP,
                                  TID_INT64);

        Attributes atts(1); atts[0] = AttributeDesc((AttributeID)0, "u", TID_DOUBLE, 0, 0);
        return ArrayDesc("U", atts, outDims);
    }
    else if (whichMatrix == "VT" || whichMatrix == "right")
    {
        Dimensions outDims(2);
        std::pair<string, string> distinctNames = ScaLAPACKDistinctDimensionNames("i", // conventional subscript for sigma
                                                                                  dims[1].getBaseName());

        // nRow out has size min(nRow,nCol). It takes from the subspace of the diagonal matrix "SIGMA"
        // note that it in a different basis than the original, so it cannot actually
        // share any meaningful integer or non-integer array dimensions with them.
        // therefore it uses just the interval 0 to minRowCol-1
        outDims[0] = DimensionDesc(distinctNames.first,
                                   Coordinate(0),  // start
                                   Coordinate(0),  // curStart
                                   Coordinate(minRowCol - 1), // end
                                   Coordinate(minRowCol - 1), // curEnd
                                   dims[1].getChunkInterval(), // inherit
                                   ZERO_OUTPUT_OVERLAP,
                                   TID_INT64);

        // nCol out is in the same space as nCol in
        outDims[1] = DimensionDesc(distinctNames.second,
                                dims[1].getStart(),
                                dims[1].getCurrStart(),
                                dims[1].getCurrEnd(),
                                dims[1].getEndMax(),
                                dims[1].getChunkInterval(),
                                ZERO_OUTPUT_OVERLAP,
                                dims[1].getType(),
                                dims[1].getFlags(),
                                dims[1].getMappingArrayName(),
                                dims[1].getComment(),
                                dims[1].getFuncMapOffset(),
                                dims[1].getFuncMapScale());

        Attributes atts(1); atts[0] = AttributeDesc((AttributeID)0, "v", TID_DOUBLE, 0, 0);
        return ArrayDesc("VT", atts, outDims);
    }
    else if (whichMatrix == "S" || whichMatrix == "SIGMA" || whichMatrix == "values")
    {
        Dimensions outDims(1);
        // nRow out has size min(nRow,nCol), and is not in the same dimensional space as the original
        // note that it in a different basis than the original, so it cannot actually
        // share any meaningful integer or non-integer array dimensions with them.
        // therefore it uses just the interval 0 to minRowCol-1
        outDims[0] = DimensionDesc("i",            // conventional subscript for sigma
                                   Coordinate(0),  // start
                                   Coordinate(0),  // curStart
                                   Coordinate(minRowCol - 1), // end
                                   Coordinate(minRowCol - 1), // curEnd
                                   dims[1].getChunkInterval(), // inherit
                                   ZERO_OUTPUT_OVERLAP,
                                   TID_INT64);

        Attributes atts(1); atts[0] = AttributeDesc((AttributeID)0, "sigma", TID_DOUBLE, 0, 0);
        return ArrayDesc("SIGMA", atts, outDims);
    } else {
        throw PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR33);
    }
}

REGISTER_LOGICAL_OPERATOR_FACTORY(SVDLogical, "gesvd");

} //namespace
