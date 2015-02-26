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

class GEMMLogical: public LogicalOperator
{
public:
    GEMMLogical(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
        ADD_PARAM_INPUT()
        ADD_PARAM_INPUT()
        // TODO: add arguments for TRANSA, TRANSB, possibly via:
        //       ADD_PARAM_CONSTANT("string") and later:
        //       const string& transAStr = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[0])->getExpression(), query, TID_STRING).getString();
        //       possibly more flexible if the argument is a bool or zero vs nonzero integer
        // TOOD: Note that TRANS is standard ScaLAPACK shorthand for transpose / conjugate transpose
        //       in the case of complex numbers.
        //
        // TODO: add arguments for ALPHA, BETA, possibly via ADD_PARAM_CONSTANT("double") ?
        // also need two doubles for Alpha, Beta
        // TODO: note that ALPHA and BETA are arguments from the PDGEMM ScaLAPACK call and to
        //       see that for documentation on their meaning.  Give a pointer to the netlib
        //       refernce page.
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query);
};

ArrayDesc GEMMLogical::inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
{
    // TODO: refactor and re-use checks on schemas by parameterizing the restrictions
    //       many of these restrictions apply equally well to SVD and other operators.
    //       Parameterize and refactor those restrictions to ScaLAPACKPhysical.
    //

    enum dummy  {ROW=0, COL=1};
    enum dummy2 {AA=0, BB, CC, NUM_MATRICES};  // which matrix: f(AA,BB,CC) = alpha AA BB + beta CC

    assert(schemas.size() == NUM_MATRICES);

    //
    // per-array checks
    //
    checkScaLAPACKInputs(schemas, query, NUM_MATRICES, NUM_MATRICES);


    //
    // cross-matrix constraints:
    //

    // check: cross-argument sizes
    const Dimensions& dimsAA = schemas[AA].getDimensions();
    const Dimensions& dimsBB = schemas[BB].getDimensions();
    const Dimensions& dimsCC = schemas[CC].getDimensions();

    if (dimsAA[COL].getLength() != dimsBB[ROW].getLength()) {
        throw (PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR4)
               << "(first matrix trailing dimension must match second matrix leading dimension)");
    }
    if (dimsAA[ROW].getLength() != dimsCC[ROW].getLength()) {
        throw (PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR4)
               << "(first and third matrix must have the same leading dimension)");
    }
    if (dimsBB[COL].getLength() != dimsCC[COL].getLength()) {
        throw (PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR4)
               << "(first and third matrix must have the same trailing dimension)");
    }

    // TODO: check: ROWS * COLS is not larger than largest ScaLAPACK fortran INTEGER

    // TODO: check: total size of "work" to scalapack is not larger than largest fortran INTEGER
    //       hint: have Cmake adjust the type of slpp::int_t
    //       hint: maximum ScaLAPACK WORK array is usually determined by the function and its argument sizes


    //
    // inputs look good, create and return the output schema
    // note that the output has the dimensions and name bases of the third argument C
    // so that we can iterate on C, by repeating the exact same query,
    // we are SUPER careful not to change its dim names if they are already distinct.
    //
    
    std::pair<string, string> distinctNames = ScaLAPACKDistinctDimensionNames(dimsCC[ROW].getBaseName(),
                                                                              dimsCC[COL].getBaseName());
    Dimensions outDims(2);
    outDims[ROW] = DimensionDesc(distinctNames.first,
                                 dimsCC[ROW].getStart(),
                                 dimsCC[ROW].getCurrStart(),
                                 dimsCC[ROW].getCurrEnd(),
                                 dimsCC[ROW].getEndMax(),
                                 dimsCC[ROW].getChunkInterval(),
                                 0,
                                 dimsCC[ROW].getType(),
                                 dimsCC[ROW].getFlags(),
                                 dimsCC[ROW].getMappingArrayName(),
                                 dimsCC[ROW].getComment(),
                                 dimsCC[ROW].getFuncMapOffset(),
                                 dimsCC[ROW].getFuncMapScale());

    outDims[COL] = DimensionDesc(distinctNames.second,
                                 dimsCC[COL].getStart(),
                                 dimsCC[COL].getCurrStart(),
                                 dimsCC[COL].getCurrEnd(),
                                 dimsCC[COL].getEndMax(),
                                 dimsCC[COL].getChunkInterval(),
                                 0,
                                 dimsCC[COL].getType(),
                                 dimsCC[COL].getFlags(),
                                 dimsCC[COL].getMappingArrayName(),
                                 dimsCC[COL].getComment(),
                                 dimsCC[COL].getFuncMapOffset(),
                                 dimsCC[COL].getFuncMapScale());

    Attributes atts(1); atts[0] = AttributeDesc(AttributeID(0), "gemm", TID_DOUBLE, 0, 0);
    return ArrayDesc("GEMM", atts, outDims);
}

REGISTER_LOGICAL_OPERATOR_FACTORY(GEMMLogical, "gemm");

} //namespace
