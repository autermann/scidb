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

#include <sstream>

#include <log4cxx/logger.h>

#include <array/Array.h>
#include "../array/ArrayExtractOp.hpp"
#include "../scalapackUtil/scalapackFromCpp.hpp"

using namespace scidb;

namespace scidb
{
// TODO: put me back in operator
// allocate scalapack-format memory for the inputArray
// inputMem = new double(rows * cols);


slpp::desc_t scidbDistrib(const slpp::desc_t& DESCA)
{
    // A is distributed cyclically in 1 dimension only.
    // we want to redistribute it to an aribitrary processor grid
    // using pdgemr2d.  To do this, we need a ScaLAPACK distribution
    // which matches the block distribution of A-in-scidb
    //
    // If we treat A as a single row of blocks, that could map onto the procesor
    // grid in the same way.  We could then copy submatrix rows of the 1-D
    // into the destination matrix with whatever distribution it had when givin
    // to this routine.
    //
    // DESCA will have the correct M,N, MB,NB etc, but it will have an illegal
    // DTYPE because its not in normal block-cyclic.  Well'll make a new descriptor
    // and then access it in chunk-row segments.

    size_t heightInChunks = (DESCA.M+DESCA.MB-1)/DESCA.MB ; // divide, rounding up

    slpp::desc_t DESC_SCIDB(DESCA) ; // most things stay the same
    DESC_SCIDB.DTYPE = 1 ;
    DESC_SCIDB.CTXT = -1 ;  // we'll use B's context to do the work
    DESC_SCIDB.M = min(DESCA.M, DESCA.MB);
    DESC_SCIDB.N = DESCA.N * heightInChunks;

    return DESC_SCIDB;
}

#if 0 // experiment for later milestone
void redistScidbToScaLAPACK(double *A, const slpp::desc_t& DESCA, double *B, const slpp::desc_t& DESCB)
{
    // see note above about how DESC_SCIDB is a view of Scidb data that ScaLAPACK
    // can access with a 1D block-cyclic
    slpp::desc_t DESCA_SCIDB = scidbDistrib(DESCA) ; 

    std::cerr << "redistScidbToScaLAPACK: DESCA ************" << std::endl ;
    std::cerr << DESCA  << std::endl;
    std::cerr << "redistScidbToScaLAPACK: DESCA_SCIDB ************" << std::endl ;
    std::cerr << DESCA_SCIDB  << std::endl;
    std::cerr << "redistScidbToScaLAPACK: DESCB ************" << std::endl ;
    std::cerr << DESCB  << std::endl;

    size_t heightInChunks = (DESCA.M+DESCA.MB-1)/DESCA.MB ; // divide, rounding up
    size_t widthInChunks =  (DESCA.N+DESCA.NB-1)/DESCA.NB ; // divide, rounding up
    for(size_t rowChunk=0 ; rowChunk < heightInChunks ; rowChunk++) {
        // or row=0 ; row < M, row += MB

        // copy a row from A, using DESCA_SCIDB as a trick
        slpp::int_t blockRows = std::min(DESCA.MB, DESCA.M-slpp::int_t(rowChunk)*DESCA.MB) ;
        size_t Arow = rowChunk*heightInChunks;
        size_t A1Dcolumn = rowChunk*widthInChunks;

        std::cerr <<"redistScidbToScaLAPACK: rowChunk: " << rowChunk << std::endl; 
        std::cerr <<"redistScidbToScaLAPACK: blockRows: " << blockRows << std::endl; 
        std::cerr <<"redistScidbToScaLAPACK: blockCols: " << DESCA.N << std::endl; 
        std::cerr <<"redistScidbToScaLAPACK: JA: " << A1Dcolumn << std::endl; 
        std::cerr <<"redistScidbToScaLAPACK: Arow: " << Arow << std::endl; 
        pdgemr2d_(blockRows, DESCA.N, A, /*IA*/0, /*JA*/A1Dcolumn, DESCA_SCIDB, B, Arow, 0, DESCB, DESCB.CTXT);
        // TODO: where's INFO in that call? how do I check for failure?
    }
}
#endif



} // end namespace scidb
