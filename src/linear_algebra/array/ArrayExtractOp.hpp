#ifndef ARRAYEXTRACTOP_HPP_
#define ARRAYEXTRACTOP_HPP_

///
/// ArrayExtractOp.hpp
///

/*
**
* BEGIN_COPYRIGHT
*
* Copyright © 2012 SciDB
* Copyright © 2011-2012 Paradigm4 Inc.
* All Rights Reserved.
*
* END_COPYRIGHT
*/

#include <sstream>

#include "log4cxx/logger.h"
#include "array/Metadata.h"
#include "array/MemArray.h"
#include <array/Array.h>

namespace scidb
{
static log4cxx::LoggerPtr extractOpLogger(log4cxx::Logger::getLogger("scidb.libdense_linear_algebra.array.extractOp"));


///
/// template version of copyStride() that extracts the data
/// to an operator, rather than directly into memory
/// this is the one to call
///
template<class ExtractOp_tt>
char* copyStrideToOp(char* src, scidb::Coordinates const& first, scidb::Coordinates const& last, scidb::Coordinates const& chunkPos, scidb::Dimensions const& dims, size_t attrSize, ExtractOp_tt& extractOp)
{
    scidb::Coordinates firstUpdated(dims.size());
    for(size_t i=0; i < dims.size(); i++) {
        firstUpdated[i] = first[i];
    }
    return copyStrideToOp2(src, firstUpdated, last, chunkPos, dims, attrSize, 0, extractOp);
}

///
/// template version of the recursive portion of copyStrideToOp, above
/// it is modeleded after copyStride()
/// this is close to being sufficiently general that copyStride() could be built on top
/// of this, by applying a copy-to-memory operator, and it should reach the same
/// speed, but then the implementation will be shared
///

template<class ExtractOp_tt>
static char* copyStrideToOp2(char* src, scidb::Coordinates& first, scidb::Coordinates const& last, scidb::Coordinates const& chunkPos, scidb::Dimensions const& dims, size_t attrSize, size_t d, ExtractOp_tt& extractOp)
{
    const bool DBG=false ;

    assert(attrSize == sizeof(double));
    size_t chunkSize = dims[d].getChunkInterval();
    if(DBG) {
        std::cerr << "copyStrideToOp2: d= " << d << " first= (" << first[0] << "," << first[1] << ")"
                                                 << " last= (" << last[0]  << "," << last[1] << std::endl;
        std::cerr << "copyStrideToOp2: chunkPos: (" << chunkPos[0] << "," << chunkPos[1] << ")" << std::endl;
    }


    if (d == dims.size()-1) {
        // reached the final dimension, there won't be any change in rows
        double * vals = reinterpret_cast<double*>(src);
        size_t row = first[0] + chunkPos[0];
        size_t colFirst = first[1] + chunkPos[1];
        if(DBG) std::cerr << "copyStrideToOp2: colFirst: " << colFirst << std::endl;

        for(size_t i=0; i<chunkSize; i++) {
            if(DBG) std::cerr << "copyStrideToOp2: (row,col)=  (" << row << "," << colFirst+i << ")" << std::endl ;
            extractOp(vals[i], row, colFirst+i);
        }
        src += chunkSize*attrSize;
    } else {
        scidb::Coordinates myFirst(dims.size());
        scidb::Coordinates myLast(dims.size());
        for(size_t i=0; i < dims.size(); i++) {
            myFirst[i] = first[i];   // TODO: consider giving Coords a copy ctor
            myLast[i] =  last[i];
        }

        for (size_t i = 0; i < chunkSize; i++) {
            // TODO: verify that the complier turns this tail recursion into iteration
            //       else recode it as such.  Removal of tail recursion would allow
            //       the loop to be unrolled, and copyStrideToOp2 to be further pipelined
            src = copyStrideToOp2(src, myFirst, myLast, chunkPos, dims, attrSize, d+1, extractOp);
            myFirst[d]++ ; // next "row" (or higher dimension, as appropriate)
            myLast[d]++ ; // next "row" (or higher dimension, as appropriate)
        }
    }

    return src;
}



/// The following is modified from Array::extractData() and copyStride()
/// if those change, then this must change as well until the commonality can be factored
/// without reducing the speed of this version.
///
/// This version does a calculation at each value rather than copying it to another
/// piece of memory, and therefore avoids tripling the bandwidth required.
/// Since chunks don't fit in the cache, this is important, because we are really talking
/// memory bandwith.
///
/// Future notes:
/// this suggests we need extractDataTile() which only extracts a tile's worth at a time
/// even better, the code below should be factored to be an item-by-item iterator
/// or a tile-row by tile-row iterator that should be sufficient to amortize the cost
/// of the external loop control, while limiting the complexity to a single dimension
/// Then this routine, extractData(), the proposed extractDataTile(), and many others
/// can all use that implementation.  In theory, it should be possible to make that
/// implementation no slower than this form due to inline template expansion.
template<class ExtractOp_tt>
void extractDataToOp(shared_ptr<scidb::Array> array, scidb::AttributeID attrID, scidb::Coordinates const& first, scidb::Coordinates const& last, ExtractOp_tt& extractOp)
{
    const bool DBG=false ;

    scidb::ArrayDesc const& arrayDesc =  array->getArrayDesc();
    bool isEmptyable = arrayDesc.getEmptyBitmapAttribute() != NULL;

    scidb::AttributeDesc const& attrDesc = arrayDesc.getAttributes()[attrID];
    bool isNullable = attrDesc.isNullable();

    scidb::Dimensions const& dims = arrayDesc.getDimensions();
    size_t nDims = dims.size();

    scidb::Type attrType(scidb::TypeLibrary::getType(attrDesc.getType()));
    if (attrType.variableSize())
        throw USER_EXCEPTION(scidb::SCIDB_SE_EXECUTION, scidb::SCIDB_LE_EXTRACT_EXPECTED_FIXED_SIZE_ATTRIBUTE);

    if (attrType.bitSize() < 8)
        throw USER_EXCEPTION(scidb::SCIDB_SE_EXECUTION, scidb::SCIDB_LE_EXTRACT_UNEXPECTED_BOOLEAN_ATTRIBUTE);

    if (first.size() != nDims || last.size() != nDims)
        throw USER_EXCEPTION(scidb::SCIDB_SE_EXECUTION, scidb::SCIDB_LE_WRONG_NUMBER_OF_DIMENSIONS);

    size_t attrSize = attrType.byteSize();
    if (attrSize != sizeof(double))
        throw USER_EXCEPTION(scidb::SCIDB_SE_EXECUTION, scidb::SCIDB_LE_WRONG_ATTRIBUTE_TYPE); // TODO: really WRONG_ATTRIBUTE_SIZE


    // TODO: I'm not sure if this is overkill, because it seems to make
    //       aligned false when there is even one chunk that is not aligned.
    //       but that kills performance for all chunks
    //       If this has it wrong, then extractData() probably as it wrong too, since this was derived from that.
    bool hasOverlap = false;
    bool aligned = true;
    size_t bufSize = 1;
    for (size_t j = 0; j < nDims; j++)
    {
        if (last[j] < first[j] || (first[j] - dims[j].getStart()) % dims[j].getChunkInterval() != 0) {
            std::cerr << "first["<<j<<"]="<<first[j]
                      << ", last[]="<<last[j]
                      << ", dims[].getStart()" <<  dims[j].getStart()
                      << ", dims[].getChunkInterval" << dims[j].getChunkInterval()
                      << ", (first[]-start[])/chunkinterval[] ="
                      <<  (first[j] - dims[j].getStart()) % dims[j].getChunkInterval()
                      << std::endl ;
            throw USER_EXCEPTION(scidb::SCIDB_SE_EXECUTION, scidb::SCIDB_LE_UNALIGNED_COORDINATES);
        }

        aligned &= (last[j] - dims[j].getStart() + 1) % dims[j].getChunkInterval() == 0;
        hasOverlap |= dims[j].getChunkOverlap() != 0;
        bufSize *= last[j] - first[j] + 1;
    }

    size_t colChunkSize = dims[1].getChunkInterval(); // only becaue tiles extend beyond array
    // when the last dimension is not an even multiple of the chunk

    if (DBG && (!aligned || hasOverlap || isEmptyable || isNullable)) {
        std::cerr << "extractDataToOp: aligned: " << aligned << std::endl ;
        std::cerr << "extractDataToOp: hasOverlap: " << hasOverlap << std::endl ;
        std::cerr << "extractDataToOp: isEmptyable: " << isEmptyable << std::endl ;
        std::cerr << "extractDataToOp: isNullable: " << isNullable << std::endl ;
    }

    boost::shared_ptr<scidb::ConstArrayIterator> chunksIt;
    for(chunksIt = array->getConstIterator(/*attrid*/0); ! chunksIt->end(); ++(*chunksIt) ) {
        if(DBG) std::cerr << "extractDataToOp: next X chunk" << std::endl ;

        // question: when would getPosition() differ from getFirstPosition()
        //           when using the chunk-level interface?
        scidb::Coordinates const& chunkPos = chunksIt->getPosition();

        scidb::ConstChunk const& chunk = chunksIt->getChunk();
        scidb::Coordinates chunkOrigin(2); chunkOrigin = chunk.getFirstPosition(false);
        scidb::Coordinates chunkLast(2); chunkLast = chunk.getLastPosition(false);

        size_t dim;
        size_t nDims = dims.size();
        assert(nDims == 2); // TODO proper raise of SCIDB style exception


#if 0   //Original, now unused code, kept for match-back
        size_t chunkSize[2];
        chunkSize[0] = dims[0].getChunkInterval();
        chunkSize[1] = dims[0].getChunkInterval();
#endif

        for (dim = 0; dim < nDims; dim++) {
        #if 0       //Original, now unused code, kept for match-back to extractData()
            if (chunkPos[dim] < first[dim] ||
                chunkPos[dim] > last[dim]) {
                break;
            }
            chunkOffs *= last[dim] - first[dim] + 1;
            chunkOffs += chunkPos[dim] - first[dim];
        #endif
        }

        assert(dim == nDims);
        if (dim == nDims) { // just to preserve structure of the original for match-back

            // in these cases we may have to skip over some items
            // this is not the primary case for SSVD, so for now,
            // we'll assert.  In the future we can decide what
            // to do.
            if (!aligned || hasOverlap || isEmptyable || isNullable || chunk.isRLE() || chunk.isSparse()) {
                for (boost::shared_ptr<scidb::ConstChunkIterator> ci =
                        chunk.getConstIterator(scidb::ChunkIterator::INTENDED_TILE_MODE|
                                               scidb::ChunkIterator::IGNORE_OVERLAPS|
                                               scidb::ChunkIterator::IGNORE_EMPTY_CELLS|
                                               scidb::ChunkIterator::IGNORE_NULL_VALUES);
                     !ci->end(); ++(*ci))
                {
                    scidb::Value& value = ci->getItem();

                    if(!(ci->getMode() & ChunkIterator::TILE_MODE)) { // XXX TODO: if tile mode is not supported, can we do better ?
                        // This is the reference version.  Short, but 20-50x slower
                        // than the optimized version below.
                        // If we were to refactor this stuff into template-based programming
                        // we could have the best of both worlds.
                        assert(!value.isNull()) ; // ssvdNorm is undefined in the presence of nulls
                        if (!value.isNull()) {
                            scidb::Coordinates const& itemPos = ci->getPosition();
                            double val = * reinterpret_cast<double*>(ci->getItem().data());
                            extractOp(val, itemPos[0], itemPos[1]);
                        }
                    } else {
                        // TODO: the fast version needs factoring into inline methods and
                        //       templates.  It has become too unweildy.
                        // This is the fast version.  The actual operation from the template
                        // can be inlined or code-hoisted directly into the loop, and the loop
                        // can be unrolled by the compiler.  This makes the template method
                        // both versatile and fast when we need speed.  If you don't need that
                        // sort of speed, but instead want to instatiate this fewer times,
                        // you can pass in a operator base class that has virtual methods,
                        // and only have it instantiated once on the base class.
                        // Hopefully, I will be able to factor "extractData()" to use a
                        // simple copy-to-memory operator, and there will be only one version
                        // of the logic then.
                        //
                        // TODO: indicate whether it wants to receive
                        // recieve tile vs dense data, by intitializing _tileMode(t/f)
                        // AND in the iterator or the mode with TILE_MODE
                        // Those two need to agree

                        // starting position of the [possible RLE] "tile" (which is not rectangular),
                        // rather its just a continuation in row-major order from where-ever we left
                        // off before.
                        // This is in stark constrast to the meaning in "tile" of most of the
                        // computational literature, where a tile is square or rectangular.
                        const scidb::Coordinates& curPos = ci->getPosition();
                        int64_t row = curPos[0];
                        int64_t col = curPos[1];

                        scidb::ConstRLEPayload* tile = value.getTile(); // Will this throw if not a tile?
                                                                        // The INTENDED_TILE_MODE check should ensure that this is a tile,
                                                                        // but the Value interface needs to be more robust and throw or whatever.
                        //
                        // Konstantin, Alex.  What I need here is to know how to intialize row & col
                        // and to know when I can no longer advance col with ++, but must instead
                        // ++row, and return col to its value at the left side of the tile.
                        // Please review this and let me know if you think this is correct
                        //
                        const int64_t colEnd = chunkLast[1]+1; // array logically stops here
                        volatile const int64_t colEndPhys = chunkOrigin[1] + colChunkSize ; // array can physically go to here

                        for (size_t seg = 0; seg < tile->nSegments(); seg++) {
                            // segments have length and value
                            // we won't check if( seg.null ), we are trying to do this in 10 machine cycles
                            // TODO: add check at array level, which raises exception if the attribute is nullable
                            const scidb::RLEPayload::Segment& segment = tile->getSegment(seg);
                            if (segment.same) {
                                // run-length coded value, to be repeated segment.length() times
                                void* values = tile->getRawValue(segment.valueIndex);
                                //assert(! (reinterpret_cast<uint64_t>(values) % sizeof(double))) ;
                                bool unaligned = reinterpret_cast<uint64_t>(values) % sizeof(double) ;
                                double val ;
                                if(unaligned) {
                                    // unaligned, must copy
                                    if(DBG) std::cerr << "WARNING unaligned RLE double" << std::endl;
                                    ::memcpy(&val, values, sizeof(double));
                                } else {
                                    val = *static_cast<double*>(values) ;
                                }
                                if(DBG) std::cerr << "RUNLEN at[" << row << "," << col << "] = " << val << " repeats " << segment.length() << std::endl ;
                                // do run-length repetition
                                for (size_t run=0; run<segment.length(); run++) {
                                    // TODO: std::cerr comments are needed to debug everytime they change
                                    //       the physical format.  We don't want to loose them, so they
                                    //       need to be turned into if(DBG) or something like that.
                                    //std::cerr << "at " << row << "," << col << "CET: " << colEndPhys << std::endl;
                                    if(col < colEnd) {
                                        //std::cerr << " RUNLEN set [" << row << "," << col << "] = " << val << std::endl ;
                                        extractOp(val, row, col);
                                    }
                                    // The format used to require this else clause ...
                                    //else {
                                    //    // when chunks extend past the logical size of the array
                                    //    // we get some run-lengths in here.
                                    //    std::cerr << " RUNLEN skip [" << row << "," << col << "], val was " << val << std::endl ;
                                    //}
                                    //if(++col >= colEndPhys || col)
                                    if(++col >= colEnd) {
                                        // when the column moves past the physical size of the chunk
                                        // then its time to revert to the first column
                                        if(DBG) {
                                            std::cerr << "RUNLEN at [" << row << "," << col << "]"
                                                      << " colEnd:" << colEnd << std::endl ;
                                        }
                                        col = chunkOrigin[1];
                                        row++;
                                        if(DBG) std::cerr << "RUNLEN pos reset [" << row << "," << col << "]" << std::endl;
                                    }
                                }
                            } else {
                                // non-run-length (literal) segment
                                const size_t end = segment.valueIndex + segment.length();
                                void* first = tile->getRawValue(segment.valueIndex);
                                
                                // we NEED the alignment of doubles.  byte-copying is too much
                                // of a slowdown on a 64-bit machine
                                // assert(!(reinterpret_cast<uint64_t>(values) % sizeof(double))) ;
                                bool unaligned = reinterpret_cast<uint64_t>(first) % sizeof(double) ;
                                for (size_t ii = segment.valueIndex; ii < end; ii++) {
                                    void* values = tile->getRawValue(ii);
                                    double val ;
                                    if(unaligned) {
                                        if(DBG) std::cerr << "@@@@@@@@@@ unaligned literal, copying" << std::endl;
                                        ::memcpy(&val, values, sizeof(double));
                                    } else {
                                        val = *static_cast<double*>(values) ;
                                    }

                                    if(col < colEnd) {
                                        if(DBG) std::cerr << "LITERAL set [" << row << "," << col << "] = " << val << std::endl ;
                                        extractOp(val, row, col);
                                    }

                                    // The format used to require this else clause ...
                                    //else {
                                    //    std::cerr << "LITERAL skip [" << row << "," << col << "], val was " << val << std::endl ;
                                    //    std::cerr << " ERROR, should have been unreachable" << std:endl;
                                    //    // no longer supposed to be in the format, reset at
                                    //    // colEnd, not colEndPhys
                                    //}
                                    //if(++col >= colEndPhysical) ... we only reset at physical limit

                                    if (++col >= colEnd) { // if the column at tile limit or beyond
                                        // when the column moves past the edge of the chunk
                                        // we reset the column to the chunk's starting column
                                        if(DBG) std::cerr << "LITERAL at [" << row << "," << col << "] exceeds colEndPhys " << colEndPhys << " = chunkOrigin[1]:" << chunkOrigin[1] << " + colChunkSize:" << colChunkSize  << std::endl ;
                                        col = chunkOrigin[1];
                                        row++;
                                        if(DBG) std::cerr << "LITERAL pos reset to  [" << row << "," << col << "]" << std::endl;
                                    }
                                }
                            } // if(segment.same)
                        } // for(seg)
                    } // if(useReferenceVersion)
                } //end for chunk iterator
            } else {
                scidb::PinBuffer scope(chunk);
                copyStrideToOp((char*)chunk.getData(), first, last, chunkPos, dims, sizeof(double), extractOp);
            }
        }
    }
}

} //namespace

#endif // ARRAYEXTRACTOP_HPP
