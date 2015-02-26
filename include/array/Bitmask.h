/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2012 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation version 3 of the License.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the GNU General Public License for the complete license terms.
*
* You should have received a copy of the GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/>.
*
* END_COPYRIGHT
*/

#ifndef _BITMASK_
#define _BITMASK_

#include <stdlib.h>
#include <stdint.h>
#include <string>
#include <boost/shared_ptr.hpp>
#include <sstream>
#include <fstream>
#include <vector>
#include <string.h>

#include "array/Array.h"

#define ENABLE_COMPRESSED_BITMASK 0

namespace scidb
{
struct rleTuple
{
    uint32_t start1; // logical offset where a run of 1s starts
    uint32_t physicalOffset; // physical offset in payload where 1s will reside
    uint32_t start0; // end of run (exclusive)
};

class BitMask
{
public:
    //CTORS

    BitMask(size_t nBits, bool compress = true, bool setAllOnes = true);

    /**
     * Create a bitmask of 1s based on chunk shape
     * @param[in] array the array desc to deduce chunk shape
     * @param[in] compressed - if true, the bitmask will be compressed, false otherwise
     * Used by: scan of non-emptyable arrays. New chunk creation.
     */
    BitMask(ArrayDesc const& array, bool compress = true);

    /**
     * Create a bitmask from a boolean payload.
     * @param[in] data a vectorized boolean value to use as payload
     * @param[in] compressed - if true, the bitmask will be compressed, false otherwise
     * Used by: filter, join, ...
     */
    BitMask(Value const& data, uint32_t nBits, bool compress = true);

    /**
     * Create a bitmask from a binary payload. The resulting mask is compressed IFF input payload is compressed.
     * @param[in] data the bitmask payload
     * @param[in] input size of the payload (if compressed)
     * @param[in] uncompressed bit count
     * @param[in] inputCompressed - true if payload is compressed, false otherwise
     * @param[in] nBits - number of bits in payload
     * Used by: scan, network receive
     */
    BitMask(uint8_t const* data, uint32_t nBits, uint32_t inputSize = 0, bool inputCompressed = true, bool handOff = false);

    /*
     * Stupid constructor for now
     */
    BitMask(std::vector<bool> const& data, bool compress = true);

    /**
     * Deep copy
     */
    BitMask(const BitMask& other);

    virtual ~BitMask()
    {
        delete[] _payload;
    }

    void clear();

    //CONST METHODS:
    /** @return true if this bitmask is compressed, false otherwise. */
    bool isCompressed() const
    {
        return _compressed;
    }

    /** Create a decompressed copy of this. */
    boost::shared_ptr<BitMask> decompress() const;

    /** Create a compressed copy of this. */
    boost::shared_ptr<BitMask> compress() const;

    /** Return total current size in bytes. */
    size_t size() const;

    /** Return the payload, guaranteed to be size() bytes long. */
    uint8_t *getPayload() const
    {
        return _payload;
    }

    /** Return the uncompressed size, matches size() if isCompressed() is false */
    size_t sizeUncompressed() const
    {
        return _sizeUncompressed;
    }

    /** Return the number of logical elements in the mask */
    uint32_t numBits() const
    {
        return _nBits;
    } // # of bits uncompressed

    bool getBit(uint32_t offset) const;
    void setBit(uint32_t offset);
    void unsetBit(uint32_t offset);

    /** Merge this bitmask with toMerge and return the result as a new BitMask. */
    boost::shared_ptr<BitMask> merge(uint8_t const* toMerge, uint32_t nBits) const;

    // merge with antoher BitMask
    boost::shared_ptr<BitMask> merge(boost::shared_ptr<BitMask> toMerge) const;

    /** OR this bitmask with toOr and return the result as a new BitMask.   */
    boost::shared_ptr<BitMask> OR(boost::shared_ptr<BitMask> toOr) const;

    /** AND this bitmask with toAnd and return the result as a new BitMask.   */
    boost::shared_ptr<BitMask> AND(boost::shared_ptr<BitMask> toAnd) const;

    /** NOT this bitmask and return the result as a new BitMask.   */
    boost::shared_ptr<BitMask> NOT() const;

    void decompressIt(uint8_t * target) const;

    /**
     *  Find the position of the next "1" in the bitmask.
     *  If bitmask contains a "1" at index K > offset, sets offset = K and returns true.
     *  Else returns false and sets offset to numBits()
     *  Caller may provide -1 to start iterating. Values less than -1 result in indeterminate behavior.
     */
    bool getNextOne(ssize_t &offset) const;

private:
    union
    {
        rleTuple *_tuples;
        uint8_t *_payload;
    };

    void initialize(void* inputData, size_t inputSize, bool allocate, bool setAllOnes);

    bool _compressed;
    uint32_t _nBits;
    uint32_t _tuplesFilled;
    uint32_t _tuplesAllocated;
    uint8_t _bitSetters[8];
    uint8_t _bitUnsetters[8];
    uint32_t _sizeUncompressed; // in bytes

    //    Black Magic:
    void initializeBitSetters();

    //both operands uncompressed
    void mergeUncompressed(uint8_t const* mergeBits, uint32_t mergeElems);

    // uncompressed case only
    inline bool getBit(uint8_t const* bitmask, uint32_t offset) const
    {
        uint8_t const *readPtr = bitmask + (offset >> 3);
        return (*readPtr & _bitSetters[offset % 8]) > 0;
    }

    inline void unsetBit(uint8_t *bitmask, uint32_t offset)
    {
        uint8_t *writePtr = bitmask + (offset >> 3);
        *writePtr &= _bitUnsetters[offset % 8];
    }

    inline void setBit(uint8_t *bitmask, uint32_t offset)
    {
        uint8_t *writePtr = bitmask + (offset >> 3);
        *writePtr |= _bitSetters[offset % 8];
    }

    // compressed accessors and mutators
    int32_t findTuple(const uint32_t &offset) const;
    bool getBitCompressed(uint32_t offset) const;
    bool splitIt(uint32_t tupleOffset, uint32_t offset);
    bool unsetBitCompressed(uint32_t offset);
    bool insert(rleTuple r, uint32_t offset);
    bool setBitCompressed(uint32_t offset);

    bool compressIt(const uint8_t * source, BitMask *dest) const;
    rleTuple mergeIt(uint32_t offset, uint32_t &runRemain, rleTuple src, uint32_t & targetLength);
    bool pushBack(boost::shared_ptr<BitMask> dest, rleTuple &r) const;
    bool pushBack(rleTuple *tuples, uint32_t & tuplesFilled, uint32_t tuplesAllocated, rleTuple &r) const;
    void putRun(uint8_t *target, uint32_t startOffset, uint32_t runLength) const;
    void decompressIt(rleTuple const * const src, uint32_t tupleCount, uint8_t * const target) const;

    // bitwise operands -- vectorized
    boost::shared_ptr<BitMask> mergeCompressed(boost::shared_ptr<BitMask> toMerge) const;
    boost::shared_ptr<BitMask> mergeHybrid(const uint8_t * const mergeBits, uint32_t mergeElems) const;
    boost::shared_ptr<BitMask> orCompressed(boost::shared_ptr<BitMask> toOr) const;
    boost::shared_ptr<BitMask> andCompressed(boost::shared_ptr<BitMask> toAnd) const;
    boost::shared_ptr<BitMask> orUncompressed(boost::shared_ptr<BitMask> toOr) const;
    boost::shared_ptr<BitMask> andUncompressed(boost::shared_ptr<BitMask> toAnd) const;
    boost::shared_ptr<BitMask> notUncompressed() const;
    boost::shared_ptr<BitMask> notCompressed() const;

    // check to see if two rle runs intersect each other in logical space
    inline bool intersection(rleTuple & a, rleTuple & b) const
    {
        // either they start at the same point, b starts in the middle of a or a starts in the middle of b
        if ((a.start1 == b.start1) || (a.start1 < b.start1 && b.start1 < a.start0) || (b.start1 < a.start1 && a.start1 < b.start0))
        {
            return true;
        }
        return false;
    }

    // check overlap of 2 runs for AND
    inline bool overlap(rleTuple & a, rleTuple & b) const
    {
        if ((a.start1 <= b.start1 && b.start1 <= a.start0) || (b.start1 <= a.start1 && a.start1 <= b.start0))
            return true;
        else
            return false;

    }

    inline std::string tupleToString(const rleTuple &r) const
    {
        std::stringstream s;
        s << "<" << r.start1 << ", " << r.physicalOffset << ", " << r.start0 << ">";
        return s.str();
    }

};

//For debug: we will need it
std::ostream& operator<<(std::ostream& stream, BitMask const& m);
std::ostream& operator<<(std::ostream& stream, rleTuple const& r);

}
#endif
