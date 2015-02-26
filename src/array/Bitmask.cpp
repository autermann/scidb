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

#include "array/Bitmask.h"
#include <iostream>
#include <sstream> 

using namespace std;
using namespace boost;

namespace scidb
{

//Blindly overrides payload, so only safe to call from constructor
// inputSize for the compressed case only
void BitMask::initialize(void* inputData, size_t inputSize, bool allocate, bool setAllOnes)
{
    _sizeUncompressed = (_nBits + 7) >> 3;
    initializeBitSetters();
    _tuplesAllocated = _sizeUncompressed / sizeof(rleTuple);
    _tuplesFilled = 0;

    // too small to compress
    if (_nBits < sizeof(rleTuple))
    {
        _compressed = false;
    }

    // in future work: make this support variable-sized allocations of _tuples
    if (_compressed)
    {
        _tuplesFilled = inputSize / sizeof(rleTuple);
        if (allocate)
        {
            _tuples = new rleTuple[_tuplesAllocated];
            if (inputData)
            {
                memcpy(_tuples, inputData, inputSize);
            }
        }
        else
        {
            if (!inputData)
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_DATA_FOR_INITIALIZE);
            _tuples = (rleTuple *) inputData;
        }
        if (setAllOnes)
        {
            _tuplesFilled = 1;
            // create one tuple to fill it
            _tuples[0].start1 = 0;
            _tuples[0].physicalOffset = 0;
            _tuples[0].start0 = _nBits;
        }

    }
    else
    {
        if (allocate)
        {
            _payload = new uint8_t[_sizeUncompressed];
            if (inputData)
            {
                // inputSize <= sizeUncompressed
                memcpy(_payload, inputData, _sizeUncompressed);
            }
        }
        else
        {
            if (!inputData)
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_DATA_FOR_INITIALIZE);
            _payload = (uint8_t*) inputData;
        }

        if (setAllOnes)
        {
            memset(_payload, 255, _sizeUncompressed);
        }
        else if (!inputData)
        {
            memset(_payload, 0, _sizeUncompressed);
        }
    }
}

BitMask::BitMask(size_t nBits, bool compress, bool setAllOnes) :
    _compressed(compress), _nBits(nBits)
{
    initialize(0, 0, true, setAllOnes);

}

BitMask::BitMask(ArrayDesc const& array, bool compress) :
    _compressed(compress)
{
    _nBits = 1;
    const Dimensions& dim = array.getDimensions();
    size_t nDims = dim.size();
    for (size_t i = 0; i < nDims; ++i)
    {
        _nBits *= dim[i].getChunkOverlap() * 2 + dim[i].getChunkInterval();
    }
    initialize(0, 0, true, true);
}

BitMask::BitMask(uint8_t const* data, uint32_t nBits, uint32_t inputSize, bool inputCompressed, bool handOff) :
    _compressed(inputCompressed), _nBits(nBits)
{
    initialize((void*) data, inputSize, !handOff, false);
}

BitMask::BitMask(Value const& data, uint32_t nBits, bool compress) :
    _compressed(compress), _nBits(nBits)
{
    initialize(data.data(), data.size(), true, false);
}

BitMask::BitMask(const BitMask& other) :
    _compressed(other._compressed), _nBits(other._nBits), _tuplesFilled(other._tuplesFilled), _tuplesAllocated(other._tuplesAllocated),
            _sizeUncompressed(other._sizeUncompressed)

{
    initialize(other._payload, other.size(), true, false);
}

BitMask::BitMask(vector<bool> const& data, bool compress) :
    _nBits(data.size())
{
    uint8_t *payload;
    // materialized creates a uncompressed representation for random access in the case of compress
    boost::shared_ptr<BitMask> materialized;
    if (compress)
    {
        materialized = boost::shared_ptr<BitMask>(new BitMask(_nBits, false));
        payload = materialized->_payload;
        initialize(0, 0, true, false);
    }
    else
    {
        initialize(0, 0, true, true);
        payload = _payload;
    }

    for (size_t i = 0; i < data.size(); i++)
    {
        if (!data[i])
        {
            unsetBit(payload, i);
        }
    }

    if (compress)
    {
        compressIt(materialized->_payload, this);
    }
}

void BitMask::clear()
{
    if (_compressed)
    {
        memset(_tuples, 0, _tuplesFilled * sizeof(rleTuple));
        _tuplesFilled = 0;
    }
    else
    {
        memset(_payload, 0, _sizeUncompressed);
    }
}

/** Create a decompressed copy of this. */
boost::shared_ptr<BitMask> BitMask::decompress() const
{
    if (!_compressed)
    {
        return shared_ptr<BitMask> (new BitMask(*this));
    }
    else
    {
        shared_ptr<BitMask> dest(new BitMask(_nBits, false, false));
        decompressIt(_tuples, _tuplesFilled, dest->_payload);
        return dest;
    }
}

bool BitMask::getNextOne(ssize_t &offset) const
{
    if (_compressed)
    {
        ++offset; // set up for the first one that we are querying
        if (!_tuplesFilled || offset >= _nBits)
        {
            offset = _nBits;
            return false;
        }

        int32_t tupleOffset = findTuple(offset);

        // if before the first tuple
        if (tupleOffset == -1)
        {
            offset = _tuples[0].start1;
            return true;
        }
        // if it is in the run
        if (offset < _tuples[tupleOffset].start0)
        {
            return true;
        }
        // if it is after the run
        if ((uint32_t) tupleOffset < _tuplesFilled - 1)
        {
            offset = _tuples[tupleOffset + 1].start1;
            return true;
        }
        offset = _nBits;
        return false;
    }
    // not compressed
    else
    {
        ++offset;
        uint32_t endOfByte = ((offset + 7) / 8) * 8 - 1;

        for (offset = offset; offset <= endOfByte && offset < _nBits; offset++)
        {
            if (getBit(_payload, offset))
            {
                return true;
            }
        }

        uint32_t byte;
        for (byte = (endOfByte + 1) / 8; byte < _sizeUncompressed && _payload[byte] == 0; byte++)
        {
        }

        endOfByte = byte * 8 + 7;
        for (offset = byte * 8; offset <= endOfByte && offset < _nBits; offset++)
        {
            if (getBit(_payload, offset))
            {
                return true;
            }
        }

        offset = _nBits;
        return false;
    }
}

/** try to create a compressed copy of this. */
boost::shared_ptr<BitMask> BitMask::compress() const
{
    if (_compressed)
    {
        return shared_ptr<BitMask> (new BitMask(*this));
    }
    else
    {
        shared_ptr<BitMask> dest(new BitMask(_nBits, true, false));
        if (compressIt(_payload, dest.get()))
        {
            return dest;
        }
        else
        {
            return shared_ptr<BitMask> (new BitMask(*this));
        }
    }
}

size_t BitMask::size() const
{
    if (_compressed)
    {
        return _tuplesFilled * sizeof(rleTuple);
    }
    else
    {
        return _sizeUncompressed;
    }
}

bool BitMask::getBit(uint32_t offset) const
{

    if (offset > _nBits)
    {
        //std::stringstream str;
        //str << "Invalid offset " << offset << " nBits " <<  _nBits;
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_INVALID_OFFSET_REQUESTED);
    }
    if (_compressed)
    {
        if (!_tuplesFilled)
        {
            return false;
        }
        return getBitCompressed(offset);
    }
    else
    {
        return getBit(_payload, offset);
    }
}

void BitMask::setBit(uint32_t offset)
{

    if (offset > _nBits)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_INVALID_OFFSET_REQUESTED);
    }
    if (_compressed)
    {
        // if there are no rleTuples present
        if (!_tuplesFilled)
        {
            rleTuple outputTuple;
            outputTuple.start1 = offset;
            outputTuple.start0 = offset + 1;
            outputTuple.physicalOffset = 0;
            if (!pushBack(_tuples, _tuplesFilled, _tuplesAllocated, outputTuple))
            {
                // decompress this
                shared_ptr<BitMask> dest(new BitMask(_nBits, false, false));
                decompressIt(_tuples, _tuplesFilled, dest->_payload);
                dest->setBit(offset);
                _compressed = false;
                delete[] _tuples;
                _tuplesFilled = 0;
                _payload = new uint8_t[_sizeUncompressed];
                memcpy(_payload, dest->_payload, _sizeUncompressed);
            }
        }

        else if (!setBitCompressed(offset))
        {
            // decompress this
            shared_ptr<BitMask> dest(new BitMask(_nBits, false, false));
            decompressIt(_tuples, _tuplesFilled, dest->_payload);
            dest->setBit(offset);
            _compressed = false;
            delete[] _tuples;
            _tuplesFilled = 0;
            _payload = new uint8_t[_sizeUncompressed];
            memcpy(_payload, dest->_payload, _sizeUncompressed);
        }
    }
    else
    {
        setBit(_payload, offset);
    }
}

void BitMask::unsetBit(uint32_t offset)
{
    if (offset > _nBits)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_INVALID_OFFSET_REQUESTED);
    }
    if (_compressed)
    {
        if (!_tuplesFilled)
        {
            return;
        }

        if (!unsetBitCompressed(offset))
        {
            // decompress this
            shared_ptr<BitMask> dest(new BitMask(_nBits, false, false));
            decompressIt(_tuples, _tuplesFilled, dest->_payload);
            dest->unsetBit(offset);
            _compressed = false;
            delete[] _tuples;
            _payload = new uint8_t[_sizeUncompressed];
            memcpy(_payload, dest->_payload, _sizeUncompressed);
        }
    }
    else
    {
        unsetBit(_payload, offset);
    }
}

shared_ptr<BitMask> BitMask::merge(uint8_t const* mergeBits, uint32_t mergeElems) const
{
    if (mergeElems > _nBits)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CANT_MERGE_BITMASK);
    }

    assert(mergeElems <= _nBits);
    if (_compressed)
    {
        shared_ptr<BitMask> result = mergeHybrid(mergeBits, mergeElems);
        return result;
    }
    else
    {
        shared_ptr<BitMask> result(new BitMask(*this));
        result->mergeUncompressed(mergeBits, mergeElems);
        return result;
    }
}

shared_ptr<BitMask> BitMask::merge(shared_ptr<BitMask> toMerge) const
{

    if (_compressed)
    {
        if (toMerge->_compressed)
        {
            shared_ptr<BitMask> result;
            result = mergeCompressed(toMerge);
            return result;
        }
        else
        {
            shared_ptr<BitMask> result;
            result = mergeHybrid(toMerge->_payload, toMerge->_nBits);
            return result;
        }

    }
    else
    {
        // to merge is compressed, this is not
        if (toMerge->_compressed)
        {
            shared_ptr<BitMask> result(new BitMask(*this));
            shared_ptr<BitMask> mergeDecompressed = toMerge->decompress();
            result->mergeUncompressed(mergeDecompressed->_payload, mergeDecompressed->_nBits);
            return result;
        }
        else
        { // both not compressed
            shared_ptr<BitMask> result(new BitMask(*this));
            result->mergeUncompressed(toMerge->_payload, toMerge->_nBits);
            return result;
        }
    }
}

// a bitmask to merge with this -- both are compressed
shared_ptr<BitMask> BitMask::mergeCompressed(shared_ptr<BitMask> toMerge) const
{
    uint32_t primaryPos = 0;
    shared_ptr<BitMask> mergedMask(new BitMask(_nBits, true, false));
    rleTuple outputTuple;
    uint32_t inputTupleCount = toMerge->_tuplesFilled;
    rleTuple *inputTuple = toMerge->_tuples;
    uint32_t i, target, offset;
    uint32_t targetLength = 0;
    uint32_t runLength;

    for (i = 0; i < inputTupleCount; ++i)
    {
        target = inputTuple->start1;
        runLength = inputTuple->start0 - inputTuple->start1;

        // scroll forward to the right run in logical space
        while (_tuples[primaryPos].physicalOffset + _tuples[primaryPos].start0 - _tuples[primaryPos].start1 <= target)
        {
            ++primaryPos;
        }

        offset = inputTuple->start1 - _tuples[primaryPos].physicalOffset; // may be 0
        outputTuple = mergedMask->mergeIt(offset, runLength, _tuples[primaryPos], targetLength);
        if (!pushBack(mergedMask, outputTuple))
        {
            // compression failed
            shared_ptr<BitMask> result = decompress();
            shared_ptr<BitMask> mergeDecompressed = toMerge->decompress();
            result->mergeUncompressed(mergeDecompressed->_payload, mergeDecompressed->_nBits);
            return result;
        }

        while (runLength)
        {
            ++primaryPos;
            outputTuple = mergedMask->mergeIt(0, runLength, _tuples[primaryPos], targetLength);
            if (!pushBack(mergedMask, outputTuple))
            {
                // compression failed
                shared_ptr<BitMask> result = decompress();
                shared_ptr<BitMask> mergeDecompressed = toMerge->decompress();
                result->mergeUncompressed(mergeDecompressed->_payload, mergeDecompressed->_nBits);
                return result;
            }

        }
        ++inputTuple;
    }

    return mergedMask;
}

// pass in an uncompressed bitmask - compressed this
shared_ptr<BitMask> BitMask::mergeHybrid(const uint8_t * const mergeBits, uint32_t mergeElems) const
{
    // create a compressed version of toMerge
    // artificially raise _nBits to force it to allocate more tuples
    shared_ptr<BitMask> toMergeCompressed(new BitMask(_nBits, true, false));
    toMergeCompressed->_nBits = mergeElems;

    // if it is uncompressible, merge decompressed
    if (!compressIt(mergeBits, toMergeCompressed.get()))
    {
        shared_ptr<BitMask> result = this->decompress();
        result->mergeUncompressed(mergeBits, mergeElems);
        return result;
    }
    else
    {
        return mergeCompressed(toMergeCompressed);
        return toMergeCompressed;
    }

}

void BitMask::initializeBitSetters()
{
    for (uint32_t i = 0; i < 8; ++i)
    {
        _bitSetters[i] = 1 << i;
        _bitUnsetters[i] = ~_bitSetters[i];
    }
}

void BitMask::mergeUncompressed(uint8_t const* mergeBits, uint32_t mergeElems)
{
    uint32_t mergeElement = 0;
    ssize_t payloadElement = -1;
    while (getNextOne(payloadElement))
    {
        if (!getBit(mergeBits, mergeElement))
        {
            unsetBit(_payload, payloadElement);
        }
        ++mergeElement;
    }

}

/** OR this bitmask with toOr and return the result as a new BitMask.   */
boost::shared_ptr<BitMask> BitMask::OR(boost::shared_ptr<BitMask> toOr) const
{

    if (_compressed && toOr->_compressed)
    {
        return orCompressed(toOr);
    }
    else
    {
        // decompress the ones rle'd, OR the contributors
        // should not have to copy this one, it is a fallout from no shared_from_this
        shared_ptr<const BitMask> uncompressed(new BitMask(*this));
        shared_ptr<BitMask> toOrUncompressed = toOr;
        shared_ptr<BitMask> result;
        shared_ptr<BitMask> resultCompressed(new BitMask(_nBits, true, false));

        if (_compressed)
        {
            uncompressed = decompress();
        }

        if (toOr->_compressed)
        {
            toOrUncompressed = toOr->decompress();
        }

        result = uncompressed->orUncompressed(toOrUncompressed);
        // try to re-compress it
        if (compressIt(result->_payload, resultCompressed.get()))
        {
            return resultCompressed;
        }
        else
        {
            return result;
        }
    }
}

/** AND this bitmask with toAnd and return the result as a new BitMask.   */
boost::shared_ptr<BitMask> BitMask::AND(boost::shared_ptr<BitMask> toAnd) const
{
    // try to vectorize the AND
    if (_compressed && toAnd->_compressed)
    {
        return andCompressed(toAnd);
    }
    else
    {
        // decompress relevant sides and compute the AND
        shared_ptr<BitMask> lhs(new BitMask(*this));
        shared_ptr<BitMask> rhs = toAnd;

        if (_compressed)
        {
            lhs = decompress();
        }

        if (toAnd->_compressed)
        {
            rhs = toAnd->decompress();
        }

        return lhs->andUncompressed(rhs);

    }
}

/** NOT this bitmask and return the result as a new BitMask.   */
boost::shared_ptr<BitMask> BitMask::NOT() const
{
    if (_compressed)
    {
        return notCompressed();
    }

    return notUncompressed();

}

void BitMask::decompressIt(uint8_t * target) const
{
    if (_compressed)
    {
        return decompressIt(_tuples, _tuplesFilled, target);
    }

    memcpy(target, _payload, _sizeUncompressed);
}

std::ostream& operator<<(std::ostream& stream, BitMask const& m)
{
    stream << "bm compressed " << m.isCompressed() << " nBits " << m.numBits() << " size " << m.size() << " :";
    for (size_t i = 0; i < m.numBits(); i++)
    {
        if (m.getBit(i))
        {
            stream << '1';
        }
        else
        {
            stream << '0';
        }
    }
    return stream;
}

std::ostream& operator<<(std::ostream& stream, rleTuple const& r)
{
    stream << "<" << r.start1 << ", " << r.physicalOffset << ", " << r.start0 << ">";
    return stream;
}

// binary search for offset
// based on KK's MemArray implementation
// returns the offset of the tuple that starts before the requested logical offset
// want the closest start1 that is <= offset
int32_t BitMask::findTuple(const uint32_t &offset) const
{

    uint32_t l = 0;
    uint32_t r = _tuplesFilled - 1;
    uint32_t m;

    // end = special case
    if (_tuples[r].start1 < offset)
    {
        return r;
    }

    // special case for beginning
    if (_tuples[l].start1 > offset)
    {
        return -1;
    }

    do
    {
        m = (l + r) >> 1;
        _tuples[m].start1 > offset ? r = m : l = m;
    } while (_tuples[m].start1 != offset && l < r - 1);

    if (_tuples[m].start1 == offset)
    {
        return m;
    }
    if (_tuples[r].start1 <= offset)
    {
        return r;
    }
    return l;

}

bool BitMask::getBitCompressed(uint32_t offset) const
{
    int32_t tupleOffset = findTuple(offset);

    if (tupleOffset == -1)
    {
        return 0;
    }

    if (_tuples[tupleOffset].start0 > offset)
    {
        return 1;
    }
    return 0;
}

// insert a 0 in middle of a run
bool BitMask::splitIt(uint32_t tupleOffset, uint32_t offset)
{
    if (_tuplesFilled == _tuplesAllocated)
    {
        return false;
    }

    rleTuple first = _tuples[tupleOffset];
    rleTuple second = _tuples[tupleOffset];

    rleTuple *start = _tuples + tupleOffset + 1;
    rleTuple *dest = _tuples + tupleOffset + 2;
    uint32_t copySize = (_tuplesFilled - tupleOffset + 1) * sizeof(rleTuple);
    memmove(dest, start, copySize);

    _tuples[tupleOffset].start0 = first.start0 = offset;
    second.start1 = offset + 1;
    second.physicalOffset = first.physicalOffset + first.start0 - first.start1;

    _tuples[tupleOffset] = first;
    _tuples[tupleOffset + 1] = second;
    ++_tuplesFilled;

    for (uint32_t i = tupleOffset + 2; i < _tuplesFilled; ++i)
    {
        --_tuples[i].physicalOffset;
    }
    return true;
}

bool BitMask::unsetBitCompressed(uint32_t offset)
{
    // the tuple that has the last start1 <= offset
    int32_t tupleOffset = findTuple(offset);
    uint32_t i;

    if (tupleOffset == -1)
    {
        return true;
    }

    // if it is on the edges, just contract by 1
    if (offset == _tuples[tupleOffset].start1)
    {
        ++_tuples[tupleOffset].start1;
        for (i = tupleOffset + 1; i < _tuplesFilled; ++i)
        {
            --_tuples[i].physicalOffset;
        }

    }
    // cut it short at the end
    else if (offset == _tuples[tupleOffset].start0 - 1)
    {
        --_tuples[tupleOffset].start0;
        for (i = tupleOffset + 1; i < _tuplesFilled; ++i)
        {
            --_tuples[i].physicalOffset;
        }

    }
    // split it into two
    else if (offset < _tuples[tupleOffset].start0)
    {
        if (!splitIt(tupleOffset, offset))
        {
            return false;
        }
    }

    return true;

}

// insert the new tuple at offset in tuples
bool BitMask::insert(rleTuple r, uint32_t offset)
{
    if (_tuplesFilled == _tuplesAllocated)
    {
        return false;
    }

    // shift everything forward
    rleTuple *start = _tuples + offset;
    rleTuple *dest = _tuples + offset + 1;
    uint32_t copySize = (_tuplesFilled - offset) * sizeof(rleTuple);
    uint32_t insertSize = r.start0 - r.start1;

    memmove((void *) dest, (void *) start, copySize);
    _tuples[offset] = r;
    ++_tuplesFilled;

    for (uint32_t i = offset + 1; i < _tuplesFilled; ++i)
    {
        _tuples[i].physicalOffset += insertSize;
    }
    return true;
}

bool BitMask::setBitCompressed(uint32_t offset)
{
    // get the run that occurs before it, but closest to it
    int32_t tupleOffset = findTuple(offset);
    uint32_t i;

    // insert at the very beginning
    if (tupleOffset == -1)// && offset < _tuples[tupleOffset].start1)
    {

        if (offset == _tuples[tupleOffset].start1 - 1)
        {
            --_tuples[0].start1;
            for (i = 1; i < _tuplesFilled; ++i)
            {
                ++_tuples[i].physicalOffset;
            }
        } // end prepend case
        else
        {
            rleTuple newOne;
            newOne.start1 = offset;
            newOne.start0 = offset + 1;
            newOne.physicalOffset = _tuples[tupleOffset + 1].physicalOffset;
            if (!insert(newOne, 0))
            {
                return false;
            }
        }

    }
    // if it is intersected, do nothing
    // if it is adjacent to the offset, increment start0
    if (offset == _tuples[tupleOffset].start0)
    {
        ++_tuples[tupleOffset].start0;
        for (i = tupleOffset + 1; i < _tuplesFilled; ++i)
        {
            ++_tuples[i].physicalOffset;
        }
    }
    // if it merges with the next tuple
    else if (offset == _tuples[tupleOffset + 1].start1 - 1)
    {
        --_tuples[tupleOffset + 1].start1;
        for (i = tupleOffset + 2; i < _tuplesFilled; ++i)
        {
            ++_tuples[i].physicalOffset;
        }
    }
    else if (offset > _tuples[tupleOffset].start0 && offset < _tuples[tupleOffset + 1].start1 - 1)
    {
        // create a new run, insert it
        rleTuple newOne;
        newOne.start1 = offset;
        newOne.start0 = offset + 1;
        newOne.physicalOffset = _tuples[tupleOffset + 1].physicalOffset;
        if (!insert(newOne, tupleOffset + 1))
        {
            return false;
        }
    }

    return true;
}

// dest.tuples != source
// dest needs to be already allocated and have a tuplesFilled of 0
bool BitMask::compressIt(const uint8_t * source, BitMask *dest) const
{
    uint32_t i;
    const uint8_t *readPtr = source;
    uint8_t maskValue;
    rleTuple run;
    uint32_t totalOnes = 0;
    uint8_t runValue;

    if (dest->_tuplesFilled)
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_COMPRESSION_DESTINATION_BITMAP_NOT_EMPTY);

    run.start0 = run.start1 = 0;

    // get the first bit
    *readPtr & _bitSetters[0] ? runValue = 1 : runValue = 0;

    for (i = 1; i < dest->_nBits; ++i)
    {
        maskValue = *readPtr & _bitSetters[i % 8];

        if (!((maskValue && runValue == 1) || (!maskValue && runValue == 0))) // if there is a transition
        {
            // maintain the output run
            if (runValue == 0)
            {
                run.start1 = i;
                runValue = 1;
            }
            else // ending a slew of 1s
            {
                run.start0 = i;
                run.physicalOffset = totalOnes;
                totalOnes += run.start0 - run.start1;
                if (!pushBack(dest->_tuples, dest->_tuplesFilled, dest->_tuplesAllocated, run))
                {
                    return false;
                }
                runValue = 0;
            }
        }
        if (i % 8 == 7)
        {

            ++readPtr;
            // jump over whole homogeneous bytes
            while (((runValue && *readPtr == 255) || (!runValue && *readPtr == 0)) && i < _nBits)
            {
                i += 8;
                ++readPtr;
            }
        }
    }

    if (runValue == 1)
    {
        run.start0 = _nBits; // denote that it goes to the end
        run.physicalOffset = totalOnes;
        if (!pushBack(dest->_tuples, dest->_tuplesFilled, dest->_tuplesAllocated, run))
        {
            return false;
        }
    }

    // denote that it is rle compressed
    dest->_compressed = true;
    return true;
}

rleTuple BitMask::mergeIt(uint32_t offset, uint32_t &runRemain, rleTuple src, uint32_t & targetLength)
{
    rleTuple out;
    out.start1 = src.start1 + offset;
    out.physicalOffset = targetLength;

    if (out.start1 + runRemain >= src.start0) // the run will occupy the whole remainder of src space
    {
        out.start0 = src.start0;
        runRemain -= out.start0 - out.start1;
    }
    else
    {
        out.start0 = out.start1 + runRemain;
        runRemain = 0;
    }

    targetLength += out.start0 - out.start1;
    return out;
}

bool BitMask::pushBack(shared_ptr<BitMask> dest, rleTuple &r) const
{
    if (dest->_tuplesFilled == dest->_tuplesAllocated)
    {
        return false;
    }

    dest->_tuples[dest->_tuplesFilled] = r;

    ++dest->_tuplesFilled;
    return true;
}

// non-const version
bool BitMask::pushBack(rleTuple *tuples, uint32_t & tuplesFilled, uint32_t tuplesAllocated, rleTuple &r) const
{
    if (tuplesFilled == tuplesAllocated)
    {
        return false;
    }

    tuples[tuplesFilled] = r;

    ++tuplesFilled;
    return true;

}

// value must be 0 or 1, runLength >=8 -- values default to 0, so it wll always be setting 1s
void BitMask::putRun(uint8_t *target, uint32_t startOffset, uint32_t runLength) const
{

    uint8_t *writePtr = target + (startOffset >> 3);
    uint32_t wholeBytes;
    uint32_t i;
    uint8_t remain;

    // write to the bit individually til we get to a boundary
    while (startOffset % 8 != 0 && runLength)
    {
        *writePtr |= _bitSetters[startOffset % 8];
        ++startOffset;
        --runLength;
    }
    // if we've written to it, advance it
    if (*writePtr != 0)
    {
        ++writePtr;
    }

    wholeBytes = runLength / 8;
    remain = runLength % 8;

    memset(writePtr, 255, wholeBytes);
    writePtr += wholeBytes;
    startOffset += 8 * wholeBytes;

    for (i = 0; i < remain; ++i)
    {
        *writePtr |= _bitSetters[startOffset % 8];
        ++startOffset;

    }

}

// target arrives initialized to 0s
void BitMask::decompressIt(rleTuple const * const src, uint32_t tupleCount, uint8_t * const target) const
{

    for (uint32_t i = 0; i < tupleCount; ++i)
    {

        putRun(target, src[i].start1, src[i].start0 - src[i].start1);
    }

}

shared_ptr<BitMask> BitMask::orUncompressed(shared_ptr<BitMask> toOr) const
{
    shared_ptr<BitMask> result(new BitMask(_nBits, false, false));
    for (uint32_t i = 0; i < _sizeUncompressed; ++i)
    {
        result->_payload[i] = _payload[i] | toOr->_payload[i];
    }

    return result;
}

shared_ptr<BitMask> BitMask::andUncompressed(shared_ptr<BitMask> toAnd) const
{
    shared_ptr<BitMask> result(new BitMask(_nBits, false, false));
    for (uint32_t i = 0; i < _sizeUncompressed; ++i)
    {
        result->_payload[i] = _payload[i] & toAnd->_payload[i];
    }

    return result;
}

// both sides compressed
shared_ptr<BitMask> BitMask::orCompressed(shared_ptr<BitMask> toOr) const
{
    rleTuple *readLhs = _tuples;
    rleTuple *readRhs = toOr->_tuples;
    rleTuple *endLhs = readLhs + _tuplesFilled;
    rleTuple *endRhs = readRhs + toOr->_tuplesFilled;
    uint32_t endLhsPos, endRhsPos;

    uint32_t totalMaterialized = 0;
    rleTuple outputTuple;
    shared_ptr<BitMask> result(new BitMask(_nBits, true, false));

    // if lhs blank && rhs has values
    if (_tuplesFilled == 0 && toOr->_tuplesFilled > 0)
    {
        return shared_ptr<BitMask> (new BitMask(*toOr));
    }
    else if (_tuplesFilled > 0 && toOr->_tuplesFilled == 0)
    {
        return shared_ptr<BitMask> (new BitMask(*this));
    }
    else if (_tuplesFilled == 0 && toOr->_tuplesFilled == 0)
    {
        return shared_ptr<BitMask> (new BitMask(_nBits, true, false));
    }

    // prime the first output tuple
    outputTuple.start1 = std::min(readLhs->start1, readRhs->start1);
    outputTuple.physicalOffset = totalMaterialized;

    if (readLhs->start1 < readRhs->start1)
    {
        outputTuple.start0 = readLhs->start0;
    }
    else if (readRhs->start1 < readLhs->start1)
    {
        outputTuple.start0 = readRhs->start0;
    }
    else // equal case
    {
        outputTuple.start0 = std::max(readLhs->start0, readRhs->start0);
    }

    if (readLhs->start0 < readRhs->start0)
    {
        ++readLhs;
        // + 2 to break up the run if we encounter the last tuple
        readLhs == endLhs ? endLhsPos = _tuples[0].start0 + 2 : endLhsPos = readRhs->start1;
        endRhsPos = readRhs->start1;
    }
    else
    {
        ++readRhs;
        readRhs == endRhs ? endRhsPos = toOr->_tuples[0].start0 + 2 : endRhsPos = readRhs->start1;
        endLhsPos = readLhs->start1;
    }

    // run is broken, check it out and restart it  
    if (outputTuple.start0 < endLhsPos && outputTuple.start0 < endRhsPos)
    {
        if (!pushBack(result, outputTuple))
        {
            // decompress both and return the outcome
            shared_ptr<BitMask> lhs = decompress();
            shared_ptr<BitMask> rhs = toOr->decompress();
            return lhs->orUncompressed(rhs);
        }
        totalMaterialized += outputTuple.start0 - outputTuple.start1;
        outputTuple.start1 = std::min(readRhs->start1, readLhs->start1);
        outputTuple.physicalOffset = totalMaterialized;
    }

    while (readLhs < endLhs && readRhs < endRhs)
    {
        // if the run ends - emit
        if (!overlap(*readLhs, *readRhs))
        {
            outputTuple.start0 = std::min(readLhs->start0, readRhs->start0);
            if (!pushBack(result, outputTuple))
            {
                // decompress both and return the outcome
                shared_ptr<BitMask> lhs = decompress();
                shared_ptr<BitMask> rhs = toOr->decompress();
                return lhs->orUncompressed(rhs);
            }
            totalMaterialized += outputTuple.start0 - outputTuple.start1;

            readLhs->start0 < readRhs->start0 ? ++readLhs : ++readRhs;

            // set up the next tuple -- min start1
            outputTuple.start1 = std::min(readRhs->start1, readLhs->start1);
            outputTuple.physicalOffset = totalMaterialized;
        }
        else
        {
            readLhs->start0 < readRhs->start0 ? ++readLhs : ++readRhs;
        }
    }

    // if it overlaps, close out the previous run
    if (readLhs != endLhs && overlap(toOr->_tuples[toOr->_tuplesFilled - 1], *readLhs))
    {
        outputTuple.start0 = readLhs->start0;
        outputTuple.physicalOffset = totalMaterialized;
        totalMaterialized += outputTuple.start0 - outputTuple.start1;
        if (!pushBack(result, outputTuple))
        {
            // decompress both and return the outcome
            shared_ptr<BitMask> lhs = decompress();
            shared_ptr<BitMask> rhs = toOr->decompress();
            return lhs->orUncompressed(rhs);
        }
        ++readLhs;
    }

    // check intersection of presently open 
    // output all remaining tuples
    while (readLhs != endLhs)
    {
        outputTuple.start1 = readLhs->start1;
        outputTuple.start0 = readLhs->start0;
        outputTuple.physicalOffset = totalMaterialized;

        totalMaterialized += readLhs->start0 - readLhs->start1;
        if (!pushBack(result, outputTuple))
        {
            // decompress both and return the outcome
            shared_ptr<BitMask> lhs = decompress();
            shared_ptr<BitMask> rhs = toOr->decompress();
            return lhs->orUncompressed(rhs);
        }

        ++readLhs;

    }

    // if it overlaps, close out the previous run
    if (readRhs != endRhs && overlap(_tuples[_tuplesFilled - 1], *readRhs))
    {
        outputTuple.start0 = readRhs->start0;
        outputTuple.physicalOffset = totalMaterialized;
        totalMaterialized += outputTuple.start0 - outputTuple.start1;
        if (!pushBack(result, outputTuple))
        {
            // decompress both and return the outcome
            shared_ptr<BitMask> lhs = decompress();
            shared_ptr<BitMask> rhs = toOr->decompress();
            return lhs->orUncompressed(rhs);
        }
        ++readRhs;
    }

    // output all remaining tuples
    while (readRhs != endRhs)
    {
        outputTuple.start1 = readRhs->start1;
        outputTuple.physicalOffset = totalMaterialized;
        outputTuple.start0 = readRhs->start0;

        totalMaterialized += readRhs->start0 - readRhs->start1;
        if (!pushBack(result, outputTuple))
        {
            // decompress both and return the outcome
            shared_ptr<BitMask> lhs = decompress();
            shared_ptr<BitMask> rhs = toOr->decompress();
            return lhs->orUncompressed(rhs);
        }
        ++readRhs;
    }

    return result;
}

// both sides compressed
shared_ptr<BitMask> BitMask::andCompressed(shared_ptr<BitMask> toAnd) const
{

    rleTuple *readLhs = _tuples;
    rleTuple *readRhs = toAnd->_tuples;
    rleTuple *endLhs = readLhs + _tuplesFilled;
    rleTuple *endRhs = readRhs + toAnd->_tuplesFilled;
    uint32_t totalMaterialized = 0;
    rleTuple outputTuple;
    shared_ptr<BitMask> result(new BitMask(_nBits, true, false));

    while (readLhs < endLhs && readRhs < endRhs)
    {
        if (intersection(*readLhs, *readRhs))
        {
            outputTuple.start1 = std::max(readLhs->start1, readRhs->start1);
            outputTuple.start0 = std::min(readLhs->start0, readRhs->start0);
            outputTuple.physicalOffset = totalMaterialized;
            if (!pushBack(result, outputTuple))
            {
                shared_ptr<BitMask> lhs = decompress();
                shared_ptr<BitMask> rhs = toAnd->decompress();
                return lhs->andUncompressed(rhs);
            }
            totalMaterialized += outputTuple.start0 - outputTuple.start1;

            if (readLhs->start0 == outputTuple.start0)
                ++readLhs;
            if (readRhs->start0 == outputTuple.start0)
                ++readRhs;
        }
        else
        {
            // advance the one with the minimum start1
            readLhs->start1 < readRhs->start1 ? ++readLhs : ++readRhs;
        }

    } // end while

    return result;
}

shared_ptr<BitMask> BitMask::notCompressed() const
{
    rleTuple *first = _tuples;
    rleTuple *second = _tuples + 1;
    shared_ptr<BitMask> result(new BitMask(_nBits, _compressed, false));
    uint32_t total = 0;
    rleTuple outputTuple;
    rleTuple *lastTuple = _tuples + _tuplesFilled - 1;

    if (first->start1 != 0)
    {
        // insert a run from beginning to first
        outputTuple.start1 = 0;
        outputTuple.start0 = first->start1;
        outputTuple.physicalOffset = total;
        total += outputTuple.start0 - outputTuple.start1;
        if (!pushBack(result, outputTuple))
        {
            shared_ptr<BitMask> decompressed = decompress();
            return decompressed->notUncompressed();
        }
    }

    // iterate over and cover the gaps
    while (first != lastTuple)
    {
        outputTuple.start1 = first->start0;
        outputTuple.start0 = second->start1;
        outputTuple.physicalOffset = total;
        total += outputTuple.start0 - outputTuple.start1;
        if (!pushBack(result, outputTuple))
        {
            shared_ptr<BitMask> decompressed = decompress();
            return decompressed->notUncompressed();
        }
        ++first;
        ++second;
    }

    // last case
    if (first->start0 != _nBits)
    {
        outputTuple.start1 = first->start0;
        outputTuple.start0 = _nBits;
        outputTuple.physicalOffset = total;
        if (!pushBack(result, outputTuple))
        {
            shared_ptr<BitMask> decompressed = decompress();
            return decompressed->notUncompressed();
        }
    }

    return result;
}

shared_ptr<BitMask> BitMask::notUncompressed() const
{
    shared_ptr<BitMask> result(new BitMask(*this));
    for (uint32_t i = 0; i < _sizeUncompressed; ++i)
    {
        result->_payload[i] = ~_payload[i];
    }

    return result;

}

} // end namespace
