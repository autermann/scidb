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

/*
 * OverlappingChunksIterator.h
 *
 *  Created on: Aug 15, 2012
 *      Author: dzhang
 *  The iterator can be used to enumerate the positions of all the chunks, starting with the 'home' chunk, that should store an item.
 */

#ifndef OVERLAPPINGCHUNKSITERATOR_H_
#define OVERLAPPINGCHUNKSITERATOR_H_

#include <assert.h>
#include <array/Array.h>
#include <system/Constants.h>
#include <bitset>

namespace scidb
{
/**
 * An iterator that iterates through the positions of all the chunks that should store an item.
 * Normally, an item is stored in a single chunk.
 * However, with overlaps, an item may also need to be stored in adjacent chunks.
 *
 */
class OverlappingChunksIterator: public ConstIterator {
    // Bit i, if set, indicates that the item spans two chunks in dimension i.
    std::bitset<MAX_NUM_DIMS_SUPPORTED> _overlapMask;

    // Bit i, if set, indicates that the item is also stored in the chunk(s) whose coordinate in the i'th dimension
    // is higher than that of the 'home' chunk.
    // Note: only valid when bit i of _overlapMask is set.
    std::bitset<MAX_NUM_DIMS_SUPPORTED> _overlapHighEnd;

    // The 'home' chunk, i.e. the chunk whose region, not counting overlaps, contains the item's position.
    Coordinates _homeChunkPos;

    // Placeholder for the chunkPos to return to the caller.
    Coordinates _chunkPosToReturn;

    // _mask grows from 0 to _overlapMask+1.
    // When _mask==0: getPosition() will return _homeChunkPos;
    // when _mask==_overlapMask+1: end() is true.
    std::bitset<MAX_NUM_DIMS_SUPPORTED> _mask;

    // The dest dims.
    Dimensions const& _dims;

    // #total chunks that should store the cell.
    size_t _numTotalChunks;

    // #chunks that have been iterated
    size_t _numIteratedChunks;

public:
    /**
     * Constructor.
     * @param destDims  A reference to the dimensions. The source must be valid throughout the lifecyle of OverlappingChunksIterator.
     * @param itemPos   The position of the item.
     * @param chunkPos  The 'home' chunk position. Must spatially include itemPos.
     */
    OverlappingChunksIterator(Dimensions const& dims, Coordinates const& itemPos, Coordinates const& chunkPos)
    :_homeChunkPos(chunkPos), _dims(dims), _numIteratedChunks(0)
    {
        for (size_t i = 0; i < dims.size(); i++) {
            assert(chunkPos[i] <= itemPos[i]);
            assert(chunkPos[i]+dims[i].getChunkInterval() > itemPos[i]);

            // If need to store in the high-end neighbor.
            if (itemPos[i] - chunkPos[i] + dims[i].getChunkOverlap() >= dims[i].getChunkInterval()
                && chunkPos[i] + dims[i].getChunkInterval() <= dims[i].getEndMax())
            {
                _overlapMask.set(i, 1);
                _overlapHighEnd.set(i, 1);
            }

            // If need to store in the low-end neighbor.
            else if (itemPos[i] - chunkPos[i] < dims[i].getChunkOverlap()
                && chunkPos[i] > dims[i].getStart())
            {
                _overlapMask.set(i, 1);
            }
        }

        // _numTotalChunks should be 2 ^ (#overlapping dims)
        _numTotalChunks = ((size_t)1) << _overlapMask.count();

        reset();
    }

    /**
     * @return whether the iterator reaches the end
     */
    virtual bool end() {
        return _numIteratedChunks == _numTotalChunks;
    }

    /**
     * Position cursor to the next element.
     */
    virtual void operator ++() {
        assert(!end());
        incMask();
        if (!end()) {
            calculatePosToReturn();
        }
    }

    /**
     * Get coordinates of the current element in the chunk
     */
    virtual Coordinates const& getPosition() {
        return _chunkPosToReturn;
    }

    /**
     * Not supported.
     */
    virtual bool setPosition(Coordinates const& pos) {
        assert(false);
        return false;
    }

    /**
     * Reset iterator to the first element.
     */
    virtual void reset() {
        _chunkPosToReturn = _homeChunkPos;
        _mask.reset();
        _numIteratedChunks = 0;
    }

private:
    // Increment mask (from the most-significant end) to the next valid mask, or set _end=true if all valid masks have been enumerated.
    // A mask is valid, if all the bits in the non-overlapping dimensions are 0.
    virtual void incMask() {
        assert(!end());

        // pre-increment
        ++ _numIteratedChunks;

        // end condition
        if (end()) {
            return;
        }

        // Consider *only* the 'valid' bits in _mask (valid if _overlapMask[bit] is true).
        // This step clears the consecutive left-most bits in _mask.
        // E.g. when _mask =                                    01101,
        // to increment from the left, this step sets _mask  to 00001;
        // and the next step will turn on the next '0', e.g. to 00011.
        //
        size_t bit = _dims.size();
        do {
            if (bit<_dims.size()) {
                _mask.set(bit, 0);
            }
            assert(bit>0);
            -- bit;
        } while (!_overlapMask[bit] || _mask[bit]);

        // Turn on the next 0
        _mask.set(bit, 1);
    }

    // Calculate a chunk position to return next, based on the mask.
    virtual void calculatePosToReturn() {
        for (size_t i = 0; i < _dims.size(); i++) {
            _chunkPosToReturn[i] = _homeChunkPos[i];
            if ( _mask[i] ) { // if the mask indicates a neighbor chunk
                if (_overlapHighEnd[i]) { // if this is the high-end neighbor
                    _chunkPosToReturn[i] += static_cast<Coordinate>(_dims[i].getChunkInterval());
                } else {
                    _chunkPosToReturn[i] -= static_cast<Coordinate>(_dims[i].getChunkInterval()) ;
                }
            }
        }
    }
};

}
#endif /* OVERLAPPINGCHUNKSITERATOR_H_ */
