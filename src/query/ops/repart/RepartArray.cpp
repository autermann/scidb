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

/**
 * @file RepartArray.cpp
 *
 * @brief Repart array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include <set>
#include "RepartArray.h"
#include "array/MemArray.h"
#include "array/DelegateArray.h"
#include "system/Exceptions.h"
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"
#include "system/Utils.h"

namespace scidb {
using namespace boost;
using namespace std;

typedef uint32_t Overlap;
typedef uint32_t Interval;
typedef size_t   Size;

typedef vector<Overlap>  OverlapVector;
typedef vector<Interval> IntervalVector;

class RepartCommonSettings;
class RepartArraySettings;
class RepartChunkSettings;

class TileDataReader;
class TileBitmapReader;
class TileDataWithBitmapReader;
class TileWriter;
template< typename Reader > class SafeTileReader;
class SafeTileWriter;

class DataWalker;
class DataAndBitmapWalker;
template< typename Walker >
class OpenOnceWalker;

class RepartArray;
class RepartArrayIterator;
class RepartChunk;
template< typename Walker, typename Reader >
class DenseChunkIteratorTile;
template< typename Walker >
class DenseChunkIteratorValue;

/** \brief RepartCommonSettings is class with common information about
 *  the repart work mode.
 */
class RepartCommonSettings : boost::noncopyable
{
  public:
    RepartAlgorithm const algorithm;
    bool const            denseOpenOnce;
    bool const            outputFullChunks;
    bool const            singleInstance;
    bool const            tileMode;     /** does repart work in tile mode?   */
    Size const n;                       /** dimension count */

  public:
    RepartCommonSettings(RepartAlgorithm const& algorithm,
                         bool denseOpenOnce,
                         bool outputFullChunks,
                         bool singleInstance,
                         bool tileMode,
                         Size n) :
        algorithm(algorithm),
        denseOpenOnce(denseOpenOnce),
        outputFullChunks(outputFullChunks),
        singleInstance(singleInstance),
        tileMode(tileMode),
        n(n)
    {
    }

    /** work mode for ChunkIterator (according to tileMode) */
    int mode(int result) const
    {
        if (tileMode) {
            result |= ConstChunkIterator::TILE_MODE;
        } else {
            result &= ~ConstChunkIterator::TILE_MODE;
        }
        result &= ~ConstChunkIterator::INTENDED_TILE_MODE;
        return result;
    }

    int mode(int result, bool useOverlap) const
    {
        result = mode(result);
        if (useOverlap) {
            result &= ~ConstChunkIterator::IGNORE_OVERLAPS;
        } else {
            result |= ConstChunkIterator::IGNORE_OVERLAPS;
        }
        return result;
    }
};

/** \brief RepartArraySettings is class with common information about
 *  the array in repart.
 */
class RepartArraySettings : boost::noncopyable
{
  public:
    RepartCommonSettings const& common;

  private:
    bool        _sequenceScan; /** does repart use sequence scan over source? */
    Coordinates _lowerBound;        /** lower bound  */
    Coordinates _upperBound;        /** upper bound  */
    Coordinates _realUpperBound;    /** "real" upper bound (upper bound for available values) */
    ArrayDesc const& _arrayDesc;
    OverlapVector  _overlap;  /** chunk overlaps  */
    IntervalVector _interval; /** chunk intervals */

    void calculateShape(RepartArraySettings const* source)
    {
        Dimensions const& d = _arrayDesc.getDimensions();
        SCIDB_ASSERT(d.size() == common.n);
        for (size_t i = 0; i < common.n; ++i) {
            /* array */
            _lowerBound[i] = d[i].getStart();
            _upperBound[i] = d[i].getEndMax() + 1;
            _realUpperBound[i] = _lowerBound[i] + d[i].getCurrLength();
            if (source) {
                _realUpperBound[i] = std::max(_realUpperBound[i], source->realUpperBound()[i]);
            }

            /* chunk */
            _interval[i] = d[i].getChunkInterval();
            _overlap[i] = d[i].getChunkOverlap();
        }
    }

    uint64_t chunkCount() const
    {
        uint64_t chunkCount = 1;
        for (size_t i = 0; i < common.n; ++i) {
            /* on current dimension */
            uint64_t arrayInterval = _realUpperBound[i] - _lowerBound[i] - 1 + _interval[i];
            chunkCount *= arrayInterval / _interval[i];
        }
        return chunkCount;
    }

  public:
    RepartArraySettings(RepartCommonSettings const& common,
                        ArrayDesc const& arrayDesc,
                        uint64_t sequenceScanThreshold,
                        RepartArraySettings const* source = 0) :
        common(common),
        _lowerBound(common.n),
        _upperBound(common.n),
        _realUpperBound(common.n),
        _arrayDesc(arrayDesc),
        _overlap(common.n),
        _interval(common.n)
    {
        calculateShape(source);
        _sequenceScan = sequenceScanThreshold < chunkCount();
    }



    /** should repart use sequence scan over source? */
    bool sequenceScan() const
    {
        return _sequenceScan;
    }

    /** lower bound */
    Coordinates const& lowerBound() const
    {
        return _lowerBound;
    }

    /** upper bound (next after maximum) */
    Coordinates const& upperBound() const
    {
        return _upperBound;
    }

    /** real upper (next after last) */
    Coordinates const& realUpperBound() const
    {
        return _realUpperBound;
    }

    /** overlap for every dimension */
    OverlapVector const& overlap() const
    {
        return _overlap;
    }

    /** interval for every dimension */
    IntervalVector const& interval() const
    {
        return _interval;
    }

    /** does @param actual inside the array? */
    bool inside(Coordinates const& actual) const
    {
        SCIDB_ASSERT(actual.size() == common.n);
        return _arrayDesc.contains(actual);
    }

    /** does @param actual aligned to first position in the chunk? */
    bool aligned(Coordinates const& actual) const
    {
        return align(actual) == actual;
    }

    void getChunkBoundaries(Coordinates const& chunkPosition,
                            bool withOverlap,
                            Coordinates& lowerBound,
                            Coordinates& upperBound) const
    {
        SCIDB_ASSERT(aligned(chunkPosition));
        _arrayDesc.getChunkBoundaries(chunkPosition, withOverlap, lowerBound, upperBound);
        for (size_t i = 0; i < common.n; ++i) {
            upperBound[i] += 1;
        }
    }

    /** align @param actual to the first position of related chunk */
    Coordinates align(Coordinates const& actual) const
    {
        SCIDB_ASSERT(actual.size() == common.n);
        Coordinates result = actual;
        if (inside(actual)) {
            _arrayDesc.getChunkPositionFor(result);
            return result;
        } else {
            /* Phantom chunk */
            for (size_t i = 0; i < common.n; ++i) {
                if (actual[i] > upperBound()[i]) {
                    Coordinate remainder = (actual[i] - upperBound()[i]) % interval()[i];
                    result[i] = actual[i] - remainder;
                } else if (actual[i] < lowerBound()[i]) {
                    Coordinate remainder = (lowerBound()[i] - actual[i]) % interval()[i];
                    result[i] = actual[i] - (interval()[i] - remainder);
                }
            }
            return result;
        }
    }
};

/** \brief RepartChunkSettings is class with common information about
 *  the chunk in repart.
 */
class RepartChunkSettings : public boost::noncopyable
{
  public:
    bool const useOverlap;  /* should repart use the overlap? */
    RepartArraySettings const&  array;
  private:
    bool         _mapped;           /** does this mapped to chunk?               */
    Coordinates  _lowerBoundMain;       /** chunk lower coordinate (without overlap) */
    Coordinates  _upperBoundMain;       /** chunk upper coordinate (without overlap) */
    Coordinates  _lowerBoundOverlap;    /** chunk lower coordinate (with overlap)    */
    Coordinates  _upperBoundOverlap;    /** chunk upper coordinate (with overlap)    */
    IntervalVector _sizeOverlap;   /** chunk sizes (with overlap)               */
    position_t     _positionCount; /** total positions in current chunk         */
    position_t     _tileSize;      /** size of tile                             */
    bool           _phantom;        /** chunk outside the array bounds */
public:
    RepartChunkSettings(RepartArraySettings const& theArray,
                        bool useOverlap) :
        useOverlap(useOverlap),
        array(theArray),
        _mapped(false)
    {
    }

    RepartChunkSettings(RepartArraySettings const& theArray,
                        bool useOverlap,
                        ConstChunk const& chunk) :
        useOverlap(useOverlap),
        array(theArray),
        _mapped(false)
    {
        /*
          perhaps, some method of Chunk class
          can be implemented over chunk's methods
        */
        map(chunk);
    }

    /** size of tile for current chunk */
    position_t tileSize() const
    {
        SCIDB_ASSERT(mapped());
        return _tileSize;
    }

    /** phantom chunk (outside array bounds) */
    bool phantom() const
    {
        SCIDB_ASSERT(mapped());
        return _phantom;
    }

    /** does this mapped to the chunk? */
    bool mapped() const
    {
        return _mapped;
    }

    /**
      * mode of work for ChunkIterator, according to:
      *   - RepartCommonSettings::mode
      *   - useOverlap
      */
    int mode(int mode) const
    {
        return array.common.mode(mode, useOverlap);
    }

    /** lower bound without overlap */
    Coordinates const& lowerBoundMain() const
    {
        SCIDB_ASSERT(mapped());
        return _lowerBoundMain;
    }

    /** upper bound without overlap (next after maximum) */
    Coordinates const& upperBoundMain() const
    {
        SCIDB_ASSERT(mapped());
        return _upperBoundMain;
    }

    /** lower bound with overlap */
    Coordinates const& lowerBoundOverlap() const
    {
        SCIDB_ASSERT(mapped());
        return _lowerBoundOverlap;
    }

    /** upper bound with overlap (next after maximum) */
    Coordinates const& upperBoundOverlap() const
    {
        SCIDB_ASSERT(mapped());
        return _upperBoundOverlap;
    }

    /** lower bound according to useOverlap setting (lowerBoundMain or lowerBoundOverlap) */
    Coordinates const& lowerBound() const
    {
        if (useOverlap) {
            return lowerBoundOverlap();
        } else {
            return lowerBoundMain();
        }
    }

    /** upper bound according to useOverlap setting (upperBoundMain or upperBoundOverlap) */
    Coordinates const& upperBound() const
    {
        if (useOverlap) {
            return upperBoundOverlap();
        } else {
            return upperBoundMain();
        }
    }

    /** does @param actual inside main (without overlap) area? */
    bool insideMain(Coordinates const& actual) const
    {
        SCIDB_ASSERT(mapped());
        return inside(lowerBoundMain(), upperBoundMain(), actual);
    }

    /** does @param actual inside main or overlap area? */
    bool insideOverlap(Coordinates const& actual) const
    {
        SCIDB_ASSERT(mapped());
        return inside(lowerBoundOverlap(), upperBoundOverlap(), actual);
    }

    /* does @param actual inside active (according to useOverlap settings) area? */
    bool inside(Coordinates const& actual) const
    {
        SCIDB_ASSERT(mapped());
        return inside(lowerBound(), upperBound(), actual);
    }

    /**
     * map calculates (lower|upper)Bound(Main|Overlap), tileSize, etc according
     * to @param chunk argument
     */
    void map(ConstChunk const& chunk)
    {
        /* need SCIDB_ASSERTs to check compatibility of chunk and array */
        Coordinates coord = chunk.getFirstPosition(false);
        SCIDB_ASSERT(array.inside(coord));
        SCIDB_ASSERT(array.aligned(coord));
        map(coord);
        SCIDB_ASSERT(!phantom());
    }

    /**
     * like map(ConstChunk const&) but used just coordinates @param coord
     * of first position in the chunk
     */
    void map(Coordinates const& chunkCoord)
    {
        SCIDB_ASSERT(chunkCoord.size() == array.common.n);
        SCIDB_ASSERT(array.aligned(chunkCoord));
        _mapped = true;
        _phantom = !array.inside(chunkCoord);
        _lowerBoundMain.resize(array.common.n);
        _upperBoundMain.resize(array.common.n);
        _lowerBoundOverlap.resize(array.common.n);
        _upperBoundOverlap.resize(array.common.n);
        _sizeOverlap.resize(array.common.n);
        _positionCount = 1;
        for (size_t i = 0; i < array.common.n; ++i) {
            Coordinate arrayLowerBound = array.lowerBound()[i];
            Coordinate arrayUpperBound = array.upperBound()[i];
            _lowerBoundMain[i] = chunkCoord[i];
            SCIDB_ASSERT(phantom() || _lowerBoundMain[i] >= array.lowerBound()[i]);

            _lowerBoundOverlap[i] = _lowerBoundMain[i] - array.overlap()[i];
            if (!phantom()) {
                _lowerBoundOverlap[i] = max(arrayLowerBound, _lowerBoundOverlap[i]);
            }
            SCIDB_ASSERT(phantom() || _lowerBoundOverlap[i] >= array.lowerBound()[i]);

            _upperBoundMain[i] = chunkCoord[i] + array.interval()[i];
            if (!phantom()) {
                _upperBoundMain[i] = min(arrayUpperBound, _upperBoundMain[i]);
            }

            _upperBoundOverlap[i] = _upperBoundMain[i] + array.overlap()[i];
            if (!phantom()) {
                _upperBoundOverlap[i] = min(arrayUpperBound, _upperBoundOverlap[i]);
            }
            _sizeOverlap[i] = _upperBoundOverlap[i] - _lowerBoundOverlap[i];

            _positionCount *= _sizeOverlap[i];
        }
#ifndef SCIDB_CLIENT
        if (array.common.tileMode) {
            _tileSize = Config::getInstance()->getOption<int>(CONFIG_TILE_SIZE);
            position_t tilesPerChunk = Config::getInstance()->getOption<int>(CONFIG_TILES_PER_CHUNK);
            if (tilesPerChunk != 0) {
                _tileSize = max(_tileSize, positionCount()/tilesPerChunk);
            }
        } else {
            _tileSize = 1;
        }
#else
        _tileSize = 1;
#endif
    }

    /** remove mapping to chunk */
    void unmap()
    {
        _mapped = false;
    }

    /** does @param actual aligned to tile start position? */
    bool aligned(Coordinates const& actual) const
    {
        SCIDB_ASSERT(mapped());
        SCIDB_ASSERT(insideOverlap(actual));
        if (array.common.tileMode) {
            return aligned(coord2position(actual));
        } else {
            return true;
        }
    }

    /**
     * align @coord to tile start position
     *   @param coord input coordinate
     *   @param diff difference between input @param coord and begin of tile
     *   @param left difference between input @param coord and end of tile
     */
    Coordinates align(Coordinates const& coord,
                      position_t& diff,
                      position_t& left) const
    {
        SCIDB_ASSERT(mapped());
        SCIDB_ASSERT(insideOverlap(coord));
        SCIDB_ASSERT(array.common.tileMode);
        SCIDB_ASSERT(position2coord(coord2position(coord)) == coord);
        position_t result = coord2position(coord);
        diff = result % tileSize();
        left = min(tileSize()-diff, (_positionCount - result));
        result -= diff;
        SCIDB_ASSERT(result == coord2position(position2coord(result)));
        return position2coord(result);
    }

    /** difference between @param coord and end of tile */
    position_t left(Coordinates const& coord) const
    {
        position_t actual = coord2position(coord);
        position_t left = positionCount() - actual;
        return min(left, tileSize());
    }

    /**
      * move forward
      *   @param coord
      *   @param step
      *   @param left - difference between end of tile and new position
      */
    bool move(Coordinates& coord,
              position_t step,
              position_t& left) const
    {
        SCIDB_ASSERT(mapped());
        SCIDB_ASSERT(insideOverlap(coord));
        SCIDB_ASSERT(step > 0);
        if (!array.common.tileMode) {
            do
            {
                size_t i = array.common.n - 1;
                while(++coord[i] >= upperBound()[i]) {
                    if (i == 0) {
                        return false;
                    }
                    coord[i] = lowerBound()[i];
                    i -= 1;
                }
                --step;
            }
            while(step > 0);
            left = 1;
            return true;
        } else {
            position_t position = coord2position(coord);
            position += step;
            bool result = position < _positionCount;
            if (result) {
                left = tileSize() -
                        (position % tileSize());
                left =  min(left, (_positionCount - position));
                coord = position2coord(position);
            } else {
                coord = upperBound();
            }
            return result;
        }
    }

    /** move foward (without @param left) */
    bool move(Coordinates& coord, position_t step) const
    {
        position_t left;
        return move(coord, step, left);
    }

    /** move forward to tileSize() positions */
    bool next(Coordinates& coord, position_t& left) const
    {
        SCIDB_ASSERT(mapped());
        SCIDB_ASSERT(insideOverlap(coord));
        return move(coord, tileSize(), left);
    }

    /** move forward to tileSize() positions (witht @param left) */
    bool next(Coordinates& coord) const
    {
        position_t left;
        return next(coord, left);
    }

    /** total position count inside mapped chunk */
    position_t positionCount() const
    {
        return _positionCount;
    }

  private:
    bool inside(Coordinates const& l,
                             Coordinates const& u,
                             Coordinates const& a) const
    {
        SCIDB_ASSERT(mapped());
        if (a.size() != array.common.n) {
            return false;
        }
        for (size_t i = 0; i < array.common.n; ++i) {
            if (l[i] > a[i] || a[i] >= u[i]) {
                return false;
            }
        }
        return true;
    }

    bool aligned(position_t const& actual) const
    {
        SCIDB_ASSERT(mapped());
        SCIDB_ASSERT(actual < _positionCount);
        if (array.common.tileMode) {
            return actual % tileSize() == 0;
        } else {
            return true;
        }
    }

    position_t coord2position(Coordinates const& coord) const
    {
        SCIDB_ASSERT(mapped());
        SCIDB_ASSERT(insideOverlap(coord));
        position_t position = 0;
        /* stride-major-order */
        for (size_t i = 0; i < array.common.n; ++i) {
            position *= _sizeOverlap[i];
            position += coord[i] - lowerBoundOverlap()[i];
        }
        SCIDB_ASSERT(position < _positionCount);
        /*
        Temporary commented:
        (need another implementation of "aligned(Coordinates const&)".
        */
        /* SCIDB_ASSERT(aligned(coord) == aligned(position)); */
        return position;
    }

    Coordinates position2coord(position_t const &position) const
    {
        SCIDB_ASSERT(mapped());
        SCIDB_ASSERT(position < _positionCount);
        Coordinates coord;
        coord.resize(array.common.n);
        position_t remainder= position;
        /* stride-major-order */
        for (Size i = array.common.n; i > 0; --i) {
            Size const k = i - 1;
            coord[k] = lowerBoundOverlap()[k] + (remainder % _sizeOverlap[k]);
            remainder /= _sizeOverlap[k];
        }
        SCIDB_ASSERT(insideOverlap(coord));
        SCIDB_ASSERT(aligned(coord) == aligned(position));
        return coord;
    }
};

/**
  * ChunkPositionWalker provides way to iterate over chunk positions inside
  * some area inside array.
  */
class ChunkPositionWalker : boost::noncopyable
{
  public:
    /** Dimension count */
    Size const                  n;
    RepartCommonSettings const& common;
    RepartArraySettings const&  array;

  private:
    /** Lower bound of iterated area */
    Coordinates   _lowerBound;
    /** Upper bound of iterated area */
    Coordinates   _upperBound;
    /** Actual position inside iterated area */
    Coordinates   _position;

  public:
    /** param @array array for iteration */
    ChunkPositionWalker(RepartArraySettings const& array) :
        n(array.common.n),
        common(array.common),
        array(array),
        _lowerBound(n, 0),
        _upperBound(n, 0),
        _position(n, 0)
    {
    }

    /** iterate chunk positions inside "real" area */
    bool reset()
    {
        return reset(array.lowerBound(), array.realUpperBound(), true);
    }

    /** iterate positions of chunks which has values between lowerBound and upperBound */
    bool reset(Coordinates const& lowerBound,
               Coordinates const& upperBound,
               bool withOverlap)
    {
        OverlapVector overlap(n, 0);
        if (withOverlap) {
            overlap = array.overlap();
        }
        for (size_t i = 0; i < n; i++) {
            _lowerBound[i] = lowerBound[i] - overlap[i];
            _upperBound[i] = upperBound[i] + overlap[i];

            _lowerBound[i] = max(_lowerBound[i], array.lowerBound()[i]);
            _upperBound[i] = min(_upperBound[i], array.realUpperBound()[i]);
        }
        _lowerBound = array.align(_lowerBound);
        _position = _lowerBound;
        _position[n-1] -= array.interval()[n-1];
        return next();
    }

    /** move to position of next chunk inside iterated area (according to stride-major-order traversal) */
    bool next()
    {
        Size dimension = n -1;
        while(true)
        {
            _position[dimension] += array.interval()[dimension];
            if (_position[dimension] < _upperBound[dimension]) {
                return true;
            }
            if (dimension == 0) {
                return false;
            }
            _position[dimension] = _lowerBound[dimension];
            dimension -= 1;
        }
    }

    /** get current chunk position */
    Coordinates const& getPosition() const
    {
        return _position;
    }
};

/**
  * How many values left on last dimension?
  * Just last dimension important, because result chunk
  * is combination of unbreakable pieces from last dimension.
  * We return minimum from two possible remainder.
  *   @param firstChunk - first chunk for analyze
  *   @param secondChunk - second chunk for analyze
  *   @param first - coordinate of the begin of tile in @param firstChunk
  *   @param second - coordinate of the begin of tile in @param secondChunk
  */
position_t leftLD(RepartChunkSettings const& firstChunk,
                  RepartChunkSettings const& secondChunk,
                  Coordinates const& first,
                  Coordinates const& second)
{
    Size const n(firstChunk.array.common.n);
    SCIDB_ASSERT(secondChunk.array.common.n == n);
    SCIDB_ASSERT(first.size() == n);
    SCIDB_ASSERT(second.size() == n);
    Size const k = n - 1;
    position_t leftFirst = firstChunk.upperBound()[k] - first[k];
    position_t leftSecond = secondChunk.upperBound()[k] - second[k];
    SCIDB_ASSERT(leftFirst > 0);
    SCIDB_ASSERT(leftSecond > 0);
    return min(leftFirst, leftSecond);
}

Coordinates getMoved(RepartChunkSettings const& chunk,
                     Coordinates const& coord,
                     position_t const skip)
{
    SCIDB_ASSERT(chunk.array.common.n == coord.size());
    Coordinates moved = coord;
    bool result = chunk.move(moved, skip);
    SCIDB_ASSERT(result);
    return moved;
}

/**
  * Wrapper on leftLD, has two additional arguments:
  *   @param skipFirst - how many positions in tile
  *     from @param firstChunk should be skipped.
  *   @param skipSecond - how many positions in tile
  *     from @param secondChunk should be skipped.
  */
position_t leftLD(RepartChunkSettings const& firstChunk,
                  RepartChunkSettings const& secondChunk,
                  Coordinates const& first,
                  Coordinates const& second,
                  position_t const skipFirst,
                  position_t const skipSecond)
{
    Size const n(firstChunk.array.common.n);
    SCIDB_ASSERT(secondChunk.array.common.n == n);
    SCIDB_ASSERT(first.size() == n);
    SCIDB_ASSERT(second.size() == n);
    SCIDB_ASSERT(skipFirst < firstChunk.tileSize());
    SCIDB_ASSERT(skipSecond < secondChunk.tileSize());
    Coordinates first_moved = first;
    Coordinates second_moved = second;
    if (skipFirst > 0) {
        bool result = firstChunk.move(first_moved, skipFirst);
        SCIDB_ASSERT(result);
    }
    if (skipSecond > 0) {
        bool result = secondChunk.move(second_moved, skipSecond);
        SCIDB_ASSERT(result);
    }
    return leftLD(firstChunk, secondChunk,
                  skipFirst > 0 ? first_moved : first,
                  skipSecond > 0 ? second_moved : second);
}

/** wrapper on leftLD - same coordinate @param actual used for both chunks */
position_t leftLD(RepartChunkSettings const& firstChunk,
                  RepartChunkSettings const& secondChunk,
                  Coordinates const& actual)
{
    return leftLD(firstChunk, secondChunk, actual, actual);
}

/**
 * /brief class TileDataReader allows read and skip data from tiles.
 *  Also supports (emulates) empty tiles (for empty chunks)
 */
class TileDataReader : boost::noncopyable
{
    friend class TileWriter;
  protected:
    bool                 _inited;
    bool                 _empty;
    TypeId const         _type;
    RLEPayload::iterator _reader;

  public:
    typedef Value* ValuePointer;

    virtual ~TileDataReader()
    {
        if (inited() && !empty()) {
            _reader.reset();
        }
    }

    TileDataReader(TypeId const& type) :
        _inited(false), _empty(false), _type(type)
    {
    }

    bool inited() const
    {
        return _inited;
    }

    bool empty() const
    {
        SCIDB_ASSERT(inited());
        return _empty;
    }

    void reset(ValuePointer a_value)
    {
        SCIDB_ASSERT(!inited());
        _empty = (0 == a_value);
        if (!_empty) {
            _reader = a_value->getTile(_type);
        }
        _inited = true;
    }

    position_t skip(position_t count)
    {
        SCIDB_ASSERT(inited());
        SCIDB_ASSERT(empty() || count == 0 || !_reader.end());
        if (!empty() && count > 0) {
            SCIDB_ASSERT(!_reader.end());
            _reader += count;
        }
        return count;
    }

    void complete()
    {
        SCIDB_ASSERT(inited());
        if (!empty()) {
            _reader.reset();
        }
        _inited = false;
    }
};

class TileBitmapReader : public TileDataReader
{
  public:
    TileBitmapReader(TypeId const& type) : TileDataReader(type)
    {
        SCIDB_ASSERT(_type == TID_BOOL);
    }

    position_t skip(position_t count)
    {
        SCIDB_ASSERT(_type == TID_BOOL);
        SCIDB_ASSERT(inited());
        SCIDB_ASSERT(count > 0);
        SCIDB_ASSERT(empty() || !_reader.end());
        if (empty()) {
            return count;
        } else {
            SCIDB_ASSERT(!_reader.end());
            return _reader.skip(count);
        }
    }
};


/**
 * /brief class TileDataWithBitmapReader allows read and skip data from data tile.
 *  Also supports (emulates) empty tiles (for empty chunks)
 */
class TileDataWithBitmapReader : boost::noncopyable
{
    friend class TileWriter;
  private:
    TileBitmapReader _bitmap;
    TileDataReader _data;

  public:
    typedef std::pair<Value*, Value*> ValuePointer;

    TileDataWithBitmapReader(TypeId const& a_dataType) :
        _bitmap(TID_BOOL),
        _data(a_dataType)
    {
    }

    bool inited() const
    {
        bool bitmap = _bitmap.inited();
        bool data = _data.inited();
        SCIDB_ASSERT(bitmap == data);
        return data;
    }

    bool empty() const
    {
        bool bitmap = _bitmap.empty();
        bool data = _data.empty();
        SCIDB_ASSERT(bitmap == data);
        return data;
    }

    void reset(ValuePointer a_value)
    {
        _bitmap.reset(a_value.first);
        _data.reset(a_value.second);
        SCIDB_ASSERT(_bitmap.empty() == _data.empty());
    }

    position_t skip(position_t count)
    {
        return _data.skip(_bitmap.skip(count));
    }

    void complete()
    {
        _bitmap.complete();
        _data.complete();
    }
};

/**
  * /brief class TileWriter allows to write result tiles by
  * Tile*Reader instances
  */
class TileWriter : boost::noncopyable
{
  private:
    Value                        _value;
    RLEPayload::append_iterator* _appender;
    TypeId                       _type;

  private:
    Value _trueValue;
    Value _falseValue;
    Value _defaultValue;

  private:
    void fill(position_t count,
              bool data) const
    {
        SCIDB_ASSERT(count > 0);
        SCIDB_ASSERT(_appender);
        if (data) {
            _appender->add(_trueValue, count);
        } else {
            _appender->add(_falseValue, count);
        }
    }

  public:
    TileWriter(TypeId const& a_type, Value const& defaultValue) :
        _appender(NULL), _type(a_type), _defaultValue(defaultValue)
    {
        _trueValue.setBool(true);
        _falseValue.setBool(false);
    }

    virtual ~TileWriter()
    {
        SCIDB_ASSERT(!_appender);
    }

    void reset()
    {
        SCIDB_ASSERT(!_appender);
        _appender = new RLEPayload::append_iterator(_value.getTile(_type));
    }

    void complete()
    {
        SCIDB_ASSERT(_appender);
        _appender->flush();
        delete _appender;
        _appender = NULL;
    }

    Value& value()
    {
        SCIDB_ASSERT(!_appender);
        return _value;
    }


    void copy(position_t count,
              TileBitmapReader& reader)
    {
        SCIDB_ASSERT(_type == TID_BOOL);
        SCIDB_ASSERT(count > 0);
        SCIDB_ASSERT(reader.inited());
        if (reader.empty()) {
            fill(count, false);
        } else while(count > 0) {
            SCIDB_ASSERT(!reader._reader.end());
            uint64_t current = min(reader._reader.getRepeatCount(),
                                   static_cast<uint64_t>(count));
            SCIDB_ASSERT(current > 0);
            fill(current, reader._reader.checkBit());
            reader._reader += current;
            count -= current;
        }
    }

    void copy(position_t count,
              TileDataReader& reader)
    {
        SCIDB_ASSERT(_type == reader._type);
        SCIDB_ASSERT(count > 0);
        SCIDB_ASSERT(reader.inited());
        if (reader.empty()) {
            _appender->add(_defaultValue, count);
        } else {
            while(count > 0) {
                SCIDB_ASSERT(!reader._reader.end());
                uint64_t processed = _appender->add(reader._reader, count, true);
                SCIDB_ASSERT(processed > 0);
                SCIDB_ASSERT(processed <= static_cast<uint64_t>(count));
                count -= processed;
            }
        }
    }

    void copy(position_t count,
              TileDataWithBitmapReader& reader)
    {
        SCIDB_ASSERT(count > 0);
        SCIDB_ASSERT(reader.inited());
        SCIDB_ASSERT(_type == reader._data._type);
        SCIDB_ASSERT(_appender);
        if (!reader.empty()) {
            while(count > 0) {
                SCIDB_ASSERT(!reader._bitmap._reader.end());
                uint64_t current = min(reader._bitmap._reader.getRepeatCount(),
                                       static_cast<uint64_t>(count));
                SCIDB_ASSERT(current > 0);
                if (reader._bitmap._reader.checkBit()) {
                    do
                    {
                        SCIDB_ASSERT(reader._bitmap._reader.checkBit());
                        SCIDB_ASSERT(!reader._data._reader.end());
                        uint64_t processed =
                                _appender->add(reader._data._reader, current);
                        SCIDB_ASSERT(processed > 0);
                        SCIDB_ASSERT(processed <= current);
                        reader._bitmap._reader += processed;
                        current -= processed;
                        count -= processed;
                    }
                    while(current > 0);
                } else {
                    reader._bitmap._reader += current;
                    count -= current;
                }
            }
        }
    }
};


/**
 * /brief class SafeTileReader is wrapper under TileBitmapReader and TileDataReader
 * which count & control the number of available in tile values
 * Class allows to catch user errors.
 */
template< typename Reader >
class SafeTileReader : boost::noncopyable
{
    friend class SafeTileWriter;
  private:
    Reader           _reader;
    bool             _work;
    position_t       _left;

  public:
    typedef typename Reader::ValuePointer ValuePointer;
    SafeTileReader(TypeId const& a_dataType) :
        _reader(a_dataType),
        _work(false),
        _left(0)
    {
    }

    SafeTileReader() :
        _reader(),
        _work(false),
        _left(0)
    {
    }

    virtual ~SafeTileReader()
    {
    }

  public:
    void reset(ValuePointer a_value, position_t a_left)
    {
        SCIDB_ASSERT(!work());
        _reader.reset(a_value);
        _left = a_left;
        _work = true;
    }

    void skip(position_t a_count)
    {
        SCIDB_ASSERT(work());
        SCIDB_ASSERT(a_count <= left());
        _reader.skip(a_count);
        _left -= a_count;
    }

    position_t left() const
    {
        SCIDB_ASSERT(work());
        return _left;
    }

    bool work() const
    {
        return _work;
    }

    void complete()
    {
        SCIDB_ASSERT(work());
        SCIDB_ASSERT(left() == 0);
        _reader.complete();
        _left = 0;
        _work = false;
    }
};

/**
 * /brief class SafeTileReader is wrapper under TileWriter
 * which count & control the number of available in tile values
 * Class allows to catch user errors.
 */
class SafeTileWriter : boost::noncopyable
{
  private:
    TileWriter       _writer;
    position_t       _left;
    bool             _work;

  public:
    SafeTileWriter(TypeId const& dataType, Value const& defaultValue) :
        _writer(dataType, defaultValue),
        _left(0),
        _work(false)
    {
    }

    virtual ~SafeTileWriter()
    {
        if (work()) {
            _writer.complete();
        }
    }

    void reset(position_t count)
    {
        SCIDB_ASSERT(!work());
        SCIDB_ASSERT(count > 0);
        _writer.reset();
        _left = count;
        _work =   true;
    }

    template< typename Reader >
    void copy(position_t a_count,
              SafeTileReader< Reader >& a_reader)
    {
        SCIDB_ASSERT(work());
        SCIDB_ASSERT(a_reader.work());
        SCIDB_ASSERT(a_count <= left());
        SCIDB_ASSERT(a_count <= a_reader.left());
        _writer.copy(a_count, a_reader._reader);
        _left -= a_count;
        a_reader._left -= a_count;
    }

    position_t left() const
    {
        SCIDB_ASSERT(work());
        return _left;
    }

    void complete()
    {
        SCIDB_ASSERT(work());
        SCIDB_ASSERT(left() == 0);
        _writer.complete();
        _work = false;
        _left = 0;
    }

    bool work() const
    {
        return _work;
    }

    Value& value()
    {
        SCIDB_ASSERT(!work());
        SCIDB_ASSERT(_left == 0);
        return _writer.value();
    }
};

class RepartArray : public DelegateArray
{
    friend class RepartChunk;
    friend class RepartArrayIterator;

private:
    RepartCommonSettings const common;
    RepartArraySettings  const source;
    RepartArraySettings  const result;
    boost::weak_ptr<Query> _query;

public:
    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const;
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;

    RepartArray(ArrayDesc const& desc,
                boost::shared_ptr<Array> const& array,
                const boost::shared_ptr<Query>& query,
                RepartAlgorithm const& algorithm,
                bool denseOpenOnce,
                bool outputFullChunks,
                bool singleInstance,
                bool tileMode,
                uint64_t sequenceScanThreshold);
};

class RepartArrayIterator : public DelegateArrayIterator
{
  private:
    typedef std::set<Coordinates, CoordinatesLess> ChunkPositionSet;
    typedef ChunkPositionSet::const_iterator ChunkPositionSetIterator;

  private:
    RepartArray const& repartArray;
    RepartCommonSettings const& common;
    RepartArraySettings  const& source;
    RepartArraySettings  const& result;

    bool const isSparseArray;
    MemChunk repartChunk; // materialized result chunk (used for sparse arrays)

    bool const hasChunkPositionSet;
    ChunkPositionSet chunkPositionSet;
    ChunkPositionSetIterator chunkPositionSetIterator;

    boost::shared_ptr<ConstArrayIterator> emptyIterator;

    ChunkPositionWalker walker;

    Coordinates current;
    bool hasCurrent;

    boost::weak_ptr<Query> _query;
    ConstChunk const* currentChunk;

  private:
    static bool calculateIsSparseArray(RepartCommonSettings const& common,
                                       boost::shared_ptr<ConstArrayIterator> inputIterator);
    void fillChunkPositionSet();
    void fillResultChunk(Coordinates const& chunkPos);
    bool walk(Coordinates const& chunkPos);
    void searchNotEmpty();

  public:
    RepartArrayIterator(RepartArray const& array, AttributeID attrID,
                        boost::shared_ptr<ConstArrayIterator> inputIterator,
                        const boost::shared_ptr<Query>& query);
    virtual void reset();
    virtual void operator ++();
    virtual bool setPosition(Coordinates const& pos);
    virtual bool end();

    virtual ConstChunk const& getChunk();
    virtual Coordinates const& getPosition();
    virtual boost::shared_ptr<Query> getQuery()
    {
        return _query.lock();
    }

};

class RepartChunk : public DelegateChunk
{
  private:
    friend class DataWalker;
    friend class DataAndBitmapWalker;
    template< typename Walker >
    friend class OpenOnceWalker;
    template< typename Walker, typename Reader >
    friend class DenseChunkIteratorTile;
    template< typename Walker >
    friend class DenseChunkIteratorValue;

  private:
    RepartArray const& repartArray;
    RepartCommonSettings const& common;
    RepartArraySettings  const& source;
    RepartArraySettings  const& result;
    MemChunk chunk;
    bool isSparseChunk;

    bool isBitmap() const
    {
        return getAttributeDesc().isEmptyIndicator();
    }

    bool hasBitmap() const
    {
        return getBitmapAttribute() != NULL;
    }

    const AttributeDesc* getBitmapAttribute() const
    {
        return array.getInputArray()->getArrayDesc().getEmptyBitmapAttribute();
    }

    TypeId getDataType() const
    {
        SCIDB_ASSERT(!isBitmap());
        return array.getArrayDesc().getAttributes()[attrID].getType();
    }

    Value const& getDefaultValue() const
    {
        return array.getArrayDesc().getAttributes()[attrID].getDefaultValue();
    }

    boost::shared_ptr<ConstArrayIterator> getInputArrayIterator() const
    {
        return getArrayIterator().getInputIterator();
    }

    boost::shared_ptr<ConstArrayIterator> getNewDataArrayIterator() const
    {
        return array.getInputArray()->getConstIterator(attrID);
    }

    boost::shared_ptr<ConstArrayIterator> getNewBitmapArrayIterator() const
    {
        SCIDB_ASSERT(!isBitmap());
        SCIDB_ASSERT(hasBitmap());
        return array.getInputArray()->getConstIterator(getBitmapAttribute()->getId());
    }

  private:
    template< typename DataWalker, typename DataAndBitmapWalker >
    ConstChunkIterator* createDenseChunkIterator(int mode) const
    {
        mode = common.mode(mode);
        if (!common.tileMode) {
            return new DenseChunkIteratorValue<DataWalker>(*this, mode);
        } else if (isBitmap()) {
            return new DenseChunkIteratorTile<
                    DataWalker, TileBitmapReader>
                    (*this, TID_BOOL, getDefaultValue(), mode);
        } else if (hasBitmap()) {
            return new DenseChunkIteratorTile<
                    DataAndBitmapWalker, TileDataWithBitmapReader >
                    (*this, getDataType(), getDefaultValue(), mode);
        } else {
            return new DenseChunkIteratorTile<
                    DataWalker, TileDataReader >
                    (*this, getDataType(), getDefaultValue(), mode);
        }
    }

  public:
    virtual bool isSparse() const;
    virtual boost::shared_ptr<ConstChunkIterator> getConstIterator(int mode) const;
    void initialize(Coordinates const& pos, bool sparse);

    RepartChunk(RepartArray const& array, DelegateArrayIterator const& iterator, AttributeID attrID);
};

/**
  * /brief class DataWalker walks over source (ArrayIterator/ChunkIterator)
  *   - return as value unified ValuePointer
  *   - return empty value for missed chunk
  *   - control the contract to interfaces of ArrayIterator/ChunkIterator
  */
class DataWalker
{
  private:
    boost::shared_ptr<ConstArrayIterator> _arrayIterator;
    boost::shared_ptr<ConstChunkIterator> _chunkIterator;
    bool _hasChunk;
    bool _hasValue;
    int const _mode;

  public:
    typedef Value* ValuePointer;

    bool hasChunk() const
    {
        return _hasChunk;
    }

    bool hasValue() const
    {
        return _hasValue;
    }

    /**
      * Try to set walker to specific chunk.
      *
      * @param where.
      *
      * @return true if chunk available false otherwise.
      */
    bool setChunkPosition(bool phantom, Coordinates const& where)
    {
        _hasValue = false;
        if (phantom || !_arrayIterator->setPosition(where)) {
            _chunkIterator.reset();
            return _hasChunk = false;
        }
        scidb::ConstChunk const& chunk = _arrayIterator->getChunk();
        _chunkIterator = chunk.getConstIterator(_mode);
        SCIDB_ASSERT(_chunkIterator);
        return _hasChunk = true;
    }

    bool setValuePosition(Coordinates const& where)
    {
        SCIDB_ASSERT(hasChunk());
        SCIDB_ASSERT(_chunkIterator);
        return _hasValue = _chunkIterator->setPosition(where);
    }

    Coordinates const& getChunkPosition() const
    {
        return _arrayIterator->getPosition();
    }

    ValuePointer value(bool valueOnEmpty = false)
    {
        if (!valueOnEmpty || hasChunk()) {
            SCIDB_ASSERT(hasValue());
            return &_chunkIterator->getItem();
        } else {
            return 0;
        }
    }

    bool isEmpty() const
    {
        SCIDB_ASSERT(hasValue());
        return _chunkIterator->isEmpty();
    }

    DataWalker(boost::shared_ptr<ConstArrayIterator> const& arrayIterator, int const mode) :
        _arrayIterator(arrayIterator),
        _hasChunk(false),
        _hasValue(false),
        _mode(mode)
    {
        SCIDB_ASSERT(_arrayIterator);
    }

    DataWalker(RepartChunk const& repartChunk, int const mode) :
        _arrayIterator(repartChunk.getInputArrayIterator()),
        _hasChunk(false),
        _hasValue(false),
        _mode(mode)
    {
        SCIDB_ASSERT(_arrayIterator);
    }
};

/**
  * /brief class DataAndBitmapWalker walks over source (ArrayIterator/ChunkIterator)
  * It synchornous walk over data and bitmap iterators (required for tile-mode).
  *   - return as value pair of ValuePointer
  *   - return pair of empty values for missed chunk
  *   - control the contract to interfaces of ArrayIterator/ChunkIterator
  */
class DataAndBitmapWalker
{
  private:
    DataWalker _bitmap;
    DataWalker _data;

  public:
    typedef std::pair<Value*, Value*> ValuePointer;

    bool hasChunk() const
    {
        bool bitmap = _bitmap.hasChunk();
        bool data = _data.hasChunk();
        SCIDB_ASSERT(bitmap == data);
        return data;
    }

    bool hasValue() const
    {
        bool bitmap = _bitmap.hasValue();
        bool data = _data.hasValue();
        SCIDB_ASSERT(bitmap == data);
        return data;
    }

    /**
      * Try to set walker to specific chunk.
      *
      * @param where.
      *
      * @return true if chunk available false otherwise.
      */
    bool setChunkPosition(bool phantom, Coordinates const& where)
    {
        bool bitmap = _bitmap.setChunkPosition(phantom, where);
        bool data = _data.setChunkPosition(phantom, where);
        SCIDB_ASSERT(bitmap == data);
        return data;
    }

    bool setValuePosition(Coordinates const& where)
    {
        bool bitmap = _bitmap.setValuePosition(where);
        bool data = _data.setValuePosition(where);
        SCIDB_ASSERT(bitmap == data);
        return data;
    }

    Coordinates const& getChunkPosition() const
    {
        Coordinates const& bitmap = _bitmap.getChunkPosition();
        Coordinates const& data = _data.getChunkPosition();
        SCIDB_ASSERT(bitmap == data);
        return data;
    }

    ValuePointer value(bool valueOnEmpty = false)
    {
        return std::make_pair(_bitmap.value(valueOnEmpty),
                              _data.value(valueOnEmpty));
    }

    /*DataWalker& bitmap()
    {
        return _bitmap;
    }*/

    DataAndBitmapWalker(
            boost::shared_ptr<ConstArrayIterator> const& bitmap,
            boost::shared_ptr<ConstArrayIterator> const& data,
            int const mode) :
        _bitmap(bitmap, mode|ConstChunkIterator::IGNORE_EMPTY_CELLS),
        _data(data, mode)
    {
    }

    DataAndBitmapWalker(RepartChunk const& repartChunk, int const mode) :
        _bitmap(repartChunk.getNewBitmapArrayIterator(),
                mode|ConstChunkIterator::IGNORE_EMPTY_CELLS),
        _data(repartChunk.getInputArrayIterator(), mode)
    {
    }
};

template< typename Walker >
class OpenOnceWalker
{
  private:
    typedef std::map< Coordinates, Walker > WalkerMap;
    typedef typename WalkerMap::iterator WalkerMapIterator;

    WalkerMap _map;
    WalkerMapIterator _current;

  public:
    typedef typename Walker::ValuePointer ValuePointer;

    bool hasChunk() const
    {
        return _map.end() != _current && _current->second.hasChunk();
    }

    bool hasValue() const
    {
        SCIDB_ASSERT(_map.end() != _current);
        return _current->second.hasValue();
    }

    /**
    * Try to set walker to specific chunk.
    *
    * @param where.
    *
    * @return true if chunk available false otherwise.
    */
    bool setChunkPosition(bool phantom, Coordinates const& where)
    {
        if (phantom) {
            _current = _map.end();
        } else {
            _current = _map.find(where);
        }
        return _map.end() != _current;
    }

    bool setValuePosition(Coordinates const& where)
    {
        SCIDB_ASSERT(_map.end() != _current);
        return _current->second.setValuePosition(where);
    }

    ValuePointer value(bool valueOnEmpty = false)
    {
        if (!valueOnEmpty || _map.end() != _current) {
            SCIDB_ASSERT(_map.end() != _current);
            return _current->second.value(valueOnEmpty);
        } else {
            return ValuePointer();
        }
    }

    bool isEmpty() const
    {
        SCIDB_ASSERT(hasValue());
        return _current->second.isEmpty();
    }

    static Walker createWalker(RepartChunk const&, int const mode);

    OpenOnceWalker(RepartChunk const& repartChunk, int const mode)
    {
        ChunkPositionWalker positionWalker(repartChunk.source);
        Coordinates lowerBound;
        Coordinates upperBound;
        repartChunk.result.getChunkBoundaries(
                    repartChunk.getFirstPosition(false), true,
                    lowerBound, upperBound);
        bool useSourceOverlap =
                repartChunk.common.singleInstance ||
                repartChunk.common.outputFullChunks;
        for(bool has = positionWalker.reset(
                lowerBound, upperBound, useSourceOverlap);
            has; has = positionWalker.next())
        {
            Walker walker(createWalker(repartChunk, mode));
            Coordinates const& position = positionWalker.getPosition();
            if (walker.setChunkPosition(false, position)) {
                _map.insert(std::make_pair(position, walker));
            }
        }
    }
};

template<>
DataWalker
OpenOnceWalker<DataWalker>::createWalker(
        RepartChunk const& rc, int const mode)
{
    return DataWalker(rc.getNewDataArrayIterator(), mode);
}

template<>
DataAndBitmapWalker
OpenOnceWalker<DataAndBitmapWalker>::createWalker(
        RepartChunk const& rc, int const mode)
{
    return DataAndBitmapWalker(rc.getNewBitmapArrayIterator(),
                              rc.getNewDataArrayIterator(),
                              mode);
}

/** State of DenseChunkIterator */
enum DenseChunkIteratorState
{
    /** current value unavailable */
    valueMissed,
    /** current value available, source on the begin of tile */
    valueOnCurrent,
    /** current value available, result tile was built, source on next position */
    valueOnNext,
    /** current value available, result tile was built, source is eof */
    valueOnEof
};

/*
   It should be used just for dense chunks.
   For iterate over materialized chunk (sparse algorithm) please use
   DelegateChunkIteator.
*/
template< typename Walker,
          typename Reader >
class DenseChunkIteratorTile : public ConstChunkIterator
{
  private:
    typedef SafeTileReader< Reader > SR;
    typedef SafeTileWriter SW;

  private:
    RepartChunk const&          _repartChunk;
    RepartArraySettings const& _sourceArray;
    RepartChunkSettings        _sourceChunk;
    RepartChunkSettings const  _resultChunk;

    Coordinates _source;
    position_t  _sourceSkip;
    position_t  _sourceLeft;

    Coordinates _result;
    position_t  _resultLeft;

    Coordinates _sourceChunkPosition;
    bool        _sourceChunkEmpty;

    int const _mode;
    DenseChunkIteratorState _state;

    Walker _walker;
    SR _reader;
    SW _writer;

  public:
    DenseChunkIteratorTile(RepartChunk const& chunk,
                           TypeId const& dataType,
                           Value const& defaultValue,
                           const int mode) :
        _repartChunk(chunk),
        _sourceArray(_repartChunk.source),
        _sourceChunk(_sourceArray,
                     _repartChunk.common.singleInstance ||
                     _repartChunk.common.outputFullChunks),
        _resultChunk(_repartChunk.result,
                      !(mode & ConstChunkIterator::IGNORE_OVERLAPS),
                      _repartChunk),
        _source(_sourceArray.common.n),
        _result(_resultChunk.array.common.n),
        _mode(_resultChunk.mode(mode)),
        _state(valueMissed),
        _walker(chunk, _sourceChunk.mode(mode)),
        _reader(dataType),
        _writer(dataType, defaultValue)
    {
        _reset();
    }

  private:
    void _reset()
    {
        _result = _resultChunk.lowerBound();
        _resultLeft = _resultChunk.left(_result);
        _resetSource();
    }

    void _resetSource()
    {
        SCIDB_ASSERT(_resultChunk.array.common.n == _result.size());
        SCIDB_ASSERT(_resultChunk.inside(_result));
        SCIDB_ASSERT(_resultChunk.aligned(_result));
        /*
           Check _sourceChunk: should be mapped.
           If mapped - chunk _currentSource:
               should be inside _sourceChunk
        */
        if (!_sourceChunk.mapped() || !_sourceChunk.inside(_result)) {
            /* value outside current source chunk */
            if (_repartChunk.common.outputFullChunks) {
                setSourceChunk(_resultChunk.lowerBoundMain());
            } else {
                setSourceChunk(_result);
            }
        }
        setSourceValue(_result);
        _state = valueOnCurrent;
    }

    void setSourceChunk(Coordinates const& coord)
    {
        _sourceChunkPosition = _sourceArray.align(coord);
        _sourceChunk.map(_sourceChunkPosition);
        _sourceChunkEmpty = !_walker.setChunkPosition(
                    _sourceChunk.phantom(), _sourceChunkPosition);
    }

    void setSourceValue(Coordinates const& coord)
    {
        SCIDB_ASSERT(_sourceChunk.mapped());
        SCIDB_ASSERT(_sourceChunk.inside(coord));
        _source = _sourceChunk.align(coord,
                                     _sourceSkip,
                                     _sourceLeft);
        SCIDB_ASSERT(_sourceChunk.insideOverlap(_source));
        SCIDB_ASSERT(_sourceChunk.aligned(_source));
        SCIDB_ASSERT(_sourceChunkEmpty || _walker.hasChunk());
        bool result = _sourceChunkEmpty || _walker.setValuePosition(_source);
        SCIDB_ASSERT(result);
        if (!result) {
            throw USER_EXCEPTION(
                        SCIDB_SE_EXECUTION,
                        SCIDB_LE_OPERATION_FAILED) << "setPosition";
        }
    }


  public:
    virtual bool setPosition(Coordinates const& position)
    {
        if (_result.size() != position.size()) {
            _state = valueMissed;
            return false;
        }
        if (!_resultChunk.inside(position)) {
            _state = valueMissed;
            return false;
        }
        if (!_resultChunk.aligned(position)) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION,
                                 SCIDB_LE_TILE_NOT_ALIGNED);
        }
        _result = position;
        _resultLeft = _resultChunk.left(_result);
        _resetSource();
        _state = valueOnCurrent;
        return true;
    }

    virtual void reset()
    {
        _reset();
    }

    virtual void operator++()
    {
        SCIDB_ASSERT(_resultChunk.inside(_result));
        SCIDB_ASSERT(_resultChunk.aligned(_result));
        if (_state == valueMissed) {
            throw USER_EXCEPTION(
                    SCIDB_SE_EXECUTION,
                    SCIDB_LE_OPERATION_FAILED)
                << "DenseChunkIteratorTile::operator++ from incorrect position";
        }
        if (!_resultChunk.next(_result, _resultLeft)) {
            _state = valueMissed;
        } else {
            switch(_state) {
                case(valueOnNext): {
                    /* source already on next position (DenseChunkIteratorTile::getItem moved it) */
                    _state = valueOnCurrent;
                    break;
                }
                case(valueOnCurrent): {
                    /* move source to begin off current value */
                    _resetSource();
                    break;
                }
                case(valueOnEof): {
                    /*
                       Internal Error.
                       DenseChunkIteratorTile::getItem found the end of the chunk,
                       but _result repositioned to next value.
                    */
                    throw USER_EXCEPTION(
                            SCIDB_SE_EXECUTION,
                            SCIDB_LE_OPERATION_FAILED)
                        << "DenseChunkIteratorTile::operator++ internal error"
                           " (last value in chunk but next position available)";
                    break;
                }
                case(valueMissed): {
                    /* unreachable case, we checked this _state upper in current method */
                    break;
                }
                default: {
                    throw USER_EXCEPTION(
                            SCIDB_SE_EXECUTION,
                            SCIDB_LE_OPERATION_FAILED)
                        << "DenseChunkIteratorTile::operator++ internal error"
                           " (unknown state of iterator)";
                }
            }
        }
    }

    virtual bool end()
    {
        return _state == valueMissed;
    }

    virtual bool isEmpty()
    {
        if (_state == valueMissed)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION,
                                 SCIDB_LE_NO_CURRENT_ELEMENT);

        SCIDB_ASSERT(_resultChunk.inside(_result));
        SCIDB_ASSERT(_resultChunk.aligned(_result));

        return false;
    }

    virtual int getMode()
    {
        return _mode;
    }

    virtual ConstChunk const& getChunk()
    {
        return _repartChunk;
    }

    virtual Coordinates const& getPosition()
    {
        if (_state == valueMissed)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION,
                                 SCIDB_LE_NO_CURRENT_ELEMENT);

        SCIDB_ASSERT(_resultChunk.inside(_result));
        SCIDB_ASSERT(_resultChunk.aligned(_result));

        return _result;
    }

    virtual Value& getItem()
    {
        if (_state == valueMissed) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION,
                                 SCIDB_LE_NO_CURRENT_ELEMENT);
        }

        SCIDB_ASSERT(_resultChunk.inside(_result));
        SCIDB_ASSERT(_resultChunk.aligned(_result));

        if (_state == valueOnNext || _state == valueOnEof) {
            return _writer.value();
        } else if (_state == valueOnCurrent) {
            _state = valueOnNext;
        } else {
            throw USER_EXCEPTION(
                    SCIDB_SE_EXECUTION,
                    SCIDB_LE_OPERATION_FAILED)
                << "DenseChunkIteratorTile::getItem internal error"
                   " (unknown state of iterator)";
        }

        SCIDB_ASSERT(_sourceChunk.mapped());
        SCIDB_ASSERT(_sourceChunk.insideOverlap(_source));
        SCIDB_ASSERT(_sourceChunk.aligned(_source));

        SCIDB_ASSERT(!_writer.work());
        _writer.reset(_resultLeft);
        position_t resultSkip = 0;

        do
        {
            SCIDB_ASSERT(_resultLeft == _writer.left());
            SCIDB_ASSERT(_sourceSkip + _sourceLeft ==
                   _sourceChunk.left(_source));

            position_t ldLeft = leftLD(_sourceChunk,
                                       _resultChunk,
                                       _source,
                                       _result,
                                       _sourceSkip,
                                       resultSkip);
            position_t count = min(min(_sourceLeft,
                                       _resultLeft),
                                   ldLeft);

            const bool endLD(count == ldLeft);
            const bool endValue(count == _sourceLeft);

            SCIDB_ASSERT(_sourceChunkEmpty || _walker.hasChunk());
            SCIDB_ASSERT(_sourceChunkEmpty || _walker.hasValue());
            if (!_reader.work()) {
                _reader.reset(_walker.value(true),
                              _sourceSkip + _sourceLeft);
                if (_sourceSkip > 0) {
                    _reader.skip(_sourceSkip);
                }
            }
            SCIDB_ASSERT(_sourceLeft == _reader.left());
            _writer.copy(count, _reader);

            resultSkip   += count;
            _resultLeft -= count;
            _sourceSkip += count;
            _sourceLeft -= count;
            SCIDB_ASSERT(_sourceChunkEmpty || _sourceLeft == _reader.left());
            SCIDB_ASSERT(_resultLeft == _writer.left());


            if (endLD) {
                Coordinates current = _source;
                if (_sourceSkip > count) {
                    bool result = _sourceChunk.move(current,
                                                     _sourceSkip - count);
                    SCIDB_ASSERT(result);
                }

                position_t resultLeft;
                if (!_resultChunk.move(current, count, resultLeft)) {
                    SCIDB_ASSERT(_resultLeft == 0);
                    SCIDB_ASSERT(_writer.left() == 0);
                    _state = valueOnEof;
                    break;
                }
                SCIDB_ASSERT(_resultLeft == 0 || resultLeft == _resultLeft);

                if (_sourceChunk.inside(current)) {
                    position_t sourceSkip;
                    position_t sourceLeft;
                    SCIDB_ASSERT(_sourceChunk.mapped());
                    SCIDB_ASSERT(_sourceChunk.inside(current));
                    current = _sourceChunk.align(current,
                                                  sourceSkip,
                                                  sourceLeft);
                    SCIDB_ASSERT(_sourceChunk.insideOverlap(current));
                    SCIDB_ASSERT(_sourceChunk.aligned(current));
                    if (_source == current) {
                        position_t skip = sourceSkip - _sourceSkip;
                        SCIDB_ASSERT(sourceSkip == _sourceSkip + skip);
                        SCIDB_ASSERT(sourceLeft + skip == _sourceLeft);
                        if (skip > 0) {
                            SCIDB_ASSERT(_sourceLeft == _reader.left());
                            _reader.skip(skip);
                            SCIDB_ASSERT(sourceLeft == _reader.left());
                        }
                        _sourceSkip = sourceSkip;
                        _sourceLeft = sourceLeft;
                        continue;
                    } else {
                        if (!endValue) {
                            _reader.skip(_sourceLeft);
                        }
                        _reader.complete();
                        bool result = _sourceChunkEmpty || _walker.setValuePosition(current);
                        SCIDB_ASSERT(result);
                        _source = current;
                        _sourceSkip = sourceSkip;
                        _sourceLeft = sourceLeft;
                    }
                } else {
                    if (_sourceLeft > 0) {
                        _reader.skip(_sourceLeft);
                    }
                    _reader.complete();
                    setSourceChunk(current);
                    setSourceValue(current);
                }
            } else {
                if (endValue) {
                    SCIDB_ASSERT(ldLeft > count);
                    SCIDB_ASSERT(_sourceLeft == 0);
                    bool result = _sourceChunk.move(_source,
                                                 _sourceSkip,
                                                 _sourceLeft);
                    SCIDB_ASSERT(result);
                    _reader.complete();
                    setSourceValue(_source);
                }
            }
        }
        while(_resultLeft > 0);
        _writer.complete();
        SCIDB_ASSERT(!_writer.work());
        return _writer.value();
    }
};

template< typename Walker >
class DenseChunkIteratorValue : public ConstChunkIterator
{
  private:
    RepartChunk const& _repartChunk;

    RepartArraySettings const& _sourceArray;
    RepartChunkSettings _sourceChunk;
    Walker _sourceWalker;

    RepartChunkSettings const  _resultChunk;

    const int _mode;
    bool      _has;
    bool      _full;

    Coordinates _current;
    Coordinates _currentSourceChunk;

  public:
    DenseChunkIteratorValue(RepartChunk const& chunk,
                            const int mode) :
        _repartChunk(chunk),
        _sourceArray(_repartChunk.source),
        _sourceChunk(_sourceArray,
                     _repartChunk.common.singleInstance ||
                     _repartChunk.common.outputFullChunks),
        _sourceWalker(chunk, _sourceChunk.mode(mode)),
        _resultChunk(_repartChunk.result,
                      !(mode & ConstChunkIterator::IGNORE_OVERLAPS),
                      _repartChunk),
        _mode(_resultChunk.mode(mode)),
        _has(false),
        _full(_repartChunk.common.outputFullChunks),
        _current(_sourceChunk.array.common.n),
        _currentSourceChunk(_sourceChunk.array.common.n)
    {
        _reset();
    }

  private:
    void _reset()
    {
        _current = _resultChunk.lowerBound();
        _has = setSource(true);
    }

    bool nextPosition()
    {
        SCIDB_ASSERT(_sourceChunk.mapped());
        SCIDB_ASSERT(_sourceChunk.inside(_current));
        return _resultChunk.next(_current);
    }

  public:
    /**
      * Old repart implementation (!Value Mode) call for source
      * arrayIterator->setPosition then chunkIterator->setPosition for
      * every coord when you call operator++ or setPosition.
      *
      * Current implementation reduce calls of arrayIterator->setPosition:
      *
      *   - if we already on required source chunk we does not call
      *     arrayIterator->setPosition.
      *
      *   - if required source chunk missed (if we know this),
      *     we can  skip arrayIterator->setPosition.
      *
      * Perhaps it is premature optimisation, but similar logic required
      * inside the Tile Mode, and this logic implemented already.
      *
      * @param "searchNotEmpty" describe how we should work when source chunk
      * or source value missed.
      *
      *   - true: we should try to skip missed value/chunk (get next value/chunk)
      *     while the end of the _resultChunk does not reached (while we have
      *     logical values inside _resultChunk).
      *
      *   - false: we should return false to user.
      */
    bool setSource(bool searchNotEmpty)
    {
        SCIDB_ASSERT(_resultChunk.array.common.n == _current.size());
        SCIDB_ASSERT(_resultChunk.inside(_current));
        do
        {
            /* _sourceChunk mapped and _current inside _sourceChunk (current source chunk) */
            bool sourceChunkmapped = _sourceChunk.mapped() && _sourceChunk.inside(_current);
            bool sourceChunkAvailable;
            if (sourceChunkmapped)
            {
                /* Does current source chunk available? */
                sourceChunkAvailable = _sourceWalker.hasChunk();
            }
            else
            {
                /* _currentSource outside _sourceChunk (or _sourceChunk does not mapped) */

                /* remap _sourceChunk */
                if (_full) {
                    _currentSourceChunk = _sourceArray.align(_resultChunk.lowerBoundMain());
                } else {
                    _currentSourceChunk = _sourceArray.align(_current);
                }
                _sourceChunk.map(_currentSourceChunk);
                /* try to walk to remapped _sourceChunk */
                sourceChunkAvailable = _sourceWalker.setChunkPosition(_sourceChunk.phantom(), _currentSourceChunk);
            }
            if (!sourceChunkAvailable) {
                /* current source chunk missed. */

                if (searchNotEmpty) {
                    /* try to switch to next chunk */
                } else {
                    /* current value missed */
                    break;
                }

                /* skip missed _sourceChunk */
                do
                {
                    position_t skip = leftLD(_sourceChunk,
                                             _resultChunk,
                                             _current);
                    if (!_resultChunk.move(_current, skip)) {
                        /* result is passed, finish */
                        return false;
                    }

                }
                while(_sourceChunk.inside(_current));

                /* source chunk skipped */
                _sourceChunk.unmap();
                continue;
            }
            SCIDB_ASSERT(_sourceWalker.hasChunk());

            /* try to walk to current value inside _sourceChunk. */
            if (_sourceWalker.setValuePosition(_current)) {
                /* value available, finish */
                return true;
            } else {
                /* current source value missed. */
            }

            if (searchNotEmpty) {
                /* try to switch to next value */
            } else {
                /* current value missed */
                break;
            }

            /* Move to next result value */
            if (nextPosition()) {
                /* next result value availabile */
                continue;
            } else {
                /* no more result values available, finish */
                return false;
            }
        }
        while(searchNotEmpty);
        return false;
    }

  public:
    virtual int getMode()
    {
        return _mode;
    }

    virtual Coordinates const& getPosition()
    {
        if (!_has)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return _current;
    }

    virtual bool setPosition(Coordinates const& a_current)
    {
        if (_current.size() != a_current.size()) {
            return _has = false;
        }
        if (!_resultChunk.inside(a_current)) {
            return _has = false;
        }
        _current = a_current;
        return _has = setSource(false);
    }

    virtual void reset()
    {
        _reset();
    }

    virtual void operator++()
    {
        _has = nextPosition() && setSource(true);
    }

    virtual bool end()
    {
        return !_has;
    }

    virtual Value& getItem()
    {
        if (!_has)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION,
                                 SCIDB_LE_NO_CURRENT_ELEMENT);
        return *_sourceWalker.value();
    }

    virtual bool isEmpty()
    {
        if (!_has)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION,
                                 SCIDB_LE_NO_CURRENT_ELEMENT);
        return _sourceWalker.isEmpty();
    }

    virtual ConstChunk const& getChunk()
    {
        return _repartChunk;
    }
};

//
// Repart chunk methods
//

RepartChunk::RepartChunk(RepartArray const& array,
                         DelegateArrayIterator const& iterator,
                         AttributeID attrID)
    : DelegateChunk(array, iterator, attrID, false),
      repartArray(array),
      common(array.common),
      source(array.source),
      result(array.result),
      isSparseChunk(false)
{
}

bool RepartChunk::isSparse() const
{
    return isSparseChunk;
}

boost::shared_ptr<ConstChunkIterator> RepartChunk::getConstIterator(int mode) const
{
    if (isSparseChunk) {
        return DelegateChunk::getConstIterator(mode);
    } else {
        if (common.denseOpenOnce) {
            return boost::shared_ptr<ConstChunkIterator>(
                        createDenseChunkIterator<
                             OpenOnceWalker< DataWalker >,
                             OpenOnceWalker< DataAndBitmapWalker > >(mode));
        } else {
            return boost::shared_ptr<ConstChunkIterator>(
                        createDenseChunkIterator<
                             DataWalker,
                             DataAndBitmapWalker>(mode));
        }
    }
}

void RepartChunk::initialize(Coordinates const& pos, bool isSparse)
{
    ArrayDesc const& desc = array.getArrayDesc();
    Address addr(attrID, pos);
    chunk.initialize(&array, &desc, addr, desc.getAttributes()[attrID].getDefaultCompressionMethod());
    isSparseChunk = isSparse;
    setInputChunk(chunk);
}

//
// RepartArrayIterator methods
//

bool RepartArrayIterator::calculateIsSparseArray(RepartCommonSettings const& common,
                                        boost::shared_ptr<ConstArrayIterator> inputIterator)
{
    switch(Config::getInstance()->getOption<int>(CONFIG_REPART_ALGORITHM)) {
    case RepartSparse: return true;
    case RepartDense: return false;
    default:
        inputIterator->reset();
        if (inputIterator->end()) {
            return false;
        }
        if (inputIterator->getChunk().isSparse()) {
            return true;
        }
        return false;
    }
}

void RepartArrayIterator::fillChunkPositionSet()
{
    ChunkPositionWalker walker(result);
    bool withOverlap = common.singleInstance || !common.outputFullChunks;
    Coordinates lowerBound;
    Coordinates upperBound;
    while (!inputIterator->end()) {
        source.getChunkBoundaries(inputIterator->getPosition(),
                                  withOverlap,
                                  lowerBound,
                                  upperBound);
        for(bool has = walker.reset(lowerBound, upperBound, withOverlap);
            has; has = walker.next()) {
            chunkPositionSet.insert(walker.getPosition());
        }
        ++(*inputIterator);
    }
}


void RepartArrayIterator::fillResultChunk(Coordinates const& resultChunkPosition)
{
    ArrayDesc const& desc = array.getArrayDesc();
    Address addr(attr, resultChunkPosition);
    repartChunk.initialize(&array, &desc, addr,
                           desc.getAttributes()[attr].getDefaultCompressionMethod());
    repartChunk.setSparse(true);

    boost::shared_ptr<Query> query(getQuery());
    boost::shared_ptr<ChunkIterator> resultChunkIterator =
            repartChunk.getIterator(query, ChunkIterator::NO_EMPTY_CHECK);

    ChunkPositionWalker walker(source);
    Coordinates lowerBound;
    Coordinates upperBound;
    result.getChunkBoundaries(resultChunkPosition, true, lowerBound, upperBound);

    int mode= common.mode(ChunkIterator::IGNORE_DEFAULT_VALUES|
                          ChunkIterator::IGNORE_EMPTY_CELLS,
                          !common.singleInstance && !common.outputFullChunks);
    /* disable tile mode according to unavailable (yet) implementation */
    mode = mode & (~ChunkIterator::TILE_MODE);

    for(bool has = walker.reset(lowerBound, upperBound, false);
        has; has = walker.next()) {
        if (inputIterator->setPosition(walker.getPosition())) {
            boost::shared_ptr<ConstChunkIterator> sourceChunkIterator =
                    inputIterator->getChunk().getConstIterator(mode);
            while (!sourceChunkIterator->end()) {
                Coordinates const& position = sourceChunkIterator->getPosition();
                if (resultChunkIterator->setPosition(position)) {
                    Value& value = sourceChunkIterator->getItem();
                    resultChunkIterator->writeItem(value);
                }
                ++(*sourceChunkIterator);
            }
        }
    }

    resultChunkIterator->flush();
    if (emptyIterator) {
        if (!emptyIterator->setPosition(resultChunkPosition)) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION,
                                 SCIDB_LE_OPERATION_FAILED) << "setPosition";
        }
        repartChunk.setBitmapChunk((Chunk*)&emptyIterator->getChunk());
    }
}

bool RepartArrayIterator::walk(Coordinates const& resultPosition)
{
    bool useResultOverlap = common.singleInstance || !common.outputFullChunks;
    Coordinates lowerBound;
    Coordinates upperBound;
    result.getChunkBoundaries(resultPosition, useResultOverlap, lowerBound, upperBound);
    ChunkPositionWalker walker(source);
    for(bool has = walker.reset(lowerBound, upperBound, false);
        has; has = walker.next()) {
        if (inputIterator->setPosition(walker.getPosition())) {
            current = resultPosition;
            hasCurrent = true;
            return true;
        }
    }
    hasCurrent = false;
    return false;
}

void RepartArrayIterator::searchNotEmpty()
{
    while(!walk(walker.getPosition())) {
        if (!walker.next()) {
            break;
        }
    }
}

RepartArrayIterator::RepartArrayIterator(RepartArray const& array,
                                         AttributeID attrID,
                                         boost::shared_ptr<ConstArrayIterator> inputIterator,
                                         const boost::shared_ptr<Query>& query)
    : DelegateArrayIterator(array, attrID, inputIterator),
      repartArray(array),
      common(array.common),
      source(array.source),
      result(array.result),
      isSparseArray(calculateIsSparseArray(common, inputIterator)),
      hasChunkPositionSet(!source.sequenceScan()),
      walker(result),
      current(array.common.n),
      _query(query),
      currentChunk(NULL)
{
    if (hasChunkPositionSet) {
        fillChunkPositionSet();
    }
    reset();
    if (isSparseArray) {
        AttributeDesc const* emptyAttr = array.getArrayDesc().getEmptyBitmapAttribute();
        if (emptyAttr != NULL && emptyAttr->getId() != attrID) {
            emptyIterator = array.getConstIterator(emptyAttr->getId());
        }
    }
}

ConstChunk const& RepartArrayIterator::getChunk()
{
    if (currentChunk == NULL) {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        if (isSparseArray) {
            if (current != repartChunk.getAddress().coords) {
                fillResultChunk(current);
            }
            currentChunk = &repartChunk;
        } else {
            ((RepartChunk&)*chunk).initialize(current, isSparseArray);
            currentChunk = chunk.get();
        }
    }
    return *currentChunk;
}

Coordinates const& RepartArrayIterator::getPosition()
{
    if (!hasCurrent)
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
    return current;
}

bool RepartArrayIterator::setPosition(Coordinates const& newCurrent)
{
    if (!result.inside(newCurrent)) {
        return hasCurrent = false;
    }
    currentChunk = NULL;
    current = newCurrent;
    array.getArrayDesc().getChunkPositionFor(current);
    if (hasChunkPositionSet) {
        chunkPositionSetIterator = chunkPositionSet.find(current);
        hasCurrent = chunkPositionSetIterator != chunkPositionSet.end();
        return hasCurrent;
    }
    else
    {
        //if containsChunk returns true, then inputIterator is set to the right position
        return walk(current);
    }
}

void RepartArrayIterator::operator ++()
{
    if (!hasCurrent)
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
    currentChunk = NULL;
    if (hasChunkPositionSet) {
        ++chunkPositionSetIterator;
        hasCurrent = chunkPositionSetIterator != chunkPositionSet.end();
        if (hasCurrent) {
            current = *chunkPositionSetIterator;
        }
    } else {
        if (walker.next()) {
            searchNotEmpty();
        } else {
            hasCurrent = false;
        }
    }
}

bool RepartArrayIterator::end()
{
    return !hasCurrent;
}

void RepartArrayIterator::reset()
{
    inputIterator->reset();
    currentChunk = NULL;
    if (hasChunkPositionSet) {
        chunkPositionSetIterator = chunkPositionSet.begin();
        hasCurrent = chunkPositionSetIterator != chunkPositionSet.end();
        if (hasCurrent) {
            current = *chunkPositionSetIterator;
        }
    } else {
        if (walker.reset()) {
            searchNotEmpty();
        } else {
            hasCurrent = false;
        }
    }
}

//
// RepartArray methods
//

DelegateChunk* RepartArray::createChunk(DelegateArrayIterator const* iterator, AttributeID id) const
{
    return new RepartChunk(*this, *iterator, id);
}

DelegateArrayIterator* RepartArray::createArrayIterator(AttributeID id) const
{
    boost::shared_ptr<Query> query(_query.lock());
    return new RepartArrayIterator(*this, id, inputArray->getConstIterator(id), query);
}

RepartArray::RepartArray(ArrayDesc const& desc,
                         boost::shared_ptr<Array> const& array,
                         const boost::shared_ptr<Query>& query,
                         RepartAlgorithm const& algorithm,
                         bool denseOpenOnce,
                         bool outputFullChunks,
                         bool singleInstance,
                         bool tileMode,
                         uint64_t sequenceScanThreshold)
    : DelegateArray(desc, array),
      common(algorithm,
             denseOpenOnce,
             outputFullChunks,
             singleInstance,
             tileMode,
             array->getArrayDesc().getDimensions().size()),
      source(common, array->getArrayDesc(), sequenceScanThreshold),
      result(common, desc, sequenceScanThreshold, &source),
      _query(query)
{
}

Array* createRepartArray(ArrayDesc const& desc,
                         boost::shared_ptr<Array> const& array,
                         const boost::shared_ptr<Query>& query,
                         RepartAlgorithm const& algorithm,
                         bool denseOpenOnce,
                         bool outputFullChunks,
                         bool singleInstance,
                         bool tileMode,
                         uint64_t sequenceScanThreshold)
{
    return new RepartArray(desc, array, query, algorithm, denseOpenOnce, outputFullChunks, singleInstance, tileMode, sequenceScanThreshold);
}

} /* namespace scidb */

