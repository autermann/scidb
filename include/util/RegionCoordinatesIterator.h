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
 * RegionCoordinatesIterator.h
 *
 *  Created on: Jun 15, 2012
 *      Author: dzhang
 *  Iterators over the chunk-start coordinates in a multi-dim region.
 */

#ifndef REGIONCOORDINATESITERATOR_H_
#define REGIONCOORDINATESITERATOR_H_

#include <array/Array.h>

namespace scidb
{

/**
 * Less than.
 */
inline bool operator< (const Coordinates& c1, const Coordinates& c2) {
    assert(c1.size()==c2.size());
    for (size_t i=0; i<c1.size(); ++i) {
        if (c1[i]<c2[i]) {
            return true;
        } else if (c1[i]>c2[i]) {
            return false;
        }
    }
    return false;
}

/**
 * Equal.
 */
inline bool operator== (const Coordinates& c1, const Coordinates& c2) {
    assert(c1.size()==c2.size());
    for (size_t i=0; i<c1.size(); ++i) {
        if (c1[i] != c2[i]) {
            return false;
        }
    }
    return true;
}

/**
 * Less than or equal.
 */
inline bool operator<= (const Coordinates& c1, const Coordinates& c2) {
    return c1<c2 || c1==c2;
}

/**
 * Greater than or equal.
 */
inline bool operator>=(const Coordinates& c1, const Coordinates& c2) {
    return !(c1<c2);
}

/**
 * Greater than.
 */
inline bool operator>(const Coordinates& c1, const Coordinates& c2) {
    return !(c1<=c2);
}

/**
 * RegionCoordinatesIteratorParam: parameters to the constructor of RegionCoordinatesIterator.
 */
struct RegionCoordinatesIteratorParam
{
    Coordinates _low;
    Coordinates _high;
    vector<size_t> _intervals;

    RegionCoordinatesIteratorParam(size_t size): _low(size), _high(size), _intervals(size)
    {}
};

/**
 * RegionCoordinatesIterator iterates through all the chunk-start coordinates, in a region described by a pair of Coordinates.
 */
class RegionCoordinatesIterator: public ConstIterator
{
    Coordinates _low;
    Coordinates _high;
    Coordinates _current;
    vector<size_t> _intervals;

public:
    /**
     * Constructor.
     */
    RegionCoordinatesIterator(const Coordinates& low, const Coordinates& high, const vector<size_t>& intervals) {
        init(low, high, intervals);
    }

    /**
     * Constructor.
     */
    RegionCoordinatesIterator(const RegionCoordinatesIteratorParam& param) {
        init(param._low, param._high, param._intervals);
    }

    /**
     * By default, every interval is 1.
     */
    RegionCoordinatesIterator(const Coordinates& low, const Coordinates& high) {
        vector<size_t> intervals(low.size());
        for (size_t i=0; i<intervals.size(); ++i) {
            intervals[i] = 1;
        }
        init(low, high, intervals);
    }

    /**
     * Destructor.
     */
    virtual ~RegionCoordinatesIterator() {
    }

    /**
     * Initialize the low and high coordinates.
     */
    void init(const Coordinates& low, const Coordinates& high, const vector<size_t>& intervals) {
        assert(low.size()==high.size());
        assert(intervals.size()==high.size());
        assert(low.size()>0);
        for (size_t i=0; i<low.size(); ++i) {
            assert(low[i] <= high[i]);
        }
        _low = low;
        _high = high;
        _intervals = intervals;
        _current = _low;
    }

    /**
     * Check if end is reached
     * @return true if iterator reaches the end of the region
     */
    virtual bool end() {
        return _current > _high;
    }

    /**
     * Position cursor to the next chunk.
     */
    virtual void operator ++() {
        for (size_t i=_current.size()-1; i>=1; --i) {
            _current[i] += _intervals[i];
            if (_current[i] <= _high[i]) {
                return;
            }
            _current[i] = _low[i];
        }
        _current[0] += _intervals[0];
    }

    /**
     * Get coordinates of the current element.
     */
    virtual Coordinates const& getPosition() {
        return _current;
    }

    /**
     * Set iterator's current positions
     * @return true if specified position is valid (in the region),
     * false otherwise
     * @note the pos MUST be a chunk start.
     */
    virtual bool setPosition(Coordinates const& pos) {
        assert(pos.size()==_current.size());
        for (size_t i=0; i<pos.size(); ++i) {
            // Out of bound?
            if (pos[i]<_low[i] || pos[i]>_high[i]) {
                return false;
            }

            // Not the first cell in a chunk?
            if ((pos[i]-_low[i]) % _intervals[i] != 0) {
               assert(false);
               throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "RegionCoordinatesIterator::setPosition";
            }
        }
        _current = pos;
        return true;
    }

    /**
     * Reset iteratot to the first coordinates.
     */
    virtual void reset() {
        _current = _low;
    }
};

}

#endif /* REGIONCOORDINATESITERATOR_H_ */
