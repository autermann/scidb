/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) © 2008-2011 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation version 3 of the License, or
* (at your option) any later version.
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

#include "array/Array.h"
#include "query/TypeSystem.h"
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"
#include "system/Utils.h"

using namespace boost;
using namespace std;

namespace scidb
{

    const uint64_t RLE_EMPTY_BITMAP_MAGIC = 0xEEEEAAAA00EEBAACLL;
    const uint64_t RLE_PAYLOAD_MAGIC = 0xDDDDAAAA000EAAACLL;
        
    bool checkChunkMagic(ConstChunk const& chunk)
    {
        if (chunk.isRLE()) {
            PinBuffer scope(chunk);
            if (chunk.getAttributeDesc().isEmptyIndicator())  {
                return *(uint64_t*)chunk.getData() == RLE_EMPTY_BITMAP_MAGIC;
            } else { 
                return *(uint64_t*)chunk.getData() == RLE_PAYLOAD_MAGIC;
            }
        }
        return true;
    }
                   
    size_t ConstRLEEmptyBitmap::packedSize() const {
        return sizeof(Header) + nSegs*sizeof(Segment);
    }

    void ConstRLEEmptyBitmap::pack(char* dst) const { 
        Header* hdr = (Header*)dst;
        hdr->magic = RLE_EMPTY_BITMAP_MAGIC;
        hdr->nSegs = nSegs;
        hdr->nNonEmptyElements = nNonEmptyElements;
        memcpy(hdr+1, seg, nSegs*sizeof(Segment));
    }

    ConstRLEEmptyBitmap::~ConstRLEEmptyBitmap()
    {
        if (chunk && chunkPinned) { 
            chunk->unPin();
        }
    }

    ConstRLEEmptyBitmap::ConstRLEEmptyBitmap(ConstChunk const& bitmapChunk) : chunk(NULL)
    {
        chunkPinned = bitmapChunk.pin();
        char* src = (char*)bitmapChunk.getData();
        if (src != NULL) { 
            Header* hdr = (Header*)src;
            assert(hdr->magic == RLE_EMPTY_BITMAP_MAGIC);
            nSegs = hdr->nSegs;            
            nNonEmptyElements = hdr->nNonEmptyElements;
            seg = (Segment*)(hdr+1);
            chunk = &bitmapChunk;
        } else { 
            nSegs = 0;
            nNonEmptyElements = 0;
            bitmapChunk.unPin();
            chunkPinned = false;
        }
    }
  

    ConstRLEEmptyBitmap::ConstRLEEmptyBitmap(char const* src) : chunk(NULL)
    {
        if (src != NULL) { 
            Header* hdr = (Header*)src;
            assert(hdr->magic == RLE_EMPTY_BITMAP_MAGIC);
            nSegs = hdr->nSegs; 
            nNonEmptyElements = hdr->nNonEmptyElements;           
            seg = (Segment*)(hdr+1);
        } else { 
            nSegs = 0;
            nNonEmptyElements = 0;
        }
    }

    boost::shared_ptr<RLEEmptyBitmap> ConstRLEEmptyBitmap::merge(ConstRLEEmptyBitmap const& other)
    {
        RLEEmptyBitmap* result = new RLEEmptyBitmap();
        result->reserve(nSegs);
        Segment segm;
        size_t i = 0, j = 0;
        while (i < nSegs && j < other.nSegs) {  
            if (seg[i].lPosition + seg[i].length <= other.seg[j].lPosition) { 
                i += 1;
            } else if (other.seg[j].lPosition + other.seg[j].length <= seg[i].lPosition) { 
                j += 1;
            } else {
                position_t start = seg[i].lPosition < other.seg[j].lPosition ? other.seg[j].lPosition : seg[i].lPosition;
                segm.lPosition = start;
                segm.pPosition = seg[i].pPosition + (start - seg[i].lPosition);
                if (seg[i].lPosition + seg[i].length < other.seg[j].lPosition + other.seg[j].length) { 
                    assert(seg[i].lPosition + seg[i].length > start);
                    segm.length = seg[i].lPosition + seg[i].length - start;
                    i += 1;
                } else {                    
                    assert(other.seg[j].lPosition + other.seg[j].length > start);
                    segm.length = other.seg[j].lPosition + other.seg[j].length - start;
                    j += 1;
                } 
                result->addSegment(segm);
            }
        }
        return shared_ptr<RLEEmptyBitmap>(result);
    }

    boost::shared_ptr<RLEEmptyBitmap> ConstRLEEmptyBitmap::join(ConstRLEEmptyBitmap const& other)
    {
        RLEEmptyBitmap* result = new RLEEmptyBitmap();
        result->reserve(nSegs);
        Segment segm;
        size_t i = 0, j = 0;
        segm.pPosition = 0;
        segm.lPosition = 0;
        segm.length = 0;
        while (i < nSegs || j < other.nSegs) {  
            if (i < nSegs && seg[i].lPosition <= segm.lPosition + segm.length) { 
                if (seg[i].lPosition + seg[i].length > segm.lPosition + segm.length) { 
                    segm.length = seg[i].lPosition + seg[i].length - segm.lPosition;
                }
                i += 1;
            } else if (j < other.nSegs && other.seg[j].lPosition <= segm.lPosition + segm.length) { 
                if (other.seg[j].lPosition + other.seg[j].length > segm.lPosition + segm.length) { 
                    segm.length = other.seg[j].lPosition + other.seg[j].length - segm.lPosition;
                }
                j += 1;
            }  else { 
                if (segm.length != 0)  {
                    result->addSegment(segm);
                    segm.pPosition += segm.length;
                }
                if (j == other.nSegs || (i < nSegs && seg[i].lPosition < other.seg[j].lPosition)) { 
                    segm.lPosition = seg[i].lPosition;
                    segm.length = seg[i].length;
                    i += 1;
                } else { 
                    segm.lPosition = other.seg[j].lPosition;
                    segm.length = other.seg[j].length;
                    j += 1;
                }
            }
        }
        if (segm.length != 0)  {
            result->addSegment(segm);
        }
        return shared_ptr<RLEEmptyBitmap>(result);
    }


    shared_ptr<RLEEmptyBitmap> ConstRLEEmptyBitmap::merge(ValueMap& vm)
    {
        shared_ptr<RLEEmptyBitmap> result (new RLEEmptyBitmap());
        result->reserve(vm.size());
        Segment segm;
        segm.pPosition = 0;
        segm.length = 0;
        segm.lPosition = -1;
        for (ValueMap::const_iterator i = vm.begin(); i != vm.end(); ++i) {
            assert(i->first >= segm.lPosition + segm.length);
            if (i->first != segm.lPosition + segm.length) { // hole
                if (segm.length != 0) {
                    result->addSegment(segm);
                    segm.length = 0;
                }
                segm.lPosition = i->first;
                segm.pPosition = getValueIndex(segm.lPosition);
                assert(segm.pPosition != -1);
            }
            segm.length += 1;
        }
        if (segm.length != 0) {
            result->addSegment(segm);
        }
        return result;
    }

    shared_ptr<RLEEmptyBitmap> ConstRLEEmptyBitmap::merge(uint8_t const* mergeBits)
    {
        shared_ptr<RLEEmptyBitmap> result (new RLEEmptyBitmap());
        Segment segm;
        segm.pPosition = 0;
        segm.length = 0;
        segm.lPosition = -1;

        ConstRLEEmptyBitmap::iterator iter = getIterator();
        size_t bitNum = 0;
        while (!iter.end())
        {
            bool isSet = mergeBits[bitNum >> 3] & (1 << (bitNum & 7));
            if (isSet)
            {
                if(segm.length==0)
                {
                    segm.lPosition = iter.getLPos();
                    segm.pPosition = iter.getPPos();
                }
                segm.length++;
            }
            else
            {
                if(segm.length!=0)
                {
                    result->addSegment(segm);
                    segm.length=0;
                }
            }
            ++iter;
            bitNum++;
        }

        if (segm.length!=0)
        {
            result->addSegment(segm);
        }
        return result;
    }


    ostream& operator<<(ostream& stream, ConstRLEEmptyBitmap const& map)
    {
        if (map.nSegments() == 0)
        {
            stream<<"[empty]";
        }

        for (size_t i=0; i<map.nSegments(); i++)
        {
            stream<<"["<<map.getSegment(i).lPosition<<","<<map.getSegment(i).pPosition<<","<<map.getSegment(i).length<<"];";
        }
        return stream;
    }


    RLEEmptyBitmap::RLEEmptyBitmap(RLEPayload& payload)
    {
        Segment bs;
        bs.lPosition = 0;
        bs.pPosition = 0;
        bs.length = 0;
        reserve(payload.nSegments());
        for (size_t i = 0, n = payload.nSegments(); i < n; i++) { 
            ConstRLEPayload::Segment const& ps = payload.getSegment(i);
            size_t bit = ps.valueIndex;
            size_t len = ps.length();
            if (ps.same) { 
                if (payload.checkBit(bit)) { 
                    if (bs.lPosition + bs.length == ps.pPosition) { 
                        bs.length += len;
                    } else { 
                        if (bs.length != 0) { 
                            container.push_back(bs);
                            bs.pPosition += bs.length;
                        }
                        bs.lPosition = ps.pPosition;
                        bs.length = len;
                    }
                } 
            } else { 
                for (size_t j = 0; j < len; j++) { 
                    if (payload.checkBit(bit+j)) { 
                        if (size_t(bs.lPosition + bs.length) == ps.pPosition + j) { 
                            bs.length += 1;
                        } else { 
                            if (bs.length != 0) { 
                                container.push_back(bs);
                                bs.pPosition += bs.length;
                            }
                            bs.lPosition = ps.pPosition + j;
                            bs.length = 1;
                        }
                    }
                }
            }
        }
        if (bs.length != 0) { 
            container.push_back(bs);
        }
        seg = &container[0];
        nSegs = container.size();
        nNonEmptyElements = bs.pPosition + bs.length;
    }
            
    position_t RLEEmptyBitmap::addRange(position_t lpos, position_t ppos, uint64_t sliceSize, size_t level, Coordinates const& chunkSize, Coordinates const& origin, Coordinates const& first, Coordinates const& last)
    {
        sliceSize /= chunkSize[level]; 
        lpos += (first[level] - origin[level])*sliceSize;
        if (level+1 < origin.size()) { 
            for (Coordinate beg = first[level], end = last[level]; beg <= end; beg++) {                 
                ppos = addRange(lpos, ppos, sliceSize, level+1, chunkSize, origin, first, last);
                lpos += sliceSize;
            }
        } else {
            assert(sliceSize == 1);
            size_t len =  last[level] - first[level] + 1;
            if (container.size() > 0 && container.back().lPosition + container.back().length == lpos) { 
                container.back().length += len;
            } else { 
                Segment segm;
                segm.lPosition = lpos;
                segm.pPosition = ppos;
                segm.length = len;
                container.push_back(segm);
            }
            ppos += len;
        }
        return ppos;
    }

    /** /brief BitmapReplicator managed copier of bitmap (replicator). */
    class BitmapReplicator : boost::noncopyable
    {
      public:
        typedef ConstRLEEmptyBitmap::Segment Segment;

      private:
        /** result bitmap */
        boost::shared_ptr<RLEEmptyBitmap> _resultBitmap;
        /** current result segment (not added to result yet) */
        Segment                           _resultSegment;

        /** array with source bitmap segments */
        Segment const * _sourceArray;
        size_t          _sourceCount;
        size_t          _sourceIndex;
        Segment         _sourceSegment;


      private:
        /** flush - add (if available) current result segment to result bitmap  */
        void _flush()
        {
            if (_resultSegment.length > 0) {
                SCIDB_ASSERT(_resultSegment.lPosition != static_cast<position_t>(-1));
                SCIDB_ASSERT(_resultSegment.pPosition != static_cast<position_t>(-1));
                _resultBitmap->addSegment(_resultSegment);
                _resultSegment.length = 0;
            }
        }

      public:
        BitmapReplicator(Segment* sourceArray, size_t sourceCount) :
            _resultBitmap(new RLEEmptyBitmap()),
            _sourceArray(sourceArray),
            _sourceCount(sourceCount),
            _sourceIndex(0)
        {
            _resultSegment.lPosition = static_cast<position_t>(-1);
            _resultSegment.pPosition = static_cast<position_t>(-1);
            _resultSegment.length = 0;
            if (!end()) {
                _sourceSegment = _sourceArray[0];
                _resultSegment.lPosition = _sourceSegment.lPosition;
                _resultSegment.pPosition = _sourceSegment.pPosition;
            }
        }

        /** logical position of current source segment. */
        position_t position() const
        {
            SCIDB_ASSERT(!end());
            return _sourceSegment.lPosition;
        }

        /** length of current source segment. */
        position_t length() const
        {
            SCIDB_ASSERT(!end());
            return _sourceSegment.length;
        }

        /** skip @param count positions from current source segment. */
        void skip(position_t count)
        {
            SCIDB_ASSERT(!end());
            SCIDB_ASSERT(count <= _sourceSegment.length);
            _sourceSegment.lPosition += count;
            _sourceSegment.pPosition += count;
            _sourceSegment.length -= count;
        }

        /**
          * copy @param count positions from current source segment
          * to current result segment.
          */
        void copy(position_t count)
        {
            SCIDB_ASSERT(!end());
            if (_resultSegment.length > 0) {
                position_t source = _sourceSegment.lPosition;
                position_t result = _resultSegment.lPosition + _resultSegment.length;
                SCIDB_ASSERT(result <= source);
                if (result < source) {
                    _flush();
                }
            }
            SCIDB_ASSERT(count <= _sourceSegment.length);
            if (_resultSegment.length == 0) {
                _resultSegment.lPosition = _sourceSegment.lPosition;
                _resultSegment.pPosition = _sourceSegment.pPosition;
            }
            _resultSegment.length += count;
            skip(count);
        }

        /** move to next source segment */
        void next()
        {
            SCIDB_ASSERT(!end());
            SCIDB_ASSERT(_sourceSegment.length == 0);
            ++_sourceIndex;
            if (!end()) {
                _sourceSegment = _sourceArray[_sourceIndex];
            }
        }

        /** source completed */
        bool end() const
        {
            SCIDB_ASSERT(_sourceIndex <= _sourceCount);
            return _sourceIndex == _sourceCount;
        }

        /** get result bitmap */
        boost::shared_ptr<RLEEmptyBitmap> result()
        {
            _flush();
            boost::shared_ptr<RLEEmptyBitmap> result = _resultBitmap;
            _resultBitmap.reset();
            return result;
        }
    };

    /** /brief Cut manager of replicator which cut the area outbound subarray */
    class Cut : boost::noncopyable
    {
      private:
        position_t _prefix;
        position_t _suffix;
        position_t _interval;
        position_t _main;
        boost::shared_ptr<Cut> _nested;

      private:
        Cut()
        {
        }

        position_t init(Coordinates const& lowerOrigin,
                        Coordinates const& upperOrigin,
                        Coordinates const& lowerResult,
                        Coordinates const& upperResult,
                        size_t index)
        {
            SCIDB_ASSERT(lowerOrigin[index] <= lowerResult[index]);
            SCIDB_ASSERT(lowerResult[index] <= upperResult[index]);
            SCIDB_ASSERT(upperResult[index] <= upperOrigin[index]);
            _prefix = lowerResult[index] - lowerOrigin[index];
            _suffix = upperOrigin[index] - upperResult[index];
            _interval = upperOrigin[index] + 1 - lowerOrigin[index];
            ++index;
            position_t multiplier = 1;
            if (index < lowerOrigin.size()) {
                _nested = boost::shared_ptr<Cut>(new Cut());
                multiplier = _nested->init(lowerOrigin, upperOrigin,
                                           lowerResult, upperResult,
                                           index);
                if (_nested->_prefix == 0 && _nested->_suffix == 0 && !_nested->_nested) {
                    _nested.reset();
                }
            }
            _prefix *= multiplier;
            _suffix *= multiplier;
            _interval *= multiplier;
            _main = _interval - _prefix - _suffix;
            return _interval;
        }

      public:
        /**
         * Constructor for Cut algorithm
         *
         * @param lowerOrigin lower coordinates of original array.
         * @param upperOrigin upper coordinates of original array.
         * @param lowerResult lower coordinates of subarray.
         * @param lowreResult lower coordinates of subarray.
         *
         */
        Cut(Coordinates const& lowerOrigin,
            Coordinates const& upperOrigin,
            Coordinates const& lowerResult,
            Coordinates const& upperResult)
        {
            size_t const n(lowerOrigin.size());
            SCIDB_ASSERT(n > 0);
            SCIDB_ASSERT(n == upperOrigin.size());
            SCIDB_ASSERT(n == lowerResult.size());
            SCIDB_ASSERT(n == upperResult.size());
            init(lowerOrigin, upperOrigin, lowerResult, upperResult, 0);
        }

        /**
          *  DataReplicator - class without following interface:
          *    - position_t position() const - logical position where source starts.
          *    - position_t length() const - length of source.
          *    - void skip(position_t count) - skip the count positions from source.
          *    - void copy(position_t count) - copy the count positions from source
          *        to result.
          *
          *  @param replicator should provide four described method
          */
        template< typename DataReplicator >
        void operator()(DataReplicator& replicator) const
        {
            process(replicator);
        }

      private:
        template< typename DataReplicator >
        position_t process(DataReplicator& replicator) const
        {
            position_t result = 0;

            if (replicator.length() == 0) {
                /* source segment completed */
                return result;
            }

            /*
             * Where source starts (relative to dimension begin)?
             *
             *                       source
             *                        <==>
             * PREFIX & MAIN & SUFFIX [__|__]
             *                           ^
             *                           |
             *                         source segment logical position
             */
            position_t source = replicator.position() % _interval;

            /*
             * How many position absent in MAIN (not overlap) area?
             *
             *                          skipMain
             *                          <==>
             * PREFIX[___] MAIN & SUFFIX[__|__]
             *                             ^
             *                             |
             *                         source segment logical position
             */
            position_t skipMain;

            if (source < _prefix) {
                /*
                 * Source starts inside PREFIX.
                 *
                 * How many position absent in MAIN (not overlap) area?
                 *          skipPrefix
                 *          <==>
                 * PREFIX[__|__] MAIN[___] SUFFIX [___]
                 *          ^
                 *          |
                 *        source segment logical position
                 */

                position_t skipPrefix = min(_prefix - source, replicator.length());
                result += skipPrefix;

                /* skip tail of PREFIX */
                replicator.skip(skipPrefix);

                if (replicator.length() == 0) {
                    /* source segment completed */
                    return result;
                }

                /*
                 * Now we exactly on begin of MAIN, and skipMain equal zero.
                 *
                 * PREFIX[___] MAIN[|__] SUFFIX[___]
                 *                  ^
                 *                  |
                 *                source segment logical position
                 */
                skipMain = 0;
            } else {
                /*
                 * Source starts in MAIN or SUFFIX.
                 *
                 * PREFIX[___] MAIN & SUFFIX [__|__]
                 *                              ^
                 *                              |
                 *                            source
                 */
                skipMain = source - _prefix;
            }

            SCIDB_ASSERT(replicator.length() > 0);

            if (skipMain < _main) {
                /*
                 * Source starts in MAIN area.
                 *
                 * How many position should we copy?
                 *                       copy
                 *                       <==>
                 * PREFIX[___] MAIN[____|____] SUFFIX[___]
                 *                      ^
                 *                      |
                 *                    source
                 */
                position_t copy = min(_main - skipMain, replicator.length());

                if (_nested) {
                    /* We have nested dimensions (not fully included) */
                    while(copy > 0) {
                        SCIDB_ASSERT(replicator.length() > 0);
                        /* Process one line from nested dimension */
                        position_t step = _nested->process(replicator);
                        copy -= step;
                        source += step;
                        result += step;
                    }
                } else {
                    /*
                      We do not have nested dimensions
                      (or their fully included).
                    */
                    position_t step = min(copy, replicator.length());
                    replicator.copy(step);
                    source += step;
                    result += step;
                }

                /*
                 * We are leaving MAIN area
                 *
                 *                      copy
                 *                   <==========>
                 * PREFIX[___] MAIN[_|_] SUFFIX[|__]
                 *                   ^          ^
                 *                   |          |
                 *                 source      source+copy
                 */
            }

            if (replicator.length() == 0) {
                /* source segment completed */
                return result;
            }

            /*
             * Source now in SUFFIX.
             *
             * PREFIX[___] MAIN[___] SUFFIX[_|_]
             *                               ^
             *                               |
             *                             source
             */
            position_t suffixLeft = min(_interval - source, replicator.length());
            replicator.skip(suffixLeft);

            result += suffixLeft;

            return result;

        }
    };

    boost::shared_ptr<RLEEmptyBitmap> ConstRLEEmptyBitmap::cut(
            Coordinates const& lowerOrigin,
            Coordinates const& upperOrigin,
            Coordinates const& lowerResult,
            Coordinates const& upperResult) const
    {
        BitmapReplicator replicator(seg, nSegs);

        /* I prevent creation of "Cut" on empty bitmaps (for fast work) */
        if (replicator.end()) {
            return replicator.result();
        }

        Cut cut(lowerOrigin, upperOrigin, lowerResult, upperResult);

        while(!replicator.end()) {
            cut(replicator);
            SCIDB_ASSERT(replicator.length() == 0);
            replicator.next();
        }

        return replicator.result();
    }

    RLEEmptyBitmap::RLEEmptyBitmap(ValueMap& vm, bool all)
    { 
        Segment segm;
        segm.pPosition = 0;
        segm.length = 0;
        segm.lPosition = 0;
        reserve(vm.size());
        for (ValueMap::const_iterator i = vm.begin(); i != vm.end(); ++i) { 
            assert(i->first >= segm.lPosition + segm.length);
            if (all || i->second.getBool()) {
                if (i->first != segm.lPosition + segm.length) { // hole 
                    if (segm.length != 0) { 
                        container.push_back(segm);
                        segm.pPosition += segm.length;
                        segm.length = 0;
                    }
                    segm.lPosition = i->first;
                } 
                segm.length += 1;
            }
        }
        if (segm.length != 0) { 
            container.push_back(segm);
        }
        nSegs = container.size(); 
        nNonEmptyElements = segm.pPosition + segm.length;
        seg = &container[0];
    }
    
    RLEEmptyBitmap::RLEEmptyBitmap(ConstChunk const& chunk)
    {         
        Segment segm;
        segm.pPosition = 0;
        segm.length = 0;
        segm.lPosition = 0;
        Coordinates origin = chunk.getFirstPosition(false);
        Dimensions const& dims = chunk.getArrayDesc().getDimensions();
        size_t nDims = dims.size();
        Coordinates chunkSize(nDims);
        for (size_t i = 0; i < nDims; i++) {
            origin[i] -= dims[i].getChunkOverlap();
            chunkSize[i] = dims[i].getChunkOverlap()*2 + dims[i].getChunkInterval();
        }
        boost::shared_ptr<ConstChunkIterator> it = chunk.getConstIterator(ConstChunkIterator::IGNORE_EMPTY_CELLS);
        assert(!(it->getMode() & ConstChunkIterator::TILE_MODE));
        while (!it->end()) { 
            Coordinates const& coord = it->getPosition();
            position_t pos = 0;
            for (size_t i = 0; i < nDims; i++) {
                pos *= chunkSize[i];
                pos += coord[i] - origin[i];
            }
            assert(pos >= segm.lPosition + segm.length);
            if (pos != segm.lPosition + segm.length) { // hole 
                if (segm.length != 0) { 
                    container.push_back(segm);
                    segm.pPosition += segm.length;
                    segm.length = 0;
                }
                segm.lPosition = pos;
            } 
            segm.length += 1;
            ++(*it);
        }
        if (segm.length != 0) { 
            container.push_back(segm);
        }
        nSegs = container.size();
        nNonEmptyElements = segm.pPosition + segm.length;        
        seg = &container[0];
    }
    
    RLEEmptyBitmap::RLEEmptyBitmap(char* data, size_t numBits)
    {
        Segment segm;
        segm.pPosition = 0;
        for (size_t i = 0; i < numBits; i++) {
            if ((data[i >> 3] & (1 << (i & 7))) != 0) { 
                segm.lPosition = i;
                while (++i < size_t(numBits) && (data[i >> 3] & (1 << (i & 7))) != 0);
                segm.length = i - segm.lPosition;                
                container.push_back(segm);
                segm.pPosition += segm.length;
            }
        }  
        nNonEmptyElements = segm.pPosition;            
        nSegs = container.size();
        seg = &container[0];
    }

    bool ConstRLEEmptyBitmap::iterator::skip(size_t n)
    {
        assert(!end());
        _currLPos += n;            
        if (_currLPos >= _cs->lPosition + _cs->length) {
            position_t ppos = getPPos();
            size_t l = 0, r = _bm->nSegs;
            ConstRLEEmptyBitmap::Segment* seg = _bm->seg;
            while (l < r) { 
                size_t m = (l + r) >> 1;
                if (seg[m].pPosition + seg[m].length <= ppos) { 
                    l = m + 1;
                } else { 
                    r = m;
                }
            }
            if (r == _bm->nSegs) {
                return false;
            }
            _currSeg = r;
            _cs = &seg[r];
            _currLPos = _cs->lPosition + ppos - _cs->pPosition;
        }
        return true;
    }


    //
    // Const Payload
    //

    void ConstRLEPayload::getValueByIndex(Value& value, size_t index) const
    {
        size_t fixedSize = elemSize == 0 ? sizeof(int) : elemSize;

        if (isBoolean)
        {
            value.setBool( payload[index>>3] & (1 << (index&7)));
            return;
        }

        char* rawData = payload + index*fixedSize;
        if (elemSize == 0) { // varying size
            int offs = *(int*)rawData;
            char* src = payload + varOffs + offs;
            size_t len;
            if (*src == 0) {
                len = *(int*)(src + 1);
                src += 5;
            } else {
                len = *src++ & 0xFF;
            }
            value.setData(src, len);
        } else {
            value.setData(rawData, fixedSize);
        }
    }

    void ConstRLEPayload::getCoordinates(ArrayDesc const& array, size_t dim, Coordinates const& chunkPos, Coordinates const& tilePos, boost::shared_ptr<Query> const& query, Value& value, bool withOverlap) const
    {
        Dimensions const& dims = array.getDimensions();
        size_t nDims = dims.size();

        RLEPayload::append_iterator appender(value.getTile(dims[dim].getType()));
        if (array.getEmptyBitmapAttribute() != NULL) {
            Coordinates origin(nDims);
            Coordinates currPos(nDims);
            Coordinates chunkIntervals(nDims);
            Coordinates overlapBegin(nDims);
            Coordinates overlapEnd(nDims);
            position_t  startPos = 0;

            for (size_t i = 0; i < nDims; i++) {             
                //Coordinate of first chunk element, including overlaps
                origin[i] = max(dims[i].getStartMin(), chunkPos[i] - dims[i].getChunkOverlap());
                //Coordinate of last chunk element, including overlaps
                Coordinate terminus = min(dims[i].getEndMax(), chunkPos[i] + dims[i].getChunkInterval() + dims[i].getChunkOverlap() - 1);
                chunkIntervals[i] = terminus - origin[i] + 1;
                startPos *= chunkIntervals[i];
                startPos += tilePos[i] - origin[i];
                overlapBegin[i] = max(dims[i].getStartMin(), chunkPos[i]);
                overlapEnd[i] = min(dims[i].getEndMax(), chunkPos[i] + dims[i].getChunkInterval() - 1);
            }


            iterator it(this);
            if (withOverlap) {
                while (!it.end()) {
                    position_t pPos = it.getPPos();
                    if (it.checkBit()) {
                        position_t pos = startPos + pPos;
                        for (size_t i = nDims; i-- != dim;) {
                            currPos[i] = origin[i] + (pos % chunkIntervals[i]);
                            pos /= chunkIntervals[i];
                        }
                        appender.add(array.getOriginalCoordinate(dim, currPos[dim], query));
                        ++it;
                    } else {
                        it += it.getRepeatCount();
                    }
                }
            } else {
                while (!it.end()) {
                    position_t pPos = it.getPPos();
                    if (it.checkBit()) {
                        bool skip = false;
                        position_t pos = startPos + pPos;
                        for (size_t i = nDims; i-- != dim;) {
                            currPos[i] = origin[i] + (pos % chunkIntervals[i]);
                            if ( ((currPos[i] < overlapBegin[i]) || (currPos[i] > overlapEnd[i])) ) {
                                skip = true;
                                break;
                            }
                            pos /= chunkIntervals[i];
                        }
                        if (!skip) {
                            appender.add(array.getOriginalCoordinate(dim, currPos[dim], query));
                        }
                        ++it;
                    } else {
                        it += it.getRepeatCount();
                    }
                }
            }
        } else {
            Coordinate start;
            Coordinate end;
            uint64_t interval = 1;
            uint64_t offset = 0;
            Coordinates pos = tilePos;
            if (withOverlap) {
                start = max(dims[dim].getStart(), Coordinate(chunkPos[dim] - dims[dim].getChunkOverlap()));
                end = min(dims[dim].getEndMax(), Coordinate(chunkPos[dim] + dims[dim].getChunkInterval() + dims[dim].getChunkOverlap() - 1));
                for (size_t i = nDims; i-- != 0; ) {
                    if (pos[i] > dims[i].getEndMax()) {
                        for (size_t j = 0; j < nDims; j++) {
                            pos[j] = max(dims[j].getStart(), Coordinate(chunkPos[j] - dims[j].getChunkOverlap()));
                        }
                        if (i != 0) {
                            pos[i-1] += 1;
                        }
                    } else if (pos[i] < dims[i].getStart()) {
                        pos[i] = dims[i].getStart();
                    }
                }
                for (size_t i = dim; ++i < nDims;) {
                    Coordinate rowStart = max(dims[i].getStart(), Coordinate(chunkPos[i] - dims[i].getChunkOverlap()));
                    Coordinate rowEnd = min(dims[i].getEndMax(), Coordinate(chunkPos[i] + dims[i].getChunkInterval() + dims[i].getChunkOverlap() - 1));
                    uint64_t rowLen = rowEnd - rowStart + 1;
                    interval *= rowLen;
                    offset *= rowLen;
                    assert(pos[i] >= rowStart);
                    offset += pos[i] - rowStart;
                }
            } else {
                start = chunkPos[dim];
                end = min(dims[dim].getEndMax(), Coordinate(chunkPos[dim] + dims[dim].getChunkInterval() - 1));
                for (size_t i = nDims; i-- != 0; ) {
                    if (pos[i] > dims[i].getEndMax() || pos[i] >= chunkPos[i] + dims[i].getChunkInterval()) {
                        for (size_t j = 0; j < nDims; j++) {
                            pos[j] = chunkPos[j];
                        }
                        if (i != 0) {
                            pos[i-1] += 1;
                        }
                    } else if (pos[i] < chunkPos[i]) {
                        pos[i] = chunkPos[i];
                    }
                }
                for (size_t i = dim; ++i < nDims;) {
                    Coordinate rowStart = chunkPos[i];
                    Coordinate rowEnd = min(dims[i].getEndMax(), Coordinate(chunkPos[i] + dims[i].getChunkInterval() - 1));
                    uint64_t rowLen = rowEnd - rowStart + 1;

                    interval *= rowLen;
                    offset *= rowLen;
                    assert(pos[i] >= rowStart);
                    offset += pos[i] - rowStart;
                }
            }
            Coordinate curr = pos[dim];
            assert(curr >= start && curr <= end);
            assert(offset < interval);
            uint64_t len = interval - offset;
            uint64_t n = count();
            if (n != 0) {
                while (true) {
                    appender.add(array.getOriginalCoordinate(dim, curr, query), len);
                    if (n <= len)  {
                        break;
                    }
                    n -= len;
                    len = interval;
                    if (++curr > end) {
                        curr = start;
                    }
                }
            }
        }
        appender.flush();
    }

    bool ConstRLEPayload::getValueByPosition(Value& value, position_t pos) const {
        size_t l = 0, r = nSegs;
        while (l < r) {
            size_t m = (l + r) >> 1;
            if (seg[m+1].pPosition <= pos) {
                l = m + 1;
            } else {
                r = m;
            }
        }
        if (r == nSegs) { 
            return false;
        }
        if (seg[r].null) {
            value.setNull(seg[r].valueIndex);
        } else {
            getValueByIndex(value, seg[r].valueIndex + (seg[r].same ? 0 : pos - seg[r].pPosition));
        }
        return true;
    }

    size_t ConstRLEPayload::packedSize() const {
        return sizeof(Header) + (nSegs+1)*sizeof(Segment) + dataSize;
    }
        
    void ConstRLEPayload::pack(char* dst) const
    {
        Header* hdr = (Header*)dst;
        hdr->magic = RLE_PAYLOAD_MAGIC;
        hdr->nSegs = nSegs;
        hdr->elemSize = elemSize;
        hdr->dataSize = dataSize;
        hdr->varOffs = varOffs;
        hdr->isBoolean = isBoolean;
        dst += sizeof(Header);
        if (seg != NULL) { // in case of tile append payload may stay without termination element
            memcpy(dst, seg, (nSegs+1)*sizeof(Segment));
        } else { 
            assert(nSegs == 0);
            ((Segment*)dst)->pPosition = 0;
        }
        dst += (nSegs+1)*sizeof(Segment);
        memcpy(dst, payload, dataSize);
    }

    char* ConstRLEPayload::getRawVarValue(size_t index, size_t& size) const
    {
        if (elemSize == 0) { // varying size
            int offs = *(int*)(payload + index * 4);
            char* src = payload + varOffs + offs;
            if (*src == 0) {
                size = *(int*)(src + 1);
                src += 5;
            } else {
                size = *src++ & 0xFF;
            }
            return src;
        } else if (isBoolean)
        {
            size = 1;
            return &(payload[index>>3]);
        }
        else
        {
            size = elemSize;
            return payload + index * elemSize;
        }
    }

    ConstRLEPayload::ConstRLEPayload(char const* src)
    {
        if (src == NULL) { 
            nSegs = 0;
            elemSize = 0;
            dataSize = 0;
            varOffs = 0;
            isBoolean = false;
            seg = NULL;
            payload = NULL;
        } else { 
            Header* hdr = (Header*)src;
            assert(hdr->magic == RLE_PAYLOAD_MAGIC);
            nSegs = hdr->nSegs;
            elemSize = hdr->elemSize;
            dataSize = hdr->dataSize;
            varOffs = hdr->varOffs;
            isBoolean = hdr->isBoolean;
            seg = (Segment*)(hdr+1);
            payload = (char*)(seg + nSegs + 1);
        }
    }

    ConstRLEPayload::iterator::iterator(ConstRLEPayload const* payload):
            _payload(payload)
    {
        reset();
    }

    bool ConstRLEPayload::iterator::isDefaultValue(Value const& defaultValue)
    {
        assert(!end());
        if (defaultValue.isNull()) {
            return _cs->null && defaultValue.getMissingReason() == _cs->valueIndex;
        } else if (_cs->null || !_cs->same) { 
            return false;
        }
        size_t index = _cs->valueIndex;
        size_t valSize;
        char* data = _payload->getRawVarValue(index, valSize);
        return _payload->isBoolean
            ? defaultValue.getBool() == ((*data & (1 << (index&7))) != 0)
            : defaultValue.size() == valSize && memcmp(data, defaultValue.data(), valSize) == 0;
    }

    void ConstRLEPayload::iterator::getItem(Value& item)
    {
        assert(!end());
        if(_cs->null)
        {
            item.setNull(_cs->valueIndex);
        }
        else
        {
            size_t index;
            size_t valSize;
            if(_cs->same)
            {
                index = _cs->valueIndex;
            }
            else
            {
                index = _cs->valueIndex + _currPpos - _cs->pPosition;
            }

            char* data = _payload->getRawVarValue(index, valSize);
            if(_payload->isBoolean)
            {
                item.setBool((*data) & (1 << (index&7)));
            }
            else
            {
                item.setData(data, valSize);
            }
        }
    }

    ostream& operator<<(ostream& stream, ConstRLEPayload const& payload)
    {
        if (payload.nSegments() == 0)
        {
            stream<<"[empty]";
        }

        stream<<"eSize "<<payload.elementSize()<<" dSize "<<payload.payloadSize()<<" segs ";
        for (size_t i=0; i<payload.nSegments(); i++)
        {
            stream<<"["<<payload.getSegment(i).pPosition<<","<<payload.getSegment(i).same<<","<<payload.getSegment(i).null<<","<<payload.getSegment(i).valueIndex<<","<<payload.getSegment(i).length()<<"];";
        }
        return stream;
    }

    //
    // Payload
    //

    void RLEPayload::setVarPart(char const* varData, size_t varSize)
    {
        varOffs = data.size();
        data.resize(varOffs + varSize);
        memcpy(&data[varOffs], varData, varSize);
        dataSize = data.size();
        payload = &data[0];
        elemSize = 0;
    }

    void RLEPayload::setVarPart(vector<char>& varPart)
    {
        varOffs = data.size();
        data.insert(data.end(), varPart.begin(), varPart.end());
        dataSize = data.size();
        payload = &data[0];
    }
                                    

    void RLEPayload::appendValue(vector<char>& varPart, Value const& val, size_t valueIndex)
    {
        if (isBoolean)
        {
            assert(val.size() == 1);
            if (valueIndex % 8 == 0)
            {
                data.resize(++dataSize);
                data[dataSize-1]=0;
            }

            if(val.getBool())
            {
                data[dataSize-1] |= (1 << (valueIndex&7));
            }
        }
        else
        {
            const size_t fixedSize = elemSize == 0 ? sizeof(int) : elemSize;
            data.resize(dataSize + fixedSize);
            if (elemSize == 0)
            {
                int offs = varPart.size();
                *(int*)&data[dataSize] = offs;
                size_t len = val.size();
                if (len-1 > 0xFF) {
                    varPart.resize(offs + len + 5);
                    varPart[offs++] = 0;
                    *(int*)&varPart[offs] = len;
                    offs += 4;
                } else {
                    varPart.resize(offs + len + 1);
                    varPart[offs++] = len;
                }
                memcpy(&varPart[offs], val.data(), len);
            }
            else
            {
                if (val.size() > elemSize) {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TRUNCATION) << val.size() << fixedSize;
                }
                memcpy(&data[dataSize], val.data(), val.size());
            }
            dataSize += fixedSize;
        }
        payload = &data[0];
    }

    RLEPayload::RLEPayload(): ConstRLEPayload(), _valuesCount(0)
    {
    }

    RLEPayload::RLEPayload(ValueMap& vm, size_t nElems, size_t elemSize, Value const& defaultVal, bool isBoolean,  bool subsequent)
    { 
        Value const* currVal = NULL;
        Segment currSeg;
        data.reserve(isBoolean ? vm.size()/8 : vm.size()*(elemSize==0 ? 4 : elemSize));
        container.reserve(vm.size());
        currSeg.same = true;
        currSeg.pPosition = 0;

        vector<char> varPart;
        this->elemSize = elemSize;
        this->isBoolean = isBoolean;
        
        // write default value
        dataSize = 0;
        size_t valueIndex = 0;
        size_t segLength = 0;
        if (!defaultVal.isNull()) {
            appendValue(varPart, defaultVal, valueIndex);
            valueIndex += 1;
        }
        for (ValueMap::const_iterator i = vm.begin(); i != vm.end(); ++i) { 
            position_t pos = i->first;
            Value const& val = i->second;
            if (subsequent) { 
                pos = currSeg.pPosition + segLength;
            } else { 
                assert(pos >= position_t(currSeg.pPosition + segLength));
                if (val == defaultVal) { // ignore explicitly specified default values
                    continue;
                }
            }
            if (currVal == NULL // first element 
                || !currSeg.same // sequence of different values
                || *currVal != val // new value is not the same as in the current segment 
                || pos != position_t(currSeg.pPosition + segLength)) // hole
            { 
                int carry = 0;
                if (pos != position_t(currSeg.pPosition + segLength)) { // hole
                    if (segLength != 0) {
                        container.push_back(currSeg); // complete current sequence
                    }
                    // .. and insert sequence of default values
                    if (defaultVal.isNull()) { 
                        currSeg.null = true;
                        currSeg.valueIndex = defaultVal.getMissingReason();
                    } else { 
                        currSeg.null = false;
                        currSeg.valueIndex = 0;
                    }
                    currSeg.same = true;
                    currSeg.pPosition += segLength;
                    container.push_back(currSeg); 
                } else if (segLength != 0) { // subsequent element
                    if ((!currSeg.same || segLength == 1) && !val.isNull() && !currVal->isNull()) { 
                        if (*currVal == val) {  // sequence of different values is termianted with the same value as new one: cut this value from the sequence and form separate sequence of repeated values 
                            assert(!currSeg.same);
                            carry = 1;
                            segLength -= 1;
                        } else {  // just add value to the sequence of different values
                            currSeg.same = false;
                            segLength += 1;
                            appendValue(varPart, val, valueIndex);
                            valueIndex += 1;
                            currVal = &val;
                            continue;
                        }
                    } 
                    container.push_back(currSeg); // complete current sequence
                }
                if (val.isNull()) {
                    currSeg.null = true;
                    currSeg.valueIndex = val.getMissingReason();
                } else { 
                    currSeg.null = false;
                    if (carry) { 
                        currSeg.valueIndex = valueIndex-1;
                    } else { 
                        appendValue(varPart, val, valueIndex);
                        currSeg.valueIndex = valueIndex++;
                    }
                }
                currSeg.same = true;
                currSeg.pPosition = pos - carry;
                segLength = 1 + carry;
                currVal = &val;
            } else { // same subsequent value 
                segLength += 1;
            }
        }
        if (segLength != 0) { 
            container.push_back(currSeg); // save current segment
        }
        if (subsequent) { 
            nElems = currSeg.pPosition + segLength;
        } else if (currSeg.pPosition + segLength != nElems) {
            // tail sequence of default values
            if (defaultVal.isNull()) { 
                currSeg.null = true;
                currSeg.valueIndex = defaultVal.getMissingReason();
            } else { 
                currSeg.null = false;
                currSeg.valueIndex = 0;
            }
            currSeg.same = true;
            currSeg.pPosition += segLength;
            container.push_back(currSeg); 
        }
        nSegs = container.size();
        currSeg.pPosition = nElems;
        container.push_back(currSeg); // terminating segment (needed to calculate size)

        seg = &container[0];
        data.resize(dataSize + varPart.size());
        memcpy(&data[dataSize], &varPart[0], varPart.size());
        payload = &data[0];
        varOffs = dataSize;
        dataSize += varPart.size();
        _valuesCount = valueIndex;
    }

    RLEPayload::RLEPayload(Value const& defaultVal, size_t logicalSize, size_t elemSize, bool isBoolean)
    {
        Segment currSeg;
        currSeg.same = true;
        currSeg.pPosition = 0;

        vector<char> varPart;
        this->elemSize = elemSize;
        this->isBoolean = isBoolean;

        // write default value
        dataSize = 0;
        size_t valueIndex = 0;
        if (!defaultVal.isNull()) {
            appendValue(varPart, defaultVal, valueIndex);
            valueIndex += 1;
        }

        // generate one segment of default values
        if (defaultVal.isNull()) {
            currSeg.null = true;
            currSeg.valueIndex = defaultVal.getMissingReason();
        } else {
            currSeg.null = false;
            currSeg.valueIndex = 0;
        }
        container.push_back(currSeg);

        // generate the terminating segment
        nSegs = container.size();
        currSeg.pPosition = logicalSize;
        container.push_back(currSeg);

        seg = &container[0];
        data.resize(dataSize + varPart.size());
        memcpy(&data[dataSize], &varPart[0], varPart.size());
        payload = &data[0];
        varOffs = dataSize;
        dataSize += varPart.size();
        _valuesCount = valueIndex;
    }

    RLEPayload::RLEPayload(char* rawData, size_t rawSize, size_t varOffs, size_t elemSize, size_t nElems, bool isBoolean)
    {
        unpackRawData(rawData, rawSize, varOffs, elemSize, nElems, isBoolean);
    }
    
    void RLEPayload::unpackRawData(char* rawData, size_t rawSize, size_t varOffs, size_t elemSize, size_t nElems, bool isBoolean)
    {
        clear();
        Segment segm;
        segm.pPosition = 0;
        segm.valueIndex = 0;
        segm.same = false;      
        segm.null = false;
        container.push_back(segm); 
        segm.pPosition = nElems;
        container.push_back(segm); 

        nSegs = 1;
        this->dataSize = rawSize;
        this->varOffs  = varOffs;
        this->elemSize = elemSize;
        this->isBoolean = isBoolean;
        seg = &container[0];
        data.resize(rawSize);
        memcpy(&data[0], rawData, rawSize);
        payload = &data[0];
        _valuesCount = nElems;
    }

    RLEPayload::RLEPayload(const class Type& type): ConstRLEPayload(),
        _valuesCount(0)
    {
        elemSize = type.byteSize();
        isBoolean = type.bitSize() == 1;
    }

    RLEPayload::RLEPayload(size_t bitSize): ConstRLEPayload(),
        _valuesCount(0)
    {
        elemSize = (bitSize + 7) >> 3;
        isBoolean = bitSize == 1;
    }

    void RLEPayload::clear()
    {
        container.clear();
        data.clear();
        nSegs = 0;
        dataSize = 0;
        _valuesCount = 0;
    }

    void RLEPayload::append(RLEPayload& payload)
    {        
        assert(isBoolean == payload.isBoolean);
        assert(elemSize == payload.elemSize);
        if (payload.container.empty()) { 
            return;
        }
        position_t lastHeadPosition = 0;
        if (!container.empty()) { // remove terminator segment
            lastHeadPosition = container.back().pPosition;
            container.pop_back();
        }
        size_t headSegments = container.size();
        if (headSegments == 0) {
            container = payload.container;
            data = payload.data;
            varOffs = payload.varOffs;
        } else { 
            container.insert(container.end(), payload.container.begin(), payload.container.end());
            size_t headItems;
            if (!isBoolean) { 
                if (elemSize == 0) { // varying size typed; adjust offsets
                    data.insert(data.begin() + varOffs, payload.data.begin(), payload.data.begin() + payload.varOffs);
                    int* p = (int*)&data[varOffs];
                    int* end = (int*)&data[varOffs + payload.varOffs];
                    size_t varHead = dataSize - varOffs;
                    while (p < end) { 
                        *p++ += varHead;
                    }
                    data.insert(data.end(), payload.data.begin() + payload.varOffs, payload.data.end());
                    headItems = varOffs/sizeof(int);
                    varOffs += payload.varOffs;
                } else { 
                    data.insert(data.end(), payload.data.begin(), payload.data.end());
                    headItems = dataSize/elemSize;
                }
            } else {
                data.insert(data.end(), payload.data.begin(), payload.data.end());
                headItems = dataSize*8;
                _valuesCount += payload._valuesCount;
            }
            Segment* s = &container[headSegments];
            Segment* end = &container[container.size()];
            while (s < end) { 
                if (!s->null) { 
                    s->valueIndex += headItems;
                }
                s->pPosition += lastHeadPosition;
                s += 1;
            }
        }
        seg = &container[0];
        nSegs = container.size() - 1;
        this->payload = &data[0];
        dataSize += payload.dataSize;
    }

    void RLEPayload::unPackTile(const ConstRLEEmptyBitmap& emptyMap, position_t vStart, position_t vEnd)
    {
        clear();
        data.resize(1);
        data[0] = 2; //index 0 - false, index 1 - true
        elemSize = 1;
        dataSize = 1;
        isBoolean = true;
        RLEPayload::Segment rs;
        rs.same = true;
        rs.null = false;
        rs.pPosition = 0;
        for (size_t i = emptyMap.findSegment(vStart);
             i < emptyMap.nSegments() && emptyMap.getSegment(i).lPosition < vEnd;
             i++)
        {
            const RLEEmptyBitmap::Segment& es = emptyMap.getSegment(i);
            const int64_t inStart = max<int64_t>(es.lPosition, vStart);
            const int64_t inEnd = min<int64_t>(es.lPosition + es.length, vEnd);

            if (inStart - vStart != rs.pPosition) { 
                rs.valueIndex = 0;
                container.push_back(rs);
            }
            rs.pPosition = inStart - vStart;
            rs.valueIndex = 1;
            container.push_back(rs);            
            rs.pPosition += inEnd - inStart;
        }
        if (rs.pPosition != vEnd - vStart) { 
            rs.valueIndex = 0;
            container.push_back(rs);            
        }            
        nSegs = container.size();
        rs.pPosition = vEnd - vStart;
        container.push_back(rs);
        _valuesCount = 2;
        seg = &container[0];
        payload = &data[0];
    }

    void RLEPayload::unPackTile(const ConstRLEPayload& payload, const ConstRLEEmptyBitmap& emptyMap, position_t vStart, position_t vEnd)
    {
        clear();
        elemSize = payload.elementSize();
        isBoolean = payload.isBool();
        
        RLEPayload::Segment rs;
        rs.pPosition = 0;
        rs.same = true;
        size_t segLength = 0;
        size_t dstValueIndex = 0;
        vector<char> varPart;

        if (emptyMap.count() == payload.count()) { // no gaps in payload
            size_t begin = emptyMap.findSegment(vStart);
            if (begin < emptyMap.nSegments()) { 
                size_t end = emptyMap.findSegment(vEnd);
                if (end >= emptyMap.nSegments() || emptyMap.getSegment(end).lPosition > vEnd) { 
                    end -= 1;
                }
                if (end != size_t(-1)) { 
                    const RLEEmptyBitmap::Segment& firstSeg = emptyMap.getSegment(begin);
                    const RLEEmptyBitmap::Segment& lastSeg = emptyMap.getSegment(end);
                    const int64_t inStart = max<int64_t>(firstSeg.pPosition, vStart - firstSeg.lPosition + firstSeg.pPosition);
                    const int64_t inEnd = min<int64_t>(lastSeg.pPosition + lastSeg.length, vEnd - lastSeg.lPosition + lastSeg.pPosition);
                    for (size_t j = payload.findSegment(inStart);
                         j < payload.nSegments() && payload.getSegment(j).pPosition < inEnd;
                         j++)
                    {
                        const RLEPayload::Segment& ps = payload.getSegment(j);
                        // physical start in payload
                        const int64_t resStart = max<int64_t>(inStart, ps.pPosition);
                        // physical end in payload
                        const int64_t resEnd = min<int64_t>(inEnd, ps.pPosition + ps.length());
                        const int64_t length = resEnd - resStart;
                        
                        if (!ps.null) { 
                            size_t srcValueIndex = ps.valueIndex + (ps.same ? 0 : resStart - ps.pPosition);
                            size_t nItems = ps.same ? 1 : length;
                            if (isBoolean) { 
                                char* otherData = payload.getFixData();
                                data.resize((dstValueIndex + nItems + 7) >> 3);                        
                                for (size_t k = 0; k < nItems; k++) { 
                                    if (otherData[(srcValueIndex + k) >> 3] & (1 << ((srcValueIndex + k) & 7))) { 
                                        data[(dstValueIndex + k) >> 3] |= 1 << ((dstValueIndex + k) & 7);
                                    }
                                }
                            } else { 
                                if (elemSize == 0) {
                                    data.resize(data.size() + nItems*sizeof(int));
                                    int* dst = (int*)&data[0] + dstValueIndex;
                                    int* src = (int*)payload.getFixData() + srcValueIndex;
                                    int* end = src + nItems;
                                    while (src < end) {
                                        size_t offs = varPart.size();
                                        *dst++ = offs;
                                        int bodyLen;
                                        char* body = payload.getVarData() + *src++;
                                        if (*body == 0) {
                                            bodyLen = 5 + *(int*)(body + 1);
                                        } else {
                                            bodyLen = 1 + (*body & 0xFF);
                                        }
                                        varPart.resize(offs + bodyLen);
                                        memcpy(&varPart[offs], body, bodyLen);
                                    }
                                } else {
                                    data.resize(data.size() + nItems*elemSize);
                                    memcpy(&data[dstValueIndex*elemSize],  
                                           payload.getFixData() + srcValueIndex*elemSize, 
                                           nItems*elemSize);
                                }
                            }
                            if (segLength > 0 && (!ps.same || ps.length() == 1) && (segLength == 1 || !rs.same)) { 
                                // append previous segment
                                segLength += length;
                                rs.same = false;
                            } else { 
                                if (segLength != 0) { 
                                    container.push_back(rs);
                                    rs.pPosition += segLength;
                                }
                                rs.same = ps.same;
                                rs.null = false;
                                rs.valueIndex = dstValueIndex;
                                segLength = length;
                            }
                            dstValueIndex += nItems;
                        } else {
                            if (segLength != 0) { 
                                container.push_back(rs);
                                rs.pPosition += segLength;
                            }
                            rs.same = true;
                            rs.null = true;
                            rs.valueIndex = ps.valueIndex;
                            container.push_back(rs);
                            rs.pPosition += length;
                            segLength = 0;
                        }
                    }
                }
            } 
        } else { 
            for (size_t i = emptyMap.findSegment(vStart);
                 i < emptyMap.nSegments() && emptyMap.getSegment(i).lPosition < vEnd;
                 i++)
            {
                const RLEEmptyBitmap::Segment& es = emptyMap.getSegment(i);
                const int64_t inStart = max<int64_t>(es.pPosition, vStart - es.lPosition + es.pPosition);
                const int64_t inEnd = min<int64_t>(es.pPosition + es.length, vEnd - es.lPosition + es.pPosition);
                for (size_t j = payload.findSegment(inStart);
                     j < payload.nSegments() && payload.getSegment(j).pPosition < inEnd;
                     j++)
                {
                    const RLEPayload::Segment& ps = payload.getSegment(j);
                    // physical start in payload
                    const int64_t resStart = max<int64_t>(inStart, ps.pPosition);
                    // physical end in payload
                    const int64_t resEnd = min<int64_t>(inEnd, ps.pPosition + ps.length());
                    const int64_t length = resEnd - resStart;
                    
                    if (!ps.null) { 
                        size_t srcValueIndex = ps.valueIndex + (ps.same ? 0 : resStart - ps.pPosition);
                        size_t nItems = ps.same ? 1 : length;
                        if (isBoolean) { 
                            char* otherData = payload.getFixData();
                            data.resize((dstValueIndex + nItems + 7) >> 3);                        
                            for (size_t k = 0; k < nItems; k++) { 
                                if (otherData[(srcValueIndex + k) >> 3] & (1 << ((srcValueIndex + k) & 7))) { 
                                    data[(dstValueIndex + k) >> 3] |= 1 << ((dstValueIndex + k) & 7);
                                }
                            }
                        } else { 
                            if (elemSize == 0) {
                                data.resize(data.size() + nItems*sizeof(int));
                                int* dst = (int*)&data[0] + dstValueIndex;
                                int* src = (int*)payload.getFixData() + srcValueIndex;
                                int* end = src + nItems;
                                while (src < end) {
                                    size_t offs = varPart.size();
                                    *dst++ = offs;
                                    int bodyLen;
                                    char* body = payload.getVarData() + *src++;
                                    if (*body == 0) {
                                        bodyLen = 5 + *(int*)(body + 1);
                                    } else {
                                        bodyLen = 1 + (*body & 0xFF);
                                    }
                                    varPart.resize(offs + bodyLen);
                                    memcpy(&varPart[offs], body, bodyLen);
                                }
                            } else {
                                data.resize(data.size() + nItems*elemSize);
                                memcpy(&data[dstValueIndex*elemSize],  
                                       payload.getFixData() + srcValueIndex*elemSize, 
                                       nItems*elemSize);
                            }
                        }
                        if (segLength > 0 && (!ps.same || ps.length() == 1) && (segLength == 1 || !rs.same)) { 
                            // append previous segment
                            segLength += length;
                            rs.same = false;
                        } else { 
                            if (segLength != 0) { 
                                container.push_back(rs);
                                rs.pPosition += segLength;
                            }
                            rs.same = ps.same;
                            rs.null = false;
                            rs.valueIndex = dstValueIndex;
                            segLength = length;
                        }
                        dstValueIndex += nItems;
                    } else {
                        if (segLength != 0) { 
                            container.push_back(rs);
                            rs.pPosition += segLength;
                        }
                        rs.same = true;
                        rs.null = true;
                        rs.valueIndex = ps.valueIndex;
                        container.push_back(rs);
                        rs.pPosition += length;
                        segLength = 0;
                    }
                }
            }
        }
        if (segLength != 0) { 
            container.push_back(rs);
            rs.pPosition += segLength;
        }
        nSegs = container.size();
        container.push_back(rs);
        seg = &container[0];
        varOffs = data.size();
        if (varPart.size() != 0) { 
            data.insert(data.end(), varPart.begin(), varPart.end());
        }
        dataSize = data.size();
        _valuesCount = dstValueIndex;
        this->payload = &data[0];
    }
        
    void RLEPayload::trim(position_t lastPos) 
    { 
        container[nSegs].pPosition = lastPos;
    }

    void RLEPayload::flush(position_t chunkSize) 
    {
        nSegs = container.size();
        assert(nSegs == 0 || container[nSegs - 1].pPosition < chunkSize);
        Segment segm;
        segm.pPosition = chunkSize;
        container.push_back(segm); // Add terminated segment (needed to calculate length)
        seg = &container[0];
    }

    void RLEPayloadAppender::append(Value const& v)
    {
        assert(!_finalized);
        if(_nextSeg == 0 || _payload.container[_nextSeg-1].null != v.isNull() || (v.isNull() && _payload.container[_nextSeg-1].valueIndex != v.getMissingReason()))
        {
            _payload.container.resize(_nextSeg+1);
            _payload.container[_nextSeg].pPosition = _nextPPos;
            _payload.container[_nextSeg].same = true;
            _payload.container[_nextSeg].null = v.isNull();

            if(v.isNull())
            {
                _payload.container[_nextSeg].valueIndex = v.getMissingReason();
            }
            else
            {
                if (v.size() > _payload.elemSize) {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TRUNCATION) << v.size() << _payload.elemSize;
                }
                _payload.container[_nextSeg].valueIndex = _nextValIndex;
                _payload.data.resize((_nextValIndex + 1) * _payload.elemSize);
                memcpy(&_payload.data[(_nextValIndex) * _payload.elemSize], v.data(), v.size());
                _nextValIndex++;
            }
            _nextSeg++;
        }
        else if (!v.isNull())
        {
            bool valuesEqual = v.size() == _payload.elemSize && memcmp(&_payload.data[(_nextValIndex-1) * _payload.elemSize], v.data(), _payload.elemSize) == 0;
            if (valuesEqual && !_payload.container[_nextSeg-1].same)
            {
                _payload.container.resize(_nextSeg+1);
                _payload.container[_nextSeg].pPosition = _nextPPos-1;
                _payload.container[_nextSeg].same = true;
                _payload.container[_nextSeg].null = false;
                _payload.container[_nextSeg].valueIndex = _nextValIndex-1;
                _nextSeg++;
            }
            else if (!valuesEqual)
            {
                if (v.size() > _payload.elemSize) {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TRUNCATION) << v.size() << _payload.elemSize;
                }
                _payload.data.resize((_nextValIndex + 1) * _payload.elemSize);
                memcpy(&_payload.data[(_nextValIndex) * _payload.elemSize], v.data(), _payload.elemSize);
                _nextValIndex++;
                if (_payload.container[_nextSeg-1].pPosition == _nextPPos-1)
                {
                    _payload.container[_nextSeg-1].same = false;
                }
                else if (_payload.container[_nextSeg-1].same)
                {
                    _payload.container.resize(_nextSeg+1);
                    _payload.container[_nextSeg].pPosition = _nextPPos;
                    _payload.container[_nextSeg].same = true;
                    _payload.container[_nextSeg].null = false;
                    _payload.container[_nextSeg].valueIndex = _nextValIndex-1;
                    _nextSeg++;
                }
            }
        }
        _nextPPos++;
    }

    //
    // Yet another appender: correct handling of boolean and varying size types
    //
    inline void RLEPayload::append_iterator::init()
    {
        valueIndex = 0;
        segLength = 0;
        segm.pPosition = 0;
        prevVal = new Value();
    }
        
    RLEPayload::append_iterator::append_iterator(size_t bitSize) 
    {
        result = new RLEPayload(bitSize);
        init();
    }

    RLEPayload::append_iterator::append_iterator(RLEPayload* dstPayload) 
    {
        result = dstPayload;
        dstPayload->clear();
        init();
    }

    void RLEPayload::append_iterator::flush() 
    { 
        if (segLength != 0) { 
            result->addSegment(segm);
        }
        result->_valuesCount = valueIndex;
        result->setVarPart(varPart);
        result->flush(segm.pPosition + segLength);
    }
        
uint64_t RLEPayload::append_iterator::add(iterator& ii, uint64_t limit, bool setupPrevVal) 
    { 
        uint64_t count = min(limit, ii.available());
        if (ii.isNull()) {                            
            if (segLength != 0 && (!segm.null || segm.valueIndex != ii.getMissingReason()))  { 
                result->addSegment(segm);
                segm.pPosition += segLength;
                segLength = 0;
            }
            segLength += count;
            segm.null = true;
            segm.same = true;
            segm.valueIndex = ii.getMissingReason();
            ii += count;
        } else { 
            if (segLength != 0 && (segm.null || (segm.same && segLength > 1) || (ii.isSame() && count > 1))) {
                result->addSegment(segm);
                segm.pPosition += segLength;
                segLength = 0;
            }
            if (segLength == 0) { 
                segm.same = (count == 1) || ii.isSame();
                segm.valueIndex = valueIndex;
                segm.null = false;
            } else { 
                segm.same = false;
            }                
            segLength += count;
            if (!result->isBoolean && result->elemSize != 0) {                
                size_t size;
                if (segm.same) {
                    size = result->elemSize;
                    valueIndex += 1;
                } else { 
                    size = result->elemSize*size_t(count);
                    valueIndex += size_t(count);
                }
                result->data.resize(result->dataSize + size);
                memcpy(& result->data[result->dataSize], ii.getFixedValues(), size);
                result->dataSize += size;
                if (setupPrevVal) {
                    if (count > 1) {
                        ii += count-1;
                    }
                    ii.getItem(*prevVal);
                    ii += 1;
                } else {
                    ii += count;
                }
            } else {
                if (segm.same) {
                    ii.getItem(*prevVal);
                    ii += count;
                    result->appendValue(varPart, *prevVal, valueIndex++);
                } else { 
                    for (uint64_t i = 0; i < count; i++) {
                        ii.getItem(*prevVal);
                        result->appendValue(varPart, *prevVal, valueIndex++);
                        ++ii;
                    }
                }
            }
        }
        return count;
    }
        
    void RLEPayload::append_iterator::add(Value const& v, uint64_t count) 
    { 
        if (v.isNull()) {                            
            if (segLength != 0 && (!segm.null || segm.valueIndex != v.getMissingReason()))  { 
                result->addSegment(segm);
                segm.pPosition += segLength;
                segLength = 0;
            }
            segLength += count;
            segm.null = true;
            segm.same = true;
            segm.valueIndex = v.getMissingReason();
        } else if (segLength != 0 && !segm.null && v == *prevVal) { 
            if (segm.same) {
                segLength += count;
            } else { 
                result->addSegment(segm);
                segm.pPosition += segLength - 1;
                segm.same = true;
                segm.valueIndex = valueIndex-1;
                segLength = 1 + count;
            }
        } else { 
            if (segLength == 0 || segm.null || count > 1) { 
                if (segLength != 0) {
                    result->addSegment(segm);
                    segm.pPosition += segLength;
                    segLength = 0;
                }                                    
                segm.same = true;
                segm.null = false;
                segm.valueIndex = valueIndex;
            } else { 
                if (segLength > 1 && segm.same) {
                    result->addSegment(segm);
                    segm.pPosition += segLength;
                    segLength = 0;
                    segm.valueIndex = valueIndex;
                } else {                                                        
                    segm.same = false;
                }
            }
            segLength += count;
            result->appendValue(varPart, v, valueIndex++);
            *prevVal = v;
        }
    }
    
    RLEPayload::append_iterator::~append_iterator()
    {
        delete prevVal;
    }
}

