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
 * @file DelegateArray.cpp
 *
 * @brief Delegate array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include "array/DelegateArray.h"
#include "system/Cluster.h"
#include "system/Exceptions.h"
#ifndef SCIDB_CLIENT
#include "system/Config.h"
#endif
#include "system/SciDBConfigOptions.h"

//#define NO_MATERIALIZE_CACHE 1

namespace scidb
{
    using namespace boost;
    using namespace std;

    //
    // Delegate chunk methods
    //
    const ArrayDesc& DelegateChunk::getArrayDesc() const
    {
        return array.getArrayDesc();
    }

    void DelegateChunk::overrideTileMode(bool enabled) {
        if (chunk != NULL) { 
            ((Chunk*)chunk)->overrideTileMode(enabled);
        }
        tileMode = enabled;
    }

    Array const& DelegateChunk::getArray() const 
    {
        return array;
    }
    
    const AttributeDesc& DelegateChunk::getAttributeDesc() const
    {
        return array.getArrayDesc().getAttributes()[attrID];
    }

    int DelegateChunk::getCompressionMethod() const
    {
        return chunk->getCompressionMethod();
    }

    Coordinates const& DelegateChunk::getFirstPosition(bool withOverlap) const
    {                                           
        return chunk->getFirstPosition(withOverlap);
    }

    Coordinates const& DelegateChunk::getLastPosition(bool withOverlap) const
    {                                           
        return chunk->getLastPosition(withOverlap);
    }

    boost::shared_ptr<ConstChunkIterator> DelegateChunk::getConstIterator(int iterationMode) const
    {
        return boost::shared_ptr<ConstChunkIterator>(array.createChunkIterator(this, iterationMode));
    }

    void DelegateChunk::setInputChunk(ConstChunk const& inputChunk)
    {
        chunk = &inputChunk;
    }

    ConstChunk const& DelegateChunk::getInputChunk() const
    {
        return *chunk;
    }

    DelegateArrayIterator const& DelegateChunk::getArrayIterator() const
    { 
        return iterator;
    }

    size_t DelegateChunk::count() const
    {
        return isClone ? chunk->count() : ConstChunk::count();
    }

    bool DelegateChunk::isCountKnown() const
    {
        return isClone ? chunk->isCountKnown() : ConstChunk::isCountKnown();
    }

    DBChunk const* DelegateChunk::getDiskChunk() const
    {
        return isClone ? chunk->getDiskChunk() : NULL;
    }

    bool DelegateChunk::isMaterialized() const
    {
        return isClone && chunk->isMaterialized()/* && !chunk->isRLE()*/;
    }

    bool DelegateChunk::isDirectMapping() const
    {
        return isClone;
    }

    bool DelegateChunk::isSparse() const
    {
        return !isDense && chunk->isSparse();
    }
    
    bool DelegateChunk::isRLE() const
    {
        return chunk->isRLE();
    }
    
    bool DelegateChunk::pin() const
    {
        return isClone && chunk->pin();
    }

    void DelegateChunk::unPin() const
    {
        if (isClone) { 
            chunk->unPin();
        }
    }

    void* DelegateChunk::getData() const
    {
        return isClone/* && !chunk->isRLE()*/ ? chunk->getData() : ConstChunk::getData();
    }

    size_t DelegateChunk::getSize() const
    {
        return isClone/* && !chunk->isRLE()*/ ? chunk->getSize() : ConstChunk::getSize();
    }

    void DelegateChunk::compress(CompressedBuffer& buf, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const
    {
        if (isClone/* && !chunk->isRLE()*/) { 
            chunk->compress(buf, emptyBitmap);
        } else { 
            ConstChunk::compress(buf, emptyBitmap);
        }
    }

    DelegateChunk::DelegateChunk(DelegateArray const& arr, DelegateArrayIterator const& iter, AttributeID attr, bool clone)
    : array(arr), iterator(iter), attrID(attr), chunk(NULL), isClone(clone), isDense(false), tileMode(false)
    {
    }

    //
    // Delegate chunk iterator methods
    //

    bool DelegateChunkIterator::supportsVectorMode() const
    {
        return chunk->isClone && inputIterator->supportsVectorMode();
    }
    
    void DelegateChunkIterator::setVectorMode(bool enabled)
    {
        inputIterator->setVectorMode(enabled);
    }
    
    int DelegateChunkIterator::getMode()
    {
        return inputIterator->getMode();
    }

     Value& DelegateChunkIterator::getItem()
    {
        return inputIterator->getItem();
    }

    bool DelegateChunkIterator::isEmpty()
    {
        return inputIterator->isEmpty();
    }

    bool DelegateChunkIterator::end()
    {
        return inputIterator->end();
    }

    void DelegateChunkIterator::operator ++()
    {
        ++(*inputIterator);
    }

    Coordinates const& DelegateChunkIterator::getPosition()
    {
        return inputIterator->getPosition();
    }

    bool DelegateChunkIterator::setPosition(Coordinates const& pos)
    {
        return inputIterator->setPosition(pos);
    }

    void DelegateChunkIterator::reset()
    {
        inputIterator->reset();
    }

    ConstChunk const& DelegateChunkIterator::getChunk()
    {
        return *chunk;
    }

    DelegateChunkIterator::DelegateChunkIterator(DelegateChunk const* aChunk, int iterationMode)
    : chunk(aChunk), inputIterator(aChunk->getInputChunk().getConstIterator(iterationMode & ~INTENDED_TILE_MODE))
    {
    }        

    //
    // Delegate array iterator methods
    //

    DelegateArrayIterator::DelegateArrayIterator(DelegateArray const& delegate, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> input)
    : array(delegate), 
      attr(attrID), 
      inputIterator(input),
      chunk(delegate.createChunk(this, attrID)),
      chunkInitialized(false)
    {
    }

    boost::shared_ptr<ConstArrayIterator> DelegateArrayIterator::getInputIterator() const
    {
        return inputIterator;
    }

	ConstChunk const& DelegateArrayIterator::getChunk()
    {
        chunk->setInputChunk(inputIterator->getChunk());
        return *chunk;
    }

	bool DelegateArrayIterator::end()
    {
        return inputIterator->end();
    }

	void DelegateArrayIterator::operator ++()
    {
        chunkInitialized = false;
        ++(*inputIterator);
    }

	Coordinates const& DelegateArrayIterator::getPosition()
    {
        return inputIterator->getPosition();
    }

	bool DelegateArrayIterator::setPosition(Coordinates const& pos)
    {
        chunkInitialized = false;
        return inputIterator->setPosition(pos);
    }

	void DelegateArrayIterator::reset()
    {
        chunkInitialized = false;
        inputIterator->reset();
    }

    //
    // Delegate array methods
    //

    DelegateArray::DelegateArray(ArrayDesc const& arrayDesc, boost::shared_ptr<Array> input, bool clone)
    : desc(arrayDesc), inputArray(input), isClone(clone)
    {
    }
        
    string const& DelegateArray::getName() const
    {
        return desc.getName();
    }

    ArrayID DelegateArray::getHandle() const
    {
        return desc.getId();
    }

    const ArrayDesc& DelegateArray::getArrayDesc() const
    {
        return desc;
    }

    boost::shared_ptr<ConstArrayIterator> DelegateArray::getConstIterator(AttributeID id) const
    {
        return boost::shared_ptr<ConstArrayIterator>(createArrayIterator(id));
    }

    DelegateChunk* DelegateArray::createChunk(DelegateArrayIterator const* iterator, AttributeID id) const
    {
        return new DelegateChunk(*this, *iterator, id, isClone);
    }

    DelegateChunkIterator* DelegateArray::createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        return new DelegateChunkIterator(chunk, iterationMode);
    }
    
    DelegateArrayIterator* DelegateArray::createArrayIterator(AttributeID id) const
    {
        return new DelegateArrayIterator(*this, id, inputArray->getConstIterator(id));
    }
    
    boost::shared_ptr<Array> DelegateArray::getInputArray() const
    {
        return inputArray;
    }

    bool DelegateArray::supportsRandomAccess() const
    {
        return inputArray->supportsRandomAccess();
    }

    //
    // NonEmptyable array
    //

    NonEmptyableArray::NonEmptyableArray(boost::shared_ptr<Array> input)
    : DelegateArray(input->getArrayDesc(), input, true)
    {
        Attributes const& oldAttrs(desc.getAttributes());
        emptyTagID = oldAttrs.size();
        Attributes newAttrs(emptyTagID+1);
        for (size_t i = 0; i < emptyTagID; i++) { 
            newAttrs[i] = oldAttrs[i];
        }
        newAttrs[emptyTagID] = AttributeDesc(emptyTagID, DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,
                                            TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0);
        desc = ArrayDesc(desc.getName(), newAttrs, desc.getDimensions());
        rle = Config::getInstance()->getOption<bool>(CONFIG_RLE_CHUNK_FORMAT);
    }

    DelegateArrayIterator* NonEmptyableArray::createArrayIterator(AttributeID id) const
    {  
        if (rle && id == emptyTagID) { 
            return new DummyBitmapArrayIterator(*this, id, inputArray->getConstIterator(0));
        }
        return new DelegateArrayIterator(*this, id, inputArray->getConstIterator(id == emptyTagID ? 0 : id));
    }

    DelegateChunkIterator* NonEmptyableArray::createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        AttributeDesc const& attr = chunk->getAttributeDesc();
        return attr.isEmptyIndicator()
            ? (DelegateChunkIterator*)new DummyBitmapChunkIterator(chunk, iterationMode)
            : (DelegateChunkIterator*)new DelegateChunkIterator(chunk, iterationMode);
    }
    
    DelegateChunk* NonEmptyableArray::createChunk(DelegateArrayIterator const* iterator, AttributeID id) const
    {
        return new DelegateChunk(*this, *iterator, id, id != emptyTagID);
    }

    Value& NonEmptyableArray::DummyBitmapChunkIterator::getItem()
    {
        return _true;
    }

    bool NonEmptyableArray::DummyBitmapChunkIterator::isEmpty()
    {
        return false;
    }

    NonEmptyableArray::DummyBitmapChunkIterator::DummyBitmapChunkIterator(DelegateChunk const* chunk, int iterationMode)
    : DelegateChunkIterator(chunk, iterationMode), 
      _true(TypeLibrary::getType(TID_BOOL))
    {        
        _true.setBool(true);
    }

    ConstChunk const& NonEmptyableArray::DummyBitmapArrayIterator::getChunk()
    {
        ConstChunk const& inputChunk = inputIterator->getChunk();
        if (!inputChunk.isRLE()) {             
            return DelegateArrayIterator::getChunk();
        }
        if (!shapeChunk.isInitialized() || shapeChunk.getFirstPosition(false) != inputChunk.getFirstPosition(false)) {
            ArrayDesc const& arrayDesc = array.getArrayDesc();
            Address addr(arrayDesc.getId(), attr, inputChunk.getFirstPosition(false));
            shapeChunk.initialize(&array, &arrayDesc, addr, inputChunk.getCompressionMethod());
            shapeChunk.setSparse(inputChunk.isSparse());
            shapeChunk.fillRLEBitmap();
        }
        return shapeChunk;
    }
    
    NonEmptyableArray::DummyBitmapArrayIterator::DummyBitmapArrayIterator(DelegateArray const& delegate, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> inputIterator)
    : DelegateArrayIterator(delegate, attrID, inputIterator)
    {
    }

    //
    // Split array
    //

SplitArray::SplitArray(ArrayDesc const& desc, const boost::shared_array<char>& src, Coordinates const& fromPos, Coordinates const& tillPos)
    : DelegateArray(desc, shared_ptr<Array>(), true),
      from(fromPos),
      till(tillPos),
      size(fromPos.size()),
      source(src),
      empty(false)
    {
        Dimensions const& dims = desc.getDimensions();
        for (size_t i = 0, n = dims.size(); i < n; i++) { 
            size[i] = till[i] - from[i] + 1;
            if (size[i] == 0) { 
                empty = true;
            }
            if (till[i] > dims[i].getEndMax()) { 
                till[i] = dims[i].getEndMax();
            }
        }
    }

    SplitArray::~SplitArray() 
    { 
    }

    bool SplitArray::supportsRandomAccess() const
    {
        return false;
    }

    DelegateArrayIterator* SplitArray::createArrayIterator(AttributeID id) const
    {  
        return new SplitArray::ArrayIterator(*this, id);
    }

    ConstChunk const& SplitArray::ArrayIterator::getChunk()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        if (!chunkInitialized) { 
            chunk.initialize(&array, &array.getArrayDesc(), addr, 0);
            chunk.setRLE(false);
            char* dst = (char*)chunk.getData();
            char* src = (char*)array.source.get();
            Coordinates const& first = chunk.getFirstPosition(false);
            Coordinates pos = first;
            const size_t nDims = dims.size();
            const size_t dstStrideSize = dims[nDims-1].getChunkInterval()*attrBitSize >> 3;
            while (true) { 
                size_t offs = 0;
                bool oob = false;
                for (size_t i = 0; i < nDims; i++) { 
                    offs *= array.size[i];
                    offs += pos[i] - array.from[i];
                    oob |= pos[i] > array.till[i];
                }
                if (!oob) { 
                    memcpy(dst, src + (offs*attrBitSize >> 3), 
                           min(size_t(array.till[nDims-1] - pos[nDims-1] + 1), size_t(dims[nDims-1].getChunkInterval()))*attrBitSize >> 3);
                }
                dst += dstStrideSize;
                size_t j = nDims-1; 
                while (true) { 
                    if (j == 0) { 
                        goto Done;
                    }
                    j -= 1;
                    if (++pos[j] >= first[j] + dims[j].getChunkInterval()) { 
                        pos[j] = first[j];
                    } else { 
                        break;
                    }
                }
            }
          Done:
            chunkInitialized = true;
        }
        return chunk;
    }

    bool SplitArray::ArrayIterator::end()
    {
        return !hasCurrent;
    }
    
    void SplitArray::ArrayIterator::operator ++()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        size_t i = dims.size()-1;
        while ((addr.coords[i] += dims[i].getChunkInterval()) > array.till[i]) { 
            if (i == 0) { 
                hasCurrent = false;
                return;
            }
            addr.coords[i] = array.from[i];
            i -= 1;
        } 
        chunkInitialized = false;
    }

    Coordinates const& SplitArray::ArrayIterator::getPosition()
    {
        return addr.coords;
    }

    bool SplitArray::ArrayIterator::setPosition(Coordinates const& pos)
    {
        for (size_t i = 0, n = dims.size(); i < n; i++) { 
            if (pos[i] < array.from[i] || pos[i] > array.till[i]) { 
                return false;
            }
        }
        addr.coords = pos;
        array.getArrayDesc().getChunkPositionFor(addr.coords);
        chunkInitialized = false;
        return hasCurrent = true;
    }

    void SplitArray::ArrayIterator::reset()
    {
        addr.coords = array.from;
        chunkInitialized = false;
        hasCurrent = !array.empty;
    }

    SplitArray::ArrayIterator::ArrayIterator(SplitArray const& arr, AttributeID attrID)
    : DelegateArrayIterator(arr, attrID, shared_ptr<ConstArrayIterator>()),
      dims(arr.getArrayDesc().getDimensions()),
      array(arr),
      attrBitSize(TypeLibrary::getType(arr.getArrayDesc().getAttributes()[attrID].getType()).bitSize())
    {
        uint64_t chunkBitSize = attrBitSize;
        size_t nDims = dims.size();
        for (size_t i = 0; i < nDims; i++) { 
            chunkBitSize *= dims[i].getChunkInterval();
        }
        if ((dims[nDims-1].getChunkInterval()*attrBitSize & 7) != 0)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_STRIDE_SHOULD_BE_BYTE_ALIGNED);
        chunk.allocate(size_t((chunkBitSize + 7) >> 3));
        addr.arrId = arr.getHandle();
        addr.attId = attrID;
        reset();
    }

    //
    // Materialized array
    //
    MaterializedArray::ArrayIterator::ArrayIterator(MaterializedArray& arr, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> input, MaterializeFormat chunkFormat)
    : DelegateArrayIterator(arr, attrID, input),
      array(arr)
    {     
    }

    ConstChunk const& MaterializedArray::ArrayIterator::getChunk() 
    { 
        ConstChunk const& chunk = inputIterator->getChunk();
        MaterializeFormat format = array.format;
        if (chunk.isMaterialized() 
            && (format == PreserveFormat 
                || (format == RLEFormat && chunk.isRLE())
                || (format == DenseFormat && !chunk.isRLE() && !chunk.isSparse())))
        {
            ((ConstChunk&)chunk).overrideTileMode(false);
            return chunk;
        }
#ifdef NO_MATERIALIZE_CACHE
        if (!materializedChunk) {
            materializedChunk = boost::shared_ptr<MemChunk>(new MemChunk());
        }
        MaterializedArray::materialize(*materializedChunk, chunk, format);
#else      
        materializedChunk = array.getMaterializedChunk(chunk);
#endif
        return *materializedChunk; 
    }

    boost::shared_ptr<MemChunk> MaterializedArray::getMaterializedChunk(ConstChunk const& inputChunk)
    {
        bool newChunk = false;
        boost::shared_ptr<MemChunk> chunk;
        boost::shared_ptr<ConstRLEEmptyBitmap> bitmap;
        Coordinates const& pos = inputChunk.getFirstPosition(false);
        AttributeID attr = inputChunk.getAttributeDesc().getId();
        {
            ScopedMutexLock cs(mutex);
            chunk = chunkCache[attr][pos];
            if (!chunk) {
                chunk = boost::shared_ptr<MemChunk>(new MemChunk());
                bitmap = bitmapCache[pos];
                newChunk = true;
            }
        }
        if (newChunk) {
            materialize(*chunk, inputChunk, format);
            if (!bitmap) { 
                bitmap = chunk->getEmptyBitmap();
            }
            chunk->setEmptyBitmap(bitmap);
            {
                ScopedMutexLock cs(mutex);
                if (chunkCache[attr].size() >= cacheSize) { 
                    chunkCache[attr].erase(chunkCache[attr].begin());
                }
                chunkCache[attr][pos] = chunk;
                if (bitmapCache.size() >= cacheSize) { 
                    bitmapCache.erase(bitmapCache.begin());
                }
                bitmapCache[pos] = bitmap;
            }
        }
        return chunk;
    }

    MaterializedArray::MaterializedArray(boost::shared_ptr<Array> input, MaterializeFormat chunkFormat)
    : DelegateArray(input->getArrayDesc(), input, true),
      format(chunkFormat),
      chunkCache(desc.getAttributes().size())
    {
#ifndef SCIDB_CLIENT
        cacheSize = Config::getInstance()->getOption<int>(CONFIG_PREFETCHED_CHUNKS);
#else
        cacheSize = 1;
#endif
    }

size_t nMaterializedChunks = 0;

    void MaterializedArray::materialize(MemChunk& materializedChunk, ConstChunk const& chunk, MaterializeFormat format)
    {
        nMaterializedChunks += 1;
        materializedChunk.initialize(chunk);
        materializedChunk.setBitmapChunk((Chunk*)chunk.getBitmapChunk());
        if (format == RLEFormat) { 
            materializedChunk.setRLE(true);
        } else if (format == DenseFormat) { 
            materializedChunk.setSparse(false);
            materializedChunk.setRLE(false);
        }
        boost::shared_ptr<ConstChunkIterator> src 
            = chunk.getConstIterator(ChunkIterator::IGNORE_DEFAULT_VALUES|ChunkIterator::IGNORE_EMPTY_CELLS|
                                     (chunk.isSolid() ? ChunkIterator::INTENDED_TILE_MODE : 0));
        shared_ptr<Query> emptyQuery;
        boost::shared_ptr<ChunkIterator> dst 
            = materializedChunk.getIterator(emptyQuery, 
                                            (src->getMode() & ChunkIterator::TILE_MODE)|ChunkIterator::ChunkIterator::NO_EMPTY_CHECK|ChunkIterator::SEQUENTIAL_WRITE);
        bool vectorMode = src->supportsVectorMode() && dst->supportsVectorMode();
        src->setVectorMode(vectorMode);
        dst->setVectorMode(vectorMode);
        size_t count = 0;
        while (!src->end()) {
            if (!dst->setPosition(src->getPosition()))
                throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            dst->writeItem(src->getItem());
            count += 1;
            ++(*src);
        }
        if (!vectorMode && !(src->getMode() & ChunkIterator::TILE_MODE) && !chunk.getArrayDesc().containsOverlaps()) {
            materializedChunk.setCount(count);
        }
        dst->flush();
    }

    DelegateArrayIterator* MaterializedArray::createArrayIterator(AttributeID id) const
    {  
        return new MaterializedArray::ArrayIterator(*(MaterializedArray*)this, id, inputArray->getConstIterator(id), format);
    }
    
}
