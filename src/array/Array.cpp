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
 * @file Array.cpp
 *
 * @brief Array API
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include <vector>
#include <string.h>
#include <boost/assign.hpp>

#include "array/MemArray.h"
#include "array/EmbeddedArray.h"
#include "array/RLE.h"
#include "system/Exceptions.h"
#include "query/FunctionDescription.h"
#include "query/TypeSystem.h"
#include "query/Statistics.h"
#ifndef SCIDB_CLIENT
#include "system/Config.h"
#endif
#include "system/SciDBConfigOptions.h"

#include <log4cxx/logger.h>

using namespace boost::assign;

namespace scidb
{

    // Logger for operator. static to prevent visibility of variable outside of file
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.Array"));

    void* SharedBuffer::getData() const
    {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "SharedBuffer::getData";
    }

    size_t SharedBuffer::getSize() const
    {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "SharedBuffer::getSize";
    }

    void SharedBuffer::allocate(size_t size)
    {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "SharedBuffer::allocate";
    }

    void SharedBuffer::reallocate(size_t size)
    {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "SharedBuffer::reallocate";
    }

    void SharedBuffer::free()
    {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "SharedBuffer::free";
    }

    bool SharedBuffer::pin() const
    {
        return false;
    }

    void SharedBuffer::unPin() const
    {
    }


    void* CompressedBuffer::getData() const
    {
        return data;
    }

    size_t CompressedBuffer::getSize() const
    {
        return compressedSize;
    }

    void CompressedBuffer::allocate(size_t size)
    {
        data = ::malloc(size);
        if (data == NULL) { 
            throw SYSTEM_EXCEPTION(SCIDB_SE_NO_MEMORY, SCIDB_LE_CANT_ALLOCATE_MEMORY);
        }
        compressedSize = size;
        currentStatistics->allocatedSize += size;
        currentStatistics->allocatedChunks++;
    }

    void CompressedBuffer::reallocate(size_t size)
    {
        data = ::realloc(data, size);
        if (data == NULL) { 
            throw SYSTEM_EXCEPTION(SCIDB_SE_NO_MEMORY, SCIDB_LE_CANT_ALLOCATE_MEMORY);
        }
        compressedSize = size;
        currentStatistics->allocatedSize += size;
        currentStatistics->allocatedChunks++;
    }

    void CompressedBuffer::free()
    {
       ::free(data);
       data = NULL;
    }

    bool CompressedBuffer::pin() const
    {
        ((CompressedBuffer*)this)->accessCount += 1;
        currentStatistics->pinnedSize += compressedSize;
        currentStatistics->pinnedChunks++;
        return true;
    }

    void CompressedBuffer::unPin() const
    {
        CompressedBuffer& self = *(CompressedBuffer*)this;
        assert(self.accessCount > 0);
        if (--self.accessCount == 0) {
            self.free();
        }
    }

    int CompressedBuffer::getCompressionMethod() const
    {
        return compressionMethod;
    }

    void CompressedBuffer::setCompressionMethod(int m)
    {
        compressionMethod = m;
    }

    size_t CompressedBuffer::getDecompressedSize() const
    {
        return decompressedSize;
    }

    void CompressedBuffer::setDecompressedSize(size_t size)
    {
        decompressedSize = size;
    }

    CompressedBuffer::CompressedBuffer(void* compressedData, int compressionMethod, size_t compressedSize, size_t decompressedSize)
    {
        data = compressedData;
        this->compressionMethod = compressionMethod;
        this->compressedSize = compressedSize;
        this->decompressedSize = decompressedSize;
        accessCount = 0;
    }

    CompressedBuffer::CompressedBuffer()
    {
        data = NULL;
        compressionMethod = 0;
        compressedSize = 0;
        decompressedSize = 0;
        accessCount = 0;
    }

    CompressedBuffer::~CompressedBuffer()
    {
        free();
    }

    size_t ConstChunk::getBitmapSize() const
    {
        if (isRLE() && isMaterialized() && !getAttributeDesc().isEmptyIndicator()) { 
            PinBuffer scope(*this);
            ConstRLEPayload payload((char*)getData());
            return getSize() - payload.packedSize();
        }
        return 0;
    }
    
    ConstChunk const* ConstChunk::getBitmapChunk() const
    {
        return this;
    }

    void ConstChunk::makeClosure(Chunk& closure, boost::shared_ptr<ConstRLEEmptyBitmap> const& emptyBitmap) const
    {
        PinBuffer scope(*this);
        closure.allocate(getSize() + emptyBitmap->packedSize());
        memcpy(closure.getData(), getData(), getSize());
        emptyBitmap->pack((char*)closure.getData() + getSize());
    }

    ConstChunk* ConstChunk::materialize() const
    {
        if (materializedChunk == NULL || materializedChunk->getFirstPosition(false) != getFirstPosition(false)) { 
            if (materializedChunk == NULL) { 
                ((ConstChunk*)this)->materializedChunk = new MemChunk();
            }
            materializedChunk->initialize(*this);
            materializedChunk->setBitmapChunk((Chunk*)getBitmapChunk());
            boost::shared_ptr<ConstChunkIterator> src 
                = getConstIterator(ChunkIterator::IGNORE_DEFAULT_VALUES|ChunkIterator::IGNORE_EMPTY_CELLS|ChunkIterator::INTENDED_TILE_MODE);
            shared_ptr<Query> emptyQuery;
            boost::shared_ptr<ChunkIterator> dst 
                = materializedChunk->getIterator(emptyQuery, 
                                                 (src->getMode() & ChunkIterator::TILE_MODE)|ChunkIterator::ChunkIterator::NO_EMPTY_CHECK);
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
            if (!vectorMode && !getArrayDesc().containsOverlaps()) {
                materializedChunk->setCount(count);
            }
            dst->flush();
        }
        return materializedChunk;
    }

    void ConstChunk::compress(CompressedBuffer& buf, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const
    {
        materialize()->compress(buf, emptyBitmap);
    }

    void* ConstChunk::getData() const
    {
        return materialize()->getData();
    }

    size_t ConstChunk::getSize() const
    {
        return materialize()->getSize();
    }

    bool ConstChunk::pin() const
    {
        return false;
    }

    void ConstChunk::unPin() const
    {
        assert(typeid(*this) != typeid(ConstChunk));
    }

    void Chunk::decompress(CompressedBuffer const& buf)
    {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Chunk::decompress";
    }

    void Chunk::merge(ConstChunk const& with, boost::shared_ptr<Query>& query)
    {
        if (getDiskChunk() != NULL)
            throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CHUNK_ALREADY_EXISTS);
        setCount(0); // unknown
        AttributeDesc const& attr = getAttributeDesc();
        char* dst = (char*)getData();
        Value const& defaultValue = attr.getDefaultValue();
        if (dst != NULL && (isSparse() || isRLE() || with.isSparse() || with.isRLE() || attr.isNullable() || TypeLibrary::getType(attr.getType()).variableSize()
                            || !defaultValue.isZero()))
        {
            int sparseMode = isSparse() ? ChunkIterator::SPARSE_CHUNK : 0;
            boost::shared_ptr<ChunkIterator> dstIterator = getIterator(query, sparseMode|ChunkIterator::APPEND_CHUNK|ChunkIterator::NO_EMPTY_CHECK);
            boost::shared_ptr<ConstChunkIterator> srcIterator = with.getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS|ChunkIterator::IGNORE_DEFAULT_VALUES);
            if (getArrayDesc().getEmptyBitmapAttribute() != NULL) { 
                while (!srcIterator->end()) {
                    if (!dstIterator->setPosition(srcIterator->getPosition()))
                        throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                    Value const& value = srcIterator->getItem();
                    dstIterator->writeItem(value);
                    ++(*srcIterator);
                }
            } else { // ignore default values
                while (!srcIterator->end()) {
                    Value const& value = srcIterator->getItem();
                    if (value != defaultValue) {
                        if (!dstIterator->setPosition(srcIterator->getPosition()))
                            throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                        dstIterator->writeItem(value);
                    }
                    ++(*srcIterator);
                }            
            }
            dstIterator->flush();
        } else {
            PinBuffer scope(with);
            char* src = (char*)with.getData();
            if (dst == NULL) {
                allocate(with.getSize());
                setSparse(with.isSparse());
                setRLE(with.isRLE());
                memcpy(getData(), src, getSize());
            } else {
                if (getSize() != with.getSize())
                    throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CANT_MERGE_CHUNKS_WITH_VARYING_SIZE);
                for (size_t j = 0, n = getSize(); j < n; j++) {
                    dst[j] |= src[j];
                }
            }
            write(query);
        }
    }

    void Chunk::aggregateMerge(ConstChunk const& with, AggregatePtr const& aggregate, boost::shared_ptr<Query>& query)
    {
        if (getDiskChunk() != NULL)
            throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CHUNK_ALREADY_EXISTS);

        if (isReadOnly())
            throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CANT_UPDATE_READ_ONLY_CHUNK);

        AttributeDesc const& attr = getAttributeDesc();

        if (aggregate->getStateType().typeId() != attr.getType())
            throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_TYPE_MISMATCH_BETWEEN_AGGREGATE_AND_CHUNK);

        if (!attr.isNullable())
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_AGGREGATE_STATE_MUST_BE_NULLABLE);//enforce equivalency w above merge()

        setCount(0);
        char* dst = (char*)getData();
        if (dst != NULL)
        {
            int sparseMode = isSparse() ? ChunkIterator::SPARSE_CHUNK : 0;
            boost::shared_ptr<ChunkIterator>dstIterator = getIterator(query, sparseMode|ChunkIterator::APPEND_CHUNK|ChunkIterator::NO_EMPTY_CHECK);
            boost::shared_ptr<ConstChunkIterator> srcIterator = with.getConstIterator(ChunkIterator::IGNORE_NULL_VALUES);
            while (!srcIterator->end())
            {
                Value& val = srcIterator->getItem();
                if (!val.isNull())
                {
                    if (!dstIterator->setPosition(srcIterator->getPosition()))
                        throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                    Value& val2 = dstIterator->getItem();
                    if (!val2.isNull())
                    {
                        aggregate->merge(val, val2);
                    }
                    dstIterator->writeItem(val);
                }
                ++(*srcIterator);
            }
            dstIterator->flush();
        }
        else
        {
            PinBuffer scope(with);
            char* src = (char*)with.getData();
            allocate(with.getSize());
            setSparse(with.isSparse());
            setRLE(with.isRLE());
            memcpy(getData(), src, getSize());
            write(query);
        }
    }

    void Chunk::setSparse(bool)
    {
    }

    void Chunk::setRLE(bool)
    {
    }

    void Chunk::write(boost::shared_ptr<Query>& query)
    {
    }

    void Chunk::setCount(size_t)
    {
    }

    void Chunk::truncate(Coordinate lastCoord)
    {
    }

    bool ConstChunk::contains(Coordinates const& pos, bool withOverlap) const
    {
        Coordinates const& first = getFirstPosition(withOverlap);
        Coordinates const& last = getLastPosition(withOverlap);
        for (size_t i = 0, n = first.size(); i < n; i++) {
            if (pos[i] < first[i] || pos[i] > last[i]) {
                return false;
            }
        }
        return true;
    }

    bool ConstChunk::isCountKnown() const
    {
        return getArrayDesc().getEmptyBitmapAttribute() == NULL
            || (materializedChunk && materializedChunk->isCountKnown());
    }

    size_t ConstChunk::count() const
    {
        if (getArrayDesc().getEmptyBitmapAttribute() == NULL) {
            return getNumberOfElements(false);
        }
        if (materializedChunk) {
            return materializedChunk->count();
        }
        shared_ptr<ConstChunkIterator> i = getConstIterator();
        size_t n = 0;
        while (!i->end()) {
            ++(*i);
            n += 1;
        }
        return n;
    }



    size_t ConstChunk::getNumberOfElements(bool withOverlap) const
    {
        Coordinates const& first = getFirstPosition(withOverlap);
        Coordinates const& last = getLastPosition(withOverlap);
        size_t size = 1;
        for (size_t i = 0, n = first.size(); i < n; i++) {
            size *= last[i] - first[i] + 1;
        }
        return size;
    }

    ConstIterator::~ConstIterator() {}

    bool ConstChunkIterator::supportsVectorMode() const
    {
        return false;
    }

    void ConstChunkIterator::setVectorMode(bool enabled)
    {
        if (enabled)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "setVectorMode";
    }

    Coordinates const& ConstChunkIterator::getFirstPosition()
    {
        return getChunk().getFirstPosition((getMode() & IGNORE_OVERLAPS) == 0);
    }

    Coordinates const& ConstChunkIterator::getLastPosition()
    {
        return getChunk().getLastPosition((getMode() & IGNORE_OVERLAPS) == 0);
    }

        bool ConstChunkIterator::forward(uint64_t direction)
    {
        Coordinates pos = getPosition();
        Coordinates const& last = getLastPosition();
        do {
            for (size_t i = 0; direction != 0; i++, direction >>= 1) {
                if (direction & 1) {
                    if (++pos[i] > last[i]) {
                        return false;
                    }
                }
            }
        } while (!setPosition(pos));
        return true;
    }

        bool ConstChunkIterator::backward(uint64_t direction)
    {
        Coordinates pos = getPosition();
        Coordinates const& first = getFirstPosition();
        do {
            for (size_t i = 0; direction != 0; i++, direction >>= 1) {
                if (direction & 1) {
                    if (--pos[i] < first[i]) {
                        return false;
                    }
                }
            }
        } while (!setPosition(pos));
        return true;
    }

    bool ConstChunk::isPlain() const
    {
        Dimensions const& dims = getArrayDesc().getDimensions();
        for (size_t i = 0, n = dims.size(); i < n; i++) {
            if (dims[i].getChunkOverlap() != 0) {
                return false;
            }
        }
        return !isSparse()
            && !isRLE()
            && !getAttributeDesc().isNullable()
            && !TypeLibrary::getType(getAttributeDesc().getType()).variableSize()
            && (getAttributeDesc().isEmptyIndicator() || getArrayDesc().getEmptyBitmapAttribute() == NULL);
    }

        bool ConstChunk::isReadOnly() const
    {
        return true;
    }

    bool ConstChunk::isMaterialized() const
    {
        return false;
    }

    DBChunk const* ConstChunk::getDiskChunk() const
    {
        return NULL;
    }

    bool ConstChunk::isSparse() const
    {
        return false;
    }


    bool ConstChunk::isRLE() const
    {
#ifndef SCIDB_CLIENT
        return Config::getInstance()->getOption<bool>(CONFIG_RLE_CHUNK_FORMAT);
#else
        return false;
#endif
    }

    boost::shared_ptr<ConstRLEEmptyBitmap> ConstChunk::getEmptyBitmap() const
    {
        if (isRLE() && getAttributeDesc().isEmptyIndicator()/* && isMaterialized()*/) { 
            PinBuffer scope(*this);
            return boost::shared_ptr<ConstRLEEmptyBitmap>(scope.isPinned() ? new ConstRLEEmptyBitmap(*this) : new RLEEmptyBitmap(ConstRLEEmptyBitmap(*this)));
        }
        AttributeDesc const* emptyAttr = getArrayDesc().getEmptyBitmapAttribute();
        if (emptyAttr != NULL) {
            if (!emptyIterator) { 
                ((ConstChunk*)this)->emptyIterator = getArray().getConstIterator(emptyAttr->getId());
            }
            if (!emptyIterator->setPosition(getFirstPosition(false))) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            }
            ConstChunk const& bitmapChunk = emptyIterator->getChunk();
            PinBuffer scope(bitmapChunk);
            return boost::shared_ptr<ConstRLEEmptyBitmap>(new RLEEmptyBitmap(ConstRLEEmptyBitmap((char*)bitmapChunk.getData())));
        }
        return boost::shared_ptr<ConstRLEEmptyBitmap>();
    }


    ConstChunk::ConstChunk() : materializedChunk(NULL)
    {
    }

    ConstChunk::~ConstChunk()
    {
        delete materializedChunk; 
    }

    std::string const& Array::getName() const
    {
        return getArrayDesc().getName();
    }

    ArrayID Array::getHandle() const
    {
        return getArrayDesc().getId();
    }


    void Array::append(boost::shared_ptr<Array> input, bool vertical)
    {
        if (vertical) {
            for (size_t i = 0, n = getArrayDesc().getAttributes().size(); i < n; i++) {
                boost::shared_ptr<ArrayIterator> dst = getIterator(i);
                boost::shared_ptr<ConstArrayIterator> src = input->getConstIterator(i);
                while (!src->end()) {
                    dst->copyChunk(src->getChunk());
                    ++(*dst);
                    ++(*src);
                }
            }
        } else {
            size_t nAttrs = getArrayDesc().getAttributes().size();
            std::vector< boost::shared_ptr<ArrayIterator> > dstIterators(nAttrs);
            std::vector< boost::shared_ptr<ConstArrayIterator> > srcIterators(nAttrs);
            for (size_t i = 0; i < nAttrs; i++) {
                dstIterators[i] = getIterator(i);
                srcIterators[i] = input->getConstIterator(i);
            }
            while (!srcIterators[0]->end()) {
                for (size_t i = 0; i < nAttrs; i++) {
                    boost::shared_ptr<ArrayIterator> dst = dstIterators[i];
                    boost::shared_ptr<ConstArrayIterator> src = srcIterators[i];
                    dst->copyChunk(src->getChunk());
                    ++(*dst);
                    ++(*src);
                }
            }
        }
    }

    bool Array::supportsRandomAccess() const
    {
        return true;
    }


    static char* copyStride(char* dst, char* src, Coordinates const& first, Coordinates const& last, Dimensions const& dims, size_t step, size_t attrSize, size_t c)
    {
        size_t n = dims[c].getChunkInterval();
        if (c+1 == dims.size()) {
            memcpy(dst, src, n*attrSize);
            src += n*attrSize;
        } else {
            step /= last[c] - first[c] + 1;
            for (size_t i = 0; i < n; i++) {
                src = copyStride(dst, src, first, last, dims, step, attrSize, c+1);
                dst += step*attrSize;
            }
        }
        return src;
    }

    size_t Array::extractData(AttributeID attrID, void* buf, Coordinates const& first, Coordinates const& last) const
    {
        ArrayDesc const& arrDesc =  getArrayDesc();
        AttributeDesc const& attrDesc = arrDesc.getAttributes()[attrID];
         Type attrType( TypeLibrary::getType(attrDesc.getType()));
        Dimensions const& dims = arrDesc.getDimensions();
        size_t nDims = dims.size();
        bool isNullable = attrDesc.isNullable();
        bool isEmptyable = arrDesc.getEmptyBitmapAttribute() != NULL;
        bool hasOverlap = false;
        bool aligned = true;
        if (attrType.variableSize())
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_EXTRACT_EXPECTED_FIXED_SIZE_ATTRIBUTE);

        if (attrType.bitSize() < 8)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_EXTRACT_UNEXPECTED_BOOLEAN_ATTRIBUTE);

        if (first.size() != nDims || last.size() != nDims)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_WRONG_NUMBER_OF_DIMENSIONS);

        size_t bufSize = 1;
        for (size_t j = 0; j < nDims; j++)
        {
            if (last[j] < first[j] || (first[j] - dims[j].getStart()) % dims[j].getChunkInterval() != 0) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_UNALIGNED_COORDINATES);
            }
            aligned &= (last[j] - dims[j].getStart() + 1) % dims[j].getChunkInterval() == 0;

            hasOverlap |= dims[j].getChunkOverlap() != 0;
            bufSize *= last[j] - first[j] + 1;
        }
        size_t attrSize = attrType.byteSize();
        memset(buf, 0, bufSize*attrSize);
        size_t nExtracted = 0;
        for (boost::shared_ptr<ConstArrayIterator> i = getConstIterator(attrID); !i->end(); ++(*i)) {
            size_t j, chunkOffs = 0;
            ConstChunk const& chunk = i->getChunk();
            Coordinates const& chunkPos = i->getPosition();
            for (j = 0; j < nDims; j++) {
                if (chunkPos[j] < first[j] || chunkPos[j] > last[j]) {
                    break;
                }
                chunkOffs *= last[j] - first[j] + 1;
                chunkOffs += chunkPos[j] - first[j];
            }
            if (j == nDims) {
                if (!aligned || hasOverlap || isEmptyable || isNullable || chunk.isRLE() || chunk.isSparse()) {
                    for (boost::shared_ptr<ConstChunkIterator> ci = chunk.getConstIterator(ChunkIterator::IGNORE_OVERLAPS|ChunkIterator::IGNORE_EMPTY_CELLS|ChunkIterator::IGNORE_NULL_VALUES);
                         !ci->end(); ++(*ci))
                    {
                        Value& v = ci->getItem();
                        if (!v.isNull()) { 
                            Coordinates const& itemPos = ci->getPosition();
                            size_t itemOffs = 0;
                            for (j = 0; j < nDims; j++) {
                                itemOffs *= last[j] - first[j] + 1;
                                itemOffs += itemPos[j] - first[j];
                            }
                            memcpy((char*)buf + itemOffs*attrSize, ci->getItem().data(), attrSize);
                        }
                    }
                } else {
                    PinBuffer scope(chunk);
                    copyStride((char*)buf + chunkOffs*attrSize, (char*)chunk.getData(), first, last, dims, bufSize, attrSize, 0);
                }
                nExtracted += 1;
            }
        }
        return nExtracted;
    }

    boost::shared_ptr<ArrayIterator> Array::getIterator(AttributeID attr)
    {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Array::getIterator";
    }


    boost::shared_ptr<ConstItemIterator> Array::getItemIterator(AttributeID attrID, int iterationMode) const
    {
        return boost::shared_ptr<ConstItemIterator>(new ConstItemIterator(*this, attrID, iterationMode));
    }

    bool ConstArrayIterator::setPosition(Coordinates const& pos)
    {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "ConstArrayIterator::setPosition";
    }

    void ConstArrayIterator::reset()
    {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "ConstArrayIterator::reset";
    }

    void ArrayIterator::deleteChunk(Chunk& chunk)
    {
    }
    Chunk& ArrayIterator::updateChunk()
    {
        ConstChunk const& constChunk = getChunk();
        if (constChunk.isReadOnly())
            throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CANT_UPDATE_READ_ONLY_CHUNK);
        Chunk& chunk = (Chunk&)dynamic_cast<const Chunk&>(constChunk);
        chunk.pin();
        return chunk;
    }

    Chunk& ArrayIterator::copyChunk(ConstChunk const& chunk, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap)
    {
        const Coordinates& pos = chunk.getFirstPosition(false);
        Chunk& outChunk = newChunk(pos, chunk.getCompressionMethod());
        try {
            boost::shared_ptr<Query> query(getQuery());
            outChunk.setRLE(chunk.isRLE());
            if (chunk.isMaterialized() && chunk.getAttributeDesc().isNullable() == outChunk.getAttributeDesc().isNullable()) {
                PinBuffer scope(chunk);
                if (emptyBitmap && chunk.getBitmapSize() == 0) { 
                    size_t size = chunk.getSize() + emptyBitmap->packedSize();
                    outChunk.allocate(size);
                    memcpy(outChunk.getData(), chunk.getData(), chunk.getSize());
                    emptyBitmap->pack((char*)outChunk.getData() + chunk.getSize());
                } else {
                    size_t size = emptyBitmap ? chunk.getSize() : chunk.getSize() - chunk.getBitmapSize();
                    outChunk.allocate(size);
                    memcpy(outChunk.getData(), chunk.getData(), size);
                }
                outChunk.setSparse(chunk.isSparse());
                outChunk.setCount(chunk.isCountKnown() ? chunk.count() : 0);
                outChunk.write(query);
            } else {
                if (emptyBitmap) { 
                    chunk.makeClosure(outChunk, emptyBitmap);
                    outChunk.write(query);
                } else { 
                    boost::shared_ptr<ConstChunkIterator> src = chunk.getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS|ChunkIterator::INTENDED_TILE_MODE);
                    boost::shared_ptr<ChunkIterator> dst = outChunk.getIterator(query,
                                                                                (src->getMode() & ChunkIterator::TILE_MODE)|ChunkIterator::NO_EMPTY_CHECK|(chunk.isSparse()?ChunkIterator::SPARSE_CHUNK:0));
                    bool vectorMode = src->supportsVectorMode() && dst->supportsVectorMode();
                    src->setVectorMode(vectorMode);
                    dst->setVectorMode(vectorMode);
                    size_t count = 0;
                    while (!src->end()) {
                        count += 1;
                        dst->setPosition(src->getPosition());
                        dst->writeItem(src->getItem());
                        ++(*src);
                    }
                    if (!vectorMode && !chunk.getArrayDesc().containsOverlaps()) {
                        outChunk.setCount(count);
                    }
                    dst->flush();
                }
            }
        } catch (...) {
            deleteChunk(outChunk);
            throw;
        }
        return outChunk;
    }

    int ConstItemIterator::getMode()
    {
        return iterationMode;
    }

     Value& ConstItemIterator::getItem()
    {
        return chunkIterator->getItem();
    }

    bool ConstItemIterator::isEmpty()
    {
        return chunkIterator->isEmpty();
    }

    ConstChunk const& ConstItemIterator::getChunk()
    {
        return chunkIterator->getChunk();
    }

    bool ConstItemIterator::end()
    {
        return !chunkIterator || chunkIterator->end();
    }

    void ConstItemIterator::operator ++()
    {
        ++(*chunkIterator);
        while (chunkIterator->end()) {
            chunkIterator.reset();
            ++(*arrayIterator);
            if (arrayIterator->end()) {
                return;
            }
            chunkIterator = arrayIterator->getChunk().getConstIterator(iterationMode);
        }
    }

    Coordinates const& ConstItemIterator::getPosition()
    {
        return chunkIterator->getPosition();
    }

    bool ConstItemIterator::setPosition(Coordinates const& pos)
    {
        if (!chunkIterator || !chunkIterator->setPosition(pos)) {
            chunkIterator.reset();
            if (arrayIterator->setPosition(pos)) {
                chunkIterator = arrayIterator->getChunk().getConstIterator(iterationMode);
                return chunkIterator->setPosition(pos);
            }
            return false;
        }
        return true;
    }

    void ConstItemIterator::reset()
    {
        chunkIterator.reset();
        arrayIterator->reset();
        if (!arrayIterator->end()) {
            chunkIterator = arrayIterator->getChunk().getConstIterator(iterationMode);
        }
    }

    ConstItemIterator::ConstItemIterator(Array const& array, AttributeID attrID, int mode)
    : arrayIterator(array.getConstIterator(attrID)),
      iterationMode(mode)
    {
        if (!arrayIterator->end()) {
            chunkIterator = arrayIterator->getChunk().getConstIterator(mode);
        }
    }


    static void dummyFunction(const Value** args, Value* res, void*) {}

    class UserDefinedRegistrator {
      public:
        UserDefinedRegistrator() {}

        void foo();
    };

    void UserDefinedRegistrator::foo()
    {
        REGISTER_FUNCTION(length, list_of(TID_STRING)(TID_STRING), TypeId(TID_INT64), dummyFunction);
        REGISTER_CONVERTER(string, char, TRUNCATE_CONVERSION_COST, dummyFunction);
        REGISTER_TYPE(decimal, 16);
    }

    UserDefinedRegistrator userDefinedRegistrator;

}
