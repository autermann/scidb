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
 * InputArray.cpp
 *
 *  Created on: Sep 23, 2010
 */
#include "InputArray.h"
#include "system/SystemCatalog.h"

namespace scidb
{
    using namespace std;

    ConstChunk const& InputArrayIterator::getChunk()
    {
        return array.getChunk(attr, currChunkIndex);
    }

    bool InputArrayIterator::end()
    {
        try
        {
            if (!hasCurrent)
                hasCurrent = array.moveNext(currChunkIndex);
        }
        catch(Exception const& x)
        {
            throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_FILE_IMPORT_FAILED)
                << array.scanner.getFilePath()
                << Query::getQueryByID(Query::getCurrentQueryID())->getInstanceID()
                << array.getName()
                << array.scanner.getLine()
                << array.scanner.getColumn()
                << array.scanner.getFilePos()
                << array.scanner.getValue()
                << x.getErrorMessage();
        }
        return !hasCurrent;
    }

    void InputArrayIterator::operator ++()
    {
        if (end())
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        hasCurrent = false;
        currChunkIndex += 1;
    }

    Coordinates const& InputArrayIterator::getPosition()
    {
        return getChunk().getFirstPosition(false);
    }

    InputArrayIterator::InputArrayIterator(InputArray& arr, AttributeID id)
    : array(arr), attr(id), hasCurrent(false), currChunkIndex(1)
    {
    }

    ArrayDesc const& InputArray::getArrayDesc() const
    {
        return desc;
    }

    boost::shared_ptr<ConstArrayIterator> InputArray::getConstIterator(AttributeID attr) const
    {
        InputArray& self = *(InputArray*)this;
        if (!self.iterators[attr]) {
            for (size_t i = 0, n = self.iterators.size(); i < n; i++) {
                self.iterators[i] = boost::shared_ptr<InputArrayIterator>(new InputArrayIterator(self, i));
            }
        }
        return self.iterators[attr];
    }

InputArray::InputArray(ArrayDesc const& array, string const& filePath, boost::shared_ptr<Query>& query)
    : desc(array),
      scanner(filePath),
      chunkPos(array.getDimensions().size()),
      currChunkIndex(0),
      iterators(array.getAttributes().size()),
      lookahead(iterators.size()),
      types(iterators.size()),
      attrVal(iterators.size()),
      converters(iterators.size()),
      coordVal(TypeLibrary::getType(TID_INT64)),
      strVal(TypeLibrary::getType(TID_STRING)),
      emptyTagAttrID(array.getEmptyBitmapAttribute() != NULL ? array.getEmptyBitmapAttribute()->getId() : INVALID_ATTRIBUTE_ID),
      state(Init),
      _query(query)
    {
        Dimensions const& dims = array.getDimensions();
        Attributes const& attrs = array.getAttributes();
        size_t nDims = chunkPos.size();
        size_t nAttrs = attrs.size();
        for (size_t i = 0; i < nDims; i++) {
            chunkPos[i] = dims[i].getStart();
        }
        for (size_t i = 0; i < nAttrs; i++) {
            types[i] = attrs[i].getType();
            attrVal[i] = Value(TypeLibrary::getType(types[i]));
            if (! isBuiltinType(types[i])) {
                converters[i] = FunctionLibrary::getInstance()->findConverter( TID_STRING, types[i]);
            }
        }
        chunkPos[nDims-1] -= dims[nDims-1].getChunkInterval();
    }

    bool InputArray::moveNext(size_t chunkIndex)
    {
        if (chunkIndex > currChunkIndex+1)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR1);
        if (chunkIndex <= currChunkIndex) {
            return true;
        }
        size_t nAttrs = iterators.size();
        if (state == EndOfStream) {
            return false;
        }
        Dimensions const& dims = desc.getDimensions();
        Attributes const& attrs = desc.getAttributes();
        size_t nDims = dims.size();
        vector< boost::shared_ptr<ChunkIterator> > chunkIterators(nAttrs);
        boost::shared_ptr<Query> query(_query.lock());
        bool isSparse = false;

      BeginScanChunk:
        {
            LOG4CXX_DEBUG(logger, "Beginning scan of chunk");

            Token tkn = scanner.get();
            if (tkn == TKN_SEMICOLON) {
                tkn = scanner.get();
            }
            if (tkn == TKN_EOF) {
                state = EndOfStream;
                return false;
            }
            bool explicitChunkPosition = false;
            if (state != InsideArray) {
                if (tkn == TKN_COORD_BEGIN) {
                    explicitChunkPosition = true;
                    for (size_t i = 0; i < nDims; i++)
                    {
                        if (i != 0 && scanner.get() != TKN_COMMA)
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << ",";
                        if (scanner.get() != TKN_LITERAL)
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR3);
                        StringToValue( TID_INT64, scanner.getValue(), coordVal);
                        chunkPos[i] = coordVal.getInt64();
                        if ((chunkPos[i] - dims[i].getStart()) % dims[i].getChunkInterval() != 0)
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR4);
                    }
                    //
                    // Check that this explicit chunkPos isn't inconsistent
                    // (ie. out of order). We should always grow chunk
                    // addresses.
                    for (size_t i = 0; i < lastChunkPos.size(); i++) {

                        if (!(lastChunkPos[i] <= chunkPos[i])) {
                            std::stringstream ss;
                            ss << "Given that the last chunk processed was { " << lastChunkPos << " } this chunk { " << chunkPos << " } is out of sequence";
                            LOG4CXX_DEBUG(logger, ss.str());
                        }
                        if (lastChunkPos[i] > chunkPos[i])
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR5);
                        if (lastChunkPos[i] < chunkPos[i]) {
                            break;
                        }
                    }
                    lastChunkPos = chunkPos;
                    if (scanner.get() != TKN_COORD_END)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "}";
                    tkn = scanner.get();
                    std::stringstream ss;
                    ss << "Explicit chunk coords are { " << chunkPos << " }";
                    LOG4CXX_DEBUG(logger, ss.str());
                }
                if (tkn != TKN_ARRAY_BEGIN)
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "[";
                tkn = scanner.get();
            }
            for (size_t i = 1; i < nDims; i++) {
                if (tkn != TKN_ARRAY_BEGIN)
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "[";
                tkn = scanner.get();
            }

            if (tkn == TKN_ARRAY_BEGIN)
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR6);
            if (!explicitChunkPosition) {
                size_t i = nDims-1;
                while (true) {
                    chunkPos[i] += dims[i].getChunkInterval();
                    if (chunkPos[i] <= dims[i].getEndMax()) {
                        break;
                    }
                    if (0 == i)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR7);
                    chunkPos[i] = dims[i].getStart();
                    i -= 1;
                }
                std::stringstream ss;
                ss << "Implicit chunk coords { " << chunkPos << " }";
                LOG4CXX_DEBUG(logger, ss.str());
            }
            Coordinates const* first = NULL;
            Coordinates const* last = NULL;
            Coordinates pos = chunkPos;

            while (true) {
                if (tkn == TKN_COORD_BEGIN) {
                    isSparse = true;
                    for (size_t i = 0; i < nDims; i++) {
                        if (i != 0 && scanner.get() != TKN_COMMA)
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << ",";
                        if (scanner.get() != TKN_LITERAL)
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR3);
                        StringToValue( TID_INT64, scanner.getValue(), coordVal);
                        pos[i] = coordVal.getInt64();
                    }
                    if (scanner.get() != TKN_COORD_END)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "}";
                    tkn = scanner.get();
                }
                bool inParen = false;
                if (tkn == TKN_TUPLE_BEGIN) {
                    inParen = true;
                    tkn = scanner.get();
                }
                if (tkn == TKN_LITERAL || (inParen && tkn == TKN_COMMA)) {
                    for (size_t i = 0; i < nAttrs; i++) {
                        if (!chunkIterators[i]) {
                            if (isSparse && !explicitChunkPosition) {
                                chunkPos = pos;
                                desc.getChunkPositionFor(chunkPos);
                            }
                            Address addr(desc.getId(), i, chunkPos);
                            MemChunk& chunk =  lookahead[i].chunks[chunkIndex % LOOK_AHEAD];
                            chunk.initialize(this, &desc, addr, attrs[i].getDefaultCompressionMethod());
                            if (first == NULL) {
                                first = &chunk.getFirstPosition(true);
                                if (!isSparse) {
                                    pos = *first;
                                }
                                last = &chunk.getLastPosition(true);
                            }
                            chunkIterators[i] = chunk.getIterator(query, ChunkIterator::NO_EMPTY_CHECK
                                                                  | (isSparse ? ConstChunkIterator::SPARSE_CHUNK : ConstChunkIterator::SEQUENTIAL_WRITE));
                        }
                        if (isSparse) {
                            if (!( chunkIterators[i]->setPosition(pos) )) {
                                std::stringstream ss;
                                ss << "From sparse load file '" << scanner.getFilePath() << "' at coord " << pos << " is out of chunk bounds :" << chunkPos;
                                LOG4CXX_DEBUG(logger, ss.str());
                                
                                if (!chunkIterators[i]->setPosition(pos))
                                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR7);
                            }
                        } else {
                            if (!(chunkIterators[i]->getPosition() == pos)) {
                                std::stringstream ss;
                                ss << "From dense load file '" << scanner.getFilePath() << "' at coord " << pos << " is out of chunk bounds :" << chunkPos;
                                LOG4CXX_DEBUG(logger, ss.str());
                            }
                            if (chunkIterators[i]->getPosition() != pos)
                                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR7);
                        }
                        if ((inParen && (tkn == TKN_COMMA || tkn == TKN_TUPLE_END)) || (!inParen && i != 0)) {
                            if (i == emptyTagAttrID) {
                                attrVal[i].setBool(true);
                                chunkIterators[i]->writeItem(attrVal[i]);
                            } else if (chunkIterators[i]->getChunk().isRLE()/* && emptyTagAttrID != INVALID_ATTRIBUTE_ID*/) {
                                chunkIterators[i]->writeItem(attrs[i].getDefaultValue());
                            }
                            if (inParen && tkn == TKN_COMMA) {
                                tkn = scanner.get();
                            }
                        } else {
                            if (tkn != TKN_LITERAL)
                                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR8);
                            if (scanner.isNull()) {
                                if (!desc.getAttributes()[i].isNullable())
                                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ASSIGNING_NULL_TO_NON_NULLABLE);
                                attrVal[i].setNull(scanner.getMissingReason());
                            } else if (converters[i]) {
                                strVal.setString(scanner.getValue().c_str());
                                const Value* v = &strVal;
                                (*converters[i])(&v, &attrVal[i], NULL);
                            } else {
                                StringToValue(types[i], scanner.getValue(), attrVal[i]);
                            }
                            if (i == emptyTagAttrID) {
                                if (!attrVal[i].getBool())
                                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR9);
                            }                                    
                            chunkIterators[i]->writeItem(attrVal[i]);
                            tkn = scanner.get();
                            if (inParen && i+1 < nAttrs && tkn == TKN_COMMA) {
                                tkn = scanner.get();
                            }
                        }
                        if (!isSparse) {
                            ++(*chunkIterators[i]);
                        }
                    }
                } else if (inParen && tkn == TKN_TUPLE_END && !isSparse) {
                    for (size_t i = 0; i < nAttrs; i++) {
                        if (!chunkIterators[i]) {
                            Address addr(desc.getId(), i, chunkPos);
                            MemChunk& chunk =  lookahead[i].chunks[chunkIndex % LOOK_AHEAD];
                            chunk.initialize(this, &desc, addr, desc.getAttributes()[i].getDefaultCompressionMethod());
                            if (first == NULL) {
                                first = &chunk.getFirstPosition(true);
                                last = &chunk.getLastPosition(true);
                                pos = *first;
                            }
                            chunkIterators[i] = chunk.getIterator(query, ChunkIterator::NO_EMPTY_CHECK|ConstChunkIterator::SEQUENTIAL_WRITE);
                        }
                        if (chunkIterators[i]->getChunk().isRLE() && emptyTagAttrID == INVALID_ATTRIBUTE_ID) {
                            chunkIterators[i]->writeItem(attrs[i].getDefaultValue());
                        }
                        ++(*chunkIterators[i]);
                    }
                }
                if (inParen) {
                    if (tkn != TKN_TUPLE_END)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << ")";
                    tkn = scanner.get();
                    if (!isSparse && tkn == TKN_MULTIPLY) { 
                        tkn = scanner.get();
                        if (tkn != TKN_LITERAL)
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "multiplier";
                        Value countVal;
                        StringToValue(TID_INT64, scanner.getValue(), countVal);
                        int64_t count = countVal.getInt64();
                        while (--count != 0) { 
                            for (size_t i = 0; i < nAttrs; i++) {
                                chunkIterators[i]->writeItem(attrVal[i]);
                                ++(*chunkIterators[i]);
                            }
                        }
                        tkn = scanner.get(); 
                        pos = chunkIterators[0]->getPosition();
                        pos[nDims-1] -= 1;
                    }
                }
                size_t nBrackets = 0;
                if (isSparse) {
                    while (tkn == TKN_ARRAY_END) {
                        if (++nBrackets == nDims) {
                            if (first == NULL) { // empty chunk
                                goto BeginScanChunk;
                            }
                            state = EndOfChunk;
                            goto EndScanChunk;
                        }
                        tkn = scanner.get();
                    }
                } else {
                    if (NULL == last ) {
                        state = EndOfStream;
                        return false;
                        /*
                          std::stringstream ss;
                          ss << "Dense load files need chunks of regular sizes - don't know size of current chunk " << chunkPos;
                          LOG4CXX_DEBUG(logger, ss.str());
                        */
                    }
                    if (!last)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR10);
                    for (size_t i = nDims-1; ++pos[i] > (*last)[i]; i--) {
                        if (i == 0) {
                            if (tkn == TKN_ARRAY_END) {
                                state = EndOfChunk;
                            } else if (tkn == TKN_COMMA) {
                                state = InsideArray;
                            } else {
                                throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR2) << "]";
                            }
                            goto EndScanChunk;
                        }
                        if (tkn != TKN_ARRAY_END)
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "]";
                        nBrackets += 1;
                        pos[i] = (*first)[i];
                        tkn = scanner.get();
                    }
                }
                if (tkn == TKN_COMMA) {
                    tkn = scanner.get();
                }
                while (nBrackets != 0 ) {
                    if (tkn != TKN_ARRAY_BEGIN)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "[";
                    nBrackets -= 1;
                    tkn = scanner.get();
                }
            }
        }
      EndScanChunk:
        if (!isSparse && emptyTagAttrID == INVALID_ATTRIBUTE_ID) {
            for (size_t i = 0; i < nAttrs; i++) {
                if (chunkIterators[i] && chunkIterators[i]->getChunk().isRLE()) { 
                    while (!chunkIterators[i]->end()) {
                        chunkIterators[i]->writeItem(attrs[i].getDefaultValue());
                        ++(*chunkIterators[i]);
                    }
                }
            }
        }                        
        for (size_t i = 0; i < nAttrs; i++) {
            if (chunkIterators[i]) {
                chunkIterators[i]->flush();
            }
        }
        currChunkIndex += 1;

        LOG4CXX_DEBUG(logger, "Finished scan of chunk number " << currChunkIndex);

        return true;
    }

    bool InputArray::supportsRandomAccess() const
    {
        return false;
    }

    ConstChunk const& InputArray::getChunk(AttributeID attr, size_t chunkIndex)
    {
        if (chunkIndex > currChunkIndex || chunkIndex + LOOK_AHEAD <= currChunkIndex)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR11);
        MemChunk& chunk = lookahead[attr].chunks[chunkIndex % LOOK_AHEAD];
        if (emptyTagAttrID != attr && emptyTagAttrID != INVALID_ATTRIBUTE_ID) {
            chunk.setBitmapChunk(&lookahead[emptyTagAttrID].chunks[chunkIndex % LOOK_AHEAD]);
        }
        return chunk;
    }
}
