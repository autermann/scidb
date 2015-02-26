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

#define __EXTENSIONS__
#define _EXTENSIONS
#define _FILE_OFFSET_BITS 64
#if ! defined(HPUX11_NOT_ITANIUM) && ! defined(L64)
#define _LARGEFILE64_SOURCE 1 // access to files greater than 2Gb in Solaris
#define _LARGE_FILE_API     1 // access to files greater than 2Gb in AIX
#endif


#include <stdio.h>
#include <ctype.h>
#include <inttypes.h>
#include <limits.h>
#include <string>
#include <errno.h>

#include "boost/format.hpp"

#include "log4cxx/logger.h"
#include "log4cxx/basicconfigurator.h"
#include "log4cxx/helpers/exception.h"

#include "system/Exceptions.h"
#include "query/TypeSystem.h"
#include "query/FunctionDescription.h"
#include "query/FunctionLibrary.h"
#include "query/Operator.h"
#include "smgr/io/DBLoader.h"
#include "array/DBArray.h"
#include "smgr/io/Storage.h"
#include "system/SystemCatalog.h"


namespace scidb
{
    using namespace std;
    using namespace boost;

    // declared static to prevent visibility of variable outside of this file
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.smgr.io.DBLoader"));

    class CompatibilityIterator : public ConstChunkIterator
    {
        Coordinates currPos;
        shared_ptr<ConstChunkIterator> inputIterator;
        Coordinates const& firstPos;
        Coordinates const& lastPos;
        Coordinates const* nextPos;
        bool hasCurrent;
        Value defaultValue;
        int mode;
        bool isEmptyable;

      public:
        CompatibilityIterator(shared_ptr<ConstChunkIterator> iterator) 
        : inputIterator(iterator),
          firstPos(iterator->getFirstPosition()),
          lastPos(iterator->getLastPosition()),
          defaultValue(iterator->getChunk().getAttributeDesc().getDefaultValue()),
          mode(iterator->getMode()),
          isEmptyable(iterator->getChunk().getArrayDesc().getEmptyBitmapAttribute() != NULL)
        {
            if (mode & ConstChunkIterator::IGNORE_DEFAULT_VALUES) {
                if (iterator->getChunk().isSparse()) { 
                    mode |= ConstChunkIterator::IGNORE_EMPTY_CELLS;
                } else { 
                    mode &= ~ConstChunkIterator::IGNORE_DEFAULT_VALUES;
                }
            }
            reset();
        }

        bool skipDefaultValue() {
            return (mode & ConstChunkIterator::IGNORE_DEFAULT_VALUES) && nextPos != NULL && currPos == *nextPos && inputIterator->getItem() == defaultValue;
        }

        int getMode() {
            return inputIterator->getMode();
        }

        Value& getItem() {
            if (!hasCurrent)
                throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_NO_CURRENT_ELEMENT);
            return (nextPos == NULL || currPos != *nextPos) ? defaultValue : inputIterator->getItem();
        }

        bool isEmpty() {
            if (!hasCurrent)
                throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_NO_CURRENT_ELEMENT);
            return isEmptyable && (nextPos == NULL || currPos != *nextPos);
        }

        bool end() {
            return !hasCurrent;
        }

        void operator ++() {
            if (!hasCurrent)
                throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_NO_CURRENT_ELEMENT);

            do { 
                if (mode & ConstChunkIterator::IGNORE_EMPTY_CELLS) {
                    ++(*inputIterator);
                    if (inputIterator->end()) { 
                        hasCurrent = false;
                        return;
                    } 
                    nextPos = &inputIterator->getPosition();
                    currPos = *nextPos;
                } else { 
                    if (nextPos != NULL && currPos == *nextPos) { 
                        ++(*inputIterator);
                        nextPos = inputIterator->end() ? NULL : &inputIterator->getPosition();
                    } 
                    size_t i = currPos.size()-1;
                    while (++currPos[i] > lastPos[i]) { 
                        if (i == 0) { 
                            hasCurrent = false;
                            return;
                        } 
                        currPos[i] = firstPos[i];
                        i -= 1;
                    }
                }
            } while (skipDefaultValue());
        }

        Coordinates const& getPosition() {
            return currPos;
        }

        bool setPosition(Coordinates const& pos) {
            throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_NOT_IMPLEMENTED) << "CompatibilityIterator::setPosition";
        }

        void reset() {
            inputIterator->reset();
            nextPos = inputIterator->end() ? NULL : &inputIterator->getPosition();
            hasCurrent = nextPos != NULL || !(mode & ConstChunkIterator::IGNORE_EMPTY_CELLS);
            currPos = (mode & ConstChunkIterator::IGNORE_EMPTY_CELLS) && nextPos ? *nextPos : firstPos;
            if (hasCurrent && skipDefaultValue()) { 
                ++(*this);
            }
        }

        ConstChunk const& getChunk() {
            return inputIterator->getChunk();
        }
    };
                   

    enum Token
    {
        TKN_TUPLE_BEGIN,
        TKN_TUPLE_END,
        TKN_ARRAY_BEGIN,
        TKN_ARRAY_END,
        TKN_COORD_BEGIN,
        TKN_COORD_END,
        TKN_COMMA,
        TKN_SEMICOLON,
        TKN_LITERAL,
        TKN_EOF
    };

    class InputScanner
    {
        FILE* f;
        string value;

      public:
        string const& getValue()
        {
            return value;
        }

        InputScanner(string const& file)
        {
            LOG4CXX_DEBUG(logger, "Attempting to open file '" << file << "' for loading");
            f = fopen(file.c_str(), "r");
            if (NULL == f )
            {
                int error = errno;
                LOG4CXX_DEBUG(logger,"Attempted to open input file '" << file << "' and failed with errno = " << error);
                if (!f)
                    throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_CANT_OPEN_FILE) << file << error;
            }
        }

        ~InputScanner()
        {
            fclose(f);
        }

                long getFilePos() { if ( f ) return ftell ( f ); return -1; }

        Token get()
        {
            int ch;

            while ((ch = getc(f)) != EOF && isspace(ch)){}; // ignore whitespaces

            if (ch == EOF) {
                return TKN_EOF;
            }

            switch (ch) {
              case '\'':
                ch = getc(f);
                if (ch == '\\') {
                    ch = getc(f);
                    switch (ch) {
                      case '0':
                        ch = 0;
                        break;
                      case 'n':
                        ch = '\n';
                        break;
                      case 'r':
                        ch = '\r';
                        break;
                      case 'f':
                        ch = '\f';
                        break;
                      case 't':
                        ch = '\t';
                        break;
                    }

                }
                if (ch == EOF)
                    throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_UNTERMINATED_CHARACTER_CONSTANT);
                value = (char)ch;
                ch = getc(f);
                if (ch != '\'')
                    throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_UNTERMINATED_CHARACTER_CONSTANT);
                return TKN_LITERAL;
              case '"':
                value.clear();
                while (true) {
                    ch = getc(f);
                    if (ch == '\\') {
                        ch = getc(f);
                    }
                    if (ch == EOF)
                        throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_UNTERMINATED_STRING_LITERAL);
                    if (ch == '\"') {
                        return TKN_LITERAL;
                    }
                    value += (char)ch;
                }
                break;
              case '{':
                return TKN_COORD_BEGIN;
              case '}':
                return TKN_COORD_END;
              case '(':
                return TKN_TUPLE_BEGIN;
              case ')':
                return TKN_TUPLE_END;
              case '[':
                return TKN_ARRAY_BEGIN;
              case ']':
                return TKN_ARRAY_END;
              case ',':
                return TKN_COMMA;
              case ';':
                return TKN_SEMICOLON;
              default:
                value.clear();
                while (ch != EOF && !isspace(ch) && ch != ')' && ch != ']'  && ch != '}' && ch != '(' && ch != '{' && ch != '[' && ch != ',') {
                    value += (char)ch;
                    ch = getc(f);
                }
                if (!value.size())
                    throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_BAD_LITERAL);
                if (ch != EOF) {
                    ungetc(ch, f);
                }
                return TKN_LITERAL;
            }
        }
    };

    uint64_t DBLoader::save(string const& arrayName, string const& file,
                            const boost::shared_ptr<Query>& query,
                            string const& format, bool append)
    {
#ifndef SCIDB_CLIENT
        ArrayDesc desc;
        SystemCatalog::getInstance()->getArrayDesc(arrayName, desc);
        return save(DBArray(desc.getId(),query), file, format, append);
#else
        return 0;
#endif
    }

    uint64_t DBLoader::save(Array const& array, string const& file,
                            string const& format, bool append)
    {
        ArrayDesc const& desc = array.getArrayDesc();
        size_t i, j;
        uint64_t n = 0;

        FILE* f = file == "console" || file == "stdout" ? stdout
            : file == "stderr" ? stderr : fopen(file.c_str(), append ? "a" : "w");

        if (NULL == f) {
            int error = errno;
            LOG4CXX_DEBUG(logger, "Attempted to open output file '" << file << "' and failed with errno = " << error);
            if (!f)
                throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_CANT_OPEN_FILE) << file << error;
        }

        Attributes const& attrs = desc.getAttributes();
        //If descriptor has empty flag we just ignore it and fill only iterators with actual data attributes
        bool omitEpmtyTag = desc.getEmptyBitmapAttribute();
        size_t iteratorsCount = attrs.size() - (omitEpmtyTag ? 1 : 0);
        if (iteratorsCount != 0)
        {
            Dimensions const& dims = desc.getDimensions();
            const size_t nDimensions = dims.size();
            assert(nDimensions > 0);
            vector< boost::shared_ptr<ConstArrayIterator> > arrayIterators(iteratorsCount);
            vector< boost::shared_ptr<ConstChunkIterator> > chunkIterators(iteratorsCount);
            vector< TypeId> types(iteratorsCount);
            vector< FunctionPointer> converters(iteratorsCount);
            Value strValue;
            Coordinates coord(nDimensions);
            int iterationMode = ConstChunkIterator::IGNORE_OVERLAPS;

            // Get array iterators for all attributes
            for (i = 0, j = 0; i < attrs.size(); i++)
            {
                if (omitEpmtyTag && attrs[i] == *desc.getEmptyBitmapAttribute())
                    continue;

                arrayIterators[j] = array.getConstIterator((AttributeID)i);
                types[j] = attrs[i].getType();
                if (! isBuiltinType(types[j])) {
                    TypeLibrary::getType(types[j]); // force loading of type
                    converters[j] =  FunctionLibrary::getInstance()->findConverter(types[j],  TID_STRING, false);
                }
                ++j;
            }

            if (format == "csv" || format == "csv+") {
                bool withCoordinates = format == "csv+";
                if (withCoordinates) {
                    for (i = 0; i < nDimensions; i++) {
                        if (i != 0) {
                            fputc(',', f);
                        }
                        fprintf(f, "%s", dims[i].getBaseName().c_str());
                    }
                }
                for (i = 0; i < iteratorsCount; i++) {
                    if (i != 0 || withCoordinates) {
                        fputc(',', f);
                    }
                    fprintf(f, "%s", attrs[i].getName().c_str());
                }
                fputc('\n', f);
                iterationMode |= ConstChunkIterator::IGNORE_EMPTY_CELLS;
                while (!arrayIterators[0]->end()) {
                    // Get iterators for the current chunk
                    for (i = 0; i < iteratorsCount; i++) {
                        ConstChunk const& chunk = arrayIterators[i]->getChunk();
                        chunkIterators[i] = chunk.getConstIterator(iterationMode);
                    }
                    while (!chunkIterators[0]->end()) {
                        if (withCoordinates) {
                            Coordinates const& pos = chunkIterators[0]->getPosition();
                            for (i = 0; i < nDimensions; i++) {
                                if (i != 0) {
                                    fputc(',', f);
                                }
                                fprintf(f, "%"PRIi64, pos[i]);
                            }
                        }
                        for (i = 0; i < iteratorsCount; i++) {
                            if (i != 0 || withCoordinates) {
                                fputc(',', f);
                            }
                            if (converters[i]) {
                                const Value* v = &chunkIterators[i]->getItem();
                                (*converters[i])(&v, &strValue, NULL);
                                fprintf(f, "\"%s\"",  strValue.getString());
                            } else {
                                Value const& val = chunkIterators[i]->getItem();
                                if (!val.isNull()) {
                                    fprintf(f, "%s",  ValueToString(types[i], val).c_str());
                                }
                            }
                            ++(*chunkIterators[i]);
                        }
                        fputc('\n', f);
                    }
                    for (i = 0; i < iteratorsCount; i++) {
                        ++(* arrayIterators[i]);
                    }
                }
            } else {
                bool sparseFormat = (format == "sparse");
                bool storeFormat = (format == "store");
                bool startOfArray = true;
                if (iteratorsCount == 1 && !storeFormat) {
                    iterationMode |= ConstChunkIterator::IGNORE_DEFAULT_VALUES;
                }
                if (sparseFormat) {
                    iterationMode |= ConstChunkIterator::IGNORE_EMPTY_CELLS;
                }
                if (storeFormat) {
                    iterationMode &= ~ConstChunkIterator::IGNORE_OVERLAPS;
                }
                // Set initial position
                Coordinates chunkPos(nDimensions);
                for (i = 0; i < nDimensions; i++) {
                    coord[i] = dims[i].getStart();
                    chunkPos[i] = dims[i].getStart();
                }

                // Check if chunking is performed in more than one dimension
                bool multisplit = false;
                for (i = 1; i < nDimensions; i++) {
                    if (dims[i].getChunkInterval() < dims[i].getLength()) {
                        multisplit = true;
                    }
                }

                coord[nDimensions-1] -= 1; // to simplify increment
                chunkPos[nDimensions-1] -= dims[nDimensions-1].getChunkInterval();
                {
                    // Iterate over all chunks
                    bool firstItem = true;
                    while (!arrayIterators[0]->end()) {
                        // Get iterators for the current chunk
                        for (i = 0; i < iteratorsCount; i++) {
                            ConstChunk const& chunk = arrayIterators[i]->getChunk();
                            chunkIterators[i] = chunk.getConstIterator(iterationMode);
                            if (chunk.isRLE()) { 
                                chunkIterators[i] = shared_ptr<ConstChunkIterator>(new CompatibilityIterator(chunkIterators[i]));
                            }
                        }
                        int j = nDimensions;
                        while (--j >= 0 && (chunkPos[j] += dims[j].getChunkInterval()) > dims[j].getEndMax()) {
                            chunkPos[j] = dims[j].getStart();
                        }
                        bool gap = !storeFormat && (sparseFormat || arrayIterators[0]->getPosition() != chunkPos);
                        chunkPos = arrayIterators[0]->getPosition();
                        if (!sparseFormat || !chunkIterators[0]->end()) {
                            if (!multisplit) {
                                Coordinates const& last = chunkIterators[0]->getLastPosition();
                                for (i = 1; i < nDimensions; i++) {
                                    if (last[i] < dims[i].getEndMax()) {
                                        multisplit = true;
                                    }
                                }
                            }
                            bool isSparse = chunkIterators[0]->getChunk().isSparse();
                            if (isSparse || storeFormat) {
                                if (!firstItem) {
                                    firstItem = true;
                                    for (i = 0; i < nDimensions; i++) {
                                        putc(']', f);
                                    }
                                    fprintf(f, ";\n");
                                    if (storeFormat) {
                                        putc('{', f);
                                        for (i = 0; i < nDimensions; i++) {
                                            if (i != 0) {
                                                putc(',', f);
                                            }
                                            fprintf(f, "%"PRIi64, chunkPos[i]);
                                        }
                                        putc('}', f);
                                    }
                                    for (i = 0; i < nDimensions; i++) {
                                        putc('[', f);
                                    }
                                }
                            }
                            if (storeFormat) {
                                coord =  chunkIterators[0]->getChunk().getFirstPosition(true);
                                coord[nDimensions-1] -= 1; // to simplify increment
                            }
                            // Iterator over all chunk elements
                            while (!chunkIterators[0]->end()) {
                                if (!isSparse) {
                                    Coordinates const& pos = chunkIterators[0]->getPosition();
                                    int nbr = 0;
                                    for (i = nDimensions-1; pos[i] != ++coord[i]; i--) {
                                        if (!firstItem) {
                                            putc(']', f);
                                            nbr += 1;
                                        }
                                        if (multisplit) {
                                            coord[i] = pos[i];
                                            if (i == 0) {
                                                break;
                                            }
                                        } else {
                                            if (i == 0) {
                                                break;
                                            } else {
                                                coord[i] = dims[i].getStart();
                                                if (sparseFormat) {
                                                    coord[i] = pos[i];
                                                    if (i == 0) {
                                                        break;
                                                    }
                                                } else {
                                                    assert(coord[i] == pos[i]);
                                                    assert(i != 0);
                                                }
                                            }
                                        }
                                    }
                                    if (!firstItem) {
                                        putc(nbr == (int)nDimensions ? ';' : ',', f);
                                    }
                                    if (gap) {
                                        putc('{', f);
                                        for (i = 0; i < nDimensions; i++) {
                                            if (i != 0) {
                                                putc(',', f);
                                            }
                                            fprintf(f, "%"PRIi64, pos[i]);
                                            coord[i] = pos[i];
                                        }
                                        putc('}', f);
                                        gap = false;
                                    }
                                    if (startOfArray) {
                                        if (storeFormat) {
                                            putc('{', f);
                                            for (i = 0; i < nDimensions; i++) {
                                                if (i != 0) {
                                                    putc(',', f);
                                                }
                                                fprintf(f, "%"PRIi64, chunkPos[i]);
                                            }
                                            putc('}', f);
                                        }
                                        for (i = 0; i < nDimensions; i++) {
                                            fputc('[', f);
                                        }
                                        startOfArray = false;
                                    }
                                    while (--nbr >= 0) {
                                        putc('[', f);
                                    }
                                    if (sparseFormat) {
                                        putc('{', f);
                                        Coordinates const& pos = chunkIterators[0]->getPosition();
                                        for (i = 0; i < nDimensions; i++) {
                                            if (i != 0) {
                                                putc(',', f);
                                            }
                                            fprintf(f, "%"PRIi64, pos[i]);
                                        }
                                        putc('}', f);
                                    }
                                } else {
                                    if (chunkIterators[0]->isEmpty() && (iterationMode & ConstChunkIterator::IGNORE_DEFAULT_VALUES)) { 
                                        goto nextItem;
                                    }
                                    if (!firstItem) {
                                        putc(',', f);
                                    }
                                    if (startOfArray) {
                                        if (storeFormat) {
                                            putc('{', f);
                                            for (i = 0; i < nDimensions; i++) {
                                                if (i != 0) {
                                                    putc(',', f);
                                                }
                                                fprintf(f, "%"PRIi64, chunkPos[i]);
                                            }
                                            putc('}', f);
                                        }
                                        for (i = 0; i < nDimensions; i++) {
                                            fputc('[', f);
                                        }
                                        startOfArray = false;
                                    }
                                    putc('{', f);
                                    Coordinates const& pos = chunkIterators[0]->getPosition();
                                    for (i = 0; i < nDimensions; i++) {
                                        if (i != 0) {
                                            putc(',', f);
                                        }
                                        fprintf(f, "%"PRIi64, pos[i]);
                                    }
                                    putc('}', f);
                                }
                                putc('(', f);
                                if (!chunkIterators[0]->isEmpty()) {
                                    for (i = 0; i < iteratorsCount; i++) {
                                        if (i != 0) {
                                            putc(',', f);
                                        }
                                        if (converters[i]) {
                                            const Value* v = &chunkIterators[i]->getItem();
                                            (*converters[i])(&v, &strValue, NULL);
                                            fprintf(f, "\"%s\"",  strValue.getString());
                                        } else {
                                            fprintf(f, "%s",  ValueToString(types[i], chunkIterators[i]->getItem(), storeFormat).c_str());
                                        }
                                    }
                                }
                                n += 1;
                                firstItem = false;
                                putc(')', f);
                             nextItem:
                                for (i = 0; i < iteratorsCount; i++) {
                                    ++(*chunkIterators[i]);
                                }
                            }
                        }
                        for (i = 0; i < iteratorsCount; i++) {
                            ++(*arrayIterators[i]);
                        }
                        if (multisplit) {
                            for (i = 0; i < nDimensions; i++) {
                                coord[i] = dims[i].getEndMax() + 1;
                            }
                        }
                    }
                    if (startOfArray) {
                        for (i = 0; i < nDimensions; i++) {
                            fputc('[', f);
                        }
                        startOfArray = false;
                    }
                    for (i = 0; i < nDimensions; i++) {
                        fputc(']', f);
                    }
                }
                fputc('\n', f);
            }
        }
        if (f != stdout && f != stderr) {
            fclose(f);
        }
        return n;
    }

    static void s_fprintValue(FILE *f, const Value* v, TypeId const& valueType, FunctionPointer const converter)
    {
        if ( converter )
        {
            Value strValue;
            (*converter)(&v, &strValue, NULL);
            fprintf(f, "\"%s\"", strValue.getString());
        }
        else
        {
            fprintf(f, "%s", ValueToString(valueType, *v).c_str());
        }
    }

    static void s_fprintCoordinate(FILE *f,
                                   Coordinate ordinalCoord,
                                   DimensionDesc const& dimension,
                                   Value const& origCoord, 
                                   FunctionPointer const dimConverter)
    {
        if (dimension.isInteger())
        {
            fprintf(f, "%"PRIi64, ordinalCoord);
        }
        else
        {
            s_fprintValue(f, &origCoord, dimension.getType(), dimConverter);
        }
    }

    static void s_fprintCoordinates(FILE *f,
                                    Coordinates const& coords,
                                    Dimensions const& dims,
                                    vector<Value> const& origCoords, 
                                    vector <FunctionPointer> const& dimConverters)
    {
        putc('{', f);
        for (size_t i = 0; i < dims.size(); i++)
        {
            if (i != 0)
            {
                putc(',', f);
            }
            s_fprintCoordinate(f, coords[i], dims[i], origCoords[i], dimConverters[i]);
        }
        putc('}', f);
    }


    uint64_t DBLoader::saveWithLabels(Array const& array,
                                      std::string const& file,
                                      std::string const& format,
                                      bool append)
    {
        ArrayDesc const& desc = array.getArrayDesc();
        size_t i;
        uint64_t n = 0;

        FILE* f = file == "console" || file == "stdout" ? stdout
            : file == "stderr" ? stderr : fopen(file.c_str(), append ? "a" : "w");

        if (NULL == f )
        {
            int error = errno;
            LOG4CXX_DEBUG(logger, "Attempted to open output file '" << file << "' and failed with errno = " << error);
            if (!f)
                throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_CANT_OPEN_FILE) << file << error;
        }

        Attributes const& attrs = desc.getAttributes();
        size_t nAttributes = attrs.size();
        if (nAttributes != 0)
        {
            Dimensions const& dims = desc.getDimensions();
            const size_t nDimensions = dims.size();
            assert(nDimensions > 0);
            vector< boost::shared_ptr<ConstArrayIterator> > arrayIterators(nAttributes);
            vector< boost::shared_ptr<ConstChunkIterator> > chunkIterators(nAttributes);
            vector< TypeId> attTypes(nAttributes);
            vector< FunctionPointer> attConverters(nAttributes);
            vector< FunctionPointer> dimConverters(nDimensions);
            vector<Value> origPos;

            int iterationMode = ConstChunkIterator::IGNORE_OVERLAPS | ConstChunkIterator::IGNORE_EMPTY_CELLS;

            for (i = 0; i < nAttributes; i++)
            {
                arrayIterators[i] = array.getConstIterator((AttributeID)i);
                attTypes[i] = attrs[i].getType();
                if (! isBuiltinType(attTypes[i]))
                {
                    TypeLibrary::getType(attTypes[i]); // force loading of type
                    attConverters[i] =  FunctionLibrary::getInstance()->findConverter(attTypes[i],  TID_STRING, false);
                }
            }

            for (i = 0; i < nDimensions; i++)
            {
                if (!dims[i].isInteger())
                {
                    TypeId dt = dims[i].getType();
                    if (! isBuiltinType(  dt ))
                    {
                        TypeLibrary::getType(dt); // force loading of type
                        dimConverters[i] = FunctionLibrary::getInstance()->findConverter(dt, TID_STRING, false);
                    }
                }
            }

            if (format == "lcsv+")
            {
                for (i = 0; i < nDimensions; i++)
                {
                    if (i != 0)
                    {
                        fputc(',', f);
                    }
                    fprintf(f, "%s", dims[i].getBaseName().c_str());
                }
                for (i = 0; i < nAttributes; i++)
                {
                    fputc(',', f);
                    fprintf(f, "%s", attrs[i].getName().c_str());
                }

                fputc('\n', f);

                Coordinates posCoords(1);

                while (!arrayIterators[0]->end())
                {
                    // Get iterators for the current chunk
                    for (i = 0; i < nAttributes; i++)
                    {
                        ConstChunk const& chunk = arrayIterators[i]->getChunk();
                        chunkIterators[i] = chunk.getConstIterator(iterationMode);
                        if (chunk.isRLE()) { 
                            chunkIterators[i] = shared_ptr<ConstChunkIterator>(new CompatibilityIterator(chunkIterators[i]));
                        }
                    }
                    while (!chunkIterators[0]->end())
                    {
                        Coordinates const& pos = chunkIterators[0]->getPosition();
                        array.getOriginalPosition(origPos, pos);
                        for (i = 0; i < nDimensions; i++)
                        {
                            if (i != 0)
                            {
                                fputc(',', f);
                            }
                            s_fprintCoordinate(f, pos[i], dims[i], origPos[i], dimConverters[i]);
                        }

                        for (i = 0; i < nAttributes; i++)
                        {
                            fputc(',', f);
                            s_fprintValue(f, &chunkIterators[i]->getItem(), attTypes[i], attConverters[i]);
                            ++(*chunkIterators[i]);
                        }
                        fputc('\n', f);
                    }
                    for (i = 0; i < nAttributes; i++)
                    {
                        ++(* arrayIterators[i]);
                    }
                }
            }
            else //format is "lsparse"
            {
                Coordinates coord(nDimensions);

                bool startOfArray = true;
                if (nAttributes == 1)
                {
                    iterationMode |= ConstChunkIterator::IGNORE_DEFAULT_VALUES;
                }

                // Set initial position
                Coordinates chunkPos(nDimensions);
                for (i = 0; i < nDimensions; i++)
                {
                    coord[i] = dims[i].getStart();
                    chunkPos[i] = dims[i].getStart();
                }

                // Check if chunking is performed in more than one dimension
                bool multisplit = false;
                for (i = 1; i < nDimensions; i++)
                {
                    if (dims[i].getChunkInterval() < dims[i].getLength())
                    {
                        multisplit = true;
                    }
                }

                coord[nDimensions-1] -= 1; // to simplify increment
                chunkPos[nDimensions-1] -= dims[nDimensions-1].getChunkInterval();
                {
                    // Iterate over all chunks
                    bool firstItem = true;
                    while (!arrayIterators[0]->end())
                    {
                        // Get iterators for the current chunk
                        for (i = 0; i < nAttributes; i++)
                        {
                            ConstChunk const& chunk = arrayIterators[i]->getChunk();
                            chunkIterators[i] = chunk.getConstIterator(iterationMode);
                        }

                        int j = nDimensions;
                        while (--j >= 0 && (chunkPos[j] += dims[j].getChunkInterval()) > dims[j].getEndMax())
                        {
                            chunkPos[j] = dims[j].getStart();
                        }
                        bool gap = true;
                        chunkPos = arrayIterators[0]->getPosition();
                        if ( !chunkIterators[0]->end())
                        {
                            if (!multisplit)
                            {
                                Coordinates const& last = chunkIterators[0]->getLastPosition();
                                for (i = 1; i < nDimensions; i++)
                                {
                                    if (last[i] < dims[i].getEndMax())
                                    {
                                        multisplit = true;
                                    }
                                }
                            }
                            if (chunkIterators[0]->getChunk().isSparse())
                            {
                                if (!firstItem)
                                {
                                    firstItem = true;
                                    for (i = 0; i < nDimensions; i++)
                                    {
                                        putc(']', f);
                                    }
                                    fprintf(f, ";\n");
                                    for (i = 0; i < nDimensions; i++)
                                    {
                                        putc('[', f);
                                    }
                                }
                            }

                            // Iterator over all chunk elements
                            while (!chunkIterators[0]->end())
                            {
                                if (!chunkIterators[0]->getChunk().isSparse())
                                {
                                    Coordinates const& pos = chunkIterators[0]->getPosition();
                                    array.getOriginalPosition(origPos, pos);
                                    int nbr = 0;
                                    for (i = nDimensions-1; pos[i] != ++coord[i]; i--)
                                    {
                                        if (!firstItem)
                                        {
                                            putc(']', f);
                                            nbr += 1;
                                        }
                                        if (multisplit)
                                        {
                                            coord[i] = pos[i];
                                            if (i == 0)
                                            {
                                                break;
                                            }
                                        }
                                        else
                                        {
                                            if (i == 0)
                                            {
                                                break;
                                            }
                                            else
                                            {
                                                coord[i] = dims[i].getStart();
                                                coord[i] = pos[i];
                                                if (i == 0)
                                                {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    if (!firstItem)
                                    {
                                        putc(nbr == (int)nDimensions ? ';' : ',', f);
                                    }
                                    if (gap)
                                    {
                                        s_fprintCoordinates(f, pos, dims, origPos, dimConverters);
                                        for (i = 0; i < nDimensions; i++)
                                        {
                                            coord[i]=pos[i];
                                        }
                                        gap = false;
                                    }
                                    if (startOfArray)
                                    {
                                        for (i = 0; i < nDimensions; i++)
                                        {
                                            fputc('[', f);
                                        }
                                        startOfArray = false;
                                    }
                                    while (--nbr >= 0)
                                    {
                                        putc('[', f);
                                    }
                                    s_fprintCoordinates(f, pos, dims, origPos, dimConverters);
                                }
                                else
                                {
                                    if (!firstItem)
                                    {
                                        putc(',', f);
                                    }
                                    if (startOfArray)
                                    {
                                        for (i = 0; i < nDimensions; i++)
                                        {
                                            fputc('[', f);
                                        }
                                        startOfArray = false;
                                    }
                                    array.getOriginalPosition(origPos, chunkIterators[0]->getPosition());
                                    s_fprintCoordinates(f, chunkIterators[0]->getPosition(), dims, origPos, dimConverters);
                                }
                                putc('(', f);
                                if (!chunkIterators[0]->isEmpty())
                                {
                                    for (i = 0; i < nAttributes; i++)
                                    {
                                        if (i != 0)
                                        {
                                            putc(',', f);
                                        }

                                        s_fprintValue(f, &chunkIterators[i]->getItem(), attTypes[i], attConverters[i]);
                                    }
                                }
                                n += 1;
                                firstItem = false;
                                putc(')', f);
                                for (i = 0; i < nAttributes; i++)
                                {
                                    ++(*chunkIterators[i]);
                                }
                            }
                        }
                        for (i = 0; i < nAttributes; i++)
                        {
                            ++(*arrayIterators[i]);
                        }
                        if (multisplit)
                        {
                            for (i = 0; i < nDimensions; i++)
                            {
                                coord[i] = dims[i].getEndMax() + 1;
                            }
                        }
                    }
                    if (startOfArray)
                    {
                        for (i = 0; i < nDimensions; i++)
                        {
                            fputc('[', f);
                        }
                        startOfArray = false;
                    }
                    for (i = 0; i < nDimensions; i++)
                    {
                        fputc(']', f);
                    }
                }
                fputc('\n', f);
            }
        }

        if (f != stdout && f != stderr)
        {
            fclose(f);
        }
        return n;
    }

}
