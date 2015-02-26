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

#include "util/FileIO.h"

#include <stdio.h>
#include <ctype.h>
#include <inttypes.h>
#include <limits.h>
#include <float.h>
#include <string>
#include <errno.h>

#include "boost/format.hpp"

#include "log4cxx/logger.h"
#include "log4cxx/basicconfigurator.h"
#include "log4cxx/helpers/exception.h"
#include <boost/archive/text_oarchive.hpp>

#include "system/Exceptions.h"
#include "query/TypeSystem.h"
#include "query/FunctionDescription.h"
#include "query/FunctionLibrary.h"
#include "query/Operator.h"
#include "smgr/io/DBLoader.h"
#include "array/DBArray.h"
#include "smgr/io/Storage.h"
#include "system/SystemCatalog.h"
#include "smgr/io/TemplateParser.h"

namespace scidb
{
    using namespace std;
    using namespace boost;
    using namespace boost::archive;

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
        CompatibilityIterator(shared_ptr<ConstChunkIterator> iterator, bool isSparse) 
        : inputIterator(iterator),
          firstPos(iterator->getFirstPosition()),
          lastPos(iterator->getLastPosition()),
          defaultValue(iterator->getChunk().getAttributeDesc().getDefaultValue()),
          mode(iterator->getMode()),
          isEmptyable(iterator->getChunk().getArrayDesc().getEmptyBitmapAttribute() != NULL)
        {
            if (isSparse) {
                mode |= ConstChunkIterator::IGNORE_EMPTY_CELLS;
            }
            mode &= ~ConstChunkIterator::IGNORE_DEFAULT_VALUES;
            reset();
        }

        bool skipDefaultValue() {
            return false;
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

    int DBLoader::defaultPrecision = 6;


    static void s_fprintValue(FILE *f, const Value* v, TypeId const& valueType, FunctionPointer const converter, int precision = 6)
    {
        if ( converter )
        {
            Value strValue;
            (*converter)(&v, &strValue, NULL);
            fprintf(f, "\"%s\"", strValue.getString());
        }
        else
        {
            fprintf(f, "%s", ValueToString(valueType, *v, precision).c_str());
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



    static uint64_t saveTextFormat(Array const& array,
                                   ArrayDesc const& desc,
                                   FILE* f,
                                   std::string const& format)
    {
        size_t i, j;
        uint64_t n = 0;        
        int precision = DBLoader::defaultPrecision;
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

            if (compareStringsIgnoreCase(format, "csv") == 0 || compareStringsIgnoreCase(format, "csv+") == 0) {
                bool withCoordinates = compareStringsIgnoreCase(format, "csv+") == 0;
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
                            Value strValue;
                            if (converters[i]) {
                                const Value* v = &chunkIterators[i]->getItem();
                                if (!v->isNull())
                                {
                                    (*converters[i])(&v, &strValue, NULL);
                                    fprintf(f, "\"%s\"",  strValue.getString());
                                }
                            } else {
                                Value const& val = chunkIterators[i]->getItem();
                                if (!val.isNull()) {
                                    fprintf(f, "%s",  ValueToString(types[i], val, precision).c_str());
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
                bool sparseFormat = compareStringsIgnoreCase(format, "sparse") == 0;
                bool denseFormat = compareStringsIgnoreCase(format, "dense") == 0;
                bool storeFormat = compareStringsIgnoreCase(format, "store") == 0;
                bool autoFormat = compareStringsIgnoreCase(format, "text") == 0;
                        
                bool startOfArray = true;
                if (sparseFormat) {
                    iterationMode |= ConstChunkIterator::IGNORE_EMPTY_CELLS;
                }
                if (storeFormat) {
                    if (precision < DBL_DIG) { 
                        precision = DBL_DIG;
                    }
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
                        bool isSparse = false;
                        for (i = 0; i < iteratorsCount; i++) {
                            ConstChunk const& chunk = arrayIterators[i]->getChunk();
                            chunkIterators[i] = chunk.getConstIterator(iterationMode);
                            if (i == 0) { 
                                isSparse = !denseFormat && ((!autoFormat && chunk.isSparse()) || (autoFormat && chunk.count()*100/chunk.getNumberOfElements(false) <= 10));
                            }
                            if (chunk.isRLE()) { 
                                chunkIterators[i] = shared_ptr<ConstChunkIterator>(new CompatibilityIterator(chunkIterators[i], isSparse));
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
                                        Value strValue;
                                        if (converters[i]) {
                                            const Value* v = &chunkIterators[i]->getItem();
                                            (*converters[i])(&v, &strValue, NULL);
                                            fprintf(f, "\"%s\"",  strValue.getString());
                                        } else {
                                            fprintf(f, "%s",  ValueToString(types[i], chunkIterators[i]->getItem(), precision).c_str());
                                        }
                                    }
                                }
                                n += 1;
                                firstItem = false;
                                putc(')', f);

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
        return n;
    }


    static uint64_t saveWithLabels(Array const& array,
                                   ArrayDesc const& desc,
                                   FILE* f,
                                   std::string const& format, 
                                   boost::shared_ptr<Query> const& query)
    {
        size_t i;
        uint64_t n = 0;

        Attributes const& attrs = desc.getAttributes();
        size_t nAttributes = attrs.size();
        
        if (desc.getEmptyBitmapAttribute())
        {
            assert(desc.getEmptyBitmapAttribute()->getId() == desc.getAttributes().size()-1);
            nAttributes--;
        }

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

            bool dcsv = compareStringsIgnoreCase(format, "dcsv") == 0;
            if (compareStringsIgnoreCase(format, "lcsv+") == 0 || dcsv)
            {
                if (dcsv)
                    fputc('{', f);

                for (i = 0; i < nDimensions; i++)
                {
                    if (i != 0)
                    {
                        fputc(',', f);
                    }
                    fprintf(f, "%s", dims[i].getBaseName().c_str());
                }
                if (dcsv)
                {
                    fputs("} ", f);
                }
                else
                {
                    fputc(',', f);
                }
                for (i = 0; i < nAttributes; i++)
                {
                    if (i != 0)
                    {
                        fputc(',', f);
                    }
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
                            chunkIterators[i] = shared_ptr<ConstChunkIterator>(new CompatibilityIterator(chunkIterators[i], chunk.isSparse()));
                        }
                    }
                    while (!chunkIterators[0]->end())
                    {
                        Coordinates const& pos = chunkIterators[0]->getPosition();
                        array.getOriginalPosition(origPos, pos, query);

                        if (dcsv)
                            fputc('{', f);
                        for (i = 0; i < nDimensions; i++)
                        {
                            if (i != 0)
                            {
                                fputc(',', f);
                            }
                            s_fprintCoordinate(f, pos[i], dims[i], origPos[i], dimConverters[i]);
                        }
                        if (dcsv)
                        {
                            fputs("} ", f);
                        }
                        else
                        {
                            fputc(',', f);
                        }
                        for (i = 0; i < nAttributes; i++)
                        {
                            if (i != 0)
                            {
                                fputc(',', f);
                            }
                            s_fprintValue(f, &chunkIterators[i]->getItem(), attTypes[i], attConverters[i], DBLoader::defaultPrecision);
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
                                    array.getOriginalPosition(origPos, pos, query);
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
                                    array.getOriginalPosition(origPos, chunkIterators[0]->getPosition(), query);
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

                                        s_fprintValue(f, &chunkIterators[i]->getItem(), attTypes[i], attConverters[i], DBLoader::defaultPrecision);
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
        return n;
    }

#ifndef SCIDB_CLIENT
    static uint64_t saveOpaque(Array const& array, ArrayDesc const& desc, FILE* f, boost::shared_ptr<Query> const& query)
    {
        size_t nAttrs = desc.getAttributes().size(); 
        vector< boost::shared_ptr<ConstArrayIterator> > arrayIterators(nAttrs);
        uint64_t n;
        OpaqueChunkHeader hdr;
        hdr.signature = OpaqueChunkHeader::calculateSignature(desc);
        hdr.magic = OPAQUE_CHUNK_MAGIC;
        
        hdr.flags = OpaqueChunkHeader::ARRAY_METADATA;
        stringstream ss;
        text_oarchive oa(ss);        
        oa & desc;
        string const& s = ss.str();
        hdr.size = s.size();
        if (fwrite(&hdr, sizeof(hdr), 1, f) != 1
            || fwrite(&s[0], 1, hdr.size, f) != hdr.size)
        { 
            throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_FILE_WRITE_ERROR) << errno;
        }
        
        for (size_t i = 0; i < nAttrs; i++) {
            arrayIterators[i] = array.getConstIterator(i);
        }
        for (n = 0; !arrayIterators[0]->end(); n++) {
            for (size_t i = 0; i < nAttrs; i++) {
                ConstChunk const* chunk = &arrayIterators[i]->getChunk();
                if (!chunk->isRLE()) { 
                    chunk = chunk->materialize();
                }
                Coordinates const& pos = chunk->getFirstPosition(false);
                PinBuffer scope(*chunk);
                hdr.size = chunk->getSize();
                hdr.attrId = i;
                hdr.compressionMethod = chunk->getCompressionMethod();
                hdr.flags = 0;                                
                if (chunk->isRLE()) { 
                    hdr.flags |= OpaqueChunkHeader::RLE_FORMAT;
                    if (!chunk->getAttributeDesc().isEmptyIndicator()) { 
                        // RLE chunks received from other nodes by SG contain empty bitmap. 
                        // There is no need to save this bitmap in each chunk - so just cut it.
                        ConstRLEPayload payload((char*)chunk->getData());
                        assert(hdr.size >= payload.packedSize());
                        hdr.size = payload.packedSize();
                    }
                }
                if (chunk->isSparse()) { 
                    hdr.flags |= OpaqueChunkHeader::SPARSE_CHUNK;
                }
                hdr.nDims = pos.size();
                if (fwrite(&hdr, sizeof(hdr), 1, f) != 1
                    || fwrite(&pos[0], sizeof(Coordinate), hdr.nDims, f) != hdr.nDims
                    || fwrite(chunk->getData(), 1, hdr.size, f) != hdr.size) 
                { 
                    throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_FILE_WRITE_ERROR) << errno;
                }
            }
            for (size_t i = 0; i < nAttrs; i++) {
                ++(*arrayIterators[i]);
            }
        }
        Dimensions const& dims = desc.getDimensions();
        for (size_t i = 0; i < dims.size(); i++) { 
            if (dims[i].getType() != TID_INT64) {
                string indexName = dims[i].getMappingArrayName();
                if (!indexName.empty()) { 
                    boost::shared_ptr<Array> indexArray = query->getArray(indexName);
                    boost::shared_ptr<ConstArrayIterator> it = indexArray->getConstIterator(0);
                    ConstChunk const& chunk = it->getChunk();
                    Coordinates const& pos = chunk.getFirstPosition(false);
                    PinBuffer scope(chunk);
                    size_t chunkInterval = indexArray->getArrayDesc().getDimensions()[0].getChunkInterval();
                    hdr.size = chunkInterval == 0 ? 0 : chunk.getSize();
                    hdr.attrId = i;
                    hdr.compressionMethod = 0;
                    hdr.flags = OpaqueChunkHeader::COORDINATE_MAPPING;                
                    hdr.nDims = pos.size();
                    assert(hdr.nDims == 1);
                    if (fwrite(&hdr, sizeof(hdr), 1, f) != 1
                        || fwrite(&pos[0], sizeof(Coordinate), hdr.nDims, f) != hdr.nDims
                        || fwrite(&chunkInterval, sizeof(chunkInterval), 1, f) != 1
                        || (chunkInterval != 0 && fwrite(chunk.getData(), 1, hdr.size, f) != hdr.size)) 
                    { 
                        throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_FILE_WRITE_ERROR) << errno;
                    }
                }
            }
        }
        return n;
    }

    static uint64_t saveUsingTemplate(Array const& array, ArrayDesc const& desc, FILE* f, string const& format, boost::shared_ptr<Query> const& query)
    {
        ExchangeTemplate templ = TemplateParser::parse(desc, format, false);
        int nAttrs = templ.columns.size();
        vector< boost::shared_ptr<ConstArrayIterator> > arrayIterators(nAttrs);
        vector< boost::shared_ptr<ConstChunkIterator> > chunkIterators(nAttrs);
        vector< Value > cnvValues(nAttrs);
        vector<char> padBuffer;
        int firstAttr = -1;
        uint64_t n;
        size_t nMissingReasonOverflows = 0;

        for (int i = 0; i < nAttrs; i++) {
            if (!templ.columns[i].skip) { 
                if (firstAttr < 0) { 
                    firstAttr = (int)i;
                }
                arrayIterators[i] = array.getConstIterator(i);
                if (templ.columns[i].converter) { 
                    cnvValues[i] = Value(templ.columns[i].externalType);
                }
                if (templ.columns[i].fixedSize > padBuffer.size()) { 
                    padBuffer.resize(templ.columns[i].fixedSize);
                }
            }
        }
        if (firstAttr < 0) { 
            return 0;
        }
        for (n = 0; !arrayIterators[firstAttr]->end(); n++) {
            for (int i = firstAttr; i < nAttrs; i++) {
                if (!templ.columns[i].skip) { 
                    chunkIterators[i] = arrayIterators[i]->getChunk().getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS|ConstChunkIterator::IGNORE_EMPTY_CELLS);
                }
            }
            while (!chunkIterators[firstAttr]->end()) {                
                for (int i = firstAttr; i < nAttrs; i++) { 
                    ExchangeTemplate::Column const& column = templ.columns[i];
                    if (!column.skip) {
                        Value const* v = &chunkIterators[i]->getItem();
                        if (column.nullable) {
                            if (v->getMissingReason() > 127) {
                                LOG4CXX_WARN(logger, "Missing reason " << v->getMissingReason() << " can not be stored in binary file");
                                nMissingReasonOverflows += 1;
                            }
                            int8_t missingReason = (int8_t)v->getMissingReason();
                            if (fwrite(&missingReason, sizeof(missingReason), 1, f) != 1) { 
                                throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_FILE_WRITE_ERROR) << errno;
                            }
                        }
                        if (v->isNull()) { 
                            if (!column.nullable) {
                                throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_ASSIGNING_NULL_TO_NON_NULLABLE);
                            }
                            char filler = 0;
                            size_t size = column.fixedSize == 0 ? 4 : column.fixedSize; // for varying size type write 4-bytes counter
                            if (fwrite(&filler, 1, size, f) != size) {
                                throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_FILE_WRITE_ERROR) << errno;
                            }
                        } else { 
                            if (column.converter) { 
                                column.converter(&v, &cnvValues[i], NULL);
                                v = &cnvValues[i];
                            }
                            uint32_t size = (uint32_t)v->size();
                            if (column.fixedSize == 0) { // varying size type
                                if (fwrite(&size, sizeof(size), 1, f) != 1
                                    || fwrite(v->data(), 1, size, f) != size) 
                                { 
                                    throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_FILE_WRITE_ERROR) << errno;
                                }
                            } else { 
                                if (size > column.fixedSize) {  
                                    throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_TRUNCATION) << size << column.fixedSize;
                                }
                                if (fwrite(v->data(), 1, size, f) != size) 
                                { 
                                    throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_FILE_WRITE_ERROR) << errno;
                                }
                                if (size < column.fixedSize) { 
                                    size_t padSize = column.fixedSize - size;
                                    assert(padSize <= padBuffer.size());
                                    if (fwrite(&padBuffer[0], 1, padSize, f) != padSize) 
                                    {         
                                        throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_FILE_WRITE_ERROR) << errno;
                                    }      
                                }
                            }
                        }        
                        ++(*chunkIterators[i]);
                    }       
                }               
            }
            for (int i = firstAttr; i < nAttrs; i++) {
                if (!templ.columns[i].skip) { 
                    ++(*arrayIterators[i]);
                }
            }
        }
        if (nMissingReasonOverflows > 0) { 
            query->postWarning(SCIDB_WARNING(SCIDB_W_MISSING_REASON_OUT_OF_BOUNDS));
        }
        return n;
    }


    uint64_t DBLoader::save(string const& arrayName, string const& file,
                            const boost::shared_ptr<Query>& query,
                            string const& format, bool append)
    {
        ArrayDesc desc;
        SystemCatalog::getInstance()->getArrayDesc(arrayName, desc);
        return save(DBArray(desc.getId(),query), file, query, format, append);
    }
#else

    uint64_t DBLoader::save(string const& arrayName, string const& file,
                            const boost::shared_ptr<Query>& query,
                            string const& format, bool append)
    {
        return 0;
    }
#endif

    uint64_t DBLoader::save(Array const& array, string const& file,
                            const boost::shared_ptr<Query>& query,
                            string const& format, bool append)
    {
        ArrayDesc const& desc = array.getArrayDesc();
        uint64_t n = 0;

        FILE* f;
        bool isBinary = compareStringsIgnoreCase(format, "opaque") == 0 || format[0] == '(';
        if (file == "console" || file == "stdout") { 
            f = stdout;
        } else if (file == "stderr") { 
            f = stderr;
        } else {
            f = fopen(file.c_str(), isBinary ? append ? "ab" : "wb" : append ? "a" : "w");
            if (NULL == f) {
                int error = errno;
                LOG4CXX_DEBUG(logger, "Attempted to open output file '" << file << "' and failed with errno = " << error);
                if (!f)
                    throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_CANT_OPEN_FILE) << file << error;
            }
            struct flock flc;
            flc.l_type = F_WRLCK;
            flc.l_whence = SEEK_SET;
            flc.l_start = 0;
            flc.l_len = 1;
            
            int rc = fcntl(fileno(f), F_SETLK, &flc);
            if (rc != 0 && errno == EACCES) { 
                throw USER_EXCEPTION(SCIDB_SE_DBLOADER, SCIDB_LE_CANT_LOCK_FILE) << file;
            }
        }
        if (compareStringsIgnoreCase(format, "lcsv+") == 0 || compareStringsIgnoreCase(format, "lsparse") == 0
            || compareStringsIgnoreCase(format, "dcsv") == 0)
        {
            n = saveWithLabels(array, desc, f, format, query);
        } 
#ifndef SCIDB_CLIENT
        else if (compareStringsIgnoreCase(format, "opaque") == 0) { 
            n = saveOpaque(array, desc, f, query);
        }
        else if (format[0] == '(') 
        { 
            n = saveUsingTemplate(array, desc, f, format, query);
        } 
#endif
        else 
        {
            n = saveTextFormat(array, desc, f, format);
        }
        if (f != stdout && f != stderr) {
            fclose(f);
        }
        return n;
    }
}
