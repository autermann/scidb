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
 * InputArray.h
 *
 *  Created on: Sep 23, 2010
 */
#ifndef INPUT_ARRAY_H
#define INPUT_ARRAY_H

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

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/MemArray.h"

#include "log4cxx/logger.h"
#include "log4cxx/basicconfigurator.h"
#include "log4cxx/helpers/exception.h"

namespace scidb
{
    using namespace std;

        // declared static to prevent visibility of variable outside of this file
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.ops.inputarray"));

    class InputArray;
    class InputArrayIterator;

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
        TKN_MULTIPLY,
        TKN_EOF
    };

    class Scanner
    {
        FILE* f;
        string value;
                string filePath;
        int missingReason;
        int lineNo;
        int columnNo;

      public:
        bool isNull() const
        {
            return missingReason >= 0;
        }

        int getMissingReason() const
        {
            return missingReason;
        }

        string const& getValue()
        {
            return value;
        }

        int getLine() const
        {
            return lineNo;
        }

        int getColumn() const
        {
            return columnNo;
        }

        Scanner(string const& file)
        {
            LOG4CXX_DEBUG(logger, "Attempting to open file '" << file << "' for input");
                        filePath = file;
            missingReason = -1;
            lineNo = 1;
            columnNo = 0;
            f = fopen(file.c_str(), "r");
            if (NULL == f ) {
                LOG4CXX_DEBUG(logger,"Attempt to open input file '" << file << "' and failed with errno = " << errno);
                if (!f)
                    throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CANT_OPEN_FILE) << file;
            }
        }

        ~Scanner()
        {
            fclose(f);
        }

        long getFilePos() { return f ? ftell(f) : -1; }

                string& getFilePath() { return filePath; }

        int getChar() {
            int ch = getc(f);
            if (ch == '\n') {
                lineNo += 1;
                columnNo = 0;
            } else {
                columnNo += 1;
            }
            return ch;
        }

        void ungetChar(int ch) {
            if (ch != EOF) {
                if (ch == '\n') {
                    lineNo -= 1;
                } else {
                    columnNo -= 1;
                }
                ungetc(ch, f);
            }
        }


        Token get()
        {
            int ch;

            while ((ch = getChar()) != EOF && isspace(ch)); // ignore whitespaces

            if (ch == EOF) {
                return TKN_EOF;
            }

            switch (ch) {
              case '\'':
                ch = getChar();
                if (ch == '\\') {
                    ch = getChar();
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
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR12);
                value = (char)ch;
                ch = getChar();
                if (ch != '\'')
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR12);
                missingReason = -1;
                return TKN_LITERAL;
              case '"':
                value.clear();
                while (true) {
                    ch = getChar();
                    if (ch == '\\') {
                        ch = getChar();
                    }
                    if (ch == EOF)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR13);
                    if (ch == '\"') {
                        missingReason = -1;
                        return TKN_LITERAL;
                    }
                    value += (char)ch;
                }
                break;
              case '{':
                return TKN_COORD_BEGIN;
              case '}':
                return TKN_COORD_END;
              case '*':
                return TKN_MULTIPLY;
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
              case '?':
                value.clear();
                missingReason = 0;
                while ((ch = getChar()) >= '0' && ch <= '9') {
                    missingReason = missingReason*10 + ch - '0';
                }
                ungetChar(ch);
                return TKN_LITERAL;
              default:
                value.clear();
                while (ch != EOF && !isspace(ch) && ch != ')' && ch != ']'  && ch != '}' && ch != '(' && ch != '{' && ch != '[' && ch != ',' && ch != '*') {
                    value += (char)ch;
                    ch = getChar();
                }
                if (!value.size())
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR14);
                ungetChar(ch);
                missingReason = value == "null" ? 0 : -1;
                return TKN_LITERAL;
            }
        }
    };

    class InputArrayIterator : public ConstArrayIterator
    {
        friend class InputArray;
      public:
        virtual ConstChunk const& getChunk();
        virtual bool end();
        virtual void operator ++();
        virtual Coordinates const& getPosition();
        InputArrayIterator(InputArray& array, AttributeID id);

      private:
        InputArray& array;
        AttributeID attr;
        bool hasCurrent;
        size_t currChunkIndex;
    };

    const size_t LOOK_AHEAD = 2;

    class InputArray : public Array
    {
       friend class InputArrayIterator;
      public:
        bool supportsRandomAccess() const;
        virtual ArrayDesc const& getArrayDesc() const;
        virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attr) const;
        InputArray(ArrayDesc const& desc, string const& filePath, boost::shared_ptr<Query>& query);

        bool moveNext(size_t chunkIndex);
        ConstChunk const& getChunk(AttributeID attr, size_t chunkIndex);

      private:
        struct LookAheadChunks {
            MemChunk chunks[LOOK_AHEAD];
        };

        ArrayDesc desc;
        Scanner scanner;
        Coordinates chunkPos;
        Coordinates lastChunkPos;
        size_t currChunkIndex;
        vector< boost::shared_ptr<InputArrayIterator> > iterators;
        vector<LookAheadChunks> lookahead;
        vector< TypeId> types;
        vector< Value> attrVal;
        vector< FunctionPointer> converters;
        Value coordVal;
        Value strVal;
        AttributeID emptyTagAttrID;

        enum State
        {
            Init,
            EndOfStream,
            EndOfChunk,
            InsideArray
        };
        State state;
        MemChunk tmpChunk;
        boost::weak_ptr<Query> _query;
    };

} //namespace scidb

#endif /* INPUT_ARRAY_H */
