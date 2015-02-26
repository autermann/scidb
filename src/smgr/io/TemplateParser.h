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
 * TemplateParser.h
 *
 *  Created on: Sep 23, 2010
 */
#ifndef TEMPLATE_PARSER_H
#define TEMPLATE_PARSER_H

#include <query/TypeSystem.h>
#include <query/FunctionLibrary.h>

#include <string>
#include <vector>
#include <ctype.h>

namespace scidb
{
    using namespace std;

    class TemplateScanner
    {        
        string format;
        string ident;
        size_t pos;
        int    num;

      public:
        enum Token { 
            TKN_EOF,
            TKN_IDENT,
            TKN_LPAR,
            TKN_RPAR,
            TKN_COMMA,
            TKN_NUMBER
        };
        
        string const& getIdent() const
        {
            return ident;
        }

        int getNumber() const { 
            return num;
        }

        size_t getPosition() const
        {
            return pos;
        }
        
        TemplateScanner(string const& fmt) : format(fmt), pos(0) {}

        Token get()
        {
            int ch = 0;

            while (pos < format.size() && isspace(ch = format[pos])) { 
                pos += 1; // ignore whitespaces
            }
            if (pos == format.size()) {
                return TKN_EOF;
            }

            switch (ch) {
              case '(':
                pos += 1;
                return TKN_LPAR;
              case ')':
                pos += 1;
                return TKN_RPAR;
              case ',':
                pos += 1;
                return TKN_COMMA;
              default:
                if (isdigit(ch)) { 
                    num = 0;
                    do { 
                        pos += 1;
                        num = num*10 + ch - '0';
                    } while (pos < format.size() && isdigit(ch = format[pos]));
                    return TKN_NUMBER;
                } else if (isalpha(ch)) { 
                    ident.clear();
                    do {
                        pos += 1;
                        ident += (char)ch;
                    } while (pos < format.size() && (isalnum(ch = format[pos]) || ch == '_'));
                    return TKN_IDENT;
                } else { 
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TEMPLATE_PARSE_ERROR) << pos;
                }
            }
        }
    };

    struct ExchangeTemplate
    {
        struct Column { 
            bool skip;
            bool nullable;
            Type internalType;
            Type externalType;
            size_t fixedSize;
            FunctionPointer converter;
        };
        vector<Column> columns;
        bool opaque;
    };
    
    class TemplateParser
    {
      public:
        static ExchangeTemplate parse(ArrayDesc const& desc, string const& format, bool isImport);
    };

    const uint32_t OPAQUE_CHUNK_MAGIC = 0xAECAC;

    struct OpaqueChunkHeader
    {
        uint32_t magic;
        uint32_t size;
        uint32_t signature;
        uint8_t  attrId;
        int8_t   compressionMethod;
        uint8_t  flags;
        uint8_t  nDims;
        
        static uint32_t calculateSignature(ArrayDesc const& desc);


        enum Flags { 
            SPARSE_CHUNK = 1,
            RLE_FORMAT = 2,
            COORDINATE_MAPPING = 4,
            ARRAY_METADATA = 8
        };
    };
}
    
#endif

