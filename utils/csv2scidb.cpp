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

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <limits.h>
#include "system/Constants.h"

void
putdelim ( char type, FILE * out ) 
{
    switch ( type ) 
    {
      case 'N':
      case 's':
        break;
      case 'S':
        putc('"',out);
        break;
      case 'C':
        putc('\'',out);
        break;
      default: 
        perror ("invalid type");
        break;
    }
}

int main(int argc, char* argv[]) 
{ 
    int i, j, ch, delim = ',';
    long count = 0;
    FILE* in = stdin;
    FILE* out = stdout;
    long skip = 0;
    long nChunkLen = 0, outCnt = 0;
    int valSize = 0;
    char chTypeString[128];
    int numAttrs = 0, attrCnt = 0;
    long startChunk = 0;
    long chunknum = 0;

    memset(chTypeString, 'N', sizeof(chTypeString));
    numAttrs = sizeof(chTypeString);

    for (i = 1; i < argc; i++) { 

        if (i > (argc - 1)) { 
            perror("invalid command line args");
            return 1;
        }
        if (strcmp(argv[i], "-i") == 0) { 
            in = fopen(argv[++i], "r");
            if (in == NULL) { 
                perror("open input file");
                return 1;
            }            
        } else if (strcmp(argv[i], "-o") == 0) { 
            in = fopen(argv[++i], "w");
            if (in == NULL) { 
                perror("open output file");
                return 1;
            }            
        } else if (strcmp(argv[i], "-a") == 0) { 
            in = fopen(argv[++i], "a");
            if (in == NULL) { 
                perror("open output file");
                return 1;
            }            
        } else if (strcmp(argv[i], "-s") == 0) { 
            skip = atol(argv[++i]);
        } else if (strcmp(argv[i], "-c") == 0) { 
            nChunkLen = atol(argv[++i]);
            if ( 0 >= nChunkLen ) { 
                perror("chunk length > 0");
                return 1;
            }
        } else if (strcmp(argv[i], "-f") == 0) { 
            startChunk = atol(argv[++i]);
            if ( 0 > startChunk ) { 
                perror("start chunk must be > 0");
                return 1;
            }
        } else if (strcmp(argv[i], "-d") == 0) { 
            if ( 0 == ( delim = atoi(argv[++i]))) { 
                perror("delim required");
                return 1;
            }
        } else if (strcmp(argv[i], "-p") == 0) { 
            strncpy(chTypeString,argv[++i],128);

            for (j = 0, numAttrs = strlen(chTypeString);j < numAttrs;j++) { 
                switch(chTypeString[j]) { 
                  case 'N':
                  case 'S':
                  case 's':
                  case 'C':
                      break;
                  default:
                    perror("type string N, S or C");
                    return 1;
                }
            }

        } else if (strcmp(argv[i], "-v") == 0) {
            printf("csv to scidb tool version %s, build type is %s\n", scidb::SCIDB_VERSION(), scidb::SCIDB_BUILD_TYPE());
            return 0;
        } else {
            fprintf(stderr, "Utility for conversion of CSV file to SciDB input text format\n"
                    "\tUsage: csv2scidb [options] [ < input-file ] [ > output-file ]\n"
                    "\tOptions:\n"
                    "\t\t-v version of tool\n"
                    "\t\t-i PATH\tinput file\n"
                    "\t\t-o PATH\toutput file\n"
                    "\t\t-a PATH\tappended output file\n"
                    "\t\t-c INT\tlength of chunk\n"
                    "\t\t-f INT\tstarting chunk number\n"
                    "\t\t-d char\tdelimieter - default ,\n"
                    "\t\t-p STR\ttype pattern - N number, S string, s nullable-string, C char\n"
                    "\t\t-s N\tskip N lines at the beginning of the file\n");
            return 2;
        }
    }
    while (skip != 0 && (ch = getc(in)) != EOF) {  
        if (ch == '\n') { 
            skip -= 1;
        }
    }

    outCnt=0;
    chunknum = startChunk;
    while ((ch = getc(in)) != EOF) { 
        bool firstCh = true;

        attrCnt = 0;
        valSize = 0;
        do { 

            if (iscntrl(ch))
                continue;

            if (firstCh)
            {
                if (outCnt) {
                    fprintf( out, ",\n");
                } else {
                    fprintf(out, "{%ld}", chunknum);
                    chunknum = chunknum + nChunkLen;
                    fprintf( out, "[\n");
                }

                putc('(', out);
                putdelim(chTypeString[attrCnt],out);
                firstCh = false;
            }

            if ( delim == ch ) {
                if (valSize == 0 && (chTypeString[attrCnt] == 'N' || chTypeString[attrCnt] =='s'))
                {
                    fputs("null", out);
                }
                else if (valSize > 0 && chTypeString[attrCnt] =='s')
                {
                    putc('"',out);
                }

                valSize = 0;
                putdelim(chTypeString[attrCnt],out);
                putc(',',out);
                if (++attrCnt > numAttrs) {
                    perror("too many attributes in csv file");
                    return 1;
                }
                putdelim(chTypeString[attrCnt],out);
            } else {
                if(valSize == 0 && chTypeString[attrCnt]=='s')
                {
                    putc('"',out);
                }
                valSize += 1;
                putc(ch, out);
            }

        } while ((ch = getc(in)) != EOF && ch != '\n');

        if (attrCnt>0 || (numAttrs == 1 && !firstCh))
        {
            if (attrCnt < numAttrs)
            {
                if (valSize == 0 && (chTypeString[attrCnt] == 'N'  || chTypeString[attrCnt] =='s'))
                {
                    fputs("null", out);
                }
                else if (valSize > 0 && chTypeString[attrCnt] =='s')
                {
                    putc('"',out);
                }

                putdelim(chTypeString[attrCnt],out);
            }
            putc(')', out);
            count++;
        }

        if (!firstCh && ++outCnt >= nChunkLen && nChunkLen)
        { 
            (void) fprintf ( out, "\n];\n");
            outCnt=0;
        }
    }
    if (outCnt) { 
        putc('\n', out);
        putc(']', out);
        putc('\n', out);
    }
    fprintf(stderr, "Processed %ld records\n", count);
    fprintf(stderr, "Next chunk prefix %ld\n", chunknum);
    return 0;
}
