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
#include <string>

using namespace std;

inline void
putdelim(char type, FILE * out) {
    switch (type) {
        case 'N':
        case 's':
            break;
        case 'S':
            putc('"', out);
            break;
        case 'C':
            putc('\'', out);
            break;
        default:
            fprintf(stderr, "invalid type provided in type string\n");
            break;
    }
}

inline char
getFormatCh(char const* chTypeString, int const attrCnt, int const nAttrs) {
    if (attrCnt < nAttrs) {
        return chTypeString[attrCnt];
    }
    return 'N';
}

int main(int argc, char* argv[]) {
    int i, j, ch;
    char delim = ',';
    long count = 0;
    FILE* in = stdin;
    FILE* out = stdout;
    long skip = 0;
    long nChunkLen = 500000, outCnt = 0;
    int valSize = 0;
    int numAttrs = 0, attrCnt = 0;
    long startChunk = 0;
    long chunknum = 0;
    long instances = 1;
    bool quoteLine = false;
    char *chTypeString = NULL;

    for (i = 1; i < argc; i++) {
        if (i > (argc - 1)) {
            fprintf(stderr, "invalid command line args\n");
            return EXIT_FAILURE;
        }

        if (strcmp(argv[i], "-i") == 0) {
            in = fopen(argv[++i], "r");
            if (in == NULL) {
                perror("open input file\n");
                return EXIT_FAILURE;
            }
        } else if (strcmp(argv[i], "-o") == 0) {
            out = fopen(argv[++i], "w");
            if (out == NULL) {
                perror("open output file\n");
                return EXIT_FAILURE;
            }
        } else if (strcmp(argv[i], "-a") == 0) {
            out = fopen(argv[++i], "a");
            if (out == NULL) {
                perror("open output file\n");
                return EXIT_FAILURE;
            }
        } else if (strcmp(argv[i], "-s") == 0) {
            skip = atol(argv[++i]);
        } else if (strcmp(argv[i], "-c") == 0) {
            nChunkLen = atol(argv[++i]);
            if (0 >= nChunkLen) {
                fprintf(stderr, "chunk size must be > 0\n");
                return EXIT_FAILURE;
            }
        } else if (strcmp(argv[i], "-f") == 0) {
            startChunk = atol(argv[++i]);
            if (0 > startChunk) {
                fprintf(stderr, "starting coordinate must be >= 0\n");
                return EXIT_FAILURE;
            }
        } else if (strcmp(argv[i], "-n") == 0) {
            instances = atol(argv[++i]);
            if (0 >= instances) {
                fprintf(stderr, "instances must be > 0\n");
                return EXIT_FAILURE;
            }
        } else if (strcmp(argv[i], "-d") == 0) {
            i++;
            if ((('"' == argv[i][0]) || ('\'' == argv[i][0])) && (3 == strlen(argv[i]))) {
                delim = argv[i][1];
            } else if (strcmp(argv[i], "\\t") == 0) {
                delim = '\t';
            } else if (0 == (delim = argv[i][0])) {
                fprintf(stderr, "delim required\n");
                return EXIT_FAILURE;
            }
        } else if (strcmp(argv[i], "-p") == 0) {
            chTypeString = argv[++i];
            for (j = 0, numAttrs = strlen(chTypeString); j < numAttrs; j++) {
                switch (chTypeString[j]) {
                    case 'N':
                    case 'S':
                    case 's':
                    case 'C':
                        break;
                    default:
                        fprintf(stderr, "type string must contain only N, S, s, and C characters\n");
                        return EXIT_FAILURE;
                }
            }
        } else if (strcmp(argv[i], "-v") == 0) {
            printf("csv2scidb utility version %s, build type is %s\n", scidb::SCIDB_VERSION(), scidb::SCIDB_BUILD_TYPE());
            return EXIT_SUCCESS;
        } else if (strcmp(argv[i], "-q") == 0) {
            quoteLine = true;
        } else {
            fprintf(stderr, "Utility for conversion of CSV file to SciDB input text format\n"
                    "\tUsage: csv2scidb [options] [ < input-file ] [ > output-file ]\n"
                    "\tOptions:\n"
                    "\t\t-v version of utility\n"
                    "\t\t-i PATH\tinput file\n"
                    "\t\t-o PATH\toutput file\n"
                    "\t\t-a PATH\tappended output file\n"
                    "\t\t-c INT\tlength of chunk\n"
                    "\t\t-f INT\tstarting coordinate\n"
                    "\t\t-n INT\tnumber of instances\n"
                    "\t\t-d char\tdelimiter - default ,\n"
                    "\t\t-p STR\ttype pattern - N number, S string, s nullable-string, C char\n"
                    "\t\t-q Quote the input line exactly, simply wrap it in ()\n"
                    "\t\t-s N\tskip N lines at the beginning of the file\n");
            return 2;
        }
    }

    while (skip != 0 && (ch = getc(in)) != EOF) {
        if (ch == '\n') {
            skip -= 1;
        }
    }

    outCnt = 0;
    chunknum = startChunk;
    while ((ch = getc(in)) != EOF) {
        bool firstCh = true;
        attrCnt = 0;
        valSize = 0;
        do {
            if (iscntrl(ch) && ch != '\t')
                continue;

            if (firstCh) {
                if (outCnt) {
                    fprintf(out, ",\n");
                } else {
                    fprintf(out, "{%ld}", chunknum);
                    chunknum = chunknum + (nChunkLen * instances);
                    fprintf(out, "[\n");
                }

                putc('(', out);
                if (!quoteLine)
                    putdelim(getFormatCh(chTypeString, attrCnt, numAttrs), out);
                firstCh = false;
            }

            if (quoteLine) {
                putc(ch, out);
            } else if (delim == ch) {
                if (valSize == 0 && (getFormatCh(chTypeString, attrCnt, numAttrs) == 'N' || getFormatCh(chTypeString, attrCnt, numAttrs) == 's')) {
                    fputs("null", out);
                } else if (valSize > 0 && getFormatCh(chTypeString, attrCnt, numAttrs) == 's') {
                    putc('"', out);
                }

                valSize = 0;
                putdelim(getFormatCh(chTypeString, attrCnt, numAttrs), out);
                putc(',', out);
                attrCnt++;
                if (numAttrs > 0 && attrCnt > numAttrs) {
                    fprintf(stderr, "too many attributes in csv file\n");
                    return EXIT_FAILURE;
                }
                putdelim(getFormatCh(chTypeString, attrCnt, numAttrs), out);
            } else {
                if (valSize == 0 && getFormatCh(chTypeString, attrCnt, numAttrs) == 's') {
                    putc('"', out);
                }
                valSize += 1;
                putc(ch, out);
            }
        } while ((ch = getc(in)) != EOF && ch != '\n');

        if (attrCnt > 0 || (numAttrs == 1 && !firstCh) || (quoteLine)) {
            if (attrCnt < numAttrs) {
                if (valSize == 0 && (getFormatCh(chTypeString, attrCnt, numAttrs) == 'N' || getFormatCh(chTypeString, attrCnt, numAttrs) == 's')) {
                    fputs("null", out);
                } else if (valSize > 0 && getFormatCh(chTypeString, attrCnt, numAttrs) == 's') {
                    if (!quoteLine)
                        putc('"', out);
                }

                if (!quoteLine)
                    putdelim(getFormatCh(chTypeString, attrCnt, numAttrs), out);
            }
            putc(')', out);
            count++;
        }

        if (!firstCh && ++outCnt >= nChunkLen && nChunkLen) {
            (void) fprintf(out, "\n];\n");
            outCnt = 0;
        }
    }
    if (outCnt) {
        putc('\n', out);
        putc(']', out);
        putc('\n', out);
    }

    fclose(out);
    fclose(in);

    return EXIT_SUCCESS;
}
