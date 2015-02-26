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
 * Initial Developer: GP
 * Created: September, 2012
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

/* Input arguments. */
int _numOutputFiles = 0;
long _numLinesToSkip = 0;
char* _inputFileName = NULL;
char* _outputFileBaseName = NULL;

/* Shared Variables. */
FILE* _inputFile = NULL;
FILE** _outputFiles = NULL;
long _linesSkipped = 0;
unsigned long long _linesProcessed = 0;
int _outputFileIndex = 0;

/* Read Buffer. */
#define RB_SIZE 262144
char _rb[RB_SIZE];
int _rbBeginPos = 0;
int _rbEndPos = -1;

inline void printUsage() {
    printf("Utility to split a CSV file into smaller files.\n"
            "USAGE: splitcsv -n NUMBER [-s SKIP] [-i INPUT] [-o OUTPUT]\n"
            "   -n NUMBER\tNumber of files to split the input file into.\n"
            "   -s SKIP\tNumber of lines to skip from the beginning of the input file (Default = 0).\n"
            "   -i INPUT\tInput file. (Default = stdin).\n"
            "   -o OUTPUT\tOutput file base name. (Default = INPUT or \"stdin.csv\").\n"
            );
}

inline void handleArgError(const char* error) {
    fprintf(stderr, "ERROR: %s\n", error);
    printUsage();
    exit(EXIT_FAILURE);
}

inline void parseArgs(int argc, char* argv[]) {
    int i = 0;

    /* Sanity check. */
    if (argc <= 1) {
        handleArgError("This utility has a required command-line argument.");
    }

    /* Iterate over the command-line arguments. */
    for (i = 1; i < argc; i++) {
        /* Number option. */ 
        if (strcmp(argv[i], "-n") == 0) {
            _numOutputFiles = atoi(argv[++i]);
            continue;
        }

        /* Skip option. */ 
        if (strcmp(argv[i], "-s") == 0) {
            _numLinesToSkip = atol(argv[++i]);
            continue;
        }

        /* Input file. */
        if (strcmp(argv[i], "-i") == 0) {
            _inputFileName = argv[++i];
            continue;
        }

        /* Output file base name. */
        if (strcmp(argv[i], "-o") == 0) {
            _outputFileBaseName = argv[++i];
            continue;
        }

        /* Help option. */
        if (strcmp(argv[i], "-h") == 0) {
            printUsage();
            exit(EXIT_SUCCESS);
        }
    }

    /* Validation. */
    if (_numOutputFiles == 0) {
        handleArgError("The number of files to be created must be specified.");
    }
}

inline int fillReadBuffer() {
    int numRead = fread(_rb, 1, RB_SIZE, _inputFile);
    if (numRead == 0) {
        _rbBeginPos = 0;
        _rbEndPos = -1;
        if (ferror(_inputFile)) {
            fprintf(stderr, "ERROR: Problem encountered while reading input file.\n");
            exit(EXIT_FAILURE);
        }
    } else {
        _rbBeginPos = 0;
        _rbEndPos = numRead - 1;
    }
    return numRead;
}

inline int getNextNlPos() {
    int ch = 0;
    int rbCurPos = _rbBeginPos;
    while (rbCurPos <= _rbEndPos) {
        ch = _rb[rbCurPos];
        if (ch == '\n') {
            return rbCurPos;
        }
        rbCurPos++;
    }
    return -1;
}

inline void skipLines() {
    while (_linesSkipped < _numLinesToSkip) {
        int nextNlPos = getNextNlPos();
        if (nextNlPos != -1) {
            _linesSkipped++;
            _rbBeginPos = nextNlPos + 1;
        } else {
            if (!fillReadBuffer()) {
                fprintf(stdout, "WARNING: All lines in the file have been skipped.\n");
                break;
            }
        }
    }
}

inline void writeData(char* offset, int count, FILE* outputFile) {
    int numWritten = fwrite(offset, 1, count, outputFile);
    if (numWritten == 0) {
        if (ferror(_inputFile)) {
            fprintf(stderr, "ERROR: Problem encountered while writing to output file.\n");
            exit(EXIT_FAILURE);
        }
    }
}

inline void splitFile() {
    while (1) {
        int nextNlPos = getNextNlPos();
        char* offset = (char *) (_rb + _rbBeginPos);
        if (nextNlPos != -1) {
            /* We have a full line of text. */        
            int count = nextNlPos - _rbBeginPos + 1;
            writeData(offset, count, _outputFiles[_outputFileIndex]);
            _linesProcessed++;
            _outputFileIndex = _linesProcessed % _numOutputFiles;
            _rbBeginPos = nextNlPos + 1;
        } else {
            if (_rbBeginPos <= _rbEndPos) {
                /* We have a line fragment left in the buffer. */
                int count = _rbEndPos - _rbBeginPos + 1;
                writeData(offset, count, _outputFiles[_outputFileIndex]);
            }
            if (!fillReadBuffer()) {
                /* End of the file. */
                break;
            }
        }
    }
}

inline void openInputFile() {
    if (_inputFileName == NULL) {
        _inputFileName = "stdin.csv";
        _inputFile = stdin;
    } else {
        _inputFile = fopen(_inputFileName, "r");
        if (_inputFile == NULL) {
            fprintf(stderr, "ERROR: Failed to open specified CSV input file.\n");
            exit(EXIT_FAILURE);
        }
    }
}

inline void openOutputFiles() {
    int i = 0;
    if (_outputFileBaseName == NULL) {
        if (_inputFileName == NULL) {
            _outputFileBaseName = "stdin.csv";
        } else {
            _outputFileBaseName = _inputFileName;
        }
    }
    _outputFiles = (FILE**) malloc(sizeof (FILE*) * _numOutputFiles);
    for (i = 0; i < _numOutputFiles; i++) {
        char outputFileName[2048];
        sprintf(outputFileName, "%.1024s_%04d", _outputFileBaseName, i);
        _outputFiles[i] = fopen(outputFileName, "w");
        if (_outputFiles[i] == NULL) {
            fprintf(stderr, "ERROR: Could not open output file '%s' for writing.\n", outputFileName);
            exit(EXIT_FAILURE);
        }
    }
}

inline void closeFile(FILE* file) {
    if (file != NULL) {
        fclose(file);
    }
}

inline void closeInputFile() {
    closeFile(_inputFile);
}

inline void closeOutputFiles() {
    int i = 0;
    for (i = 0; i < _numOutputFiles; i++) {
        closeFile(_outputFiles[i]);
    }
    free(_outputFiles);
    _outputFiles = NULL;
}

int main(int argc, char* argv[]) {
    parseArgs(argc, argv);
    openInputFile();
    openOutputFiles();
    skipLines();
    splitFile();
    fprintf(stdout, "Lines Skipped: %ld / Lines Processed: %llu\n", _linesSkipped, _linesProcessed);
    closeOutputFiles();
    closeInputFile();
    return EXIT_SUCCESS;
}