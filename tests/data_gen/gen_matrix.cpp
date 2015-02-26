/*
**  File:   gen_matrix.cpp
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
* 
** 
** About: 
**
** How to Build:

all: clean gen_matrix.o
    g++ -o gen_matrix gen_matrix.o -lm

clean:
    rm -f gen_matrix gen_matrix.o

gen_matrix.o: gen_matrix.cpp
    g++ -c -o gen_matrix.o -g gen_matrix.cpp

**
** How to use.
**
gen_matrix 2000 2000 100 100 0.002

** 
*/
#include <vector>
#include <iostream>
#include <string.h>

#include <stdlib.h>
#include <stdio.h>
#include <limits.h>

#include <math.h>

#include <assert.h>

void
int2str ( const int v, 
		  char * out ) 
{
	int lv = v;
	int off = 0;

	assert((NULL != out));
	assert((0 <= v));

	do { 
		out[off]=('A'+(lv%26));
		lv/=26;
		off++;
	} while (0 < lv);

	out[off]=0;
}

double uniform () 
{
    return ((random()/2147483648.0));
}

long			
geomdev (const double pr)
{
	double p = pr;
	long   r = 1;

	assert((p > 0.0));
	assert((p <= 1.0));
  
	if (1.0 != p )
     r+=(log(uniform()) / log(1.0 - p));

	return r;
}

void
usage(char * szProg)
{
	printf("%s -[rd] int int int int sparse_prob attr_zipf_prob string\n", szProg);
	printf("%s (-[r]andom or -[c]calculated) #rowchunks #colchunks #rowsperchunk #coldperchunk sparsity_probability[0.0->1.0] attr_val_probability[0.0->1.0] string\n", szProg);
	printf("   The last 'string' is some combination of G - double, N - integer, C - char \n");
	printf("   S - string, F - double w/ zipfian, D - int w/ zipfian, E - char with zipfian.\n");
	printf("   For example, to generate a 1000x1000 matrix with 100 chunks (10x10 chunks) \n");
	printf("   of 100 rows x 100 columns each, with a uniformly distributed double, \n");
	printf("   integer, char and string attributes in each cell ... \n");	
	printf("%s -r 10 10 100 100 1.0 0.9 GNCS\n", szProg);
	printf(" To generate a 30Kx30K sparse matrix with a probability that any cell contains\n");
	printf(" a non-empty cell is 0.001, chunk sized of 10Kx10K, where each cell contains \n");
	printf(" a zipfian integer and uniformly distributed double is ... \n");
	printf("%s -r 10 10 100 100 1.0 0.9 DG\n", szProg);

	exit(1);
}

void
print_random_attr_p ( const int nTypeCnt, const char * szTypesListStr, 
					  const double p)
{
	char szString[32]; 
	int ints[11]={0,1,2,3,4,5,6,7,8,9};
	double dbls[11]={0.0,1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0};
	char chars[11]={'A','B','C','D','E','F','G','H','I','J'};

	printf("(");
	for(int c = 0; c < nTypeCnt; c++) { 
		if (c) printf(", ");

		switch(toupper(szTypesListStr[c])) {
			case 'G':	/* double */
				printf("%g", uniform());
			 break;

			case 'N':	/* integer */
				printf("%ld", random());
			break;

			case 'S':	/* string */
				int2str((random()%20000), szString);
				printf("\"%s\"", szString);
			break;

			case 'C':  /* char */
				int2str((random()%26), szString);
				printf("'%1s'", szString);
			 break;

			case 'F':	/* double zipfian*/
				printf("%g", dbls[geomdev(p)%10]);
			 break;

			case 'D':	/* integer zipfian*/
				printf("%d", ints[geomdev(p)%10]);
			 break;

			case 'E':	/* char zipfian*/
				printf("'%c'", chars[geomdev(p)%10]);
			 break;

			default:	/* nothing */
		    break;
		}
	}
	printf(")");
}

void
print_random_attr ( const int nTypeCnt, const char * szTypesListStr) 
{
	print_random_attr_p ( nTypeCnt, szTypesListStr, 1.0 );
}

void
print_det_attr ( const int nTypeCnt, const char * szTypesListStr, 
				 const int nCellNum, const int nCellMax ) 
{
	char szString[32]; 

	printf("(");
	for(int c = 0; c < nTypeCnt; c++) { 
		if (c) printf(", ");

		switch(toupper(szTypesListStr[c])) {
			case 'G':	/* double */
				printf("%g", ((double)nCellNum/(double)nCellMax));
			 break;

			case 'N':	/* integer */
				printf("%d", nCellNum);
			break;

			case 'S':	/* string */
				int2str(nCellNum, szString);
				printf("\"%s\"", szString);
			break;

			case 'C':	/* char */
				int2str((nCellNum%26), szString);
				printf("'%1s'", szString);
			break;
				

			default:	/* nothing */
		    break;
		}
	}
	printf(")");
}

void
print_empty_attr()
{
	printf("()");
}

int
main (int argc, char ** argv ) 
{
    /*
    ** Loop over the input until EOF, placing a line's worth of tokens
    ** into the list as you go.
    */
    int         nRowChunks        =    0;
	int			nColChunks		  =    0;
    int         nRowsInChunk      =    0;
	int			nColsInChunk	  =    0;
	double	    dbProb			  =  0.0;
	double	    dbZProb			  =  0.0;
	int			nCount            =    0;
	char        szTypesListStr[1024];
	char        szFlags[8]; 
	int			nTypeCnt 		  =    0;
	int			nIsDense          =    0;
	int			nIsRandom         =    0;

	int  		nCellNum		  =    0;
	int 		nCellMax		  =    0;

	if (9 != argc)
		usage(argv[0]);
	
	strncpy(szFlags, argv[1], 3);
	nRowChunks   = atoi(argv[2]);
	nColChunks   = atoi(argv[3]);
	nRowsInChunk = atoi(argv[4]);
	nColsInChunk = atoi(argv[5]);
	dbProb = atof(argv[6]);
	dbZProb = atof(argv[7]);
	strncpy(szTypesListStr, argv[8], 1024);
	nTypeCnt = strlen(szTypesListStr);

	if (0.1 <= dbProb) 
		nIsDense = 1; 

	/*
	** Some checks.
	*/
	assert(('-' == szFlags[0]));
	assert((0.0 < dbProb));
	assert((1.0 >= dbProb));

	assert((0 < nRowChunks));
	assert((0 < nColChunks));
	assert((0 < nRowsInChunk));
	assert((0 < nColsInChunk));

	switch(toupper(szFlags[1])) { 
		case 'R':
			nIsRandom = 1; 
		 break;
		case 'D':
			nIsRandom = 0; 
		 break;
		default:
			usage(argv[0]);
			exit(0);
		 break;

	}
	/*
	** Some calculation.
	*/
	nCellMax = nRowChunks * nRowsInChunk * nColChunks * nColsInChunk;
    srandom(time(0));

/*
	while ( 1 ) { 
		long p = geomdev ( dbProb );
		printf("geomdev ( %g ) = %ld\n", dbProb, p);
	}
*/

	if (0 == nIsDense) { 
		/*
		** This is the SPARSE representation.
		*/
		/*
		** How many to step over initially?
		*/
		long nStep = geomdev ( dbProb );

		for ( int i = 0;
              i < nRowChunks;
              i++ ) { 

			for ( int j = 0;
                  j < nColChunks; 
                  j++ ) { 

				if (i+j) { 
					printf("\n;\n{ %d, %d }[[", 
							i * nRowsInChunk, j * nColsInChunk);
/*
i, j);
*/
				} else { 
					printf("{ %d, %d }[[",
							i * nRowsInChunk, j * nColsInChunk);
				}

				nCount = 0;
				int firstInChunk = 1;
				
				/*
				** ROWS in the CHUNK	
				*/
				for (int n = 0;
					 n < nRowsInChunk; 
					 n++) { 

					for (int m = 0; 
						 m < nColsInChunk; 
						 m+=0 ) { 

						nCellNum = (((i * nRowsInChunk) + n) * 
										(nRowsInChunk * nRowChunks )) + 
									 ((j * nColsInChunk) + m) ;

						if (( m + nStep ) < nColsInChunk ) { 

						  m+=nStep;

						  /* Print a comma separator except for the first */
						  /* cell in chunk. 							  */


						  if (firstInChunk) {
						    firstInChunk = 0;
						  } else {
						    printf(",\n ");
						  }

						  printf(" {%d, %d} ",
								i * nRowsInChunk + n, 
								j * nColsInChunk + m
							);
							
						  if (nIsRandom) 
							print_random_attr_p( nTypeCnt, szTypesListStr, 
											   dbZProb );
						  else
							print_det_attr( nTypeCnt, szTypesListStr, 
										    nCellNum, nCellMax);

						  nStep = geomdev ( dbProb );
						  nCount++;

						  if (( m + nStep ) > nColsInChunk) { 
							nStep-=(nColsInChunk - m);
							break;
						  }

						} else {
							nStep-=(nColsInChunk-m);
							break;
						}
					}
				}
				printf(" ]]");
   			}
		}
		printf("\n");

	} else { 
		/*
		** Dense data.
		*/
		for(int i = 0;i < nRowChunks; i++ ) { 
			for(int j = 0;j < nColChunks; j++ ) { 
				nCount = 0;

				if (i+j)
					printf(";\n[\n");
				else 
					printf("[\n");

				for (int n = 0; n < nRowsInChunk; n++ ) { 

					if (n)
						printf(",\n[ ");
					else 
						printf("[ ");

					for (int m = 0; m < nColsInChunk; m++ ) { 

						if (m) printf(", "); 

#ifdef  _UNDEFINED 

printf("\n+=============================+\n");
printf("||  i (RowChunks)    =   %3d ||\n", i);
printf("||      RowsInChunk  =   %3d ||\n", nRowsInChunk);
printf("||  n (RowsInChunk)  =   %3d ||\n", n);
printf("||      nRowChunks   =   %3d ||\n", nRowChunks );
printf("||  j (ColChunks)    =   %3d ||\n", j); 
printf("||      nColsInChunk =   %3d ||\n", nColsInChunk); 
printf("||  m (nColsInChunk) =   %3d ||\n", m);
printf("||      nColChunks   =   %3d ||\n", nColChunks);
printf("+=============================+\n");

#endif 
						//
						// Tricky bit here .... 
						// 
						nCellNum = (((i * nRowsInChunk) + n) * 
						//				(nRowsInChunk * nRowChunks )) + 
						  				(nColsInChunk * nColChunks )) + 
									 ((j * nColsInChunk) + m) ;

						if ((1.0 == dbProb) || 
					        (dbProb > ((double)random() / (double)INT_MAX))) 
						{

							if (nIsRandom) 
								print_random_attr_p( nTypeCnt, szTypesListStr, 
												   dbZProb );
							else {
								print_det_attr( nTypeCnt, szTypesListStr, 
											    nCellNum, nCellMax);
							}
						} else { 
							print_empty_attr();
						}
						nCount++;
					}
					printf("]");
				}
				printf("\n]");
			}
		}
		printf("\n");
	}
}
