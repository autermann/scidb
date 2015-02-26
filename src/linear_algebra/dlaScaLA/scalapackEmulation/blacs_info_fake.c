#include <stdio.h>



int S_contxt = -1;
int S_nprow = -1;
int S_npcol = -1;
int S_myrow = -1;
int S_mycol = -1;

/**
 * a verson of the FORTRAN blacs_gridinfo interface that allows
 * the blacs scheme for info about the process grid to function
 * in SciDB even though it is not linked to ScaLAPACK
 * TODO JHM ; This will be replaced at a later milestone in the
 *            DLA/ScaLAPACK project once we decide how we will
 *            deal with multiple contxt-s, which will be required
 *            for multi-user execution
 *
 * NOTE: this is iso C, not C++, to avoid name-mangling, so it will
 *       have the same symbol as the FORTRAN version
 */
void blacs_gridinfo_(int *contxt, int *nprow, int *npcol,
                                  int *myrow, int *mycol)
{
    *contxt = S_contxt;
    *nprow = S_nprow;
    *npcol = S_npcol;
    *myrow = S_myrow;
    *mycol= S_mycol;
}

void set_fake_blacs_gridinfo_(int *contxt, int *nprow, int *npcol,
                              int *myrow, int *mycol)
{
    S_contxt = *contxt;
    S_nprow = *nprow;
    S_npcol = *npcol;
    S_myrow = *myrow;
    S_mycol = *mycol;
    //fprintf(stderr, "****************************************\n");
    //fprintf(stderr, "WARNING: temporary hack that prevents multi-user and multi-op operation\n");
    //fprintf(stderr, "set_fake_blacs_gridinfo_ ...\n");
    //fprintf(stderr, "context: %d, nprow: %d, npcol %d, myrow %d, mycol %d\n",
    //                S_contxt,  S_nprow,   S_npcol,  S_myrow,  S_mycol);
    //fprintf(stderr, "****************************************\n");
}

