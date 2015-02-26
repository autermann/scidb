#!/bin/bash
#

if [[ "$#" < 1 ]] ; then
    echo "$0: usage: $0 directory-containing-setup.py"
    exit 1;
fi

SETUP_DIR=$1
echo "SETUP_DIR is $SETUP_DIR"

# seems to break above -j1
MAKEFLAGS="-j1" \
    ${SETUP_DIR}/setup.py \
        --verbose \
        --mpibindir=/usr/bin \
        --mpirun=mpiexec.mpich2 \
        --mpicc=mpicc.mpich2 \
        --mpif90=mpif90.mpich2 \
        --mpiincdir="-I/usr/include/mpich2" \
        --lapacklib "/usr/lib/liblapack.so /usr/lib/libblas.so" \
        --noopt="-fPIC -g -O0" \
        --notesting \
        --make='make'
        # --ccflags="-fPIC -g -O0" \   # for a debug libscalapack.a
        # --fcflags="-fPIC -g -O0" \   # for a debug libscalapack.a
        # --blaslib "/usr/lib/libblas.so" \ # given with --lapack because if it tests liblapack for dscal_ (blas function) and it fails
        #                                   # it gives up completely, rather than backing up and using the separate --blaslib switch
        # --make='make -j6' # doesn't work right, trying MAKEFLAGS="-j6" instead
STATUS=$?

echo "setup.py exited with $STATUS"
exit $STATUS


