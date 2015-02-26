#!/bin/bash
#

if [[ "$#" < 1 ]] ; then
    echo "$0: usage: $0 directory-containing-setup.py"
    exit 1;
fi

PATCH=$(dirname $0)/scalapack.patch
SETUP_DIR=$1
echo "SETUP_DIR is $SETUP_DIR"
echo "PATCH is ${PATCH}"
shift
echo "ARGS#: $#"
echo "ARG1: ${1}"
echo "ARG2: ${2}"
echo "ARG3: ${3}"
echo "ARG4: ${4}"
echo "ARG5: ${5}"

(cd ${SETUP_DIR}; patch -p1 < ${PATCH})

# seems to break above -j1
MAKEFLAGS="-j1" \
    ${SETUP_DIR}/setup.py \
        --verbose  \
        --mpibindir="${2}" \
        --mpirun=/usr/bin/mpiexec.hydra \
        --mpicc="mpicc${5}" \
        --mpif90="mpif90${5}" \
        --mpiincdir="${1}" \
        --lapacklib="${3} ${4}" \
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
