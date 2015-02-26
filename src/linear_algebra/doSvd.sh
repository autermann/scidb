#!/bin/bash
#

#
# TODO:
# add getopts to get switches
# switch to print name of test to stderr during test
# switch to print timing of test to stderr
# switch to print matrices to stderr
# can use a variable to represent "| tee /dev/stderr"  perhaps
#


# defaults if not given on command line
SIZE_MIN=2
SIZE_MAX_VERBOSE=8
SIZE_MAX=8
SIZE_STEP_TYPE="x" # or "+"
SIZE_STEP=2


if [ -z "$SCRIPT_DIR" ] ; then
    SCRIPT_DIR="src/linear_algebra"
fi

if [ "$1" != "" ] ; then
    SIZE_MIN=$1  # TODO: replace this with getopt
    SIZE_MAX_VERBOSE=$1
    SIZE_MAX=$1
fi

if [ "$2" != "" ] ; then
    SIZE_MAX_VERBOSE=$2
    SIZE_MAX=$2
fi

if [ "$3" != "" ] ; then
    SIZE_MAX=$3
fi

if [ "$4" != "" ] ; then
    SIZE_STEP_TYPE=$4
fi

if [ "$5" != "" ] ; then
    SIZE_STEP=$5
fi



echo "$0: ====================================="
echo "$0: $0 $SIZE_MIN,$SIZE_MAX_VERBOSE,$SIZE_MAX begin"

sleep 2 # if we get here too soon after runN.py 4 mydb --istart , then there's a connection error on this command
iquery -aq "load_library('dense_linear_algebra')"

#
# time gesvd operator on matrices of small size, printing their results
#
echo "$0: **********************************************************************" 
echo "$0: ****** verbose, remultiplied svd('U'), svd('VT'), svd('values') "
echo "$0: ****** from $SIZE_MIN to $SIZE_MAX_VERBOSE (if any)"

PI="3.14159265359"
NAMES_USED="IN LEFT RIGHT VALS VALSmat PRODUCT DIAG_VEC_1 DIAG_OUTER_PRODUCT"
PFX="TEST_DOSVD_"   # array name prefix

SIZE=$SIZE_MIN
while [ "$SIZE" -le "$SIZE_MAX_VERBOSE" ] ; do
    SIZE_M1=`expr "$SIZE" - 1`
    CSIZE=$SIZE_M1 # just to be annoying and test at least one edge condition
    if [ "$CSIZE" -le "1" ] ; then
        CSIZE=1
    fi
    # NOPE, the release is too close.  we will just use csize 32 and generalize later.
    CSIZE=32
    
    echo "$0: verbose svd test @ ${SIZE} x ${SIZE} csize ${CSIZE} x ${CSIZE}"  | tee /dev/stderr

    for NAME in $NAMES_USED ; do
        iquery -aq "remove(${PFX}${NAME})"   > /dev/null 2>&1 # completely silently
    done

    iquery -naq "create array ${PFX}IN <v:double>[r=0:${SIZE_M1},${CSIZE},0 , c=0:${SIZE_M1},${CSIZE},0]" #> /dev/null

    # test matrix is designed to
    # 1. be integers
    # 2. have no zeros (allows least-significant errors to "disappear")
    # 3. have a condition number better than O^2
    #
    BUILD="build(${PFX}IN, 1+c+r*${SIZE})" # numbered by columns
    #iquery -ocsv+ -aq "$BUILD"  | sort       | tee /dev/stderr
    #echo

    #
    # now execute the SVD's  they must be silent because they don't generate
    # the same answer on different #'s of instances, because the matrix used
    # will have eigenvalues near zero. (can I say rank deficient?)
    #
    # remove the n in -naq to see the matrix for debugging
    #
    echo "$0: verbose U test @ ${SIZE} x ${SIZE} csize ${CSIZE} x ${CSIZE}"   | tee /dev/stderr
    iquery -ocsv+ -naq "store(gesvd(${BUILD}, 'U'),${PFX}LEFT)"  | sort       #| tee /dev/stderr
    echo                                                                     #| tee /dev/stderr
    echo "$0: verbose VT test @ ${SIZE} x ${SIZE} csize ${CSIZE} x ${CSIZE}"  | tee /dev/stderr
    iquery -ocsv+ -naq "store(gesvd(${BUILD}, 'VT'),${PFX}RIGHT)" | sort      #| tee /dev/stderr
    echo                                                                     #| tee /dev/stderr
    echo "$0: verbose S test @ ${SIZE} x ${SIZE} csize ${CSIZE} x ${CSIZE}"   | tee /dev/stderr
    iquery -ocsv+ -naq "store(gesvd(${BUILD}, 'S'),${PFX}VALS)"   | sort      #| tee /dev/stderr
    echo                                                                     #| tee /dev/stderr

    # convert S vector to a matrix
    ${SCRIPT_DIR}/diag.sh ${PFX}VALS ${PFX}VALSmat ${PFX}DIAG_VEC_1 ${PFX}DIAG_OUTER_PRODUCT
    #echo "$0: ${PFX}VALSmat:" ;
    #iquery -ocsv+ -aq "scan(${PFX}VALSmat)" | sort                          #| tee /dev/stderr

    # remultiply (should be very close to ${PFX}IN)
    iquery -ocsv+ -aq "multiply(${PFX}LEFT, multiply(${PFX}VALSmat, ${PFX}RIGHT))" | sort #| tee /dev/stderr
    echo

    # at small sizes, increment (to catch bugs), at larger sizes, double the size (to scale up faster)
    if [ "${SIZE_STEP_TYPE}" = "+" ] ; then
        SIZE=`expr "$SIZE" '+' "$SIZE_STEP"`
    elif [ "${SIZE_STEP_TYPE}" = "x" ] ; then
        SIZE=`expr "$SIZE" '*' "$SIZE_STEP"`
    else
        echo "$0: illegal value for SIZE_STEP_TYPE, = ${SIZE_STEP_TYPE}"
        exit 5
    fi
done


#
# now run up to a limiting size for performance more than
# edge condition testing
#
echo "$0: *****************************************************************************" 
echo "$0: ****** quick test, svd('U') only"
echo "$0: ****** from $SIZE to $SIZE_MAX (if any)"

while [ "$SIZE" -le "$SIZE_MAX" ] ; do
    SIZE_M1=`expr "$SIZE" - 1`
    # NOPE, the release is too close.  we will just use csize 32 and generalize later.
    CSIZE=32

    echo "$0: U-only test @ ${SIZE} x ${SIZE} csize ${CSIZE} x ${CSIZE}" | tee /dev/stderr

    iquery -aq "remove(${PFX}IN)"   > /dev/null 2>&1 # completely silently
    iquery -naq "create array ${PFX}IN <v:double>[r=0:${SIZE_M1},${CSIZE},0 , c=0:${SIZE_M1},${CSIZE},0]" 
    #
    # see explanation in previous loop
    #
    BUILD="build(${PFX}IN, 1+c+r*${SIZE})"

    if [ "$SIZE" -ge 16384 ] ; then
        echo "${0}: warning, a 32-bit ScaLAPACK/LAPACK will fail at this size"
        echo "${0}: with error 'xxmr2d: out of memory'"
        # NOTE LWORK is 532 MB still 4x short of the 2G barrier, fails earlier than i expect
    fi

    # only elapsed/real time E makes sense from iquery
    echo "count(gesvd(${BUILD}, 'U'))"    | tee /dev/stderr
    /usr/bin/time -f'%E s' iquery -ocsv+ -aq "count(gesvd(${BUILD}, 'U'))"    | tee /dev/stderr
    echo                                                      #| tee /dev/stderr

    if [ "${SIZE_STEP_TYPE}" = "+" ] ; then
        SIZE=`expr "$SIZE" '+' "$SIZE_STEP"`
    elif [ "${SIZE_STEP_TYPE}" = "x" ] ; then
        SIZE=`expr "$SIZE" '*' "$SIZE_STEP"`
    else
        echo "$0: illegal value for SIZE_STEP_TYPE, = ${SIZE_STEP_TYPE}"
        exit 5
    fi
done

for NAME in $NAMES_USED ; do
    iquery -aq "remove(${PFX}${NAME})"   > /dev/null 2>&1 # completely silently
done

echo "$0: $0 $SIZE_MIN,$SIZE_MAX_VERBOSE,$SIZE_MAX end"
echo 

