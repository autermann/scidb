#!/bin/bash
#

#######################################
# was: source iqfuncs.bash
#######################################
IQFUNCS_TRACE=""
TMPTIME=/tmp/time.$$
TIME="time -f %E -o $TMPTIME"
showtime() {
    echo '    real ' `cat $TMPTIME`
}

afl() {
    if [ "$IQFUNCS_TRACE" ] ; then
        #set -x
        echo -n "afl $* -> "
        $TIME iquery -a $*
        showtime
        #set +x
    else
        iquery -a $*
    fi
}

nafl() {
    if [ "$IQFUNCS_TRACE" ] ; then
        #set -x
        echo "nafl $* "
        $TIME iquery -na $* | grep -v 'Query was executed successfully'
        showtime
        #set +x
    else
        iquery -na $* | grep -v 'Query was executed successfully'
    fi
}

eliminate() {
    if [ "$IQFUNCS_TRACE" ] ; then
        #set -x
        echo "eliminate $1"  # no -n
        iquery -naq "remove(${1})" 2> /dev/null | grep -v 'Query was executed successfully'
        #set +x
    else
        iquery -naq "remove(${1})" 2> /dev/null | grep -v 'Query was executed successfully'
    fi
}
######################
# end was iqfuncs.bash
######################

echo "doSvdMetric begin -----------------------------------------"


SIZE_LIMIT=16 # default if not given on command line
if [ -z "$SCRIPT_DIR" ] ; then
    SCRIPT_DIR="src/linear_algebra"
fi

if [ "$#" -ne 5 ] ; then
    #echo "$0: numargs is $#"
    echo "usage: $0 ORIGINAL S U VT OUTPUT_METRICS" >&2  # all array names
    exit 2
fi


MAT_IN=$1
VEC_S=$2
MAT_U=$3
MAT_VT=$4
METRICS=$5

if false;
then
    echo "MAT_IN is $MAT_IN"
    echo "VEC_S is $VEC_S"
    echo "MAT_U is $MAT_U"
    echo "MAT_VT is $MAT_VT"
    echo "METRICS is $METRICS"
fi

IQFTM=""
#IQFMT="-ocsv+"   # for explicit row,column printing
#IQFMT="-osparse" # enable this when debugging distribution issues

# multiply the factors back together
# first must turn the S vector into a diagonal matrix
TRACE="" ${SCRIPT_DIR}/diag.sh ${VEC_S} METRIC_TMP_DIAG_SS TMP_VEC_1 TMP_OUTER_PRODUCT

#echo -n METRIC_TMP_DIAG_SS"; iquery ${IQFMT} -aq "show(METRIC_TMP_DIAG_SS)"
#echo -n "${MAT_U}";    iquery ${IQFMT} -aq "show(${MAT_U})"
#echo -n "${MAT_VT}";   iquery ${IQFMT} -aq "show(${MAT_VT})"
eliminate METRIC_TMP_PRODUCT
nafl -q "store(multiply(${MAT_U},multiply(METRIC_TMP_DIAG_SS,${MAT_VT})),METRIC_TMP_PRODUCT)"

# difference of |original| and the |product of the svd matrices|
eliminate ${METRICS}
# 2^55= 36028797018963968 ~= reciprocal of 1 LSB
nafl -q "store(apply(join(${MAT_IN},METRIC_TMP_PRODUCT),nelsb,(multiply-v)/abs(v)*36028797018963968.0),${METRICS})"


#TODO: review the number of fields being printed, there are a lot of columns.
#      Use project to keep only the ones we want

eliminate METRIC_TMP_DIAG_SS
eliminate METRIC_TMP_PRODUCT
eliminate TMP_VEC_1
eliminate TMP_OUTER_PRODUCT

#output in ${METRICS}
echo "${0}: output is in array ${METRICS}"

#set +x
echo "$0: doSvdMetric end -----------------------------------------"
exit 0

