#!/bin/bash


test_file()
{
	FILENAME=$1
	echo
	echo ">>>running $FILENAME"
	time iquery -a -f $FILENAME 2>&1
}

MYDIR=`dirname $0`
pushd $MYDIR > /dev/null

iquery -a -q "dimensions(opt_dense_quad)" > /dev/null
if [ $? -ne 0 ]; then
	echo "Test query fafled... Rerun create.sh?"
	exit 1
fi

for FILE in \
	subarray1.afl \
	subarray2.afl \
	subarray_nested.afl \
	filter.afl \
	between.afl \
	join.afl \
	join_between_lhs.afl \
	join_filter_rhs.afl	\
	join_filter_lhs.afl \
	join_filter2_rhs.afl \
	join_nested.afl \
	join_nested2_lucky.afl \
	join_nested2_unlucky.afl \
	join_nested_sg_override.afl 
do
	test_file $FILE
done 
	
