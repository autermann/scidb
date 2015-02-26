#!/bin/sh
#
#   File:  Arrays.sh
# 
#   About: 
#
#   This script takes 5 arguments: 
#
#   Arrays.sh Chunk_Count Chunk_Length Zipf Sparsity Instance
#
#
echo ""
echo "+================================================"
echo "||                                             ||"
echo "||                  Arrays.sh                  ||"
echo "|| DENSE Array, UNIFORM Distribution of Values ||"
echo "|| Chunk Count = $1, Chunk Length = $2 ||"
echo "||       Sparsity = $3,   Zipf = $4    ||"
echo "||        Instance_LABEL = $5       ||"
echo "||                                             ||"
echo "+================================================"
echo ""
date
echo ""
#  
# set -x
#
LEN_I=`expr $1 "*" $2 - 1`;
LEN_J=`expr $1 "*" $2 - 1`;
ZIPF=$3;
SPRS=$4;
INST=$5;
LABEL="Array_${ZIPF}_${SPRS}";
#
#  ADMIN 1: Nuke the previous instance to clean up the space. 
yes | scidb.py initall $INST
scidb.py startall $INST
#
sleep 3;
#
#  DDL 1: Create array with 3 attributes
#
CMD="CREATE ARRAY Test_Array <
    int32_attr  : int32,
    int64_attr  : int64,
    double_attr : double
>
[ I=0:$LEN_I,$2,0, J=0:$LEN_J,$2,0 ]"
#
time -p iquery -aq "$CMD"
#
#    Populating this array using the build() is problematic, as here are three 
#  attributes and the distributions are awkward. So instead I will use the 
#  external gen_matrix executable to generate the data, and load it using a
#  pipe.
#
rm /tmp/Load.pipe
mkfifo /tmp/Load.pipe
echo "gen_matrix -r $1 $1 $2 $2 $ZIPF $SPRS NNG > /tmp/Load.pipe"
gen_matrix -r $1 $1 $2 $2 $ZIPF $SPRS NNG > /tmp/Load.pipe &
#
#  Time the load. 
/usr/bin/time -f "Q0 $LABEL Load %e" iquery -naq "load(Test_Array, '/tmp/Load.pipe')"
#
#  DML 1: How many cells in this array?
CMD="join ( 
        build ( < s : string > [ I=0:0,1,0 ], 'Size_Count_For ${LABEL}'),
        count ( Test_Array )
)";
#
echo $CMD
#
/usr/bin/time -f "${LABEL} Cell_Count %e" iquery -taq "$CMD"
# 
#  Find out how big storage.data1 on the 0 instance is. 
#
du -b $SCIDB_DATA_DIR/000/0/storage.data1
#
# END 
#
