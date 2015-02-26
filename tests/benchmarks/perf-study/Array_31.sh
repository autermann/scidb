#!/bin/sh
#
#   File:  Array_31.sh
#
#   About:
#       This script creates an array with:
#        0.01% sparse, UNIFORM
echo ""
echo "+=============================================="
echo "||                                           ||"
echo "||                Array_31.sh                ||"
echo "|| SPARSE(0.01%) Array, UNIFORM Distribution ||"
echo "||                                           ||"
echo "+=============================================="
echo ""
date
echo ""
#
set -v
#
LEN_I=`expr $1 "*" $2 - 1`;
LEN_J=`expr $1 "*" $2 - 1`;
#
#  ADMIN 1: Nuke the previous instance to clean up the space.
scidb.py stopall local
scidb.py initall local
scidb.py startall local
#
#  DDL 1: Hygiene. 
time -p iquery -aq "remove ( Test_Array )"
#
#  DDL 2: Create SPARSE array with 3 attributes, where the values in the 
#         attributes are uniformly distributed over a large range of 
#         values, and the probability that a particular cell is non-empty 
#         is 1% (p = 0.01). 
#
#         Each chunk is 10000x10000 (logical space), and there are 8x8
#         of 'em. 
#        
CMD="CREATE EMPTY ARRAY Test_Array <
    int32_attr  : int32,
    int64_attr  : int64,
    double_attr : double
>
[ I=0:$LEN_I,$2,0, J=0:$LEN_J,$2,0 ]"
#
time -p iquery -aq "$CMD"
#
#    Populating this array using the build_sparse() is problematic, as 
#    there are three attributes. So instead I will use the external 
#    gen_matrix executable to generate the data, and load it using the
#    pipe. 
#
rm /tmp/Load.pipe
mkfifo /tmp/Load.pipe
gen_matrix -r $1 $1 $2 $2 0.0001 0.99 NNG > /tmp/Load.pipe &
#
/usr/bin/time -f "Array_31 %e" iquery -naq "load(Test_Array, '/tmp/Load.pipe')"
#
# DML 1: How many cells?
#
/usr/bin/time -f "Array_31 Cell_Count %e" iquery -taq "
join ( 
        build ( < s : string > [ I=0:0,1,0 ], 'Array_31'),
        count ( Test_Array )
)"
#
#  Size
#
du -sh $SCIDB_DATA_DIR/000/0/storage.data1
#
