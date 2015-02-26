#!/bin/sh
#
#   About: 
#       This script creates an array with: 
#        DENSE, ZIPF=0.1
#
echo ""
echo "+==========================================="
echo "||                                        ||"  
echo "||              Array_13.sh               ||"
echo "|| DENSE Array, Zipf (0.1) Distribution   ||"
echo "||                                        ||"
echo "+==========================================="
echo ""
date
echo ""
#  
set -v
#
LEN_I=`expr $1 "*" $2 - 1`;
LEN_J=`expr $1 "*" $2 - 1`;
#
#  ADMIN 1: Nuke the previous instance. 
scidb.py stopall local
scidb.py initall local
scidb.py startall local
#
#  DDL 1: Hygiene. 
time -p iquery -aq "remove ( Test_Array )"
#
#  DDL 2: Create DENSE array with 3 attributes, where the values in the 
#         attributes are distribued in a zipfian manner over a range of 
#         10 distinct values, having a p=0.5.
#
#   Build an array with 3 attributes, and 8000x8000 or 64,000,000 elements. 
#  This array is divided into 64 chunks, each of which has 1,000,000 elements. 
#  The total size of the data is therefore 64,000,000x(8x2+4) = 1200 Meg.
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
#    Populating this array using the build() is problematic, as
#    there are three attributes. So instead I will use the external
#    gen_matrix executable to generate the data, and load it using the
#    pipe.
#
rm /tmp/Load.pipe
mkfifo /tmp/Load.pipe
gen_matrix -r $1 $1 $2 $2 1.0 0.1 DDF > /tmp/Load.pipe &
#
/usr/bin/time -f "Array_13 Load %e" iquery -naq "load(Test_Array, '/tmp/Load.pipe')"
#
#   DML 1: How many cells?
#
/usr/bin/time -f "Array_13 Cell_Count %e" iquery -taq "
join ( 
        build ( < s : string > [ I=0:0,1,0 ], 'Array_13'),
        count ( Test_Array )
)"
#
#  Size
#
du -sh $SCIDB_DATA_DIR/000/0/storage.data1
#
