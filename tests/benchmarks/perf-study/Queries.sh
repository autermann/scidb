#!/bin/sh
#
#  File: Queries.sh 
#
#   This script is intended to run against an arrays with the 
#  following size and shape: 
#
#  CREATE ARRAY Test_Array <
#     int32_attr  : int32,
#     int64_attr  : int64,
#     double_attr : double
#  >
#  [ I=0:Array_I,Chunk_Len,0, J=0:Array_J,Chunk_Len,0 ]"
#
#   The script runs a range of queries against this array, and times 
#  how long each of them takes. In order to keep the query response 
#  times "in the same ballpark" I run the faster ones multiple times. 
#
#   All of the various measurements factors here need to be made divisible 
#  by 8 in order to work around a couple of (current - see scidb:ticket:1680) 
#  problems. 
#
#  Usage: 
#
#   ./Queries.sh Chunk_Count Chunk_Length 
#
LEN_I=`dc -e "$1 $2 * d 8 % 8 - -1 * n"`
LEN_J=`dc -e "$1 $2 * d 8 % 8 - -1 * n"`
#
ONE_PERCENT_I=`dc -e "$LEN_I 128 / d 8 % 8 - -1 * n"`
ONE_PERCENT_J=`dc -e "$LEN_I 128 / d 8 % 8 - -1 * n"`
#
OVERLAP_I=`dc -e "$2 128 / d 16 % 16 - -1 * n"`
OVERLAP_J=`dc -e "$2 128 / d 16 % 16 - -1 * n"`
#
#  Regrid into 2% blocks. 
REGRID_I_LEN=`dc -e "$LEN_I 64 / d 8 % 8 - -1 * n"`
REGRID_J_LEN=`dc -e "$LEN_J 64 / d 8 % 8 - -1 * n"`
#
echo "+==========================================+"
echo "||                                        ||"
echo "||     Queries.sh $1 $2              ||"
echo "|| Length I = $LEN_I, Length J = $LEN_J ||"
echo "|| 1% of I = $ONE_PERCENT_I, 1% of J = $ONE_PERCENT_J ||"
echo "||                                        ||"
echo "+==========================================+"
#
#  Hygiene
#
iquery -aq "remove ( Test_Array_2 )"
iquery -aq "remove ( Test_Array_3 )"
iquery -aq "remove ( Test_Array_4 )"
#
#
# set -v
#
#  Q: Check the size and shape of the array we'll be working with. 
#
date;
/usr/bin/time -f "%e" iquery -aq "show ( Test_Array )"
ps -eo comm,%mem | grep SciDB-000-0 
#
#  Q1: Simple sum() and count() grand aggregates - use a join() to 
#      wire them together. 
#
#  NOTE: We use the repitition of the query to try to even out the proportion
#        of the run-time devoted to each query. Repeat this query 
#        10 times. 
#
CMD="join ( 
    count ( Test_Array ), 
    join ( 
        sum ( Test_Array, int32_attr ),
        join ( 
            sum ( Test_Array, int64_attr ),
            sum ( Test_Array, double_attr )
        )
    )
)"
#
echo "$CMD"
#
#  Aggregates are very fast. Repeat it 10 times. 
date;
/usr/bin/time -f "Q1 %e" iquery -aq "$CMD;$CMD;$CMD;$CMD;$CMD;$CMD;$CMD;$CMD;$CMD;$CMD" 
ps -eo comm,%mem | grep SciDB-000-0 
#
#  Q2: Simple sum() with group-by on column major order
#
CMD="aggregate ( 
    Test_Array, 
    sum(int32_attr), 
    I 
)"
#
echo "$CMD"
#
date;
/usr/bin/time -f "Q2 %e" iquery -r /dev/null -aq "$CMD;$CMD;$CMD;$CMD;$CMD"
ps -eo comm,%mem | grep SciDB-000-0 
#
#  Q3: Simple sum() with group-by on row major order
#
CMD="aggregate ( 
    Test_Array, 
    sum(int32_attr), 
    J 
)"
#
echo "$CMD"
#
date;
/usr/bin/time -f "Q3 %e" iquery -r /dev/null -aq "$CMD;$CMD"
ps -eo comm,%mem | grep SciDB-000-0 
#
#  Q4: sum(between()) - lots and lots (64) of 1% array probes. 
#
#  NOTE: A 1% array has 2 x 10% dimension sides.
#
date;
CMD="
join (
 join (
  join (
   join (
    join (
     join (
      aggregate (
       between ( 
        Test_Array, 
        1 * $ONE_PERCENT_I, 1 * $ONE_PERCENT_J,
        10 * $ONE_PERCENT_I, 10 * $ONE_PERCENT_J
        ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        1 * $ONE_PERCENT_I, 10 * $ONE_PERCENT_J,
        10 * $ONE_PERCENT_I, 20 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( 
        Test_Array, 
        1 * $ONE_PERCENT_I, 20 * $ONE_PERCENT_J,
        10 * $ONE_PERCENT_I, 30 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        1 * $ONE_PERCENT_I, 30 * $ONE_PERCENT_J,
        10 * $ONE_PERCENT_I, 40 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( 
        Test_Array, 
        1 * $ONE_PERCENT_I, 40 * $ONE_PERCENT_J,
        10 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        1 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J,
        10 * $ONE_PERCENT_I, 60 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( 
        Test_Array, 
        1 * $ONE_PERCENT_I, 60 * $ONE_PERCENT_J,
        10 * $ONE_PERCENT_I, 70 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        1 * $ONE_PERCENT_I, 70 * $ONE_PERCENT_J,
        10 * $ONE_PERCENT_I, 80 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))))  ,
   join (
    join (
     join (
      aggregate (
       between ( 
        Test_Array, 
        1 * $ONE_PERCENT_I, 80 * $ONE_PERCENT_J,
        10 * $ONE_PERCENT_I, 90 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        1 * $ONE_PERCENT_I, 90 * $ONE_PERCENT_J,
        10 * $ONE_PERCENT_I, 100 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( 
        Test_Array, 
        10 * $ONE_PERCENT_I, 1 * $ONE_PERCENT_J,
        20 * $ONE_PERCENT_I, 10 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        10 * $ONE_PERCENT_I, 10 * $ONE_PERCENT_J,
        20 * $ONE_PERCENT_I, 20 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( 
        Test_Array, 
        10 * $ONE_PERCENT_I, 20 * $ONE_PERCENT_J,
        20 * $ONE_PERCENT_I, 30 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        10 * $ONE_PERCENT_I, 30 * $ONE_PERCENT_J,
        20 * $ONE_PERCENT_I, 40 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( 
        Test_Array, 
        10 * $ONE_PERCENT_I, 40 * $ONE_PERCENT_J,
        20 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        10 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J,
        20 * $ONE_PERCENT_I, 60 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))))) ,
  join (
   join (
    join (
     join (
      aggregate (
       between ( 
        Test_Array, 
        10 * $ONE_PERCENT_I, 70 * $ONE_PERCENT_J,
        20 * $ONE_PERCENT_I, 80 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        10 * $ONE_PERCENT_I, 80 * $ONE_PERCENT_J,
        20 * $ONE_PERCENT_I, 90 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( 
        Test_Array, 
        10 * $ONE_PERCENT_I, 90 * $ONE_PERCENT_J,
        20 * $ONE_PERCENT_I, 100 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        20 * $ONE_PERCENT_I, 0 * $ONE_PERCENT_J,
        30 * $ONE_PERCENT_I, 10 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( 
        Test_Array, 
        20 * $ONE_PERCENT_I, 10 * $ONE_PERCENT_J,
        30 * $ONE_PERCENT_I, 20 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        20 * $ONE_PERCENT_I, 20 * $ONE_PERCENT_J,
        30 * $ONE_PERCENT_I, 30 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( 
        Test_Array, 
        20 * $ONE_PERCENT_I, 30 * $ONE_PERCENT_J,
        30 * $ONE_PERCENT_I, 40 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        20 * $ONE_PERCENT_I, 40 * $ONE_PERCENT_J,
        30 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))))  ,
   join (
    join (
     join (
      aggregate (
       between ( 
        Test_Array, 
        20 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J,
        30 * $ONE_PERCENT_I, 60 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        20 * $ONE_PERCENT_I, 60 * $ONE_PERCENT_J,
        30 * $ONE_PERCENT_I, 70 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( 
        Test_Array, 
        20 * $ONE_PERCENT_I, 70 * $ONE_PERCENT_J,
        30 * $ONE_PERCENT_I, 80 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        20 * $ONE_PERCENT_I, 80 * $ONE_PERCENT_J,
        30 * $ONE_PERCENT_I, 90 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( 
        Test_Array, 
        20 * $ONE_PERCENT_I, 90 * $ONE_PERCENT_J,
        30 * $ONE_PERCENT_I, 100 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        30 * $ONE_PERCENT_I, 0 * $ONE_PERCENT_J,
        40 * $ONE_PERCENT_I, 10 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( 
        Test_Array, 
        30 * $ONE_PERCENT_I, 10 * $ONE_PERCENT_J,
        40 * $ONE_PERCENT_I, 20 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        30 * $ONE_PERCENT_I, 20 * $ONE_PERCENT_J,
        40 * $ONE_PERCENT_I, 30 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))))))),
 join (
  join (
   join (
    join (
     join (
      aggregate (
       between ( 
        Test_Array, 
        30 * $ONE_PERCENT_I, 30 * $ONE_PERCENT_J,
        40 * $ONE_PERCENT_I, 40 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        30 * $ONE_PERCENT_I, 40 * $ONE_PERCENT_J,
        40 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( 
        Test_Array, 
        30 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J,
        40 * $ONE_PERCENT_I, 60 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        30 * $ONE_PERCENT_I, 60 * $ONE_PERCENT_J,
        40 * $ONE_PERCENT_I, 70 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( 
        Test_Array, 
        30 * $ONE_PERCENT_I, 70 * $ONE_PERCENT_J,
        40 * $ONE_PERCENT_I, 80 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        30 * $ONE_PERCENT_I, 80 * $ONE_PERCENT_J,
        40 * $ONE_PERCENT_I, 90 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( 
        Test_Array, 
        30 * $ONE_PERCENT_I, 90 * $ONE_PERCENT_J,
        40 * $ONE_PERCENT_I, 100 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        40 * $ONE_PERCENT_I, 0 * $ONE_PERCENT_J,
        50 * $ONE_PERCENT_I, 10 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))))  ,
   join (
    join (
     join (
      aggregate (
       between ( 
        Test_Array, 
        40 * $ONE_PERCENT_I, 10 * $ONE_PERCENT_J,
        50 * $ONE_PERCENT_I, 20 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        40 * $ONE_PERCENT_I, 20 * $ONE_PERCENT_J,
        50 * $ONE_PERCENT_I, 30 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( 
        Test_Array, 
        40 * $ONE_PERCENT_I, 30 * $ONE_PERCENT_J,
        50 * $ONE_PERCENT_I, 40 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        40 * $ONE_PERCENT_I, 40 * $ONE_PERCENT_J,
        50 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( 
        Test_Array, 
        40 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J,
        50 * $ONE_PERCENT_I, 60 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        40 * $ONE_PERCENT_I, 60 * $ONE_PERCENT_J,
        50 * $ONE_PERCENT_I, 70 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( 
        Test_Array, 
        40 * $ONE_PERCENT_I, 70 * $ONE_PERCENT_J,
        50 * $ONE_PERCENT_I, 80 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        40 * $ONE_PERCENT_I, 80 * $ONE_PERCENT_J,
        50 * $ONE_PERCENT_I, 90 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))))) ,
  join (
   join (
    join (
     join (
      aggregate (
       between ( 
        Test_Array, 
        40 * $ONE_PERCENT_I, 90 * $ONE_PERCENT_J,
        50 * $ONE_PERCENT_I, 100 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        50 * $ONE_PERCENT_I, 0 * $ONE_PERCENT_J,
        60 * $ONE_PERCENT_I, 10 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( 
        Test_Array, 
        50 * $ONE_PERCENT_I, 10 * $ONE_PERCENT_J,
        60 * $ONE_PERCENT_I, 20 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        50 * $ONE_PERCENT_I, 20 * $ONE_PERCENT_J,
        60 * $ONE_PERCENT_I, 30 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( 
        Test_Array, 
        50 * $ONE_PERCENT_I, 30 * $ONE_PERCENT_J,
        60 * $ONE_PERCENT_I, 40 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        50 * $ONE_PERCENT_I, 40 * $ONE_PERCENT_J,
        60 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( 
        Test_Array, 
        50 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J,
        60 * $ONE_PERCENT_I, 60 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        50 * $ONE_PERCENT_I, 60 * $ONE_PERCENT_J,
        60 * $ONE_PERCENT_I, 70 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))))  ,
   join (
    join (
     join (
      aggregate (
       between ( 
        Test_Array, 
        50 * $ONE_PERCENT_I, 70 * $ONE_PERCENT_J,
        60 * $ONE_PERCENT_I, 80 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        50 * $ONE_PERCENT_I, 80 * $ONE_PERCENT_J,
        60 * $ONE_PERCENT_I, 90 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( 
        Test_Array, 
        50 * $ONE_PERCENT_I, 90 * $ONE_PERCENT_J,
        60 * $ONE_PERCENT_I, 100 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        60 * $ONE_PERCENT_I, 0 * $ONE_PERCENT_J,
        70 * $ONE_PERCENT_I, 10 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( 
        Test_Array, 
        60 * $ONE_PERCENT_I, 10 * $ONE_PERCENT_J,
        70 * $ONE_PERCENT_I, 20 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        60 * $ONE_PERCENT_I, 20 * $ONE_PERCENT_J,
        70 * $ONE_PERCENT_I, 30 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( 
        Test_Array, 
        60 * $ONE_PERCENT_I, 30 * $ONE_PERCENT_J,
        70 * $ONE_PERCENT_I, 40 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( 
        Test_Array, 
        60 * $ONE_PERCENT_I, 40 * $ONE_PERCENT_J,
        70 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J
       ),
       sum(int32_attr), count(*)))))))
)
"
#
echo "$CMD"
#
date;
/usr/bin/time -f "Q4 %e" iquery -aq "$CMD; $CMD; $CMD;"
ps -eo comm,%mem | grep SciDB-000-0 
#
#  Q5: sum(subarray()) - 10%. 
#  
if [ 1 = 1 ]; then

CMD="
join ( 
  aggregate ( 
    subarray(Test_Array, 
             $ONE_PERCENT_I, $ONE_PERCENT_J,
             50 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J 
    ),
    sum(int32_attr), count(*)
  ),
  aggregate ( 
    subarray(Test_Array, 
             50 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J,
             99 * $ONE_PERCENT_I, 99 * $ONE_PERCENT_J 
    ),
    sum(int32_attr), count(*)
  )
)
"
#
echo "$CMD"
#
date;
ps -eo comm,%mem | grep SciDB-000-0 
/usr/bin/time -f "Q5 %e" iquery -aq "$CMD"
#
else

echo "Q5 DNC"

fi
#
#  Q6: sum ( between ()) - 16 queries, each of which scans 10% of the 
#      input array. 
#
#   NOTE: 10% of the overall array is about 33% of each dimension.   
#
CMD="
join (
 join (
  join (
   join (
    aggregate (
     between(Test_Array, 
              1 * $ONE_PERCENT_I, 1 * $ONE_PERCENT_J, 
              33 * $ONE_PERCENT_I, 33 * $ONE_PERCENT_J),
     sum(int32_attr), count(*))   ,
    aggregate (
     between(Test_Array, 
              33 * $ONE_PERCENT_I, 1 * $ONE_PERCENT_J, 
              66 * $ONE_PERCENT_I, 33 * $ONE_PERCENT_J),
     sum(int32_attr), count(*)))  ,
   join (
    aggregate (
     between(Test_Array, 
              1 * $ONE_PERCENT_I, 33 * $ONE_PERCENT_J, 
              33 * $ONE_PERCENT_I, 66 * $ONE_PERCENT_J),
     sum(int32_attr), count(*))   ,
    aggregate (
     between(Test_Array, 
              33 * $ONE_PERCENT_I, 33 * $ONE_PERCENT_J, 
              66 * $ONE_PERCENT_I, 66 * $ONE_PERCENT_J),
     sum(int32_attr), count(*)))) ,
  join (
   join (
    aggregate (
     between(Test_Array, 
              1 * $ONE_PERCENT_I, 66 * $ONE_PERCENT_J, 
              33 * $ONE_PERCENT_I, 99 * $ONE_PERCENT_J),
     sum(int32_attr), count(*))   ,
    aggregate (
     between(Test_Array, 
              33 * $ONE_PERCENT_I, 66 * $ONE_PERCENT_J, 
              66 * $ONE_PERCENT_I, 99 * $ONE_PERCENT_J),
     sum(int32_attr), count(*)))  ,
   join (
    aggregate (
     between(Test_Array, 
              66 * $ONE_PERCENT_I, 1 * $ONE_PERCENT_J, 
              99 * $ONE_PERCENT_I, 33 * $ONE_PERCENT_J),
     sum(int32_attr), count(*))   ,
    aggregate (
     between(Test_Array, 
              66 * $ONE_PERCENT_I, 33 * $ONE_PERCENT_J, 
              99 * $ONE_PERCENT_I, 66 * $ONE_PERCENT_J),
     sum(int32_attr), count(*))))),
 join (
  join (
   join (
    aggregate (
     between(Test_Array, 
              10* $ONE_PERCENT_I, 10* $ONE_PERCENT_J, 
              43 * $ONE_PERCENT_I, 43 * $ONE_PERCENT_J),
     sum(int32_attr), count(*))   ,
    aggregate (
     between(Test_Array, 
              43 * $ONE_PERCENT_I, 10 * $ONE_PERCENT_J, 
              76 * $ONE_PERCENT_I, 43 * $ONE_PERCENT_J),
     sum(int32_attr), count(*)))  ,
   join (
    aggregate (
     between(Test_Array, 
              10 * $ONE_PERCENT_I, 43 * $ONE_PERCENT_J, 
              43 * $ONE_PERCENT_I, 56 * $ONE_PERCENT_J),
     sum(int32_attr), count(*))   ,
    aggregate (
     between(Test_Array, 
              43 * $ONE_PERCENT_I, 43 * $ONE_PERCENT_J, 
              56 * $ONE_PERCENT_I, 56 * $ONE_PERCENT_J),
     sum(int32_attr), count(*)))) ,
  join (
   join (
    aggregate (
     between(Test_Array, 
              10 * $ONE_PERCENT_I, 76 * $ONE_PERCENT_J, 
              43 * $ONE_PERCENT_I, 109 * $ONE_PERCENT_J),
     sum(int32_attr), count(*))   ,
    aggregate (
     between(Test_Array, 
              43 * $ONE_PERCENT_I, 76 * $ONE_PERCENT_J, 
              76 * $ONE_PERCENT_I, 109 * $ONE_PERCENT_J),
     sum(int32_attr), count(*)))  ,
   join (
    aggregate (
     between(Test_Array, 
              76 * $ONE_PERCENT_I, 10 * $ONE_PERCENT_J, 
              109 * $ONE_PERCENT_I, 43 * $ONE_PERCENT_J),
     sum(int32_attr), count(*))   ,
    aggregate (
     between(Test_Array, 
              76 * $ONE_PERCENT_I, 43 * $ONE_PERCENT_J, 
              109 * $ONE_PERCENT_I, 76 * $ONE_PERCENT_J),
     sum(int32_attr), count(*))))
 )
);
"
#
echo "$CMD"
#
date;
/usr/bin/time -f "Q6 %e" iquery -aq "$CMD; $CMD; $CMD;"
ps -eo comm,%mem | grep SciDB-000-0 
#
#  NOTE: The answers to Q5 and Q6 should be identical.
#
# Q7: sum(between()) - 4 (overlapping) sub-arrays of 25% each. 
#
CMD="
join(
  join ( 
    aggregate ( 
      between(Test_Array, 
              1 * $ONE_PERCENT_I, 1 * $ONE_PERCENT_J, 
              50 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J),
      sum(int64_attr), count(*)),
    aggregate ( 
      between(Test_Array, 
              50 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J, 
              99 * $ONE_PERCENT_I, 99 * $ONE_PERCENT_J),
      avg(int64_attr), count(*))
  ), 
  join ( 
    aggregate ( 
      between(Test_Array, 
              1 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J, 
              50 * $ONE_PERCENT_I, 99 * $ONE_PERCENT_J),
      sum(int32_attr), count(*)),
    aggregate ( 
      between(Test_Array, 
              50 * $ONE_PERCENT_I, 1 * $ONE_PERCENT_J, 
              99 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J),
      avg(int64_attr), count(*))
  )
)
"
#
echo "$CMD"
#
date;
/usr/bin/time -f "Q7 %e" iquery -aq "$CMD; $CMD; $CMD;" 
ps -eo comm,%mem | grep SciDB-000-0 
#
# Q8: sum(filter(between()...) - 2 x 12.5% sub-arrays, 50% filter
#
#  NOTE: Q8 is intended to check the performance of the chunk iterators, 
#        not the performance of the vectorized executor. So there are
#        only relatively simple filter operators here; ones that do not 
#        benefit quite so much from vectorizing. 
#
CMD="
join(
  join ( 
    aggregate ( 
      filter ( 
        between(Test_Array, 
                1 * $ONE_PERCENT_I, 1 * $ONE_PERCENT_J, 
                50 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J),
        double_attr < 0.5 ),
    sum(int32_attr), count(*)),
    aggregate ( 
      filter ( 
        between(Test_Array, 
                50 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J, 
                99 * $ONE_PERCENT_I, 99 * $ONE_PERCENT_J),
        double_attr > 0.5 ),
    avg(int64_attr), count(*))
  ), 
  join ( 
    aggregate ( 
      filter ( 
        between(Test_Array, 
                1 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J, 
                50 * $ONE_PERCENT_I, 99 * $ONE_PERCENT_J),
        double_attr < 0.5 ),
    sum(int32_attr), count(*)),
    aggregate ( 
      filter ( 
        between(Test_Array, 
                50 * $ONE_PERCENT_I, 1 * $ONE_PERCENT_J, 
                99 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J),
        double_attr < 0.5 ),
    avg(int64_attr), count(*))
  )
)
"
#
echo "$CMD"
#
date;
ps -eo comm,%mem | grep SciDB-000-0 
/usr/bin/time -f "Q8 %e" iquery -aq "$CMD; $CMD; $CMD;"
#
# Q9: sum(apply(filter(between()...)...) - 4 x 12.5% sub-arrays, 50% filter
#
CMD="
join(
  join ( 
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 
                  1 * $ONE_PERCENT_I, 1 * $ONE_PERCENT_J, 
                  50 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J),
          double_attr < 0.5 ),
      res,
      int32_attr + int64_attr 
    ),
    sum(res), count(*)),
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 
                  50 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J, 
                  99 * $ONE_PERCENT_I, 99 * $ONE_PERCENT_J),
          double_attr > 0.5 ),
      res,
      int32_attr + int64_attr),
    avg(int64_attr), count(*))
  ), 
  join ( 
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 
                  1 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J, 
                  50 * $ONE_PERCENT_I, 99 * $ONE_PERCENT_J),
          double_attr < 0.5 ),
      res,
      int32_attr + int64_attr),
    sum(int32_attr), count(*)),
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 
                  50 * $ONE_PERCENT_I, 1 * $ONE_PERCENT_J, 
                  99 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J),
          double_attr < 0.5 ),
      res,
      int32_attr + int64_attr),
    avg(int64_attr), count(*))
  )
)
"
#
echo "$CMD"
#
date;
/usr/bin/time -f "Q9 %e" iquery -aq "$CMD"
ps -eo comm,%mem | grep SciDB-000-0 
#
# Q10: sum(apply(filter(between(), dimensions_expr) ...) ...) - 2 x 12.5%, 50% filter
#
#  NOTE: The goal of Q10 is to assess how well we process dimension 
#        coordinates. In these queries, there is no reference at all to 
#        data chunks. In theory Q10 could be answered by reference to the 
#        Empty bitmask alone. 
#
CMD="
join ( 
  aggregate (
    apply(
      filter(
        between(Test_Array, 
                1 * $ONE_PERCENT_I, 1 * $ONE_PERCENT_J, 
                50 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J),
       (I * $LEN_I + J) % 10 < 5
      ),
      add_em,
      I + J
    ),
    sum(add_em),
    count(*)
  ),
  aggregate (
    apply(
      filter(
        between(Test_Array, 
                50 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J, 
                99 * $ONE_PERCENT_I, 99 * $ONE_PERCENT_J),
       (I * $LEN_I + J) % 10 < 5
      ),
      add_em,
      I + J
    ),
    sum(add_em),
    count(*)
  )
)
"
#
echo "$CMD"
#
date;
/usr/bin/time -f "Q10 %e" iquery -aq "$CMD"
ps -eo comm,%mem | grep SciDB-000-0 
# 
# Q11: sum(apply(filter(between()...)...) - 2 x 12.5% sub-array, 50% filter
#
#   NOTE: Q11 is designed to test the effectiveness of the vectorized 
#         executor. The query contains a complex (6 step) expression in 
#         the apply, and a 3 step expression in the filter. When the 
#         number of distinct values is low, this query should benefit 
#         a lot from the RLE encoding. 
#
CMD="
join(
  join (
    aggregate (
      apply (
        filter (
          between(Test_Array, 
                    1 * $ONE_PERCENT_I, 1 * $ONE_PERCENT_J, 
                    50 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J),
          ((double_attr + double_attr) / 2.0)  < 0.5),
      res,
      log(double(((2 * int32_attr) + (2 * int64_attr)) * double_attr))),
    sum(res), count(*)),
    aggregate (
      apply (
        filter (
          between(Test_Array, 
                    50 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J, 
                    99 * $ONE_PERCENT_I, 99 * $ONE_PERCENT_J),
          ((double_attr + double_attr) / 2.0)  > 0.5),
      res,
      log(double(((2 * int32_attr) + (2 * int64_attr)) * double_attr))),
    avg(res), count(*))
  ),
  join (
    aggregate (
      apply (
        filter (
          between(Test_Array, 
                    1 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J, 
                    50 * $ONE_PERCENT_I, 99 * $ONE_PERCENT_J),
          ((double_attr + double_attr) / 2.0) < 0.5),
      res,
      log(double(((2 * int32_attr) + (2 * int64_attr)) * double_attr))),
    sum(res), count(*)),
    aggregate (
      apply (
        filter (
          between(Test_Array, 
                    50 * $ONE_PERCENT_I, $ONE_PERCENT_J, 
                    99 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J),
          ((double_attr + double_attr) / 2.0) > 0.5),
      res,
      log(double(((2 * int32_attr) + (2 * int64_attr)) * double_attr))),
    avg(res), count(*))
  )
)
"
#
echo "$CMD"
#
date;
/usr/bin/time -f "Q11 %e" iquery -aq "$CMD; $CMD; $CMD;"
ps -eo comm,%mem | grep SciDB-000-0 
#
# Q12: regrid(...)
#
#      Regrid the Test_Array into 2% blocks. 
#
CMD="regrid ( 
     Test_Array, 
     $REGRID_I_LEN,
     $REGRID_J_LEN,
     avg(int32_attr) as avg_attr1,
     avg(int64_attr) as avg_attr2, 
     avg(double_attr) as avg_attr3
)"
#
echo "$CMD"
#
date;
/usr/bin/time -f "Q12 %e" iquery -r /dev/null -aq "$CMD" 
ps -eo comm,%mem | grep SciDB-000-0 
#
# Q13: repart()
#
#  NOTE 1: I tried this step at a range of overlapping sizes, from +/1 10 
#          through +/- 100. The timing differences were small, although
#          increasing. For now, going with twice the initial overlap,
#          because that will be OK for the window in Q14. 
#
#  NOTE 2: repart() and window() have big issues with sparse data. The 
#          performance degrades quite quickly as the size of the overlap 
#          increases. 
#        
CHUNK_LEN_I=`dc -e "$2 2 / d 8 % 8 - -1 * n"`
CHUNK_LEN_J=`dc -e "$2 2 / d 8 % 8 - -1 * n"`
#
REPART_OVERLAP_I=`expr $OVERLAP_I "*" 2`
REPART_OVERLAP_J=`expr $OVERLAP_J "*" 2`
#
CMD="store (
  repart ( Test_Array,
             <int32_attr:int32,int64_attr:int64,double_attr:double>
             [I=0:$LEN_I,$CHUNK_LEN_I,$REPART_OVERLAP_I,
              J=0:$LEN_J,$CHUNK_LEN_J,$REPART_OVERLAP_J ]
  ),
  Test_Array_2
)"
#
echo "$CMD"
#
date;
/usr/bin/time -f "Q13 %e" iquery -r /dev/null -aq "$CMD"
ps -eo comm,%mem | grep SciDB-000-0 
#
# Q14: window()
#
#  NOTE: Increasing the window size increases the run-time of this query
#        dramatically. 
#
#  NOTE: Doing the window() for the whole array takes a long time (~30x the 
#        scan time). So instead, let's just perform this query over 33% of 
#        the data.
#
WINDOW_STEP_I=`dc -e "$OVERLAP_I 4 / n"`
WINDOW_STEP_J=`dc -e "$OVERLAP_J 2 / n"`
#        
CMD="
aggregate (
    window (
        between(Test_Array_2, 
                $ONE_PERCENT_I, $ONE_PERCENT_J,
                33 * $ONE_PERCENT_I, 33 * $ONE_PERCENT_J),
        $WINDOW_STEP_I, $WINDOW_STEP_I, $WINDOW_STEP_J, $WINDOW_STEP_J, 
        avg(int32_attr) as avg_attr1,
        avg(int64_attr) as avg_attr2,
        avg(double_attr) as avg_attr3
    ),
    sum ( avg_attr1 ),
    sum ( avg_attr2 ),
    sum ( avg_attr3 )
)"
#
#  NOTE: 
#
#    There's a bug that means I can't query Test_Array_4 without 
#    disconnecting and re-connecting. I'm going to fake that this way: 
CMD1="
store ( 
    window ( 
        between ( Test_Array_2,
                  $ONE_PERCENT_I, $ONE_PERCENT_J,
                  33 * $ONE_PERCENT_I, 33 * $ONE_PERCENT_J),
        $WINDOW_STEP_I, $WINDOW_STEP_I, $WINDOW_STEP_J, $WINDOW_STEP_J, 
        avg(int32_attr) as avg_attr1,
        avg(int64_attr) as avg_attr2,
        avg(double_attr) as avg_attr3
    ),
    Test_Array_4
)"
#
CMD2="aggregate ( 
    Test_Array_4,
    sum ( avg_attr1 ),
    sum ( avg_attr2 ),
    sum ( avg_attr3 )
)"
#
echo "$CMD1;$CMD2"
#
date;
/usr/bin/time -f "Q14 %e" iquery -r /dev/null -aq "$CMD1;$CMD2"
ps -eo comm,%mem | grep SciDB-000-0 
#
# Q15: thin()
#
#  OVERLAP_I and OVERLAP_J are calculated for Test_Array_2. There is a 
#  limit to the thin() operator that, at the moment, the thin() step needs 
#  to evenly divide the chunk_length. As the CHUNK_LEN_I and CHUNK_LEN_J 
#  are evenly divisible by 8, these calculations satisfy that condition. 
#
THIN_STEP_I=`dc -e "$CHUNK_LEN_I 8 / n"`
THIN_STEP_J=`dc -e "$CHUNK_LEN_J 4 / n"`
#
CMD="
aggregate ( 
    thin ( 
        window ( 
            Test_Array_2, 
            $WINDOW_STEP_I, $WINDOW_STEP_I, $WINDOW_STEP_J, $WINDOW_STEP_J, 
            avg(int32_attr) as avg_attr1, 
            avg(int64_attr) as avg_attr2, 
            avg(double_attr) as avg_attr3
        ),
        0, $THIN_STEP_I,
        0, $THIN_STEP_J 
    ),
    sum ( avg_attr1 ),
    sum ( avg_attr2 ),
    sum ( avg_attr3 )
)"
#
echo "$CMD"
#
date;
/usr/bin/time -f "Q15 %e" iquery -aq "$CMD"
ps -eo comm,%mem | grep SciDB-000-0
#
# Q16: redimension_store()
#
CMD="CREATE EMPTY ARRAY Test_Array_3 
    <
      int32_attr  : int32,
      int64_attr  : int64,
      double_attr : double,
      X           : int64,
      Y           : int64
    >
    [ I=0:$LEN_I,$2,0, J=0:$LEN_J,$2,0 ]"
#
time -p iquery -aq "$CMD"
#
#
CMD="redimension_store ( 
  join ( 
    apply ( Test_Array, X, int64(int32_attr % 30) * 11),
    apply ( Test_Array, Y, int64(int64_attr % 30) * 13)
  ),
  Test_Array_3 
)"
#
date;
/usr/bin/time -f "Q16 %e" iquery -r /dev/null -aq "$CMD"
ps -eo comm,%mem | grep SciDB-000-0 
#
# Q17: cross_join()
#
date;
#
CMD="
aggregate (
  cross_join (
    between ( Test_Array,  
              1 * $ONE_PERCENT_I, 25 * $ONE_PERCENT_J, 
              99 * $ONE_PERCENT_I, 50 * $ONE_PERCENT_J ) AS T1,
    between ( Test_Array, 
              25 * $ONE_PERCENT_I,  1 * $ONE_PERCENT_J,
              50 * $ONE_PERCENT_I, 99 * $ONE_PERCENT_J) AS T2,
    T1.I, T2.J, T1.J, T2.I
  ),
  count(*),
  min(T1.int32_attr), max(T1.int32_attr),
  min(T1.int64_attr), max(T1.int64_attr),
  min(T1.double_attr), max(T1.double_attr),
  T1.I
)
"
#
echo "$CMD"
#
/usr/bin/time -f "Q17 %e" iquery -r /dev/null -aq "$CMD"
#
ps -eo comm,%mem | grep SciDB-000-0 
#
#  --== END ==-- 
