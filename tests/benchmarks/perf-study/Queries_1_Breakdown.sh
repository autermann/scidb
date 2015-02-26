#!/bin/sh
#
#  File: Queries_1_Breakdown.sh 
#
#   This script is intended to run against the Array_1 arrays, which has the 
#   following size and shape: 
#
#  CREATE ARRAY Test_Array <
#     int32_attr  : int32,
#     int64_attr  : int64,
#     double_attr : double
#  >
#  [ I=0:3999,1000,0, J=0:3999, 1000,0 ]"
#
#   The goal is to break down the longer, more complex queries in the 
#   Queries_1.sh file into more discrete queries, each focussing on a single 
#   part of the complex; a single operator or idea. 
#
set -v
#
#  Q1: Simple aggregates; min(), max(), sum(), avg(), stdev(), one for each 
#      attribute. 
#
/usr/bin/time -f "Breakdown Q1.1 %e" iquery -aq "
count ( Test_Array )"
#
/usr/bin/time -f "Breakdown Q1.2 %e" iquery -aq "
aggregate ( Test_Array, min(double_attr))"
#
/usr/bin/time -f "Breakdown Q1.3 %e" iquery -aq "
aggregate ( Test_Array, max(double_attr))"
#
/usr/bin/time -f "Breakdown Q1.4 %e" iquery -aq "
aggregate ( Test_Array, sum(double_attr))"
#
/usr/bin/time -f "Breakdown Q1.5 %e" iquery -aq "
aggregate ( Test_Array, avg(double_attr))"
#
/usr/bin/time -f "Breakdown Q1.6 %e" iquery -aq "
aggregate ( Test_Array, avg(double_attr))"
#
/usr/bin/time -f "Breakdown Q1.7 %e" iquery -aq "
aggregate ( Test_Array, min(int32_attr))"
#
/usr/bin/time -f "Breakdown Q1.8 %e" iquery -aq "
aggregate ( Test_Array, max(int32_attr))"
#
/usr/bin/time -f "Breakdown Q1.9 %e" iquery -aq "
aggregate ( Test_Array, sum(int32_attr))"
#
/usr/bin/time -f "Breakdown Q1.10 %e" iquery -aq "
aggregate ( Test_Array, avg(int32_attr))"
#
/usr/bin/time -f "Breakdown Q1.11 %e" iquery -aq "
aggregate ( Test_Array,  stddev(int32_attr))"
#
/usr/bin/time -f "Breakdown Q1.12 %e" iquery -aq "
aggregate ( Test_Array, min(int64_attr))"
#
/usr/bin/time -f "Breakdown Q1.13 %e" iquery -aq "
aggregate ( Test_Array, max(int64_attr))"
#
/usr/bin/time -f "Breakdown Q1.14 %e" iquery -aq "
aggregate ( Test_Array, sum(int64_attr))"
#
/usr/bin/time -f "Breakdown Q1.15 %e" iquery -aq "
aggregate ( Test_Array, avg(int64_attr))"
#
/usr/bin/time -f "Breakdown Q1.16 %e" iquery -aq "
aggregate ( Test_Array, stddev(int64_attr))"
#
#  Quick test to see whether the aggregate() works as well as we hoped. 
#
/usr/bin/time -f "Breakdown Q1.17 %e" iquery -aq "
aggregate ( Test_Array, 
    count(*),
    min(double_attr),
    max(double_attr),
    sum(double_attr),
    avg(double_attr),
    stddev(double_attr),
    min(int32_attr),
    max(int32_attr),
    sum(int32_attr),
    avg(int32_attr),
    stddev(int32_attr),
    min(int64_attr),
    max(int64_attr),
    sum(int64_attr),
    avg(int64_attr),
    stddev(int32_attr)
)"
#
#  Q2: between() queries - using a count(*) to minimize the return results. 
#      Note that there are a lot of them here, but it's important to see what 
#      the differences are between small "snap-shot" queries, and the larger 
#      between() queries that process a lot of data. 
#
#      This little block creates 100 queries, each of which grabs 1% of the 
#      data in the Test_Array, and counts how many values it finds in that 
#      1% block. 
#
for i in 0 1 2 3 4 5 6 7 8 9; do
        MinI=`expr \$i "*" 400`;
        MaxI=`expr $MinI + 40`;
        for j in 0 1 2 3 4 5 6 7 8 9; do
                Q=`expr $i "*" 10 + $j + 1`;
                MinJ=`expr $j "*" 400`;
                MaxJ=`expr $MinJ + 40`;

echo            /usr/bin/time -f "Q2.$Q %e" iquery -aq "count(between(Test_Array, $MinI, $MinJ, $MaxI, $MaxJ))"

        done;
done;
#
#  Q3: count ( between ()) - 10% 
#
for i in 0 1 2 3 4 5 6 7 8 9; do
        MinI=`expr \$i "*" 400`;
        MaxI=`expr $MinI + 400`;
        for j in 0 1 2 3 4 5 6 7 8 9; do
                Q=`expr $i "*" 10 + $j + 1`;
                MinJ=`expr $j "*" 400`;
                MaxJ=`expr $MinJ + 400`;

echo            /usr/bin/time -f "Q2.$Q %e" iquery -aq "count(between(Test_Array, $MinI, $MinJ, $MaxI, $MaxJ))"

        done;
done;
#
#  NOTE: The subarray() problem is particularly bad (the cost of subarray() is 
#        100x the cost of between()) but a single query like Q5 is enough to 
#        reproduce the problem. 
#
#  Q3: count ( between() ) - 3 overlapping sub-arrays of 25% each. 
#
/usr/bin/time -f "Q3.1 %e" iquery -aq "
join (
  aggregate ( between(Test_Array, 60, 60, 2060, 2060), 
              count(*)),
  join ( 
  	aggregate ( between(Test_Array, 760, 760, 2760, 2760), 
                    count(*)),
        aggregate ( between(Test_Array, 1960, 1960, 3960, 3960), 
                    count(*))
  )
)"
#
#  Q4:  count ( filter () ) - scan the array 10 times, filtering 10, 20, etc% 
#       out each time. 
#
for i in 0 1 2 3 4 5 6 7 8 9; do
    Q=`expr \$i + 1`;
    /usr/bin/time -f "Q4.$Q %e" iquery -aq "count(filter(Test_Array, double_attr < 0.$i))"
done
#
#  Q5: sum ( apply ( Test_Array, complex_expr ))
#
/usr/bin/time -f "Q5.1 %e" iquery -aq "
aggregate(
  apply(
    Test_Array, 
    res, 
    1.0+(2.0*(log(double_attr+2.0)+sin(double_attr*2.0)+cos(double_attr-1.0)))
  ),
  sum ( res ) 
)"
#
#  Q6: sum ( apply ( Test_Array, dimension_expr ) ) - manipulate dimensions
#
/usr/bin/time -f "Q6.1 %e" iquery -aq "
aggregate ( 
   apply (
      Test_Array, 
      res, 
      (I * 4000 + J) % 10
   ),
   sum( res ) 
)"
# 
# Q7: regrid(...) Regrid the Test_Array into 1%, 2% and 4%  blocks, 
#      using count(*) only
#
N=0;
for i in 4 8 16; do
    Q=`expr \$i "*" 10`;
	N=`expr \$N + 1`;
/usr/bin/time -f "Q7.$N %e" iquery -naq "
regrid ( Test_Array, 
		 $Q,$Q, 
         count(*)
)"
done
#
