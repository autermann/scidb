#!/bin/sh
#
#  File: Queries_2.sh 
#
#   This script is intended to run against the Array_1 arrays, which has the 
#   following size and shape: 
#
#  CREATE ARRAY Test_Array <
#     int32_attr  : int32,
#     int64_attr  : int64,
#     double_attr : double
#  >
#  [ I=0:79999,100000,00, J=0:79999, 100000,0 ]"
#
set -v
#
LEN_I=`expr $1 "*" $2 - 1`;
LEN_J=`expr $1 "*" $2 - 1`;
#
#  Q0: Check the size and shape of the array we'll be working with. 
#
date;
/usr/bin/time -f "%e" iquery -aq "show ( Test_Array )"
#
#  Q1: Simple sum() and count().
#
date;
/usr/bin/time -f "Q1 %e" iquery -aq "
join ( 
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
#  Q2: Simple sum() with group-by on column major order
#
date;
/usr/bin/time -f "Q2 %e" iquery -naq "
aggregate ( Test_Array, sum(int32_attr), I )"
#
#  Q3: Simple sum() with group-by on row major order
#
date;
/usr/bin/time -f "Q3 %e" iquery -naq "
aggregate ( Test_Array, sum(int32_attr), J )"
#
#  Q4: sum(between()) - 1%. 
#
date;
/usr/bin/time -f "Q4 %e" iquery -aq "
join (
 join (
  join (
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 1000, 1000, 9000, 9000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 11000, 1000, 19000, 9000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 1000, 11000, 9000, 19000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 11000, 11000, 19000, 19000),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 11000, 1000, 19000, 9000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 31000, 1000, 39000, 9000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 11000, 21000, 19000, 29000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 31000, 21000, 39000, 29000),
       sum(int32_attr), count(*)))))  ,
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 1000, 11000, 9000, 19000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 11000, 11000, 19000, 19000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 1000, 31000, 9000, 39000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 11000, 31000, 19000, 39000),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 11000, 11000, 19000, 19000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 31000, 11000, 39000, 19000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 11000, 31000, 19000, 39000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 31000, 31000, 39000, 39000),
       sum(int32_attr), count(*)))))) ,
  join (
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 11000, 1000, 19000, 9000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 41000, 1000, 49000, 9000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 11000, 11000, 19000, 19000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 41000, 11000, 49000, 19000),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 31000, 1000, 39000, 9000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 71000, 1000, 79000, 9000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 31000, 41000, 39000, 49000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 71000, 41000, 79000, 49000),
       sum(int32_attr), count(*)))))  ,
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 11000, 21000, 19000, 29000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 41000, 21000, 49000, 29000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 11000, 61000, 19000, 69000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 41000, 61000, 49000, 69000),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 31000, 21000, 39000, 29000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 71000, 21000, 79000, 29000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 31000, 61000, 39000, 69000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 71000, 61000, 79000, 69000),
       sum(int32_attr), count(*))))))),
 join (
  join (
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 1000, 11000, 9000, 19000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 11000, 11000, 19000, 19000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 1000, 41000, 9000, 49000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 11000, 41000, 19000, 49000),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 11000, 11000, 19000, 19000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 31000, 11000, 39000, 19000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 11000, 51000, 19000, 59000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 31000, 51000, 39000, 59000),
       sum(int32_attr), count(*)))))  ,
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 1000, 31000, 9000, 39000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 11000, 31000, 19000, 39000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 1000, 71000, 9000, 79000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 11000, 71000, 19000, 79000),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 11000, 31000, 19000, 39000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 31000, 31000, 39000, 39000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 11000, 71000, 19000, 79000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 31000, 71000, 39000, 79000),
       sum(int32_attr), count(*)))))) ,
  join (
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 11000, 11000, 19000, 19000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 41000, 11000, 49000, 19000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 11000, 41000, 19000, 49000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 41000, 41000, 49000, 49000),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 31000, 11000, 39000, 19000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 71000, 11000, 79000, 19000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 31000, 51000, 39000, 59000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 71000, 51000, 79000, 59000),
       sum(int32_attr), count(*)))))  ,
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 11000, 31000, 19000, 39000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 41000, 31000, 49000, 39000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 11000, 71000, 19000, 79000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 41000, 71000, 49000, 79000),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 31000, 31000, 39000, 39000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 71000, 31000, 79000, 39000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 31000, 71000, 39000, 79000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 71000, 71000, 79000, 79000),
       sum(int32_attr), count(*))))))));
"
#
#  Q5: sum(subarray()) - 10%. 
#
if [ 1 = 0 ]; then

date;
/usr/bin/time -f "Q5 %e" iquery -aq "
join ( 
  aggregate ( subarray(Test_Array, 600, 600, 40600, 40600), 
              sum(int32_attr), count(*)),
  aggregate ( subarray(Test_Array, 34600, 34600, 38600, 38600), 
              sum(int32_attr), count(*))
)"

else

echo "Q5 DNC"

fi
#
#  NOTE: I could write a query that processes (say) 50% of the input 
#        matrix by joining a lot of 10% subarays() together. Unfortunately 
#        the resulting query doesn't complete on even moderately large 
#        sparse arrays. 
#
#  Q6: sum ( between ()) - 10% 
#
date;
/usr/bin/time -f "Q6 %e" iquery -aq "
join (
 join (
  join (
   join (
    aggregate (
     between ( Test_Array, -2000, -2000, 22000, 22000),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 18000, -2000, 42000, 22000),
     sum(int32_attr), count(*)))  ,
   join (
    aggregate (
     between ( Test_Array, -2000, 18000, 22000, 42000),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 18000, 18000, 42000, 42000),
     sum(int32_attr), count(*)))) ,
  join (
   join (
    aggregate (
     between ( Test_Array, 18000, -2000, 42000, 22000),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 58000, -2000, 82000, 22000),
     sum(int32_attr), count(*)))  ,
   join (
    aggregate (
     between ( Test_Array, 18000, 38000, 42000, 62000),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 58000, 38000, 82000, 62000),
     sum(int32_attr), count(*))))),
 join (
  join (
   join (
    aggregate (
     between ( Test_Array, -2000, 18000, 22000, 42000),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 18000, 18000, 42000, 42000),
     sum(int32_attr), count(*)))  ,
   join (
    aggregate (
     between ( Test_Array, -2000, 58000, 22000, 82000),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 18000, 58000, 42000, 82000),
     sum(int32_attr), count(*)))) ,
  join (
   join (
    aggregate (
     between ( Test_Array, 18000, 18000, 42000, 42000),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 58000, 18000, 82000, 42000),
     sum(int32_attr), count(*)))  ,
   join (
    aggregate (
     between ( Test_Array, 18000, 58000, 42000, 82000),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 58000, 58000, 82000, 82000),
     sum(int32_attr), count(*))))));
"
#
#  NOTE: The answers to Q5 and Q6 should be identical.
#
# Q7: sum(between()) - 2 (overlapping) sub-arrays of 25% each. 
#
date;
/usr/bin/time -f "Q7 %e" iquery -aq "
join(
  join ( 
    aggregate ( between(Test_Array, 600, 600, 40600, 40600), 
        sum(int32_attr), count(*)),
    aggregate ( between(Test_Array, 38600, 38600, 78600, 78600), 
        avg(int64_attr), count(*))
  ), 
  join ( 
    aggregate ( between(Test_Array, 600, 38600, 40600, 78600), 
        sum(int32_attr), count(*)),
    aggregate ( between(Test_Array, 38600, 600, 78600, 40600), 
        avg(int64_attr), count(*))
  )
)"
#
# Q8: sum(filter(between()...) - 2 x 12.5% sub-arrays, 50% filter
#
#  NOTE: Q8 is intended to check the performance of the chunk iterators, 
#        not the performance of the vectorized executor. So there are
#        only relatively simple filter operators here; ones that do not 
#        benefit quite so much from vectorizing. 
# 
date;
/usr/bin/time -f "Q8 %e" iquery -aq "
join(
  join ( 
    aggregate ( 
      filter ( 
         between(Test_Array, 600, 600, 40600, 40600), 
        double_attr < 0.5 ),
    sum(int32_attr), count(*)),
    aggregate ( 
      filter ( 
        between(Test_Array, 38600, 38600, 78600, 78600), 
        double_attr > 0.5 ),
    avg(int64_attr), count(*))
  ), 
  join ( 
    aggregate ( 
      filter ( 
        between(Test_Array, 600, 38600, 40600, 78600), 
        double_attr < 0.5 ),
    sum(int32_attr), count(*)),
    aggregate ( 
      filter ( 
        between(Test_Array, 38600, 600, 78600, 40600), 
        double_attr < 0.5 ),
    avg(int64_attr), count(*))
  )
)
"
#
# Q9: sum(apply(filter(between()...)...) - 2 x 12.5% sub-arrays, 50% filter
#
#  NOTE: Q9 is intended to check the performance of the chunk iterators, 
#        not the performance of the vectorized executor. The goal here is 
#        to see how efficiently we can pull data out of a pair of chunk 
#        lists for different attributes, align them, and so on. 
#
date;
/usr/bin/time -f "Q9 %e" iquery -aq "
join(
  join ( 
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 600, 600, 40600, 40600), 
          double_attr < 0.5 ),
      res,
      int32_attr + int64_attr 
    ),
    sum(res), count(*)),
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 38600, 38600, 78600, 78600), 
          double_attr > 0.5 ),
      res,
      int32_attr + int64_attr),
    avg(res), count(*))
  ), 
  join ( 
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 600, 38600, 40600, 78600), 
          double_attr < 0.5 ),
      res,
      int32_attr + int64_attr),
    sum(res), count(*)),
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 38600, 600, 78600, 40600), 
          double_attr < 0.5 ),
      res,
      int32_attr + int64_attr),
    avg(res), count(*))
  )
)
"
#
# Q10: sum(apply(filter(between(), dimensions_expr) ...) ...) - 2 x 12.5%, 50% filter
#
#  NOTE: The goal of Q10 is to assess how well we process dimension 
#        coordinates. In these queries, there is no reference at all to 
#        data chunks. In theory Q10 could be answered by reference to the 
#        Empty bitmask alone. 
#
date;
/usr/bin/time -f "Q10 %e" iquery -aq "
join ( 
  aggregate (
    apply(
      filter(
        between(Test_Array, 600, 600, 40600, 40600),
       (I * 8000 + J) % 10 < 5
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
        between(Test_Array, 38600, 38600, 78600, 78600),
       (I * 8000 + J) % 10 < 5
      ),
      add_em,
      I + J
    ),
    sum(add_em),
    count(*)
  )
)"
# 
# Q11: sum(apply(filter(between()...)...) - 2 x 12.5% sub-array, 50% filter
#
#   NOTE: Q11 is designed to test the effectiveness of the vectorized 
#         executor. The query contains a complex (6 step) expression in 
#         the apply, and a 3 step expression in the filter. It should 
#         benefit a lot from the RLE encoding. 
#
date;
/usr/bin/time -f "Q11 %e" iquery -aq "
join(
  join ( 
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 600, 600, 40600, 40600), 
          ((double_attr + double_attr) / 2.0)  < 0.5),
      res,
      log(double(((2 * int32_attr) + (2 * int64_attr)) * double_attr))),
    sum(res), count(*)),
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 38600, 38600, 78600, 78600), 
          ((double_attr + double_attr) / 2.0)  > 0.5),
      res,
      log(double(((2 * int32_attr) + (2 * int64_attr)) * double_attr))),
    avg(res), count(*))
  ), 
  join ( 
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 600, 38600, 40600, 78600), 
          ((double_attr + double_attr) / 2.0) < 0.5),
      res,
      log(double(((2 * int32_attr) + (2 * int64_attr)) * double_attr))),
    sum(res), count(*)),
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 38600, 600, 78600, 40600), 
          ((double_attr + double_attr) / 2.0) > 0.5),
      res,
      log(double(((2 * int32_attr) + (2 * int64_attr)) * double_attr))),
    avg(res), count(*))
  )
)
"
#
# Q12: regrid(...)
#
#      Regrid the Test_Array into 1% blocks. 
#
date;
/usr/bin/time -f "Q12 %e" iquery -naq "
regrid ( Test_Array, 
     800,800, 
     avg(int32_attr), avg(int64_attr), avg(double_attr))"
#
# Q13: repart()
#
#  NOTE 1: I tried this step at a range of overlapping sizes, from +/1 10 
#          through +/- 100. The timing differences were small, although
#          increasing. For now, going with +/- 1000, because that will 
#          support the Q14. 
#
#  NOTE 2: repart() and window() have big issues with sparse data. The 
#          performance degrades quite quickly as the size of the overlap 
#          increases. 
#        
if [ 1 = 1 ]; then 

date;
/usr/bin/time -f "Q13 %e" iquery -naq "
store ( 
  repart ( Test_Array, 
           EMPTY <int32_attr:int32 NOT NULL,int64_attr:int64 NOT NULL,double_attr:double NOT NULL> 
             [I=0:79999,10000,1000,J=0:79999,10000,1000]
  ),
  Test_Array_2
)"
#
# Q14: window()
#
#  NOTE: Increasing the window size increases the run-time of this query
#        dramatically. 
#
date;
/usr/bin/time -f "Q14 %e" iquery -naq "
window ( Test_Array_2, 
   100,100, 
   avg(int32_attr), avg(int64_attr), avg(double_attr))"

else 

echo "Q13 DNC"
echo "Q14 DNC"

fi 

#
# Q15: redimension_store()
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
date;
/usr/bin/time -f "Q15 %e" iquery -naq "
redimension_store ( 
  join ( 
    apply ( Test_Array, X, int64(int32_attr % 30) * 11),
    apply ( Test_Array, Y, int64(int64_attr % 30) * 13)
  ),
  Test_Array_3 
)"
#
# Q16: cross_join()
#
date;
/usr/bin/time -f "Q16 %e" iquery -naq "
aggregate ( 
  cross_join ( 
    
                between ( Test_Array,  1000, 20000, 79000, 50000) AS T1,
                between ( Test_Array, 20000,  1000, 50000, 79000) AS T2, 

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
