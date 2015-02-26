#!/bin/sh
#
#  File: Queries_1.sh 
#
#   This script is intended to run against the Array_1 arrays, which has the 
#   following size and shape: 
#
#  CREATE ARRAY Test_Array <
#     int32_attr  : int32,
#     int64_attr  : int64,
#     double_attr : double
#  >
#  [ I=0:Array_I,Chunk_Len,0, J=0:Array_J,Chunk_Len,0 ]"
#
LEN_I=`expr $1 "*" $2 - 1`;
LEN_J=`expr $1 "*" $2 - 1`;
#
set -v
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
       between ( Test_Array, 100, 100, 900, 900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 1100, 100, 1900, 900),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 100, 1100, 900, 1900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 1100, 1100, 1900, 1900),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 1100, 100, 1900, 900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 3100, 100, 3900, 900),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 1100, 2100, 1900, 2900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 3100, 2100, 3900, 2900),
       sum(int32_attr), count(*)))))  ,
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 100, 1100, 900, 1900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 1100, 1100, 1900, 1900),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 100, 3100, 900, 3900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 1100, 3100, 1900, 3900),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 1100, 1100, 1900, 1900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 3100, 1100, 3900, 1900),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 1100, 3100, 1900, 3900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 3100, 3100, 3900, 3900),
       sum(int32_attr), count(*)))))) ,
  join (
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 1100, 100, 1900, 900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 4100, 100, 4900, 900),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 1100, 1100, 1900, 1900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 4100, 1100, 4900, 1900),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 3100, 100, 3900, 900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 7100, 100, 7900, 900),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 3100, 4100, 3900, 4900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 7100, 4100, 7900, 4900),
       sum(int32_attr), count(*)))))  ,
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 1100, 2100, 1900, 2900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 4100, 2100, 4900, 2900),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 1100, 6100, 1900, 6900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 4100, 6100, 4900, 6900),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 3100, 2100, 3900, 2900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 7100, 2100, 7900, 2900),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 3100, 6100, 3900, 6900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 7100, 6100, 7900, 6900),
       sum(int32_attr), count(*))))))),
 join (
  join (
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 100, 1100, 900, 1900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 1100, 1100, 1900, 1900),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 100, 4100, 900, 4900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 1100, 4100, 1900, 4900),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 1100, 1100, 1900, 1900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 3100, 1100, 3900, 1900),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 1100, 5100, 1900, 5900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 3100, 5100, 3900, 5900),
       sum(int32_attr), count(*)))))  ,
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 100, 3100, 900, 3900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 1100, 3100, 1900, 3900),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 100, 7100, 900, 7900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 1100, 7100, 1900, 7900),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 1100, 3100, 1900, 3900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 3100, 3100, 3900, 3900),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 1100, 7100, 1900, 7900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 3100, 7100, 3900, 7900),
       sum(int32_attr), count(*)))))) ,
  join (
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 1100, 1100, 1900, 1900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 4100, 1100, 4900, 1900),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 1100, 4100, 1900, 4900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 4100, 4100, 4900, 4900),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 3100, 1100, 3900, 1900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 7100, 1100, 7900, 1900),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 3100, 5100, 3900, 5900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 7100, 5100, 7900, 5900),
       sum(int32_attr), count(*)))))  ,
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 1100, 3100, 1900, 3900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 4100, 3100, 4900, 3900),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 1100, 7100, 1900, 7900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 4100, 7100, 4900, 7900),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 3100, 3100, 3900, 3900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 7100, 3100, 7900, 3900),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 3100, 7100, 3900, 7900),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 7100, 7100, 7900, 7900),
       sum(int32_attr), count(*))))))));
"
#
#  Q5: sum(subarray()) - 10%. 
#
if [ 1 = 0 ]; then

date;
/usr/bin/time -f "Q5 %e" iquery -aq "
join ( 
  aggregate ( subarray(Test_Array, 60, 60, 460, 460), 
              sum(int32_attr), count(*)),
  aggregate ( subarray(Test_Array, 3460, 3460, 3860, 3860), 
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
     between ( Test_Array, -200, -200, 2200, 2200),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 1800, -200, 4200, 2200),
     sum(int32_attr), count(*)))  ,
   join (
    aggregate (
     between ( Test_Array, -200, 1800, 2200, 4200),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 1800, 1800, 4200, 4200),
     sum(int32_attr), count(*)))) ,
  join (
   join (
    aggregate (
     between ( Test_Array, 1800, -200, 4200, 2200),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 5800, -200, 8200, 2200),
     sum(int32_attr), count(*)))  ,
   join (
    aggregate (
     between ( Test_Array, 1800, 3800, 4200, 6200),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 5800, 3800, 8200, 6200),
     sum(int32_attr), count(*))))),
 join (
  join (
   join (
    aggregate (
     between ( Test_Array, -200, 1800, 2200, 4200),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 1800, 1800, 4200, 4200),
     sum(int32_attr), count(*)))  ,
   join (
    aggregate (
     between ( Test_Array, -200, 5800, 2200, 8200),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 1800, 5800, 4200, 8200),
     sum(int32_attr), count(*)))) ,
  join (
   join (
    aggregate (
     between ( Test_Array, 1800, 1800, 4200, 4200),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 5800, 1800, 8200, 4200),
     sum(int32_attr), count(*)))  ,
   join (
    aggregate (
     between ( Test_Array, 1800, 5800, 4200, 8200),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 5800, 5800, 8200, 8200),
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
    aggregate ( between(Test_Array, 60, 60, 4060, 4060), 
        sum(int32_attr), count(*)),
    aggregate ( between(Test_Array, 3860, 3860, 7860, 7860), 
        avg(int64_attr), count(*))
  ), 
  join ( 
    aggregate ( between(Test_Array, 60, 3860, 4060, 7860), 
        sum(int32_attr), count(*)),
    aggregate ( between(Test_Array, 3860, 60, 7860, 4060), 
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
         between(Test_Array, 60, 60, 4060, 4060), 
        double_attr < 0.5 ),
    sum(int32_attr), count(*)),
    aggregate ( 
      filter ( 
        between(Test_Array, 3860, 3860, 7860, 7860), 
        double_attr > 0.5 ),
    avg(int64_attr), count(*))
  ), 
  join ( 
    aggregate ( 
      filter ( 
        between(Test_Array, 60, 3860, 4060, 7860), 
        double_attr < 0.5 ),
    sum(int32_attr), count(*)),
    aggregate ( 
      filter ( 
        between(Test_Array, 3860, 60, 7860, 4060), 
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
          between(Test_Array, 60, 60, 4060, 4060), 
          double_attr < 0.5 ),
      res,
      int32_attr + int64_attr 
    ),
    sum(res), count(*)),
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 3860, 3860, 7860, 7860), 
          double_attr > 0.5 ),
      res,
      int32_attr + int64_attr),
    avg(int64_attr), count(*))
  ), 
  join ( 
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 60, 3860, 4060, 7860), 
          double_attr < 0.5 ),
      res,
      int32_attr + int64_attr),
    sum(int32_attr), count(*)),
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 3860, 60, 7860, 4060), 
          double_attr < 0.5 ),
      res,
      int32_attr + int64_attr),
    avg(int64_attr), count(*))
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
        between(Test_Array, 60, 60, 4060, 4060),
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
        between(Test_Array, 3860, 3860, 7860, 7860),
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
          between(Test_Array, 60, 60, 4060, 4060), 
          ((double_attr + double_attr) / 2.0)  < 0.5),
      res,
      log(double(((2 * int32_attr) + (2 * int64_attr)) * double_attr))),
    sum(res), count(*)),
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 3860, 3860, 7860, 7860), 
          ((double_attr + double_attr) / 2.0)  > 0.5),
      res,
      log(double(((2 * int32_attr) + (2 * int64_attr)) * double_attr))),
    avg(res), count(*))
  ), 
  join ( 
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 60, 3860, 4060, 7860), 
          ((double_attr + double_attr) / 2.0) < 0.5),
      res,
      log(double(((2 * int32_attr) + (2 * int64_attr)) * double_attr))),
    sum(res), count(*)),
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 3860, 60, 7860, 4060), 
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
     80,80, 
     avg(int32_attr), avg(int64_attr), avg(double_attr))"
#
# Q13: repart()
#
#  NOTE 1: I tried this step at a range of overlapping sizes, from +/1 10 
#          through +/- 100. The timing differences were small, although
#          increasing. For now, going with +/- 100, because that will 
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
             <int32_attr:int32 NOT NULL,int64_attr:int64 NOT NULL,double_attr:double NOT NULL> 
             [I=0:7999,1000,100,J=0:7999,1000,100]
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
   10,10, 
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
    
    between ( Test_Array,  100, 2000, 7900, 5000) AS T1,
    between ( Test_Array, 2000,  100, 5000, 7900) AS T2, 

    T1.I, T2.J, T1.J, T2.I
  ),
  count(*),
  min(T1.int32_attr), max(T1.int32_attr),
  min(T1.int64_attr), max(T1.int64_attr),
  min(T1.double_attr), max(T1.double_attr),
  T1.I
)
"

