#!/bin/sh
#
#  File: Queries_3.sh 
#
#   This script is intended to run against the Array_1 arrays, which has the 
#   following size and shape: 
#
#  CREATE ARRAY Test_Array <
#     int32_attr  : int32,
#     int64_attr  : int64,
#     double_attr : double
#  >
#  [ I=0:Array_I,Chunk_Len_I,0, J=0:Array_J, Chunk_Len_J,0 ]"
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
       between ( Test_Array, 10000, 10000, 90000, 90000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 110000, 10000, 190000, 90000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 10000, 110000, 90000, 190000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 110000, 110000, 190000, 190000),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 110000, 10000, 190000, 90000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 310000, 10000, 390000, 90000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 110000, 210000, 190000, 290000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 310000, 210000, 390000, 290000),
       sum(int32_attr), count(*)))))  ,
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 10000, 110000, 90000, 190000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 110000, 110000, 190000, 190000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 10000, 310000, 90000, 390000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 110000, 310000, 190000, 390000),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 110000, 110000, 190000, 190000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 310000, 110000, 390000, 190000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 110000, 310000, 190000, 390000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 310000, 310000, 390000, 390000),
       sum(int32_attr), count(*)))))) ,
  join (
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 110000, 10000, 190000, 90000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 410000, 10000, 490000, 90000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 110000, 110000, 190000, 190000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 410000, 110000, 490000, 190000),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 310000, 10000, 390000, 90000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 710000, 10000, 790000, 90000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 310000, 410000, 390000, 490000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 710000, 410000, 790000, 490000),
       sum(int32_attr), count(*)))))  ,
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 110000, 210000, 190000, 290000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 410000, 210000, 490000, 290000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 110000, 610000, 190000, 690000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 410000, 610000, 490000, 690000),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 310000, 210000, 390000, 290000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 710000, 210000, 790000, 290000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 310000, 610000, 390000, 690000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 710000, 610000, 790000, 690000),
       sum(int32_attr), count(*))))))),
 join (
  join (
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 10000, 110000, 90000, 190000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 110000, 110000, 190000, 190000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 10000, 410000, 90000, 490000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 110000, 410000, 190000, 490000),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 110000, 110000, 190000, 190000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 310000, 110000, 390000, 190000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 110000, 510000, 190000, 590000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 310000, 510000, 390000, 590000),
       sum(int32_attr), count(*)))))  ,
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 10000, 310000, 90000, 390000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 110000, 310000, 190000, 390000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 10000, 710000, 90000, 790000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 110000, 710000, 190000, 790000),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 110000, 310000, 190000, 390000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 310000, 310000, 390000, 390000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 110000, 710000, 190000, 790000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 310000, 710000, 390000, 790000),
       sum(int32_attr), count(*)))))) ,
  join (
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 110000, 110000, 190000, 190000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 410000, 110000, 490000, 190000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 110000, 410000, 190000, 490000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 410000, 410000, 490000, 490000),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 310000, 110000, 390000, 190000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 710000, 110000, 790000, 190000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 310000, 510000, 390000, 590000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 710000, 510000, 790000, 590000),
       sum(int32_attr), count(*)))))  ,
   join (
    join (
     join (
      aggregate (
       between ( Test_Array, 110000, 310000, 190000, 390000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 410000, 310000, 490000, 390000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 110000, 710000, 190000, 790000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 410000, 710000, 490000, 790000),
       sum(int32_attr), count(*))))   ,
    join (
     join (
      aggregate (
       between ( Test_Array, 310000, 310000, 390000, 390000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 710000, 310000, 790000, 390000),
       sum(int32_attr), count(*)))    ,
     join (
      aggregate (
       between ( Test_Array, 310000, 710000, 390000, 790000),
       sum(int32_attr), count(*))     ,
      aggregate (
       between ( Test_Array, 710000, 710000, 790000, 790000),
       sum(int32_attr), count(*))))))));
"
#
#  Q5: sum(subarray()) - 10%. 
#
if [ 1 = 0 ]; then

date;
/usr/bin/time -f "Q5 %e" iquery -aq "
join ( 
  aggregate ( subarray(Test_Array, 6000, 6000, 406000, 406000), 
              sum(int32_attr), count(*)),
  aggregate ( subarray(Test_Array, 346000, 346000, 386000, 386000), 
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
     between ( Test_Array, -20000, -20000, 220000, 220000),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 180000, -20000, 420000, 220000),
     sum(int32_attr), count(*)))  ,
   join (
    aggregate (
     between ( Test_Array, -20000, 180000, 220000, 420000),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 180000, 180000, 420000, 420000),
     sum(int32_attr), count(*)))) ,
  join (
   join (
    aggregate (
     between ( Test_Array, 180000, -20000, 420000, 220000),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 580000, -20000, 820000, 220000),
     sum(int32_attr), count(*)))  ,
   join (
    aggregate (
     between ( Test_Array, 180000, 380000, 420000, 620000),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 580000, 380000, 820000, 620000),
     sum(int32_attr), count(*))))),
 join (
  join (
   join (
    aggregate (
     between ( Test_Array, -20000, 180000, 220000, 420000),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 180000, 180000, 420000, 420000),
     sum(int32_attr), count(*)))  ,
   join (
    aggregate (
     between ( Test_Array, -20000, 580000, 220000, 820000),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 180000, 580000, 420000, 820000),
     sum(int32_attr), count(*)))) ,
  join (
   join (
    aggregate (
     between ( Test_Array, 180000, 180000, 420000, 420000),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 580000, 180000, 820000, 420000),
     sum(int32_attr), count(*)))  ,
   join (
    aggregate (
     between ( Test_Array, 180000, 580000, 420000, 820000),
     sum(int32_attr), count(*))   ,
    aggregate (
     between ( Test_Array, 580000, 580000, 820000, 820000),
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
    aggregate ( between(Test_Array, 6000, 6000, 406000, 406000), 
        sum(int32_attr), count(*)),
    aggregate ( between(Test_Array, 386000, 386000, 786000, 786000), 
        avg(int64_attr), count(*))
  ), 
  join ( 
    aggregate ( between(Test_Array, 6000, 386000, 406000, 786000), 
        sum(int32_attr), count(*)),
    aggregate ( between(Test_Array, 386000, 6000, 786000, 406000), 
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
         between(Test_Array, 6000, 6000, 406000, 406000), 
        double_attr < 0.5 ),
    sum(int32_attr), count(*)),
    aggregate ( 
      filter ( 
        between(Test_Array, 386000, 386000, 786000, 786000), 
        double_attr > 0.5 ),
    avg(int64_attr), count(*))
  ), 
  join ( 
    aggregate ( 
      filter ( 
        between(Test_Array, 6000, 386000, 406000, 786000), 
        double_attr < 0.5 ),
    sum(int32_attr), count(*)),
    aggregate ( 
      filter ( 
        between(Test_Array, 386000, 6000, 786000, 406000), 
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
          between(Test_Array, 6000, 6000, 406000, 406000), 
          double_attr < 0.5 ),
      res,
      int32_attr + int64_attr 
    ),
    sum(res), count(*)),
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 386000, 386000, 786000, 786000), 
          double_attr > 0.5 ),
      res,
      int32_attr + int64_attr),
    avg(res), count(*))
  ), 
  join ( 
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 6000, 386000, 406000, 786000), 
          double_attr < 0.5 ),
      res,
      int32_attr + int64_attr),
    sum(res), count(*)),
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 386000, 6000, 786000, 406000), 
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
        between(Test_Array, 6000, 6000, 406000, 406000),
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
        between(Test_Array, 386000, 386000, 786000, 786000),
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
          between(Test_Array, 6000, 6000, 406000, 406000), 
          ((double_attr + double_attr) / 2.0)  < 0.5),
      res,
      log(double(((2 * int32_attr) + (2 * int64_attr)) * double_attr))),
    sum(res), count(*)),
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 386000, 386000, 786000, 786000), 
          ((double_attr + double_attr) / 2.0)  > 0.5),
      res,
      log(double(((2 * int32_attr) + (2 * int64_attr)) * double_attr))),
    avg(res), count(*))
  ), 
  join ( 
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 6000, 386000, 406000, 786000), 
          ((double_attr + double_attr) / 2.0) < 0.5),
      res,
      log(double(((2 * int32_attr) + (2 * int64_attr)) * double_attr))),
    sum(res), count(*)),
    aggregate ( 
      apply ( 
        filter ( 
          between(Test_Array, 386000, 6000, 786000, 406000), 
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
     8000,8000, 
     avg(int32_attr), avg(int64_attr), avg(double_attr))"
#
# Q13: repart()
#
#  NOTE 1: I tried this step at a range of overlapping sizes, from +/1 10 
#          through +/- 100. The timing differences were small, although
#          increasing. For now, going with +/- 10000, because that will 
#          support the Q14. 
#
#  NOTE 2: repart() and window() have big issues with sparse data. The 
#          performance degrades quite quickly as the size of the overlap 
#          increases. 
#        

if  [ 1 = 1 ]; then 

date;
/usr/bin/time -f "Q13 %e" iquery -naq "
store ( 
  repart ( Test_Array, 
           EMPTY <int32_attr:int32 NOT NULL,int64_attr:int64 NOT NULL,double_attr:double NOT NULL> 
           [I=0:799999,100000,10000,J=0:799999,100000,10000]
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
   1000,1000, 
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
    
    between ( Test_Array,  10000, 200000, 790000, 500000) AS T1,
    between ( Test_Array, 200000,  10000, 500000, 790000) AS T2, 

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
