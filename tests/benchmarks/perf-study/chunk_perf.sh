#!/bin/sh
#
#  About: This script is intended to measure the performance hit we're going 
#         to be taking with the new chunk format. The idea is to build a set
#         of arrays of various kinds, with various kinds of data distributions
#         etc, and see how they work over a range of queries. 
#
#
#  The basic idea is to build a mechanism that will allow us to build a large 
#  number of arrays with various distributions of values. The best way to do 
#  this is to use a Zipfian() distribution and to vary the 'p'. In the 
#  following queries, we use p = 0.5. 
#
#  Part 1: How to generate a variety of data distributions in attributes. 
#
#  Uniform random distributions.
#
#  Uniform double. 
iquery -aq "
build ( < db_uniform : double > [ I=0:99,100,0 ], 
      (double(random())/2147483648.0))"
#
#  Uniform int32
iquery -aq "build ( < int32_uniform : int32 > [ I=0:99,100,0 ], 
      random())"
#
#  Uniform int64
iquery -aq "build ( < int64_uniform : int64 > [ I=0:99,100,0 ], 
      random())"
#
#  Uniform distribution - char - pop = 10. 
# 
#  Yes. I know. I'm trying to emulate a uniform distribution over a set of 
#  population of 10. 
iquery -aq "
build ( < char_zipfian : char > [ I=0:99,100,0 ], 
  iif(((double(random())/2147483648.0) < 0.1),'A',
    iif(((double(random())/2147483648.0) < 0.1),'B',
      iif(((double(random())/2147483648.0) < 0.1),'C',
        iif(((double(random())/2147483648.0) < 0.1),'D',
          iif(((double(random())/2147483648.0) < 0.1),'E',
            iif(((double(random())/2147483648.0) < 0.1),'F',
              iif(((double(random())/2147483648.0) < 0.1),'G',
                iif(((double(random())/2147483648.0) < 0.1),'H',
                  iif(((double(random())/2147483648.0) < 0.5),'I','J'))))))))))"
#
#  Zipfian dostribution - 
#
#  Zipfian distribution - int32 - pop = 10. 
iquery -aq "
build ( < int32_zipfian : int32 > [ I=0:99,100,0 ], 
  iif(((double(random())/2147483648.0) < 0.5),1,
    iif(((double(random())/2147483648.0) < 0.5),2,
      iif(((double(random())/2147483648.0) < 0.5),3,
        iif(((double(random())/2147483648.0) < 0.5),4,
          iif(((double(random())/2147483648.0) < 0.5),5,
            iif(((double(random())/2147483648.0) < 0.5),6,
              iif(((double(random())/2147483648.0) < 0.5),7,
                iif(((double(random())/2147483648.0) < 0.5),8,
                  iif(((double(random())/2147483648.0) < 0.5),9,10))))))))))"
#
#  Zipfian distribution - int64 - pop = 10. 
iquery -aq "
build ( < int64_zipfian : int32 > [ I=0:99,100,0 ], 
  iif(((double(random())/2147483648.0) < 0.5),1,
    iif(((double(random())/2147483648.0) < 0.5),2,
      iif(((double(random())/2147483648.0) < 0.5),3,
        iif(((double(random())/2147483648.0) < 0.5),4,
          iif(((double(random())/2147483648.0) < 0.5),5,
            iif(((double(random())/2147483648.0) < 0.5),6,
              iif(((double(random())/2147483648.0) < 0.5),7,
                iif(((double(random())/2147483648.0) < 0.5),8,
                  iif(((double(random())/2147483648.0) < 0.5),9,10))))))))))"
#
#  Zipfian distribution - double - pop = 10. 
iquery -aq "
build ( < double_zipfian : double > [ I=0:99,100,0 ], 
  iif(((double(random())/2147483648.0) < 0.5),1.0,
    iif(((double(random())/2147483648.0) < 0.5),2.0,
      iif(((double(random())/2147483648.0) < 0.5),3.0,
        iif(((double(random())/2147483648.0) < 0.5),4.0,
          iif(((double(random())/2147483648.0) < 0.5),5.0,
            iif(((double(random())/2147483648.0) < 0.5),6.0,
              iif(((double(random())/2147483648.0) < 0.5),7.0,
                iif(((double(random())/2147483648.0) < 0.5),8.0,
                  iif(((double(random())/2147483648.0) < 0.5),9.0,10.0))))))))))"
#
#  Part 2: Arrays with a variety of sparsities. 
#
iquery -aq "build_sparse ( < int32_uniform : int32 > [ I=0:99,100,0 ], 
      random(),
      (double(random())/2147483648.0) > 0.5
)"
#
#  Array 1: Dense, Uniform, Three Attributes. 
#
#   Build an array with 3 attributes, and 16,000,000 elements. 
#  This array is divided into 16 chunks, each of which has 
#  1,000,000 elements. The total size of the data is therefore
#  16,000,000x(8x2+4) = 300 Meg. 
#
#   Data in the chunks is distributed uniformly over the range of the 
#  type which means we will get relatively little benefit from the RLE 
#  encoding. 
#
#  The Dense_Uniform array presents us with a "worst reasonable" case.
#  
time iquery -aq "remove ( Dense_Uniform)"
time iquery -naq "
store ( 
  join ( 
    build ( < int32_uniform : int32 > [ I=0:3999,1000,0, J=0:3999,1000,0 ], 
        random()),
    join ( 
      build ( < int64_uniform : int64 > [ I=0:3999,1000,0, J=0:3999,1000,0 ], 
          random()),
      build ( < double_uniform : double > [ I=0:3999,1000,0, J=0:3999,1000,0 ], 
          (double(random())/2147483648.0))
    )
  ),
  Dense_Uniform
)"
#  Size with RLE: 
#
du -sh /home/plumber/scidb/data/000/0/storage.data1 
#
#  Queries: 
#
#  Q1: Simple sum()
time iquery -aq "sum ( Dense_Uniform, int32_uniform )"
#
#  Q2: Simple sum() with group-by
time iquery -naq "aggregate ( Dense_Uniform, sum(int32_uniform), I )"
#
#  Q3: sum(subarray()) - 1%. 
time iquery -naq "
join ( 
  aggregate ( subarray(Dense_Uniform, 60, 60, 100, 100), 
              sum(int32_uniform)),
  join ( 
    aggregate ( subarray(Dense_Uniform, 260, 260, 300, 300), 
                sum(int32_uniform)),
    join ( 
      aggregate ( subarray(Dense_Uniform, 780, 780, 820, 820), 
                  sum(int32_uniform)),
      join ( 
        aggregate ( subarray(Dense_Uniform, 980, 980, 1020, 1020), 
                    sum(int32_uniform)),
        join ( 
          aggregate ( subarray(Dense_Uniform, 1980, 1980, 2020, 2020), 
                      sum(int32_uniform)),
          join ( 
            aggregate ( subarray(Dense_Uniform, 2000, 2000, 2040, 2040), 
                        sum(int32_uniform)),
            join ( 
            aggregate ( subarray(Dense_Uniform, 2020, 2020, 2060, 2060), 
                        sum(int32_uniform)),
            join ( 
              aggregate ( subarray(Dense_Uniform, 2000, 60, 2040, 100), 
                          sum(int32_uniform)),
              join ( 
              aggregate ( subarray(Dense_Uniform, 60, 2000, 100, 2040), 
                          sum(int32_uniform)),
              aggregate ( subarray(Dense_Uniform, 2420, 2420, 2460, 2460), 
                          sum(int32_uniform))
)))))))));"
#
#  Q4: sum(subarray()) - 10%. 
time iquery -naq "
join ( 
  aggregate ( subarray(Dense_Uniform, 60, 60, 460, 460), 
              sum(int32_uniform)),
  join ( 
    aggregate ( subarray(Dense_Uniform, 260, 260, 660, 660), 
                sum(int32_uniform)),
    join ( 
      aggregate ( subarray(Dense_Uniform, 660, 660, 1060, 1060), 
                  sum(int32_uniform)),
      join ( 
        aggregate ( subarray(Dense_Uniform, 1060, 1060, 1460, 1460), 
                    sum(int32_uniform)),
        join ( 
          aggregate ( subarray(Dense_Uniform, 1460, 1460, 1860, 1860), 
                      sum(int32_uniform)),
          join ( 
            aggregate ( subarray(Dense_Uniform, 1860, 1860, 2260, 2260), 
                        sum(int32_uniform)),
            join ( 
              aggregate ( subarray(Dense_Uniform, 2260, 2260, 2660, 2660), 
                          sum(int32_uniform)),
              join ( 
                aggregate ( subarray(Dense_Uniform, 2660, 2660, 3060, 3060), 
                            sum(int32_uniform)),
                join ( 
                  aggregate ( subarray(Dense_Uniform, 3060, 3060, 3460, 3460), 
                              sum(int32_uniform)),
                  aggregate ( subarray(Dense_Uniform, 3460, 3460, 3860, 3860), 
                              sum(int32_uniform))
)))))))))";
#
# Q5: sum(subarray()) - 50%
time iquery -naq "
join(
  aggregate ( subarray(Dense_Uniform, 60, 60, 2060, 2060), 
        sum(int32_uniform)),
  join ( 
    aggregate ( subarray(Dense_Uniform, 260, 260, 2260, 2260), 
          sum(int32_uniform)),
    join ( 
      aggregate ( subarray(Dense_Uniform, 660, 660, 2660, 2660), 
            sum(int32_uniform)),
      join ( 
        aggregate ( subarray(Dense_Uniform, 1060, 1060, 3060, 3060), 
              sum(int32_uniform)),
        join ( 
        aggregate ( subarray(Dense_Uniform, 1460, 1460, 3460, 3460), 
              sum(int32_uniform)),  
        aggregate ( subarray(Dense_Uniform, 1860, 1860, 3860, 3860), 
              sum(int32_uniform))
)))));"
#
# Q6: sum(filter(subarray()...) - 50% subarray, 50% filter
time iquery -naq "
join ( 
  aggregate ( filter( subarray(Dense_Uniform, 60, 60, 2060, 2060), 
                  double_uniform < 0.5), 
    sum(int32_uniform)),
  aggregate ( filter( subarray(Dense_Uniform, 1460, 1460, 3460, 3460), 
                double_uniform > 0.5), 
  sum(int32_uniform))
)"
#
# Q7: sum(apply(filter(subarray()...)...) - 50% subarray, 50% filter
time iquery -naq "
join ( 
  aggregate ( 
    apply(
      filter( 
        subarray(Dense_Uniform, 60, 60, 2060, 2060), 
          double_uniform < 0.5
      ),
      add_em,
      int32_uniform + int64_uniform
    ),
    sum(add_em)
  ),
  aggregate ( 
    apply(
      filter( 
        subarray(Dense_Uniform, 1460, 1460, 3460, 3460), 
          double_uniform > 0.5
      ),
      add_em,
      int32_uniform + int64_uniform
    ),
    sum(add_em)
  )
)"
#
# Q8: regrid(...)
time iquery -naq "
regrid ( Dense_Uniform, 
		 40,40, 
		 avg(int32_uniform), avg(int64_uniform), avg(double_uniform))"
#
# Q9: repart()
#  NOTE: I tried this step at a range of overlapping sizes, from +/1 10 
#        through +/- 100. The timing differences were small, although
#        increasing. For now, going with +/- 100, because that feels 
#        about what we might expect to get. 
#        
time iquery -naq "
	repart ( Dense_Uniform, 
		 	 <int32_uniform:int32 NOT NULL,int64_uniform:int64 NOT NULL,double_uniform:double NOT NULL> 
				[I=0:3999,1000,100,J=0:3999,1000,100]
	)"
,#
# Q10: redimension_store()
time iquery -aq "remove ( Foo )"
time iquery -aq " CREATE EMPTY ARRAY Foo 
< int32_uniform:int32 NOT NULL,int64_uniform:int64 NOT NULL,double_uniform:double NOT NULL> 
[ K=0:*,100000,0 ]"
#
time iquery -naq "
redimension ( 
	apply ( Dense_Uniform,
			K,
			(J*4000)+I
	  	  ),
	Foo
)"
			

#
#
time iquery -aq "remove ( Dense_Uniform)"
# 
#
#   Build an array with 3 attributes and 64,000,000 elements 
#  divided into 16 chunks, each of which has 4,000,000 elements. The 
#  total size of the data therefore is 64,000,000x(8x2+4) bytes,
#  or 1.2Gig. 
#
#   Data in the chunks is distributed uniformly over the range of the 
#  type which means we will get relatively little benefit from the RLE 
#  encoding. 
#
time iquery -aq "remove ( Dense_Zipfian )"
time iquery -naq "
store (
    join (
        build ( < int32_zipfian : int32 > [ I=0:3999,1000,0, J=0:3999,1000,0 ],
                iif(((double(random())/2147483648.0) < 0.5),1,
                    iif(((double(random())/2147483648.0) < 0.5),2,
                      iif(((double(random())/2147483648.0) < 0.5),3,
                        iif(((double(random())/2147483648.0) < 0.5),4,
                          iif(((double(random())/2147483648.0) < 0.5),5,
                            iif(((double(random())/2147483648.0) < 0.5),6,
                              iif(((double(random())/2147483648.0) < 0.5),7,
                                iif(((double(random())/2147483648.0) < 0.5),8,
                                  iif(((double(random())/2147483648.0) < 0.5),9,10)))))))))),
        join (
            build ( < int64_zipfian : int64 > [ I=0:3999,1000,0, J=0:3999,1000,0 ],
                iif(((double(random())/2147483648.0) < 0.5),1,
                    iif(((double(random())/2147483648.0) < 0.5),2,
                      iif(((double(random())/2147483648.0) < 0.5),3,
                        iif(((double(random())/2147483648.0) < 0.5),4,
                          iif(((double(random())/2147483648.0) < 0.5),5,
                            iif(((double(random())/2147483648.0) < 0.5),6,
                              iif(((double(random())/2147483648.0) < 0.5),7,
                                iif(((double(random())/2147483648.0) < 0.5),8,
                                  iif(((double(random())/2147483648.0) < 0.5),9,10)))))))))),
            build ( < double_uniform : double > [ I=0:3999,1000,0, J=0:3999,1000,0 ],
                    iif(((double(random())/2147483648.0) < 0.5),1.0,
                      iif(((double(random())/2147483648.0) < 0.5),2.0,
                        iif(((double(random())/2147483648.0) < 0.5),3.0,
                          iif(((double(random())/2147483648.0) < 0.5),4.0,
                            iif(((double(random())/2147483648.0) < 0.5),5.0,
                              iif(((double(random())/2147483648.0) < 0.5),6.0,
                                iif(((double(random())/2147483648.0) < 0.5),7.0,
                                  iif(((double(random())/2147483648.0) < 0.5),8.0,
                                    iif(((double(random())/2147483648.0) < 0.5),9.0,10.0))))))))))
      )
  ),
    Dense_Zipfian
)"
#
# Time with RLE Encoding: 
#
#  real  40m40.414s
#  user  0m0.052s
#  sys  0m0.104s

time iquery -aq "count ( Dense_Zipfian )"
#
time iquery -aq "sum ( Dense_Zipfian, int32_zipfian )"
#
# real  0m3.186s
# user  0m0.016s
# sys  0m0.080s
#
iquery -aq "show ( Dense_Uniform )"
iquery -aq "count ( Dense_Uniform )"

#  
iquery -aq "build_sparse ( < int32_uniform : int32 > [ I=0:99,100,0 ],
            random(),
            (double(random())/2147483648.0) > 0.5
)"
# 
#
time iquery -aq "remove ( Raw_Objects )";
time iquery -aq "remove ( ObjectsInter )";
time iquery -aq "remove ( Objects )";
#
#   Q2: Create (abbreviated) Raw_Objects array.
#
time iquery -aq "
CREATE EMPTY ARRAY Raw_Objects <
    RA : int64,
    DECL : int64,
    B_mag : double,
    B_mag_flag : int32,
    R_mag : double,
    R_mag_flag : int32 >
[ LN=0:59999,60000,0 ]"
#
#  Q3: Load the small (60K rows) CSV file, eliminating many of its attributes.
#
rm -rf /tmp/Load_File.pipe
mkfifo /tmp/Load_File.pipe
awk -F, -f ./scr.awk ./Small_LSST.csv | csv2scidb -c 60000 > /tmp/Load_File.pipe  &
time iquery -naq "load (Raw_Objects, '/tmp/Load_File.pipe')"
#
#  Result with rle-chunk-format: 0.03 user 0.05 system 0:04.41 elapsed
#
#  Result with old format: 0.02 user 0.06 system 0:01.68 elapsed
#
#  Time 
#
#   Q4: What did we end up with from the load?
#
time iquery -aq "count ( Raw_Objects )"
#
#  Should load [(60000)] cells. 
#
#   Q5: Create the intermediate Objects array.
#
time iquery -aq "
CREATE EMPTY ARRAY ObjectsInter <
    B_mag : double,
    B_mag_flag : int32,
    R_mag : double,
    R_mag_flag : int32 >
[ RA(int64)=10000000,10000,0, DECL(int64)=10000000,10000,0]"
#
#  Q6: redimension_store() the raw load into the intermediate array.
#
time iquery -naq "redimension_store ( Raw_Objects, ObjectsInter )"
#
#  Result with rle-chunk-format: 0.04 user 0.04 system 0:09.31 elapsed
#
#  Result with old format: 0.02 user 0.06 system 0:10.26 elapsed
#
#  Q7: Count the result. 
#
time iquery -aq "count ( ObjectsInter )"
#
#  Again, should be [(60000)]. 
#
#  Q8: Build the array with the overlaps.
#
time iquery -aq "
CREATE EMPTY ARRAY Objects <
    B_mag : double,
    B_mag_flag : int32,
    R_mag : double,
    R_mag_flag : int32 >
[ RA(int64)=10000000,10000,100, DECL(int64)=10000000,10000,100]"
#
# Q10: This is the query that appears to wedge. I have left it running for 
#      30 mins. Note that the time taken to perform the redimension_store()
#      above is on the order of a 10s of seconds, so it puzzles me why 
#      repart() is taking so very long. 
#
time iquery -anq "store ( repart ( ObjectsInter, Objects ), Objects )"
#
#  Result with rle-chunk-format: 0.22 user 0.36 system 16:07:28 elapsed
#
#  Result with old format: 0.04 user 0.04 system 3:56.82 elapsed
#
