#!/bin/sh
#
#
~/Devel/trunk/tests/data_gen/gen_matrix -d 10 10 10 10 1.0 NG > /tmp/Array.data

~/Devel/trunk/tests/data_gen/gen_matrix -d 2 2 3 3 1.0 NG > /tmp/Array.data
#
./iquery -q "list('types')"
./iquery -q "CREATE ARRAY Test_10 < Num_One: int32, Num_Two: double > [ I=0:99,10,0, J=0:99,10,0 ]"
./iquery -q "load ('Test_10', '/tmp/Array.data')"
#
./iquery -q "CREATE ARRAY Test_09 < Num_One: int32, Num_Two: double > [ I=0:5,3,0, J=0:5,3,0 ]"
./iquery -q "load ('Test_09', '/tmp/Array.data')"
#
./iquery -q "count (scan ('Test_04'))"

./iquery -q "list ('arrays')"
