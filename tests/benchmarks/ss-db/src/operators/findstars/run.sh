#!/bin/sh
make &&cp ../../../../../../bin/plugins/libfind_stars.so /opt/scidb/11.10/lib/scidb/plugins 
scidb -t 1 -x 1 -q 1 -i localhost -p 1239 --merge-sort-buffer 512 --cache 256 --chunk-cluster-size 1048576 -k -l /opt/scidb/current/share/scidb/log4cxx.properties --plugins /opt/scidb/current/lib/scidb/plugins -s /mnt/data/scidb/dbs4/000/0/storage.cfg -c 'host=localhost port=5432 dbname=test11 user=drkwolf password=test'&
iquery -aq "findstars(subarray(dense3x3,0,0,1,2) ,a,400)"
