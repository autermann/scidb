#!/bin/bash
#INFO:
#This is a utility scripts that should be modified as needed
#Each node must have the same path ex:/data/normal and binaries (ssdbgen,tiledata,chunkDistributer.sh)
serverId=0
for i in 10.140.5.79 10.64.73.162 10.198.125.59 10.198.71.191 10.191.206.57 10.111.55.229 10.110.245.80 10.206.37.236
do
  echo "$serverId $i"
  pdsh -w $i "cp /opt/scidb/12.3/bin/ssdbgen /opt/scidb/12.3/bin/tileData /opt/scidb/12.3/bin/chunkDistributer.sh /data/normal"
done 
