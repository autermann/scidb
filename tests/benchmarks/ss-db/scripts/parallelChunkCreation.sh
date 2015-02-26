#!/bin/bash
serverId=0
for i in 10.140.5.79 10.64.73.162 10.198.125.59 10.198.71.191 10.191.206.57 10.111.55.229 10.110.245.80 10.206.37.236
do
  echo "$serverId $i"
  pdsh -w $i "cd /data/normal; sudo ./chunkDistributer.sh normal 400 50 $serverId" &
  serverId=$(($serverId+1))
done 
