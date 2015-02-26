#!/bin/bash 
# Usage ./chunkGenerator config(tiny,small,very_small,normal) numChunk numChunkPerInstance ServerNum
#
for ((i=0; i<$3; i++)); 
do
  chunk=$(($i*$2/$3+$4))
  echo "$4 $chunk"
  ./ssdbgen -n $2 -i $chunk -s -c $1 ./tileData >> bench 
  echo ";" >> bench
done
