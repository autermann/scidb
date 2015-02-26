#!/bin/bash 
# Usage ./chunkGenerator config(tiny,small,very_small,normal) numChunk groupChunks ServerNum
#
for ((i=0; i<$3; i++)); 
do
  #echo $(($4*$3+$4))
  #echo $(($i*$2/$3+$4))
  ssdbgen -n $2 -i $(($i*$2/$3+$4)) -s -c $1 ./bin/tileData >> bench 
  echo ";" >> bench
done
