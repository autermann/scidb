#!/bin/bash

### Input parameters
XSTARTS=(503000 504000 500000 504000 504000)
YSTARTS=(503000 491000 504000 501000 493000)
size=5000
window=25
count=1
### SSDB directory
THIS=`pwd`
while [ -h "$THIS" ]; do
  ls=`ls -ld "$THIS"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    THIS="$link"
  else
    THIS=`dirname "$THIS"`/"$link"
  fi
done
SSDB="$THIS"
###

gen(){
 # Data Generation
 ssdbgen -o -s -c normal $SSDB/bin/tileData
 mv bench bench.pos $SSDB/data/normal
}
init(){
 # Load the Findstars library
 echo "Loading Findstars"
 iquery -r /dev/null -aq "load_library('findstars')"
 
 # Create Data Set Normal 
 echo "Create Normal Array"
 iquery -r /dev/null -aq "CREATE IMMUTABLE ARRAY normal <a:int32, b:int32, c:int32, d:int32, e:int32,f:int32,g:int32,h:int32, i:int32, j:int32, k:int32>[Z=0:399,1,0 ,J=0:7499,7500,0, I=0:7499,7500,0]"
 
 # Load Data Normal 
 echo "Loading Normal data .."
 iquery -r /dev/null -aq "load(normal, '$SSDB/data/normal/bench')"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Loading Time: $DIFF seconds"
 
 # Cook Data Normal 
 echo "Cooking Normal data into normal_obs array .."
 iquery -r /dev/null -aq "store(findstars(normal,a,900),normal_obs)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Cooking Time: $DIFF seconds"
 
 # pre-reparting the array 
 echo "Pre-Reparting the Array"
 iquery -r /dev/null -aq "store(repart(subarray(normal,0,0,0,19,7499,7499),<a:int32, b:int32, c:int32, d:int32, e:int32,f:int32,g:int32,h:int32, i:int32,j:int32, k:int32>[Z=0:19,1,0,J=0:7499,7500,2,I=0:7499,7500,2]),normal_reparted)"
 
 # Split the observations
 echo "Pre-Observation spliting"
 python scripts/split_normal.py
}

q1(){
 START=$(date +%s)
 iquery -r /dev/null -aq "avg(subarray(normal,0,0,0,19,7499,7499),a)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Q1: $DIFF seconds"
}

q2(){
 START=$(date +%s)
 iquery -r /dev/null -aq "findstars(subarray(normal,0,0,0,0,7499,7499),a,900)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Q2: $DIFF seconds"
}

q3(){
 START=$(date +%s)
 iquery -r /dev/null -aq "thin(window(subarray(normal_reparted,0,0,0,19,7499,7499),1,4,4,avg(a)),0,1,2,3,2,3)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Q3: $DIFF seconds"
}

q4(){
  START=$(date +%s)
  for (( i=0; i < 20 ; i++ )) do
    iquery -r /dev/null -aq  "avg(filter(subarray(normal_obs_`printf $i`,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$size,${YSTARTS[$ind]}+$size),center is not null),sumPixel)" &
  done
  wait
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q4: $DIFF seconds"
}

q5(){
  START=$(date +%s)
  for (( i=0; i < 20 ; i++ )) do
    iquery -r /dev/null -aq  "filter(subarray(normal_obs_`printf $i`,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$size,${YSTARTS[$ind]}+$size),polygon is not null)" &
  done
  wait
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q5: $DIFF seconds"
}

q6(){
  START=$(date +%s)
  for (( i=0; i < 20 ; i++ )) do
    iquery -o csv+ -r /dev/null -aq  "filter(window(filter(subarray(normal_obs_`printf $i`,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$size,${YSTARTS[$ind]}+$size),center is not null),$window,$window,count(center)),center_count>$count)"
  done
  wait
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q6: $DIFF seconds"
}

echo "** SSDB BENCHMARK [normal configuration] *****************"

if [[ $1 = "-g" ]]; then
 echo "[.... Data Generation ....]"
 gen 
 echo "[..... Initialization ....]"
 init
fi
if [[ $1 = "-i" ]]; then
 echo "[.... Initialization ....]"
 init
fi

echo "[Begin]"
## ADD repetition here if needed
for rep in 0 #1 2
do
 for ind in 0 1 2 3 4
 do
  echo "run [$(($rep*5+$ind))]:"
  q1
  q2
  q3
  q4
  q5
  q6
 done
done

echo "[End]"
