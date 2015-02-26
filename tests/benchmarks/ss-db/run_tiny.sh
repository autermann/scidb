#!/bin/bash

### Input parameters
XSTARTS=(503000 504000 500000 504000 504000)
YSTARTS=(503000 491000 504000 501000 493000)
U1=9
U2=50
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
 ssdbgen -o -s -c tiny $SSDB/bin/tileData
 mv bench bench.pos $SSDB/data/tiny
}
init(){
 # Load the special libraries
 echo "Loading libs"
 iquery -naq "load_library('findstars')"
 iquery -naq "load_library('groupstars')"
 
 # Create Data Set tiny
 echo "Create Tiny Array"
 iquery -aq "remove (tiny)"
 iquery -aq "remove (tiny_space)"
 iquery -r /dev/null -aq "CREATE IMMUTABLE ARRAY tiny <a:int32, b:int32, c:int32, d:int32, e:int32,f:int32,g:int32,h:int32, i:int32, j:int32, k:int32>[Z=0:19,20,0 ,J=0:9,10,0, I=0:9,10,0]"
 iquery -r /dev/null -aq "CREATE IMMUTABLE ARRAY tiny_space <I:int32, J:int32, index:int32>[id=0:19,20,0]"
 
 # Load Data tiny
 echo "Loading Tiny data .."
 START=$(date +%s)
 iquery -r /dev/null -aq "load(tiny, '$SSDB/data/tiny/bench')"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Loading Time: $DIFF seconds"

 # Cook Data tiny
 echo "Cooking Tiny data into tiny_obs array .."
 START=$(date +%s)
 iquery -r /dev/null -aq "store(findstars(tiny,a,450),tiny_obs)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Cooking Time: $DIFF seconds"

 # Group Data Normal 
 echo "Grouping Tiny observations into tiny_groups array .."
 START=$(date +%s)
 iquery -naq "store(groupstars(filter(tiny_obs,oid is not null and center is not null),tiny_space,10,20),tiny_groups)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Grouping Time: $DIFF seconds"
 

 #  Pre-Reparting the array
 echo "Pre-Reparting the Array"
 iquery -r /dev/null -aq "store(repart(tiny,<a:int32, b:int32, c:int32, d:int32, e:int32,f:int32,g:int32,h:int32, i:int32, j:int32, k:int32>[Z=0:19,20,0 ,J=0:9,12,0, I=0:9,12,0]),tiny_reparted)"
 
 # Split the observation
 echo "Pre-Observation spliting"
 python scripts/split_tiny.py
}

q1(){
 START=$(date +%s)
 iquery -r /dev/null -aq "avg(subarray(tiny,0,0,0,19,$U1,$U1),a)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Q1: $DIFF seconds"
}

q2(){
 START=$(date +%s)
 iquery -r /dev/null -aq "findstars(subarray(tiny,0,0,0,0,$U1,$U1),a,450)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Q2: $DIFF seconds"
}

q3(){
 START=$(date +%s)
 iquery -r /dev/null -aq "thin(window(subarray(tiny_reparted,0,0,0,19,$U1,$U1),1,4,4,avg(a)),0,1,2,3,2,3)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Q3: $DIFF seconds"
}

q4(){
 START=$(date +%s)
 for (( i=0; i < 20 ; i++ )) do
   iquery -r /dev/null -aq  "avg(filter(subarray(tiny_obs_`printf $i`,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$U2,${YSTARTS[$ind]}+$U2),center is not null),sumPixel)" &
 done
 wait
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Q4: $DIFF seconds"
}

q5(){
  START=$(date +%s)
  for (( i=0; i < 20 ; i++ )) do
    iquery -r /dev/null -aq  "filter(subarray(tiny_obs_`printf $i`,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$U2,${YSTARTS[$ind]}+$U2),polygon is not null)" &
  done
  wait
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q5: $DIFF seconds"
}

q6(){
  START=$(date +%s)
  for (( i=0; i < 20 ; i++ )) do
    iquery -o csv+ -r /dev/null -aq  "filter(window(filter(subarray(tiny_obs_`printf $i`,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$U2,${YSTARTS[$ind]}+$U2),center is not null),$window,$window,count(center)),center_count>$count)"
  done
  wait
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q6: $DIFF seconds"
}

q7(){
  START=$(date +%s)
  iquery -o csv+ -r /dev/null -aq  "filter(aggregate(tiny_groups,avg(x),avg(y),group),x_avg > ${XSTARTS[$ind]} and y_avg > ${YSTARTS[$ind]} and x_avg < ${XSTARTS[$ind]}+$U2 and y_avg < ${YSTARTS[$ind]}+$U2)"
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q7: $DIFF seconds"
}

q8(){

  echo "[
(0,${XSTARTS[$ind]},${YSTARTS[$ind]}),
(1,$[${XSTARTS[$ind]}+$U2],${YSTARTS[$ind]}),
(2,$[${XSTARTS[$ind]}+$U2],$[${YSTARTS[$ind]}+$U2]),
(3,${XSTARTS[$ind]},$[${YSTARTS[$ind]}+$U2])
]" > /tmp/Points.dat
  iquery -naq "remove(Points)"
  iquery -naq "create empty array Points<ID:int64,x:int64,y:int64>[INDEX=0:3,4,0]"
  iquery -naq "load(Points,'/tmp/Points.dat')"


  START=$(date +%s)
  iquery -r /dev/null -o csv+ -aq  "
       aggregate(
          cross_join(tiny_groups,
          filter(
                sum ( 
                        project ( 
                                apply ( 
                                        cross ( 
                                                subarray ( Points, 0,3 ),
                                                join ( 
                                                        subarray (tiny_groups, NULL,NULL,NULL,18) AS Pi,
                                                        subarray (tiny_groups, NULL,1,NULL,NULL) AS Pj
                                                )
                                        ),
                                        crosses,
                                        iif (((((Pi.y <= Points.y) AND (Pj.y > Points.y)) OR 
                                        ((Pi.y > Points.y) AND (Pj.y <= Points.y))) AND 
                                        (Points.x < Pi.x + ((Points.y - Pi.y) / (Pj.y - Pi.y)) * (Pj.x - Pi.x)) AND (Pi.x is not null and Pi.y is not null and Pj.x is not null and Pj.y is not null)),
                                        1, 0 )
                                ),
                                crosses
                        ),
                        crosses,Pj.group
                ),
           crosses_sum > 0)
        ),
        avg(tiny_groups.x),avg(tiny_groups.y),tiny_groups.group)"
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q8: $DIFF seconds"
}

q9(){
  START=$(date +%s)
  iquery -r /dev/null -o csv+ -aq  "Aggregate(filter(filter(cross(subarray(tiny_obs_0,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$U2,${YSTARTS[$ind]}+$U2) as A, tiny_groups),A.polygon is not null), A.oid=tiny_groups.oid),avg(tiny_groups.x),avg(tiny_groups.y),tiny_groups.group)"
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q9: $DIFF seconds"
}

echo "SSDB:"
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
## ADD more repetitions here
for rep in 0 #1 2
do
 for ind in 0 1 2 3 4
 do
  echo "Run [$(($rep*5+$ind))]:"
  q1 
  q2
  q3
  q4
  q5
  q6
  q7
  q8
  q9
 done
done

echo "[End]"
