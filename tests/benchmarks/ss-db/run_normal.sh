#!/bin/bash

### Input parameters
YSTARTS=(503000 504000 500000 504000 504000)
XSTARTS=(503000 491000 504000 501000 493000)
U1=7499
U2=9999
U3=999
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
 # Load the special libraries
 echo "Loading libs"
 iquery -naq "load_library('findstars')"
 iquery -naq "load_library('groupstars')"
 
 # Create Data Set Normal 
 echo "Create Normal Array"
 iquery -naq "CREATE IMMUTABLE ARRAY normal <a:int32, b:int32, c:int32, d:int32, e:int32,f:int32,g:int32,h:int32, i:int32, j:int32, k:int32>[Z=0:399,1,0 ,J=0:7499,7500,0, I=0:7499,7500,0]"
 iquery -naq "CREATE IMMUTABLE ARRAY normal_space <I:int32, J:int32, index:int32>[id=0:399,400,0]"
 
 # Hack array for Q9
 iquery -naq "create array normal_groups_augmented<normal_groups_augmented<oid:int64,x:int64 NULL,y:int64 NULL> [group=0:*,1000,0,oid_2=0:*,100,0,observation=0:19,20,0]"
 iquery -naq "redimension_store(cross_join(substitute(normal_groups,build(<oid:int64>[i=0:0,1,0],0)) as a,substitute(normal_groups,build(<oid:int64>[i=0:0,1,0],0)) as b,a.group,b.group),normal_groups_augmented)"
 
 # Load Data Normal 
 echo "Loading Normal data .."
 START=$(date +%s)
 ## Parallel version
 iquery -naq "load(normal, '/data/normal/bench',-1)"
 iquery -naq "load(normal_space, '/data/normal/bench.pos')"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Loading Time: $DIFF seconds"
 
 # Cook Data Normal 
 echo "Cooking Normal data into normal_obs array .."
 START=$(date +%s)
 iquery -naq "store(findstars(normal,a,900),normal_obs)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Cooking Time: $DIFF seconds"
 
 # Group Data Normal 
 echo "Grouping Normal observations into normal_groups array .."
 START=$(date +%s)
 iquery -naq "store(groupstars(filter(normal_obs,oid is not null and center is not null),normal_space,0.1,20),normal_groups)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Grouping Time: $DIFF seconds"
 

 # pre-reparting the array 
 echo "Pre-Reparting the Array"
 iquery -naq "store(repart(subarray(normal,0,0,0,19,U1,U1),<a:int32, b:int32, c:int32, d:int32, e:int32,f:int32,g:int32,h:int32, i:int32,j:int32, k:int32>[Z=0:19,1,0,J=0:7499,7500,2,I=0:7499,7500,2]),normal_reparted)"
 
 # Split the observations
 echo "Pre-Observation spliting"
 python scripts/split_normal.py
}

q1(){
 START=$(date +%s)
 iquery -r /dev/null -aq "avg(subarray(normal,0,0,0,19,U1,U1),a)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Q1: $DIFF seconds"
}

q2(){
 START=$(date +%s)
 iquery -r /dev/null -aq "findstars(subarray(normal,0,0,0,0,U1,U1),a,900)"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Q2: $DIFF seconds"
}

q3(){
 START=$(date +%s)
 #iquery -r /dev/null -aq "thin(window(subarray(normal_reparted,0,0,0,19,7499,7499),1,4,4,avg(a)),0,1,2,3,2,3)"
 #This is another version that should be faster
 iquery -r /dev/null -aq "avg(thin(window(subarray(project(normal_reparted,a), 0,0,0,19,$U1,$U1),1,4,4,avg(a)),0,1,2,3,2,3))"
 END=$(date +%s)
 DIFF=$(( $END - $START ))
 echo "Q3: $DIFF seconds"
}

q4(){
  START=$(date +%s)
  for (( i=0; i < 20 ; i++ )) do
    iquery -r /dev/null -aq  "avg(filter(subarray(normal_obs_`printf $i`,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$U2,${YSTARTS[$ind]}+$U2),center is not null),sumPixel)" 
  done
  wait
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q4: $DIFF seconds"
}

q5(){
  START=$(date +%s)
  for (( i=0; i < 20 ; i++ )) do
    iquery -r /dev/null -aq  "filter(subarray(normal_obs_`printf $i`,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$U2,${YSTARTS[$ind]}+$U2),polygon is not null)" 
  done
  wait
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q5: $DIFF seconds"
}

q6(){
  START=$(date +%s)
  for (( i=0; i < 20 ; i++ )) do
    iquery -o csv+ -r /dev/null -aq  "filter(window(filter(subarray(normal_obs_`printf $i`,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$U2,${YSTARTS[$ind]}+$U2),center is not null),$window,$window,count(center)),center_count>$count)"
  done
  wait
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q6: $DIFF seconds"
}

q7(){
  START=$(date +%s)
  iquery -o csv+ -r /dev/null -aq  "filter(aggregate(normal_groups,avg(x),avg(y),group),x_avg > ${XSTARTS[$ind]} and y_avg > ${YSTARTS[$ind]} and x_avg < ${XSTARTS[$ind]}+$U2 and y_avg < ${YSTARTS[$ind]}+$U2)"
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q7: $DIFF seconds"
}

q8(){

  echo "[
(0,${XSTARTS[$ind]},${YSTARTS[$ind]}),
(1,$[${XSTARTS[$ind]}+$U3],${YSTARTS[$ind]}),
(2,$[${XSTARTS[$ind]}+$U3],$[${YSTARTS[$ind]}+$U3]),
(3,${XSTARTS[$ind]},$[${YSTARTS[$ind]}+$U3])
]" > /tmp/Points.dat
  iquery -naq "remove(Points)"
  iquery -naq "create empty array Points<ID:int64,x:int64,y:int64>[INDEX=0:3,4,0]"
  iquery -naq "load(Points,'/tmp/Points.dat')"


  START=$(date +%s)
  iquery -r /dev/null -o csv+ -aq  "
       aggregate(
          cross_join(normal_groups,
          filter(
                sum ( 
                        project ( 
                                apply ( 
                                        cross ( 
                                                subarray ( Points, 0,3 ),
                                                join ( 
                                                        subarray (normal_groups, NULL,NULL,NULL,18) AS Pi,
                                                        subarray (normal_groups, NULL,1,NULL,NULL) AS Pj
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
        avg(normal_groups.x),avg(normal_groups.y),normal_groups.group)"
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q8: $DIFF seconds"
}

q9(){
  START=$(date +%s)
  iquery -r /dev/null -o csv+ -aq  "Aggregate(filter(cross(normal_groups_augmented,project(filter(subarray(normal_obs_12,${XSTARTS[$ind]},${YSTARTS[$ind]},${XSTARTS[$ind]}+$U3,${YSTARTS[$ind]}+$U3),polygon is not null),oid)),normal_obs_12.oid=normal_groups_augmented.oid_2 and normal_groups_augmented.oid_2>0),avg(normal_groups_augmented.x),avg(normal_groups_augmented.y),normal_groups_augmented.group)"
  END=$(date +%s)
  DIFF=$(( $END - $START ))
  echo "Q9: $DIFF seconds"
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
for rep in 0 1 2
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
  q7
  q8
  q9
 done
done

echo "[End]"
