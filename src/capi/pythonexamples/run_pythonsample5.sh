#/bin/bash

export X=`pwd`;echo ${X};sed "s:CWD:"$X":g" test1.csv > test1.input
export PYTHONPATH=$PYTHONPATH:/opt/scidb/1.0/lib
python pythonsample5.py `pwd`/test1.input  /opt/scidb/1.0/lib 
