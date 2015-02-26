#/bin/bash
#

export X=`pwd`;echo ${X};sed "s:CWD:"$X":g" test2.csv > test2.input
export PYTHONPATH=$PYTHONPATH:/opt/scidb/1.0/lib
python pythonsample2.py `pwd` /opt/scidb/1.0/lib
