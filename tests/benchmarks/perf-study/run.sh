#!/bin/sh 
#
#  Check that the environment variable used to discover the location of the 
#  storage is set. 
#
#
if [ "$SCIDB_DATA_DIR" = "" ]; then
	echo "You must set the variable \$SCIDB_DATA_DIR to the directory in which the "
	echo "SciDB data is stored. Use the the 'base-path' configuration from the "
	echo "config.ini file."
	exit;
fi
#  Run the scripts. 
#
C=$1
L=$2
#
./Array_11.sh $C $L
./Queries_1.sh $C $L
./Array_12.sh $C $L
./Queries_1.sh $C $L
./Array_13.sh $C $L
./Queries_1.sh $C $L
#
L=`expr $L "*" 10`
#
./Array_21.sh $C $L
./Queries_2.sh $C $L
./Array_22.sh $C $L
./Queries_2.sh $C $L
./Array_23.sh $C $L
./Queries_2.sh $C $L
#
L=`expr $L "*" 10`
#
./Array_31.sh $C $L
./Queries_3.sh $C $L
./Array_32.sh $C $L
./Queries_3.sh $C $L
./Array_33.sh $C $L
./Queries_3.sh $C $L
