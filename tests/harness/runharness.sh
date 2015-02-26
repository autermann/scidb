# Usage :
# runharness.sh <path_to_this_directory> <arg_2> <arg_3> .... <arg_n>
#
# <path_to_this_directory> : if not given, then "." is considered
#                            and all the arguments (i.e. arg_2, arg_3,....., arg_n) are passed to 'scidbtestharness'
# <path_to_this_directory> : if given, then it becomes working directory for 'scidbtestharness'
#                            and remaining arguments (i.e. arg_2, arg_3,....., arg_n) are passed to 'scidbtestharness' using "$@"

#!/bin/bash

harnessdir=$1
dbname=$SCIDB_NIGHTLY_DBNAME
cfgfile=$SCIDB_NIGHTLY_CFGFILE
export IQUERY_HOST=localhost
export IQUERY_PORT=8888

if test -z $harnessdir
then
	harnessdir="."
elif test -d $harnessdir
then
	shift 1
fi

# change directory
cd $harnessdir

if ! [[ -n $dbname ]]; then
    dbname="nightly"
    cfgfile="./config.ini"
fi

# If planet coordinator
if [[ `hostname` == "sciDB-Master" ]]; then
    dbname="nightlyplanet"
    cfgfile="./config.ini"
fi
echo $dbname
echo $cfgfile
echo "Initializing and Starting SciDB..."

scidb.py stopall $dbname  $cfgfile
scidb.py initall $dbname  $cfgfile
scidb.py startall $dbname  $cfgfile
pwd
./mktestsymlink.py $dbname  $cfgfile

#echo "Executing scidbtestharness..."
#initialization run
scidbtestharness --port 8888 --debug 5 --root-dir=testcases --test-id=no $@

#echo "Executing runN.py."
h_dir=`pwd`
#cd ../basic
# ./runN.py 1 $dbname unit 8888
#scidbtestharness --port 8888 --debug 5 --root-dir=testcases --suite-id=checkin --report-file=Report_1.xml $@

echo "Re-initializing SciDB..."
scidb.py stopall $dbname  $cfgfile
scidb.py initall $dbname  $cfgfile
scidb.py startall $dbname  $cfgfile

# Set dbname for unit tests
sedexpr="s/mydb/$dbname/g"
sed -i -e "$sedexpr" testcases/t/checkin/other/unit.test

echo "Executing scidbtestharness..."
scidbtestharness --port 8888 --debug 5 --root-dir=testcases --report-file=Report.xml $@

echo "Stopping SciDB..."
scidb.py stopall $dbname  $cfgfile
