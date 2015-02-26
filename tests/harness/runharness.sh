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

# rhel54-vm
if [[ `hostname` == "rhel54-vm" ]]; then
    dbname="nightly"
    cfgfile="./config.ini"
fi

if [[ `hostname` == "rhel6-vm" ]]; then
    dbname="rh6"
    cfgfile="./config.ini"
fi

if [[ `hostname` == "ubuntu1204-vm" ]]; then
    dbname="ubuntu1204vm"
    cfgfile="./config.ini"
fi

# If ubuntu1104-vm coordinator
if [[ `hostname` == "ubuntu1104-vm" ]]; then
    ninst=$NUM_INSTANCES
    dbname="ubuntu1104vm_i${ninst}"
    cfgfile="./config.ini"
fi
echo $dbname
echo $cfgfile


if [[ $USE_VALGRIND == 1 ]]; then
    echo 'Killing SciDB instances on port 1239.'
    ps aux | grep 1239 | awk '{print $2}'|xargs kill
    echo 'Running SciDB under Valgrind...'
    ./runN.py 1 $dbname --istart
#    ./mktestsymlink.py $dbname $cfgfile
    sleep 10
    scidbtestharness --root-dir=testcases --suite-id=checkin,z_valgrind --report-file=Report.xml $@
    exit 0
fi


function scidb_init_start() {
    if [[ $SCIDB_INIT == "1" ]]; then
        echo "Initializing and Starting SciDB..."
        scidb.py stopall $dbname  $cfgfile
	sudo -u postgres /opt/Continuous/bin/scidb.py init_syscat $dbname $cfgfile
        scidb.py initall-force $dbname  $cfgfile
        scidb.py startall $dbname  $cfgfile
    else
        echo "Stopping and Starting SciDB... (SCIDB_INIT variable set, most likely in ScidbSubmitBuild)"
        scidb.py stopall $dbname  $cfgfile
        scidb.py startall $dbname  $cfgfile
    fi
    scidb.py purge $dbname $cfgfile
}

pwd
scidb_init_start

scidbtestprep.py "linkdata" `pwd`/../../tests tests $dbname $cfgfile
scidbtestprep.py "linkdata" `pwd`/../../doc/user/examples examples $dbname $cfgfile

scidbtestharness --port 8888 --debug 5 --root-dir=testcases --test-id=no $@

h_dir=`pwd`
scidb_init_start
# Set dbname for unit tests
sedexpr="s/mydb/$dbname/g"
sed -i -e "$sedexpr" testcases/t/checkin/other/unit.test


suites="aggregates,aql_neg_misc,compression,fits,iqueryabort,newaql,sparse,aql_misc,checkin,data_model,expression,flip_n_store,negative,other,pload,update,rankquantile,docscript,mpi"

delim=","
debugsuites="injecterror"
skipfile='disable.tests'

# Do not run these in CC until more tests are written -- very large gen code.
clientsuites="client" 

bldtype=`scidb --version | awk '{if ($1 == "Build") print $3}'`
if [[ $bldtype == "Debug" || $bldtype == "CC" ]]; then
    # Exclude the single instance configuration. injecterror does not work with this config.
    if [[ $dbname != "nightly" ]]; then 
	export suites=$suites$delim$debugsuites
    fi
fi

# Some queries are not supported in single instance configuration. 
# See ticket no. 1861
if [[ $dbname == 'nightly' ]]; then
    export skipfile='disable1i.tests'
fi

if [[ $bldtype == "RelWithDebInfo" ]]; then
    export suites=$suites$delim$clientsuites
fi

echo "Executing scidbtestharness... $suites"
export SCIDB_CLUSTER_NAME=$dbname
export SCIDB_CONFIG_FILE=$cfgfile

scidbtestharness --port 8888 --debug 5 --root-dir=testcases --suite-id=$suites --skip-tests=$skipfile --report-file=Report.xml $@

echo "Stopping SciDB..."
scidb.py stopall $dbname  $cfgfile
