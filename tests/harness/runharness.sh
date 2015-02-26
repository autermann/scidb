#!/bin/bash

set -eux

dbname=scidb
username=scidb
password=scidb123
cfgfile=''


SCP="scp -r -q -o StrictHostKeyChecking=no"
SSH="ssh -o StrictHostKeyChecking=no"

export TESTCASES=${BUILD_PATH}/tests/harness/testcases

# change directory

#./scidbtestprep.py "linkdata" `pwd`/../../tests tests $dbname $cfgfile
#./scidbtestprep.py "linkdata" `pwd`/../../doc/user/examples examples $dbname $cfgfile

# Set dbname for unit tests
sedexpr="s/dbname=mydb/dbname=${dbname}/g"
sed -i -e "$sedexpr" ${TESTCASES}/t/checkin/other/unit.test
sedexpr="s/user=mydb/user=${username}/g"
sed -i -e "$sedexpr" ${TESTCASES}/t/checkin/other/unit.test
sedexpr="s/password=mydb/password=${password}/g"
sed -i -e "$sedexpr" ${TESTCASES}/t/checkin/other/unit.test

suites="aggregates,aql_neg_misc,compression,fits,iqueryabort,newaql,sparse,aql_misc,checkin,data_model,expression,flip_n_store,negative,other,update,rankquantile,mpi"

delim=","
debugsuites="injecterror"
skipfile='disable.tests'

# Do not run these in CC until more tests are written -- very large gen code.
clientsuites="client" 


function cleanup_coordinator ()
{
${SSH} scidb@${COORDINATOR} "rm -rf ${BASE_PATH}/000/tests"
${SSH} scidb@${COORDINATOR} "rm -rf ${BASE_PATH}/000/examples"
${SSH} scidb@${COORDINATOR} "rm -f ${BASE_PATH}/000/0/log4j.properties"
${SSH} scidb@${COORDINATOR} "rm -rf ${BASE_PATH}/000/0/testcases"
${SSH} scidb@${COORDINATOR} "rm -rf ${BASE_PATH}/000/unit"
${SSH} scidb@${COORDINATOR} "rm -rf ${BASE_PATH}/bin"
}

# Prepare coordinator
cleanup_coordinator

# Copy directories required for start scidbtestharness
mkdir -p ${TESTCASES}/log
${SSH} scidb@${COORDINATOR} "mkdir -p ${BASE_PATH}/000/tests/harness"
${SSH} scidb@${COORDINATOR} "mkdir -p ${BASE_PATH}/bin"
${SCP} ${TESTCASES} scidb@${COORDINATOR}:${BASE_PATH}/000/tests/harness/testcases
${SCP} ${BUILD_PATH}/tests/harness/log4j.properties scidb@${COORDINATOR}:${BASE_PATH}/000/0
(${SCP} ${BUILD_PATH}/bin/* scidb@${COORDINATOR}:${BASE_PATH}/bin) || true
${SCP} ${BUILD_PATH}/tests/examples scidb@${COORDINATOR}:${BASE_PATH}/000/examples

${SSH} scidb@${COORDINATOR} "mkdir -p ${BASE_PATH}/000/0/testcases"
${SCP} ${TESTCASES}/data scidb@${COORDINATOR}:${BASE_PATH}/000/0/testcases

${SSH} scidb@${COORDINATOR} "mkdir -p ${BASE_PATH}/000/0/testcases/t/client"
${SCP} ${TESTCASES}/t/client/basic.py scidb@${COORDINATOR}:${BASE_PATH}/000/0/testcases/t/client
${SCP} ${TESTCASES}/t/client/querylist.csv scidb@${COORDINATOR}:${BASE_PATH}/000/0/testcases/t/client

${SCP} ${BUILD_PATH}/tests/unit scidb@${COORDINATOR}:${BASE_PATH}/000

suites="aggregates,aql_neg_misc,compression,fits,iqueryabort,newaql,sparse,aql_misc,checkin,data_model,expression,flip_n_store,negative,other,update,rankquantile,mpi,client,pload,docscriptq"

# Start scidbtestharess
${SSH} scidb@${COORDINATOR} "(cd ${BASE_PATH}/000/0 && scidbtestharness --debug 5 --root-dir=../tests/harness/testcases --skip-tests=$skipfile --report-file=Report.xml)"

# Grab result
rm -rf ${TESTCASES}
${SCP} scidb@${COORDINATOR}:${BASE_PATH}/000/tests/harness/testcases ${TESTCASES}

# Clean coordinator
cleanup_coordinator
