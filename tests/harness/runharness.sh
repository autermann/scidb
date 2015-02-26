#!/bin/bash

set -eux

dbname=scidb
username=scidb
password=scidb123
skipfile='disable.tests'

SCP="scp -r -q -o StrictHostKeyChecking=no"
SSH="ssh -o StrictHostKeyChecking=no"

export TESTCASES=${BUILD_PATH}/tests/harness/testcases

# Set dbname for unit tests
sedexpr="s/dbname=mydb/dbname=${dbname}/g"
sed -i -e "$sedexpr" ${TESTCASES}/t/checkin/other/unit.test
sedexpr="s/user=mydb/user=${username}/g"
sed -i -e "$sedexpr" ${TESTCASES}/t/checkin/other/unit.test
sedexpr="s/password=mydb/password=${password}/g"
sed -i -e "$sedexpr" ${TESTCASES}/t/checkin/other/unit.test

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

# Bin directory
${SSH} scidb@${COORDINATOR} "mkdir -p ${BASE_PATH}/bin"
(${SCP} ${BUILD_PATH}/bin/* scidb@${COORDINATOR}:${BASE_PATH}/bin) || true

# Harness directory
${SSH} scidb@${COORDINATOR} "mkdir -p ${BASE_PATH}/000/tests/harness"
${SCP} ${TESTCASES} scidb@${COORDINATOR}:${BASE_PATH}/000/tests/harness
${SSH} scidb@${COORDINATOR} "ln -s ${BASE_PATH}/000/tests/harness/testcases ${BASE_PATH}/000/0/testcases"

# Other
${SCP} ${BUILD_PATH}/tests/harness/log4j.properties scidb@${COORDINATOR}:${BASE_PATH}/000/0
${SCP} ${BUILD_PATH}/tests/examples                 scidb@${COORDINATOR}:${BASE_PATH}/000/examples
${SCP} ${BUILD_PATH}/tests/unit                     scidb@${COORDINATOR}:${BASE_PATH}/000

# Start scidbtestharess
${SSH} scidb@${COORDINATOR} "(export SCIDB_CLUSTER_NAME=${SCIDB_CLUSTER_NAME} && export SCIDB_CONFIG_FILE=${SCIDB_CONFIG_FILE} && cd ${BASE_PATH}/000/0 && scidbtestharness --debug 5 --root-dir=../tests/harness/testcases --skip-tests=$skipfile --report-file=Report.xml)"

# Grab result
rm -rf ${TESTCASES}
${SCP} scidb@${COORDINATOR}:${BASE_PATH}/000/tests/harness/testcases ${TESTCASES}

# Clean coordinator
cleanup_coordinator
