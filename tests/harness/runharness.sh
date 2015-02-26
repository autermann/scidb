#!/bin/bash

set -eu

export SCIDB_PATH=`readlink -f $(dirname $0)/../..`

# PostgreSQL catalog/config:
export DB_NAME=${DB_NAME}
export DB_USER=${DB_USER}
export DB_PASSWD=${DB_PASSWD}

export BASE_PATH=${BASE_PATH}

# linux username (on coordinator host)
export USERNAME=${USERNAME:=scidb}

# Read SciDB version
export SCIDB_VER=`awk -F . '{print $1"."$2}' ${SCIDB_PATH}/version`

# Required for failover and failover2 tests
export SCIDB_CLUSTER_NAME=${DB_NAME}
export SCIDB_CONFIG_FILE=/opt/scidb/${SCIDB_VER}/etc/config.ini

# skip filename (file with list of disabled tests)
export SKIPFILE=${SKIPFILE:=disable.tests}

# Define some usefull variables
export TESTCASES=${SCIDB_PATH}/tests/harness/testcases
SCP="scp -r -q -o StrictHostKeyChecking=no "
SSH="ssh -o StrictHostKeyChecking=no ${USERNAME}@${COORDINATOR}"

export COORDINATOR=${COORDINATOR}

# Print help message and exit
function print_help ()
{
echo <<EOF "Usage: 
  runharness.sh <all|prepare|run|collect|cleanup>
    all        prepare + run + collect + cleanup
    prepare    prepare coordinator (for execute scidbtestharness)
    run        run scidbtestharness
    collect    collect test results (after scidbtestharness execution)
    cleanup    clean cooordinator (after scidbtestharness execution)

  List of environment variables which control script execution:
    Variable_name    Varible_default    Description
    DB_NAME          (none)                  database name
    DB_USER          (none)                  PostgreSQL user name
    DB_PASSWD        (none)                  PostgreSQL user password
    USERNAME         (none)                  linux username (for access to coordinator)
    SKIPFILE         disable.tests           file with list of disabled tests
    SCIDB_PATH       ($(dirname 0)/../..)    path to SciDB source code or build
    COORDINATOR      (none)                  coordinator hostname or IP address
    BASE_PATH        (none)                  path to SciDB database

Environment variables without default options should be specified.
You should have password-less ssh access to coordinator."
EOF
exit 1;
}

function cleanup_coordinator ()
{
echo "Cleaning coordinator ${COORDINATOR}"
${SSH} "rm -rf ${BASE_PATH}/000/tests"
${SSH} "rm -rf ${BASE_PATH}/000/examples"
${SSH} "rm -f  ${BASE_PATH}/000/0/log4j.properties"
${SSH} "rm -rf ${BASE_PATH}/000/0/testcases"
${SSH} "rm -rf ${BASE_PATH}/000/unit"
${SSH} "rm -rf ${BASE_PATH}/bin"
}

function prepare_coordinator ()
{
echo "Prepare scidbtestharness on coordinator ${COORDINATOR}"
# Prepare coordinator
cleanup_coordinator

# Copy directories required for start scidbtestharness
mkdir -p ${TESTCASES}/log

# Bin directory
${SSH} "mkdir -p ${BASE_PATH}/bin"
(${SCP} ${SCIDB_PATH}/bin/* ${USERNAME}@${COORDINATOR}:${BASE_PATH}/bin) || true

# Harness directory
${SSH} "mkdir -p ${BASE_PATH}/000/tests/harness"
${SCP} ${TESTCASES} ${USERNAME}@${COORDINATOR}:${BASE_PATH}/000/tests/harness
${SSH} "ln -s ${BASE_PATH}/000/tests/harness/testcases ${BASE_PATH}/000/0/testcases"

# Other
${SCP} ${SCIDB_PATH}/tests/harness/log4j.properties ${USERNAME}@${COORDINATOR}:${BASE_PATH}/000/0
${SCP} ${SCIDB_PATH}/tests/examples                 ${USERNAME}@${COORDINATOR}:${BASE_PATH}/000/examples
${SCP} ${SCIDB_PATH}/tests/unit                     ${USERNAME}@${COORDINATOR}:${BASE_PATH}/000
}

function run_test ()
{
COMMAND="export SCIDB_CLUSTER_NAME=${DB_NAME} &&"
COMMAND="${COMMAND} export SCIDB_CONFIG_FILE=${SCIDB_CONFIG_FILE} &&"
COMMAND="${COMMAND} export DB_NAME=${DB_NAME} &&"
COMMAND="${COMMAND} export DB_USER=${DB_USER} &&"
COMMAND="${COMMAND} export DB_PASSWD=${DB_PASSWD} &&"
COMMAND="${COMMAND} cd ${BASE_PATH}/000/0 &&"
COMMAND="${COMMAND} scidbtestharness --debug 5 --root-dir=../tests/harness/testcases --skip-tests=${SKIPFILE} --report-file=Report.xml"
COMMAND="${COMMAND} | tee ${BASE_PATH}/000/tests/harness/testcases/runharness.log"
echo "Run scidbtestharness on ${USERNAME}@${COORDINATOR}, command is:"
echo "${COMMAND}"
${SSH} "${COMMAND}"
}

function collect_test ()
{
echo "Collect scidbtestharness result"
rm -rf ${TESTCASES}
${SCP} ${USERNAME}@${COORDINATOR}:${BASE_PATH}/000/tests/harness/testcases ${TESTCASES}
}

if [ $# -ne 1 ]; then
  print_help
fi;

case ${1} in
    "prepare")
	prepare_coordinator
	;;
    "run")
	run_test
	;;
    "collect")
	collect_test
	;;
    "cleanup")
	cleanup_coordinator
	;;
    "all")
	prepare_coordinator
	run_test
	collect_test
	cleanup_coordinator
	;;
    *)
	print_help
	;;
esac
