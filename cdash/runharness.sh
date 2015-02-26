#!/bin/bash
#
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2008-2013 SciDB, Inc.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
#

set -eu

export SCIDB_PATH=`readlink -f $(dirname $0)/..`
cd ${SCIDB_PATH}/tests/harness

# PostgreSQL catalog/config:
export DB_NAME=${DB_NAME}
export DB_USER=${DB_USER}
export DB_PASSWD=${DB_PASSWD}

export BASE_PATH=${BASE_PATH}

export TEST_TYPE=${TEST_TYPE}
case ${TEST_TYPE} in
    release|debug|valgrind) ;;
    *) 
	echo "TEST_TYPE should be 'release', 'debug' or 'valgrind' (you provided '${TEST_TYPE}')"
	exit -1
	;;
esac

case ${TEST_TYPE} in
    valgrind)
	SUITE_ID="--suite-id=checkin,z_valgrind"
	;;
    *)
	SUITE_ID=""
	;;
esac

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
export TESTUTILS=${SCIDB_PATH}/tests/utils
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
    SCIDB_PATH       ($(dirname 0)/..)    path to SciDB source code or build
    TEST_TYPE        (none)                  test type: 'release' or 'debug'
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
${SSH} "rm -rf ${BASE_PATH}/000/utils"
${SSH} "rm -f  ${BASE_PATH}/000/0/log4j.properties"
${SSH} "rm -rf ${BASE_PATH}/000/0/testcases"
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

# Test utils
${SCP} ${TESTUTILS} ${USERNAME}@${COORDINATOR}:${BASE_PATH}/000/tests/utils
${SSH} "ln -s ${BASE_PATH}/000/tests/utils ${BASE_PATH}/000/utils"

# Other
${SCP} ${SCIDB_PATH}/tests/harness/log4j.properties ${USERNAME}@${COORDINATOR}:${BASE_PATH}/000/0
}

function run_test ()
{
C="export SCIDB_CLUSTER_NAME=${DB_NAME} &&"
C="${C} export SCIDB_CONFIG_FILE=${SCIDB_CONFIG_FILE} &&"
C="${C} export DB_NAME=${DB_NAME} &&"
C="${C} export DB_USER=${DB_USER} &&"
C="${C} export DB_PASSWD=${DB_PASSWD} &&"
C="${C} export DOC_DATA=${BASE_PATH}/000/tests/harness/testcases/data/doc &&"
C="${C} export TESTCASES_DIR=${BASE_PATH}/000/tests/harness/testcases/ &&"
C="${C} cd ${BASE_PATH}/000/0 &&"
C="${C} scidbtestharness --debug 5 --root-dir=../tests/harness/testcases --skip-tests=${SKIPFILE} ${SUITE_ID} --report-file=Report.xml"
C="${C} | tee ${BASE_PATH}/000/tests/harness/testcases/runharness.log"
echo "Run scidbtestharness on ${USERNAME}@${COORDINATOR}, command is:"
echo "${C}"
${SSH} "${C}"
}

function collect_test ()
{
echo "Collect scidbtestharness result"
rm -rf ${TESTCASES}
${SCP} ${USERNAME}@${COORDINATOR}:${BASE_PATH}/000/tests/harness/testcases ${TESTCASES}

# Generate the scidb.py tar file with logs and core files, and copy it over.
${SSH} "${BASE_PATH}/bin/scidb.py dbginfo ${DB_NAME} \
       >${BASE_PATH}/000/scidb-dbginfo.log"
${SCP} ${USERNAME}@${COORDINATOR}:${BASE_PATH}/000/scidb-dbginfo.log \
       ${USERNAME}@${COORDINATOR}:${BASE_PATH}/000/0/all*tar ${TESTCASES}/log

# Clean up,
${SSH} "rm ${BASE_PATH}/000/scidb-dbginfo.log ${BASE_PATH}/000/0/*tar \
           ${BASE_PATH}/000/*/*tgz"
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
