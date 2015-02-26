#!/bin/bash
########################################
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2008-2011 SciDB, Inc.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the GNU General Public License for the complete license terms.
#
# You should have received a copy of the GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/>.
#
# END_COPYRIGHT
########################################

set -eu

# Print help message and exit
function print_help ()
{
echo <<EOF "Usage: 
  SciDBSubmitBuild.sh <source_path> <build_path> <packages_path> <platform> <build_thread_count> <db_user> <db_passwd> <db_name> <network> <base_path> <instance_count> <redundancy> <username> <coordinator> [host_list]

    source_path           path to SciDB source
    build_path            path to SciDB build (if source_path != build_path, then build path !!!would be recreated!!!)
    packages_path         path to SciDB packages
    platform              cdash platform name (used in CDash panel build name)
    build_thread_count    used in 'make -f<build_thread_count>'
    db_user               database user name (PostgreSQL)
    db_passwd             database user password (PostgreSQL)
    db_name               database name (SciDB)
    network               network of test cluster ('a.b.c.d/e' format)
    base_path             base path (SciDB)
    instance_count        count of SciDB instance per machine
    redundancy            redundancy config option
    username              linux username for access to test machines
    coordinator           IP address or hostname of the coordinator
    [host_list]           IP addresses or hostnames of another test machines"
EOF
exit 1;
}

# Read arguments
if [ $# -lt 14 ]; then
  print_help
fi;

export SOURCE_PATH=`readlink -f ${1}`
export BUILD_PATH=`readlink -f ${2}`
export PACKAGES_PATH=`readlink -f ${3}`
export PLATFORM=${4}
export THREAD_COUNT=${5}
export DB_USER=${6}
export DB_PASSWD=${7}
export DB_NAME=${8}
export NETWORK="${9}"
export BASE_PATH=${10}
export INSTANCE_COUNT=${11}
export REDUNDANCY=${12}
export USERNAME=${13}
shift 13
TEST_HOST_LIST=($@)
export COORDINATOR=${TEST_HOST_LIST[0]}
echo "Coordinator: ${COORDINATOR}"
export TEST_HOST_LIST="$@"
echo "Host list: ${TEST_HOST_LIST}"

# We use in-source build for simplification (for right now)
# Feel free to change it to out-of-source build - we can find extra bugs
if [ "${SOURCE_PATH}" != "${BUILD_PATH}" ]; then
    rm -rf ${BUILD_PATH}
    mkdir -p ${BUILD_PATH}
fi;
rm -f ${BUILD_PATH}/CMakeCache.txt
cp -R ${SOURCE_PATH}/cdash ${BUILD_PATH}/cdash

cd ${BUILD_PATH}

# Workaround CTest problem: variables with space inside value are not available
# inside SciDB_DashboardSubmission.cmake 
# (for example TEST_HOST_LIST="10.0.20.233 10.0.20.236")
echo "${COORDINATOR}" > ${BUILD_PATH}/coordinator
echo "${TEST_HOST_LIST}" > ${BUILD_PATH}/host_list

# Define variables 
export BUILD_TYPE="RelWithDebInfo"
export SCIDB_VER=`awk -F . '{print $1"."$2}' ${SOURCE_PATH}/version`
export TEST_MODEL="Continuous"
export TIMESTAMP=$(date +%Y.%m.%d_%H-%M-%S)
export BUILD_REVISION=`(cd ${SOURCE_PATH} && svn info | grep Revision | awk '{ print $2 }')`
export BRANCH_NAME=`(cd ${SOURCE_PATH} && svn info | grep '^URL:' | egrep -o '(tags|branches)/[^/]+|trunk' | egrep -o '[^/]+$')`
export BUILD_TAG="${PLATFORM}-${BRANCH_NAME}-${BUILD_REVISION}-${TIMESTAMP}" 

# Path to CDash log, added as "Notes" to build
export CDASH_LOG="/tmp/${BUILD_TAG}"
echo "CDash log: ${CDASH_LOG}"

export CDASH_PATH_ROOT="/var/www"
export CDASH_PATH_RELATIVE="cdash_logs/scidb-${BUILD_TAG}"
export CDASH_PATH_RESULT="${CDASH_PATH_ROOT}/${CDASH_PATH_RELATIVE}"
mkdir -p ${CDASH_PATH_RESULT}
echo "CDash result path: ${CDASH_PATH_RESULT}"

# Copy CTest config file
rm -f ${BUILD_PATH}/CTestConfig.cmake
cp ${SOURCE_PATH}/cdash/CTestConfig.cmake ${BUILD_PATH}/CTestConfig.cmake

export scidbtestresultsURL=$CDASH_PATH_RELATIVE/r/
export scidbtestcasesURL=$CDASH_PATH_RELATIVE/t/

echo "Build revision ${BUILD_REVISION}"
echo "Build tag is ${BUILD_TAG}"
/usr/bin/ctest -S "cdash/SciDB_DashboardSubmission.cmake" -VV > "${CDASH_LOG}" 2>&1

export USE_VALGRIND=0
echo "Done."
echo ""
exit 0
