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

#
# if required set EXECUTABLE_CMAKE_PATH and EXECUTABLE_CTEST_PATH to those paths with cmake, ctest versions >= 2.8
# and then use this script
# <source_path> is a path of the directory containing scidb code
# <build_path> is a path to be used for out-of-source builds

set -u

function print_help ()
{
echo "Usage : SciDBSubmitBuild.sh <source_path> <build_path> <base_path> <branch_name> <coordinator> <platform>"
echo "        <source_path> : is a path of the directory containing scidb code"
echo "        <build_path>  : is a path to be used for out-of-source builds"
echo "        <base_path>   : is a path to database"
echo "        <branch_name> : is the tag of branch (trunk, rel_11_12)"
echo "        <coordinator>    : platform name"
echo "        <platform>    : platform name used for cdash build name"
echo "        <thread_count>: thread count (-jX)"
echo ""
exit 1
}

if [ $# -ne 7 ]; then
    print_help 
fi

export TESTMODEL="Continuous"

export SOURCE_PATH=`readlink -f ${1}`
export BUILD_TYPE="RelWithDebInfo"
export BUILD_PATH=`readlink -f ${2}`
export BASE_PATH=`readlink -f ${3}`
export BRANCH_NAME="${4}"
export COORDINATOR="${5}"
export PLATFORM="${6}"
export THREAD_COUNT="${7}"

CTESTScriptLOCATION="$BUILD_PATH/cdash"

if ! test -f "${SOURCE_PATH}/cdash/SciDB_DashboardSubmission.cmake"
then
echo "File $CTESTScriptLOCATION/SciDB_DashboardSubmission.cmake does not exist"
exit 1
fi

rm -f ${SOURCE_PATH}/CTestConfig.cmake
cp ${SOURCE_PATH}/cdash/CTestConfig.cmake ${SOURCE_PATH}/CTestConfig.cmake
if ! test -f "$SOURCE_PATH/CTestConfig.cmake"
then
echo "File $SOURCE_PATH/CTestConfig.cmake does not exist"
exit 1
fi
export EXECUTABLE_CMAKE_PATH=/usr/bin/cmake
export EXECUTABLE_CTEST_PATH=/usr/bin/ctest

domain='.local.paradigm4.com'
export cdashclientip=`hostname`$domain

export LOCALHOST_ROOT_DIRECTORY=/var/www
CDASH_CUSTOM_FOLDER="cdash_logs"
echo ""

origdir=$(pwd)
echo "Info : Cleaning up build directory $BUILD_PATH"
rm -rf $BUILD_PATH
mkdir -p `dirname ${BUILD_PATH}`
 
cp -R $SOURCE_PATH $BUILD_PATH
export SOURCE_PATH=$BUILD_PATH
cd $BUILD_PATH
rm -f CMakeCache.txt
cmake . -DSCIDB_DOC_TYPE=NONE

export BUILD_REVISION=$(cat $BUILD_PATH/revision)
export BUILD_TAG="${PLATFORM}-${BRANCH_NAME}-${BUILD_REVISION}"
export PATH=${BUILD_PATH}/bin:${PATH}

TIMESTAMP=$(date +%H-%M-%S_%d.%m.%Y)

LOGFILE="/tmp/scidb_${TESTMODEL}_${BRANCH_NAME}_build.log_${TIMESTAMP}"
export CDASH__LOGFILE=$LOGFILE

export CDASH_TESTCASES_FOLDER="$CDASH_CUSTOM_FOLDER/scidb_${TESTMODEL}Build.log_${TIMESTAMP}"
mkdir -p "$LOCALHOST_ROOT_DIRECTORY/$CDASH_TESTCASES_FOLDER"
export scidbtestresultsURL=$CDASH_TESTCASES_FOLDER/r/
export scidbtestcasesURL=$CDASH_TESTCASES_FOLDER/t/

echo "Log file is ${LOGFILE}"
echo "Build rev $BUILD_REVISION"
echo "Build tag is ${BUILD_TAG}"
$EXECUTABLE_CTEST_PATH -S "cdash/SciDB_DashboardSubmission.cmake" -VV > "$LOGFILE" 2>&1
cd $origdir

export USE_VALGRIND=0
echo "Done."
echo ""
exit 0
