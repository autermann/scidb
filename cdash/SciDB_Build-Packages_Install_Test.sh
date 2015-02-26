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

# Path
export SOURCE_PATH=${SOURCE_PATH}
export BUILD_PATH=${BUILD_PATH}
export PACKAGES_PATH=${PACKAGES_PATH}

# Build type
export BUILD_TYPE=${BUILD_TYPE}

# PostgreSQL catalog/config:
export DB_NAME=${DB_NAME}
export DB_USER=${DB_USER}
export DB_PASSWD=${DB_PASSWD}
export NETWORK=${NETWORK}

# SciDB config
export BASE_PATH=${BASE_PATH}
export INSTANCE_COUNT=${INSTANCE_COUNT}
export NO_WATCHDOG=${NO_WATCHDOG}
export REDUNDANCY=${REDUNDANCY}
export CHUNK_SEGMENT_SIZE=${CHUNK_SEGMENT_SIZE}

# linux username (on coordinator host)
export USERNAME=${USERNAME}

# IP address or hostname of coordinator
export COORDINATOR=$(cat ${BUILD_PATH}/coordinator)

# IP addresses or hostname of all test machines from cluster (including coordinator)
export TEST_HOST_LIST="$(cat ${BUILD_PATH}/host_list)"

export DEPLOY_SH="${SOURCE_PATH}/deployment/deploy.sh"

echo "Build SciDB packages"
${DEPLOY_SH} build ${BUILD_TYPE} ${PACKAGES_PATH}

echo "Prepare PostgreSQL"
${DEPLOY_SH} prepare_postgresql ${DB_USER} ${DB_PASSWD} ${NETWORK} ${COORDINATOR}

echo "Install SciDB packages"
${DEPLOY_SH} scidb_install ${PACKAGES_PATH} ${TEST_HOST_LIST}
echo "Run post-install hook '${POST_INSTALL_HOOK}'"
${POST_INSTALL_HOOK}

echo "Prepare SciDB cluster"
USER_PASSWD="${DB_PASSWD}" #XXX fix me, we should not mix the two passwords
echo "${USER_PASSWD}" | ${DEPLOY_SH} scidb_prepare ${USERNAME} "" ${DB_USER} "${DB_PASSWD}" ${DB_NAME} ${BASE_PATH} ${INSTANCE_COUNT} ${NO_WATCHDOG} ${REDUNDANCY} ${CHUNK_SEGMENT_SIZE} ${TEST_HOST_LIST}

echo "Start SciDB cluster"
echo "${USER_PASSWD}" | ${DEPLOY_SH} scidb_start ${USERNAME} ${DB_NAME} ${COORDINATOR}
echo "Run post-start hook '${POST_START_HOOK}'"
${POST_START_HOOK}

${BUILD_PATH}/cdash/runharness.sh all
