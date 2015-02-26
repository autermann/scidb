#!/bin/bash

set -eu

# Path
export SOURCE_PATH=${SOURCE_PATH}
export BUILD_PATH=${BUILD_PATH}
export PACKAGES_PATH=${PACKAGES_PATH}

# PostgreSQL catalog/config:
export DB_NAME=${DB_NAME}
export DB_USER=${DB_USER}
export DB_PASSWD=${DB_PASSWD}
export NETWORK=${NETWORK}

# SciDB config
export BASE_PATH=${BASE_PATH}
export INSTANCE_COUNT=${INSTANCE_COUNT}
export REDUNDANCY=${REDUNDANCY}

# linux username (on coordinator host)
export USERNAME=${USERNAME}

# IP address or hostname of coordinator
export COORDINATOR=$(cat ${BUILD_PATH}/coordinator)

# IP addresses or hostname of all test machines from cluster (including coordinator)
export TEST_HOST_LIST="$(cat ${BUILD_PATH}/host_list)"

export DEPLOY_SH="${SOURCE_PATH}/deployment/deploy.sh"

echo "Build SciDB packages"
${DEPLOY_SH} build ${PACKAGES_PATH}

echo "Prepare PostgreSQL"
${DEPLOY_SH} prepare_postgresql ${DB_USER} ${DB_PASSWD} ${NETWORK} ${COORDINATOR}

echo "Install SciDB packages"
${DEPLOY_SH} scidb_install ${PACKAGES_PATH} ${TEST_HOST_LIST}

echo "Prepare SciDB cluster"
USER_PASSWD="${DB_PASSWD}" #XXX fix me, we should not mix the two passwords
${DEPLOY_SH} scidb_prepare ${USERNAME} "${USER_PASSWD}" ${DB_USER} "${DB_PASSWD}" ${DB_NAME} ${BASE_PATH} ${INSTANCE_COUNT} ${REDUNDANCY} ${TEST_HOST_LIST}

echo "Start SciDB cluster"
${DEPLOY_SH} scidb_start ${USERNAME} ${DB_NAME} ${COORDINATOR}

${BUILD_PATH}/tests/harness/runharness.sh all
