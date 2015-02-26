#!/bin/bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=${SCIDB_NAME:=mydb}
export DB_USER=${SCIDB_DB_USER:=mydb}
export DB_PASSWD=${SCIDB_DB_PASSWD:=mydb}
export IQUERY_HOST=${SCIDB_HOST:=localhost}
export IQUERY_PORT=${SCIDB_PORT:=1239}
export SCIDB_CLUSTER_NAME=$DB_NAME

export PYTHONPATH="${SCIDB_SOURCE_PATH}/tests/harness/pyLib/"

if [ "${SCIDB_BUILD_PATH}" != "" -a "${SCIDB_DATA_PATH}" != "" ] ; then
   rm -f ${SCIDB_DATA_PATH}/000/tests
   ln -s ${SCIDB_BUILD_PATH}/tests ${SCIDB_DATA_PATH}/000/tests

   export DOC_DATA="${SCIDB_BUILD_PATH}/tests/harness/testcases/data/doc"
   export TESTCASES_DIR="${SCIDB_BUILD_PATH}/tests/harness/testcases/"
fi
