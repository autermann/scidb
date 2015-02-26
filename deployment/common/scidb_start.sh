#!/bin/bash

set -u

database=${1}
SCIDB_VER="${2}"
/opt/scidb/${SCIDB_VER}/bin/scidb.py startall ${database}
until /opt/scidb/${SCIDB_VER}/bin/iquery -aq "list()" > /dev/null 2>&1; do sleep 1; done
