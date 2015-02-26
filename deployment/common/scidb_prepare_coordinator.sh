#!/bin/bash

set -eu

username="${1}"
password="${2}"
database="${3}"
SCIDB_VER="${4}"

expect <<EOF
set timeout -1
spawn sudo -u postgres /opt/scidb/${SCIDB_VER}/bin/scidb.py init_syscat ${database}
expect eof
spawn scidb.py initall ${database}
expect "This will delete all data and reinitialize storage"
send "y\r"
expect eof
EOF
