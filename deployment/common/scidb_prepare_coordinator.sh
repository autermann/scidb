#!/bin/bash

set -eu

username="${1}"
database="${2}"
SCIDB_VER="${3}"

expect <<EOF
set timeout -1
spawn sudo -u postgres /opt/scidb/${SCIDB_VER}/bin/scidb.py init_syscat ${database}
expect eof
catch wait result
if {[lindex \$result 3]!=0} { exit [lindex \$result 3] }
EOF
su ${username} -c "/opt/scidb/${SCIDB_VER}/bin/scidb.py initall-force ${database}"
