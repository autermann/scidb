#!/bin/bash

set -eu

SCIDB_VER="${1}"
ORIG=~/.bashrc
NEW=~/.bashrc.new

if grep SCIDB_VER ${ORIG} ; then
    cp ${ORIG} ${NEW}
    cat ${ORIG} | grep -v return > ${NEW}
    echo "export SCIDB_VER=${SCIDB_VER}" >> ${NEW}
    echo "export PATH=/opt/scidb/\$SCIDB_VER/bin:/opt/scidb/\$SCIDB_VER/share/scidb:\$PATH" >> ${NEW}
    echo "export LD_LIBRARY_PATH=/opt/scidb/\$SCIDB_VER/lib:\$LD_LIBRARY_PATH" >> ${NEW}
    echo "export IQUERY_PORT=1239" >> ${NEW}
    echo "export IQUERY_HOST=localhost" >> ${NEW}
    mv ${NEW} ${ORIG}
    echo "source ${ORIG}" >> ~/.bash_profile
fi;
