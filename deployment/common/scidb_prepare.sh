#!/bin/bash

set -eu

SCIDB_VER="${1}"
ACTUAL=~/.bashrc
NEW=~/.bashrc.new

if [ "0" == "`cat ${ACTUAL} | grep -i scidb | wc -l`" ]; then
    cp ${ACTUAL} ${NEW}
    cat ${ACTUAL} | grep -v return > ${NEW}
    echo "export SCIDB_VER=${SCIDB_VER}" >> ${NEW}
    echo "export PATH=/opt/scidb/\$SCIDB_VER/bin:/opt/scidb/\$SCIDB_VER/share/scidb:\$PATH" >> ${NEW}
    echo "export LD_LIBRARY_PATH=/opt/scidb/\$SCIDB_VER/lib:\$LD_LIBRARY_PATH" >> ${NEW}
    echo "export IQUERY_PORT=1239" >> ${NEW}
    echo "export IQUERY_HOST=localhost" >> ${NEW}
    mv ${NEW} ${ACTUAL}
    echo "source ${ACTUAL}" >> ~/.bash_profile
fi;
