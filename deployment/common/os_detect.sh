#!/bin/bash

OS="not supported"
FILE=/etc/issue
if [ $# -eq 1 ]; then
    FILE=`readlink -f ${1}`
fi;
if [ `cat ${FILE} | grep "CentOS" | grep "6.3" | wc -l` = "1" ]; then
    OS="CentOS 6.3"
fi

if [ `cat ${FILE} | grep "Ubuntu" | grep "12.04" | wc -l` = "1" ]; then
    OS="Ubuntu 12.04"
fi

if [ `cat ${FILE} | grep "Red Hat" | grep "6.3" | wc -l` = "1" ]; then
    OS="RedHat 6.3"
fi

if [ "${OS}" = "not supported" ]; then
    echo "Not supported: "`cat ${FILE} | head -n1`
    exit 1
fi

echo ${OS}

exit 0
