#!/bin/bash
# Requires root privs
if [ $# -lt 1 ]; then
   echo "Usage: install_scidb_packages.sh [<package_file_name>]+"
   exit 1
fi

PACKAGE_FILE_NAME_LIST=$@

function ubuntu1204
{
echo "Install SciDB packages: ${PACKAGE_FILE_NAME_LIST}"
dpkg -i ${PACKAGE_FILE_NAME_LIST}
}

function centos63
{
echo "Install SciDB packages: ${PACKAGE_FILE_NAME_LIST}"
rpm -i ${PACKAGE_FILE_NAME_LIST}
}


OS=`./os_detect.sh`

if [ "${OS}" = "CentOS 6.3" ]; then
    centos63
fi

if [ "${OS}" = "RedHat 6.3" ]; then
    centos63
fi

if [ "${OS}" = "Ubuntu 12.04" ]; then
    ubuntu1204
fi

rm -rf ${PACKAGE_FILE_NAME_LIST}
