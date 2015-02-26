#!/bin/bash

function centos63()
{
REPO_FILE=/etc/yum.repos.d/SciDB.repo
echo "[scidb]" > ${REPO_FILE}
echo "name=SciDB repo" >> ${REPO_FILE}
echo "baseurl= http://downloads.paradigm4.com/yum/centos6.3/12.12.0" >> ${REPO_FILE}
echo "gpgcheck=0" >> ${REPO_FILE}
yum clean all
}

function ubuntu1204()
{
REPO_FILE=/etc/apt/sources.list.d/scidb.list
echo "deb http://downloads.paradigm4.com/apt/ precise/12.12.0/" > ${REPO_FILE}
echo "deb-src http://downloads.paradigm4.com/apt/ precise/12.12.0/" >> ${REPO_FILE}
wget -O- http://downloads.paradigm4.com/apt/key | apt-key add -
apt-get update
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
