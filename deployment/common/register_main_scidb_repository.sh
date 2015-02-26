#!/bin/bash

SCIDB_VER="${1}"

function centos63()
{
yum install -y wget
REPO_FILE=/etc/yum.repos.d/SciDB.repo
echo "[scidb]" > ${REPO_FILE}
echo "name=SciDB repo" >> ${REPO_FILE}
echo "baseurl= http://downloads.paradigm4.com/centos6.3/${SCIDB_VER}" >> ${REPO_FILE}
echo "gpgkey=http://downloads.paradigm4.com/key" >> ${REPO_FILE}
echo "gpgcheck=1" >> ${REPO_FILE}
wget http://downloads.paradigm4.com/key
rpm --import key
rm -f key
yum clean all
}

function ubuntu1204()
{
apt-get update
apt-get install -y wget
REPO_FILE=/etc/apt/sources.list.d/scidb.list
echo "deb http://downloads.paradigm4.com/ ubuntu12.04/${SCIDB_VER}/" > ${REPO_FILE}
echo "deb-src http://downloads.paradigm4.com/ ubuntu12.04/${SCIDB_VER}/" >> ${REPO_FILE}
wget -O- http://downloads.paradigm4.com/key | apt-key add -
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
