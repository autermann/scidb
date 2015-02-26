#!/bin/bash

function ubuntu1204
{
# SciDB
apt-get install -y python2.7  libpqxx-3.1 libprotobuf7 liblog4cxx10 libboost-filesystem1.46.1 libboost-system1.46.1 libboost-regex1.46.1 libboost-serialization1.46.1 libboost-program-options1.46.1 libboost-thread1.46.1 libbz2-1.0 libreadline6 libgfortran3 libscalapack-mpi1 liblapack3gf libopenmpi1.3 openmpi-common openmpi-bin libmpich2-3 mpich2 expect python-paramiko libcppunit-1.12-1
}

function centos63()
{
/sbin/chkconfig iptables off
/sbin/service iptables stop
yum remove -y boost*
yum install -y zlib bzip2 readline python-paramiko libpqxx protobuf log4cxx boost-system boost-program-options boost-serialization boost-regex boost-filesystem boost-thread python-argparse sudo postgresql expect cppunit
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
