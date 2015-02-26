#!/bin/bash

set -eu

SCIDB_VER="${1}"

function centos63_ccache()
{
wget -c http://dl.fedoraproject.org/pub/epel/6/x86_64/ccache-3.1.6-2.el6.x86_64.rpm
yum install -y ccache-3.1.6-2.el6.x86_64.rpm || true
}

function ubuntu1204 ()
{
echo "Prepare Ubuntu 12.04 for build SciDB"
apt-get update

# Build dependencies:
apt-get install -y build-essential cmake libboost1.46-all-dev libpqxx-3.1 libpqxx3-dev libprotobuf7 libprotobuf-dev protobuf-compiler doxygen flex bison liblog4cxx10 liblog4cxx10-dev libcppunit-1.12-1 libcppunit-dev libbz2-dev zlib1g-dev subversion libreadline6-dev libreadline6 python-paramiko python-crypto xsltproc gfortran libscalapack-mpi1 liblapack-dev libopenmpi-dev swig2.0 libmpich2-dev mpich2 expect
apt-get install -y git git-svn

# Reduce rebuild time:
apt-get install -y ccache

# Documentation: 
apt-get install -y fop docbook-xsl

# Testing:
apt-get install -y postgresql-8.4 postgresql-contrib-8.4

# ScaLAPACK tests:
apt-get install -y time
echo "DONE"
}

function centos63 ()
{
echo "Prepare CentOS 6.3 for build SciDB"

# Build dependencies:
yum install -y gcc gcc-c++ gcc-gfortran subversion doxygen flex bison zlib-devel bzip2-devel readline-devel rpm-build python-paramiko postgresql-devel cppunit-devel python-devel cmake make scidb-boost-${SCIDB_VER}-devel swig2 protobuf-devel log4cxx-devel libpqxx-devel expect mpich2-devel lapack-devel blas-devel

yum install -y git git-svn
# Reduce build time
yum install -y wget
centos63_ccache

# Documentation
yum install -y fop libxslt docbook-style-xsl

# Testing:
yum install -y postgresql postgresql-server postgresql-contrib python-argparse
echo "DONE"
}

function redhat63 ()
{
echo "We do not support build SciDB under RedHat 6.3. Please use CentOS 6.3 instead"
exit 1
}

OS=`./os_detect.sh`

if [ "${OS}" = "CentOS 6.3" ]; then
    centos63
fi

if [ "${OS}" = "RedHat 6.3" ]; then
    redhat63
fi

if [ "${OS}" = "Ubuntu 12.04" ]; then
    ubuntu1204
fi
