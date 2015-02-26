#!/bin/bash

set -eu

username=${1}
password="${2}"
network=${3}

if !(echo "${network}" | grep / 1>/dev/null); then
   echo "Invalid network format in ${network}"
   echo "Usage: configure_postgresql.sh network_ip (where network_ip=W.X.Y.Z/N) "
   exit 1;
fi

function postgresql_sudoers ()
{
    POSTGRESQL_SUDOERS=/etc/sudoers.d/postgresql
    echo "Defaults:${username} !requiretty" > ${POSTGRESQL_SUDOERS}
    echo "${username} ALL =(postgres) NOPASSWD: ALL" >> ${POSTGRESQL_SUDOERS}
    chmod 0440 ${POSTGRESQL_SUDOERS}
}

function centos63()
{
    yum install -y postgresql postgresql-server postgresql-contrib expect
    /sbin/chkconfig postgresql on
    service postgresql initdb || true
    restart="service postgresql restart"
    status="service postgresql status"
}

OS=`./os_detect.sh`
case ${OS} in
    "CentOS 6.3")
	centos63
	;;
    "RedHat 6.3")
	centos63
	;;
    "Ubuntu 12.04")
	apt-get install -y python-paramiko python-crypto postgresql-8.4 postgresql-contrib-8.4 expect
	restart="/etc/init.d/postgresql restart"
	status="/etc/init.d/postgresql status"
	;;
    *)
	echo "Not supported OS";
	exit 1
esac;

postgresql_sudoers
./configure_postgresql.py "${OS}" "${username}" "${password}" "${network}" || true
${restart}
${status}
