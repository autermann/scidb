#!/bin/bash

set -eu

username=${1}
OS=`./os_detect.sh`

function chroot_sudoers_mock ()
{
CHROOT_SUDOERS=/etc/sudoers.d/chroot_builder
echo "Defaults:${username} !requiretty" > ${CHROOT_SUDOERS}
echo "Cmnd_Alias RMSCIDB_PACKAGING = /bin/rm -rf /tmp/scidb_packaging.*" >> ${CHROOT_SUDOERS}
echo "${username} ALL = NOPASSWD:/usr/sbin/mock, NOPASSWD:/bin/which, NOPASSWD:RMSCIDB_PACKAGING" >> ${CHROOT_SUDOERS}
chmod a-wx,o-r,ug+r ${CHROOT_SUDOERS}
}

function update_mock_config ()
{
MOCK_CONFIG=/etc/mock/centos-6.3-x86_64.cfg
if [ "0" == `cat ${MOCK_CONFIG} | grep scidb | wc -l` ]; then
    NEW_MOCK_CONFIG=${MOCK_CONFIG}.new
    REPO_FILE=/etc/yum.repos.d/SciDB.repo
    head -n-1 "${MOCK_CONFIG}" > ${NEW_MOCK_CONFIG}
    echo "" >> ${NEW_MOCK_CONFIG}
    cat ${REPO_FILE} >> ${NEW_MOCK_CONFIG}
    echo "\"\"\"" >> ${NEW_MOCK_CONFIG}
    cat ${NEW_MOCK_CONFIG} > ${MOCK_CONFIG}
    rm ${NEW_MOCK_CONFIG}
fi;
}

function centos63 ()
{
yum install -y gcc make rpm-build mock python-argparse git git-svn
chroot_sudoers_mock
update_mock_config
}

function chroot_sudoers_pbuilder ()
{
CHROOT_SUDOERS=/etc/sudoers.d/chroot_builder
echo "Defaults:${username} !requiretty" > ${CHROOT_SUDOERS}
echo "Cmnd_Alias RMSCIDB_PACKAGING = /bin/rm -rf /tmp/scidb_packaging.*" >> ${CHROOT_SUDOERS}
echo "${username} ALL = NOPASSWD:/usr/sbin/pbuilder, NOPASSWD:/bin/which, NOPASSWD:RMSCIDB_PACKAGING" >> ${CHROOT_SUDOERS}
chmod a-wx,o-r,ug+r ${CHROOT_SUDOERS}
}

function ubuntu1204 ()
{
apt-get update
apt-get install -y build-essential dpkg-dev pbuilder debhelper m4 git git-svn
chroot_sudoers_pbuilder
}


function redhat63 ()
{
echo "We do not support build SciDB under RedHat 6.3. Please use CentOS 6.3 instead"
exit 1
}

if [ "${OS}" = "CentOS 6.3" ]; then
    centos63
fi

if [ "${OS}" = "RedHat 6.3" ]; then
    redhat63
fi

if [ "${OS}" = "Ubuntu 12.04" ]; then
    ubuntu1204
fi
