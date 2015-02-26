#!/bin/bash

set -eu

user=${1}
key=${2}
OS=`./os_detect.sh`
update=0

function add_public_key ()
{
    local new_key="${1}"
    if [ "0" == `cat ${HOME}/.ssh/authorized_keys | grep "${new_key}" | wc -l || true` ]; then 
	echo "${new_key}" >> ${HOME}/.ssh/authorized_keys
	update=1
    fi;
}

# Otherwise, ssh connect would/can ask about the "adding the host to the known host list"
function disable_host_checking ()
{
    echo "Host *" > ~/.ssh/config
    echo "   StrictHostKeyChecking no" >> ~/.ssh/config
    echo "   UserKnownHostsFile=/dev/null" >> ~/.ssh/config
}

function selinux_home_ssh ()
{
    chcon -R -v -t user_ssh_home_t ~/.ssh
}

# Update right to ~/.ssh directory
function update_rights ()
{
    disable_host_checking
    chmod go-rwx,u+rwx ${HOME}/.ssh
    chmod a-x,go-rw,u+rw ${HOME}/.ssh/*
    if [ "${OS}" = "CentOS 6.3" ]; then
	selinux_home_ssh
    fi 

    if [ "${OS}" = "RedHat 6.3" ]; then
	selinux_home_ssh
    fi
}

mkdir -p ${HOME}/.ssh
private=${HOME}/.ssh/id_rsa
public=${HOME}/.ssh/id_rsa.pub

cat ~/.bashrc | grep -v return > ~/.bashrc.new
cat ~/.bashrc.new > ~/.bashrc
rm ~/.bashrc.new

if [[ ("1" != `ls ${private} | wc -l || true`) || ("1" != `ls ${public} | wc -l || true`)]]; then
    rm -f ${private}
    rm -f ${public}
    echo "" | ssh-keygen -t rsa
fi;

add_public_key "${key}"
add_public_key "`cat ~/.ssh/id_rsa.pub`"
update_rights
