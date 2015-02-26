#!/bin/bash

set -eu

user=${1}
key=${2}

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

# Update right to ~/.ssh directory
function update_rights ()
{
    if [ 1 -eq $update ]; then 
	disable_host_checking
	chmod go-rwx,u+rwx ${HOME}/.ssh
	chmod a-x,go-rw,u+rw ${HOME}/.ssh/*
# RedHat and CentOS selinux issue: http://stackoverflow.com/questions/9741574/redhat-6-oracle-linux-6-is-not-allowing-key-authentication-via-ssh
	restorecon=`whereis restorecon | awk '{print $2}'`
	if [ -n "${restorecon}" ]; then
	    echo "Fix SELinux contextes for ${HOME}/.ssh"
	    ${restorecon} -R -v ${HOME}/.ssh
	fi;
    fi;
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
