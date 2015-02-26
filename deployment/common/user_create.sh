#!/bin/bash

set -eu

username=$1
password=$2
OS=`./os_detect.sh`

available=0
for name in `cat /etc/passwd | awk -F : '{print $1}' | grep ${username}`; do
    if [ "${name}" == "${username}" ]; then
	available=1
    fi;
done;


function adduser_centos63 ()
{
adduser ${username}
echo "$password" > ua
echo "$password" >> ua
cat ua | passwd ${username}
rm -f ua
chown -R ${username}:${username} /home/${username}
}


function adduser_ubuntu1204 ()
{
echo "$password" > ua
echo "$password" >> ua
for i in 1 2 3 4 5; do
    echo "" >> ua
done;
echo "y" >> ua
cat ua | adduser ${username}
rm -f ua
chown -R ${username}:${username} /home/${username}
}


if [[ ($available -eq 1) || ("${username}" == "root") ]]; then
    echo "User ${username} available"
else
    echo "Create user ${username}"
    case ${OS} in
	"CentOS 6.3")
	    #yum install -y sudo
	    adduser_centos63
	    ;;
	"RedHat 6.3")
	    adduser_centos63
	    ;;
	"Ubuntu 12.04")
	    adduser_ubuntu1204
	    ;;
    esac
fi;
