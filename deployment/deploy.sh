#!/bin/bash

set -eux

function print_help ()
{
echo <<EOF "Usage: 
Access to remote hosts:
  deploy.sh access    <username> <password> <public_key> [host]

Prepare remote machines:
  deploy.sh prepare_toolchain    <username> [host]
  deploy.sh prepare_chroot       <username> [host]
  deploy.sh prepare_runtime      <username> [host]
  
  deploy.sh prepare_postgresql   <postgresql_username> <postgresql_password> <network> <host>

Build packages/repositories:
  deploy.sh build         <packages_path>
  deploy.sh build_fast    <packages_path>
  deploy.sh build_deps    <packages_path>

SciDB control on remote machines:
  deploy.sh scidb_install    <packages_path> [host]
  deploy.sh scidb_remove     <packages_path> [host]
  deploy.sh scidb_prepare    <username> <password> <database> <base_path> <instance_count> [host]
  deploy.sh scidb_start      <username> <coordinator>

Description:
  access               provide password-less ssh access to [host] for <username> with <password> by <public_key> (use "" for if you want use ~/.ssh/id_rsa.pub)

  prepare_toolchain    prepare [host] for building SciDB as <username>. Install the packages required for building SciDB from source.
  prepare_chroot       prepare [host] for building SciDB packages as <username>. Install required packages, configure tools, prepare chroot.
  prepare_runtime      install dependencies on [host] for running SciDB as <username>. Required for installation SciDB from files (would be removed after change installation way from files to repository).
  prepare_postgresql   install & configure PostgreSQL as SciDB catalog
  build                build SciDB packages to <packages_path> in clean enviroment (chroot)
  build_fast           dirty build SciDB packages (from in-source already compiled build)
  build_deps           build packages for dependencies to <packages_path> (on localhost)

  scidb_install        Install SciDB packages on [host]. The required repositories for the SciDB packages are expected to be already registered on [host]. First host would considered as coordinator.
  scidb_remove         remove SciDB packages from [host]
  scidb_prepare        prepare for running SciDB cluster on [host] (with <instance_count> on every host). First host would considered as coordinator.
  scidb_start          start SciDB cluster asm <username> on <coordinator>"
EOF
exit 1
}

# detect directory where we run
source_path=`dirname $0`
source_path=`readlink -f ${source_path}/../`
SCIDB_VER=`awk -F . '{print $1"."$2}' ${source_path}/version`
bin_path=${source_path}/deployment/common
echo "Source path: ${source_path}"
echo "Script common path: ${bin_path}"

SCP="scp -r -q -o StrictHostKeyChecking=no"
SSH="ssh -o StrictHostKeyChecking=no"

# run command on remote host
# if password specified, it would used on password prompt
function remote_no_password ()
{
local username=${1}
local password="${2}"
local hostname=${3}
shift 3
if [ "${password}" == "" ]; then
expect <<EOF
log_user 1
set timeout -1
spawn $@
expect {
  eof                                   { }  
}
EOF
else
expect <<EOF
log_user 1
set timeout -1
spawn $@
expect {
  "${username}@${hostname}'s password:" { send "${password}\r"; exp_continue }
  eof                                   { }  
}
EOF
fi;
}

# Run command on remote host (with some prepared scripts/files)
# 1) copy ./deployment/common to remote host to /tmp/deployment
# 2) (If) specified files would be copied to remote host to /tmp/deployment
# 3) execute ${4} command on remote host
# 4) remove /tmp/deployment from remote host
function remote ()
{
local username=${1}
local password=${2}
local hostname=${3}
local files=${5-""}
remote_no_password "${username}" "${password}" "${hostname}" "${SCP} ${bin_path} ${username}@${hostname}:/tmp/deployment"
if [ -n "${files}" ]; then
    remote_no_password "${username}" "${password}" "${hostname}" "${SCP} ${files} ${username}@${hostname}:/tmp/deployment"
fi;
remote_no_password "${username}" "${password}" "${hostname}" "${SSH} ${username}@${hostname} \"cd /tmp/deployment && ${4}\""
remote_no_password "${username}" "${password}" "${hostname}" "${SSH} ${username}@${hostname}  \"rm -rf /tmp/deployment\""
}

# Provide password-less access to remote host
# If user missed - it would be created (with specified password)
function provide_password_less_ssh_access ()
{
    local username=${1}
    local password="${2}"
    local key=${3}
    local hostname=${4}
    if [ "${username}" != "root" ]; then
	echo "Check for ${username} on ${hostname}"
	remote root "${password}" "${hostname}" "./user_create.sh \\\"${username}\\\" \\\"${password}\\\""
    fi;
    echo "Provide access by ~/.ssh/id_rsa.pub to ${username}@${hostname}"
    remote "${username}" "${password}" "${hostname}" "./user_access.sh \\\"${username}\\\" \\\"${key}\\\""
}

# Copy source code to remote host to result
function push_source ()
{
    local username=${1}
    local hostname=${2}
    local source_path=`readlink -f ${3}`
    local source_name=`basename ${source_path}`
    local remote_path=`readlink -f ${4}`
    local remote_name=`basename ${remote_path}`
    echo "Archive the ${source_path} to ${source_path}.tar.gz"
    rm -f ${source_path}.tar.gz
    (cd ${source_path}/.. && tar -czpf ${source_path}.tar.gz ${source_name})
    echo "Remove ${username}@${hostname}:${remote_path}"
    remote_no_password "${username}" "" "${hostname}" "${SSH} ${username}@${hostname} \"rm -rf ${remote_path} && rm -rf ${remote_path}.tar.gz\""
    echo "Copy ${source_path} to ${username}@${hostname}:${remote_path}"
    remote_no_password "${username}" "" "${hostname}" "${SCP} ${source_path}.tar.gz ${username}@${hostname}:${remote_path}.tar.gz"
    echo "Unpack ${remote_path}.tar.gz to ${remote_path}"
    remote_no_password "${username}" "" "${hostname}" "${SSH} ${username}@${hostname} \"cd `dirname ${remote_path}` && tar xf ${remote_name}.tar.gz \""    
    if [ "${source_name}" != "${remote_name}" ]; then 
        remote_no_password "${username}" "" "${hostname}" "${SSH} ${username}@${hostname} \"cd `dirname ${remote_path}` && mv ${source_name} ${remote_name}\""
    fi;
}

# Configure script for work with rpm/yum
function configure_rpm ()
{
    # build target
    target=centos-6.3-x86_64
    # package kind
    kind=rpm
    # get package name from filename
    function package_info ()
    {
	rpm -qip ${1} | grep Name | awk '{print $3}'
    }
    # command for install all rpm packages from local current working path
    install="yum install -y *.rpm"
    # command for remove packages
    remove="yum remove -y"
}

# Configure script for work with deb/apt-get
function configure_deb ()
{
    # build target
    target=ubuntu-precise-amd64
    # package king
    kind=deb
    # get package name from filename
    function package_info ()
    {
	dpkg -I ${1} | grep Package | awk '{print $2}'
    }
    # command for install all deb packages from local current working path
    install="dpkg -R -i ."
    # command for remove packages
    remove="apt-get remove -y"
}

# Detect hostname OS and configure package manager for with it
# You can restrict work with Red Hat (if you want build packages, for example)
function configure_package_manager ()
{
    local hostname=${1}
    local with_redhat=${2}
    # Get file for detect OS
    FILE=/etc/issue
    if [ "${hostname}" != "localhost" ]; then
	# grab remote /etc/issue to local file
	remote_no_password root "" "${hostname}" "${SCP} root@${hostname}:/etc/issue ./issue"
	FILE=./issue
    fi;
    # Detech OS
    local OS=`${bin_path}/os_detect.sh ${FILE}`
    if [ "${hostname}" != "localhost" ]; then
	rm -f ./issue
    fi;
    # Match OS
    case "${OS}" in
	"CentOS 6.3")
	    configure_rpm
	    ;;
	"RedHat 6.3")
	    if [ ${with_redhat} == 1 ]; then
		configure_rpm
	    else
		echo "We do not support build SciDB under RedHat 6.3. Please use CentOS 6.3 instead"
		exit 1
	    fi;
	    ;;
	"Ubuntu 12.04")
	    configure_deb
	    ;;
	*)
	    echo "Not supported OS"
	    exit 1;
	    ;;
    esac
}

# Pull/Push packages from/to remote host
function push_and_pull_packages ()
{
    local username=${2}
    local hostname=${3}
    local push=${5}
    configure_package_manager ${hostname} 1
    local path_local=`readlink -f ${1}`
    local path_remote=`readlink -f ${4}`
    local scp_args_remote="${username}@${hostname}:${path_remote}/*"
    if [ $push == 1 ]; then
	remote_no_password "${username}" "" "${hostname}" "rm -rf ${path_remote}"
	remote_no_password "${username}" "" "${hostname}" "mkdir -p ${path_remote}"
	remote_no_password "${username}" "" "${hostname}" "${SCP} ${path_local} ${scp_args_remote}"
    else
	rm -rf ${path_local}
	mkdir -p ${path_local}
	remote_no_password "${username}" "" "${hostname}" "${SCP} ${scp_args_remote} ${path_local}"
    fi;
}

# Build packages ("chroot" or "insource")
function build_scidb_packages ()
{
    configure_package_manager "localhost" 0
    local packages_path=`readlink -f ${1}`
    local way="${2}"
    rm -rf ${packages_path}
    (cd ${source_path}; /bin/bash -x ./utils/make_packages.sh ${kind} ${way} ${packages_path} ${target})
}

# Setup ccache on remote host
function setup_ccache ()
{
    local username=${1}
    local hostname=${2}
    remote ${username} "" ${hostname} "./setup_ccache.sh"
}

# Register main SciDB repository on remote host
function setup_main_scidb_repository()
{
    local hostname=${1}
    echo "Setup main SciDB repository on ${hostname}"
    remote root "" ${hostname} "./setup_main_scidb_repository.sh"
}

# Stop virtual bridge on remote host
function stop_virtual_bridge_zero ()
{
    local hostname=${1}
    remote root "" ${hostname} "./stop_virbr0.sh"
}

# Install & configure PostgreSQL
function install_and_configure_postgresql ()
{
    local username=${1}
    local password="${2}"
    local network=${3}
    local hostname=${4}
    remote root "" ${hostname} "./configure_postgresql.sh ${username} \\\"${password}\\\" ${network}"
}

# Prepare machine for developer (for build Packages)
function prepare_host_for_developer ()
{
    local username=${1}
    local hostname=${2}
    echo "Prepare ${username}@${hostname} for developer"
    setup_main_scidb_repository "${hostname}"
    remote root "" ${hostname} "./prepare_toolchain.sh"
    setup_ccache ${username} ${hostname}
    stop_virtual_bridge_zero "${hostname}"
}

# Prepare chroot on remote machine for build packages 
function prepare_chroot ()
{
    local username=${1}
    local hostname=${2}
    echo "Prepare for build SciDB packages in chroot on ${hostname}"
    setup_main_scidb_repository "${hostname}"
    remote root "" ${hostname} "./prepare_chroot.sh ${username}"
    remote ${username} "" ${hostname} "./chroot_build.sh" "${source_path}/utils/chroot_build.py"
}

# Prepare runtime for install SciDB (install all required packages)
function prepare_runtime ()
{
    local username=${1}
    local hostname=${2}
    echo "Prepare for run SciDB on ${hostname}"
    setup_main_scidb_repository "${hostname}"
    remote root "" ${hostname} "./prepare_runtime.sh"
    stop_virtual_bridge_zero "${hostname}"
}

# Get package names from filenames
function package_names()
{
    local filename
    for filename in $@; do
	package_info ${filename}
    done;
}

# Remove SciDB from remote host
function scidb_remove()
{
    local hostname=${2}
    configure_package_manager ${hostname} 1
    local packages_path=`readlink -f ${1}`
    local packages=`ls ${packages_path}/*.${kind} | xargs`
    remote root "" "${hostname}" "${remove} `package_names ${packages} | xargs`"
}

# Install SciDB to remote host
function scidb_install()
{
    local hostname=${2}
    local with_coordinator=${3}
    configure_package_manager ${hostname} 1
    local packages_path=`readlink -f ${1}`
    if [ ${with_coordinator} -eq 1 ]; then
	local packages=`ls ${packages_path}/*.${kind} | xargs`
	remote root "" "${hostname}" "${install}" "${packages}"
    else
	local packages=`ls ${packages_path}/*.${kind} | grep -v coord | xargs`
	remote root "" "${hostname}" "${install}" "${packages}"
    fi;
}

# Generate SciDB config
function scidb_config ()
{
local username="${1}"
local password="${2}"
local database="${3}"
local base_path="${4}"
local instance_count="${5}"
local coordinator="${6}"
shift 6
echo "[${database}]"
local coordinator_instance_count=${instance_count}
let coordinator_instance_count--
echo "server-0=${coordinator},${coordinator_instance_count}"
node_number=1
local hostname
for hostname in $@; do
    echo "server-${node_number}=${hostname},${instance_count}"
    let node_number++
done;
echo "db_user=${username}"
echo "db_passwd=${password}"
echo "install_root=/opt/scidb/${SCIDB_VER}"
echo "pluginsdir=/opt/scidb/${SCIDB_VER}/lib/scidb/plugins"
echo "logconf=/opt/scidb/${SCIDB_VER}/share/scidb/log4cxx.properties"
echo "base-path=${base_path}"
echo "tmp-path=/tmp"
echo "base-port=1239"
echo "interface=eth0"
echo "no-watchdog=false"
}

# Prepare machine for run SciDB (setup environment, generate config file, etc)
function scidb_prepare_node ()
{
    local username=${1}
    local hostname=${2}
    remote ${username} "" ${hostname} "./scidb_prepare.sh ${SCIDB_VER}"
    remote root "" ${hostname} "cat config.ini > /opt/scidb/${SCIDB_VER}/etc/config.ini" `readlink -f ./config.ini`
}

# Prepare SciDB cluster
function scidb_prepare ()
{
    local username=${1}
    local password=${2}
    local database=${3}
    local base_path=${4}
    local instance_count=${5}
    local coordinator=${6}
    shift 6
    local coordinator_key="`${SSH} ${username}@${coordinator} \"cat ~/.ssh/id_rsa.pub\"`"
    scidb_config ${username} "${password}" ${database} ${base_path} ${instance_count} ${coordinator} "$@" | tee ./config.ini
    scidb_prepare_node ${username} ${coordinator}
    local hostname
    for hostname in $@; do
	scidb_prepare_node ${username} ${hostname}
	provide_password_less_ssh_access ${username} "" "${coordinator_key}" ${hostname}
    done;
    rm -f ./config.ini
    remote ${username} "" ${coordinator} "./scidb_prepare_coordinator.sh ${username} \\\"${password}\\\" ${database} ${SCIDB_VER}" 
}

# Start SciDB
function scidb_start ()
{
    local username=${1}
    local coordinator=${2}
    remote ${username} "" ${coordinator} "./scidb_start.sh ${username}"
}

# Install & configure Apache (required for CDash on build machines)
function prepare_httpd_cdash ()
{
    local username=${1}
    local build_machine=${2}
    remote root "" ${build_machine} "./prepare_httpd_cdash.sh ${username}"
}

if [ $# -lt 1 ]; then
    print_help
fi

case ${1} in
    access)
	if [ $# -lt 4 ]; then
	    print_help
	fi
	username=${2}
	password=${3}
	key="${4}"
	if [ "${key}" == "" ]; then
	    key="`cat ~/.ssh/id_rsa.pub`"
	fi;
	shift 4
	for hostname in $@; do 
	    provide_password_less_ssh_access ${username} "${password}" "${key}" "${hostname}"
	done;
	;;
    push_source)
	if [ $# -lt 4 ]; then
	    print_help
	fi
	username=${2}
	remote_path=${3}
	shift 3
	for hostname in $@; do
	    push_source ${username} ${hostname} ${source_path} ${remote_path}
        done;
	;;
    pull_packages)
	if [ $# -lt 5 ]; then
	    print_help
	fi
	path_local=`readlink -f ${2}`
	username=${3}
	path_remote=`readlink -f ${4}`
	shift 4
	for hostname in $@; do
	    push_and_pull_packages ${path_local} ${username} ${hostname} ${path_remote} 0
	done;
	;;
    push_packages)
	if [ $# -lt 5 ]; then
	    print_help
	fi
	path_local=`readlink -f ${2}`
	username=${3}
	path_remote=`readlink -f ${4}`
	shift 4
	for hostname in $@; do
	    push_and_pull_packages ${path_local} ${username} ${hostname} ${path_remote} 1
	done;
	;;
    prepare_toolchain)
	if [ $# -lt 3 ]; then
	    print_help
	fi
	username=${2}
	shift 2
	for hostname in $@; do 
	    prepare_host_for_developer ${username} ${hostname}
	done;
	;;
    prepare_chroot)
	if [ $# -lt 3 ]; then
	    print_help
	fi
	username=$2
	shift 2
	for hostname in $@; do
	    prepare_chroot ${username} "${hostname}"
	done;
	;;
    prepare_runtime)
	if [ $# -lt 3 ]; then
	    print_help
	fi
	username=${2}
	coordinator=${3}
	echo "Coordinator IP: ${coordinator}"
	shift 3
	for hostname in ${coordinator} $@; do
	    prepare_runtime ${username} ${hostname}
	done;
	;;
    prepare_postgresql)
	if [ $# -ne 5 ]; then
	    print_help
	fi
	username=${2}
	password="${3}"
	network=${4}
	hostname=${5}
	install_and_configure_postgresql ${username} "${password}" ${network} ${hostname}
	;;
    build)
	if [ $# -ne 2 ]; then
	    print_help
	fi
	packages_path=${2}
	build_scidb_packages "${packages_path}" "chroot"
	;;
    build_fast)
	if [ $# -ne 2 ]; then
	    print_help
	fi
	packages_path=${2}
	build_scidb_packages "${packages_path}" "insource"
	;;
    build_deps)
	if [ $# -ne 2 ]; then
	    print_help
	fi
	packages_path=${2}
	echo "TODO build SciDB dependencies packages"
	;;
    scidb_install)
	if [ $# -lt 3 ]; then
	    print_help
	fi
	packages_path=${2}
	coordinator=${3}
	echo "Coordinator IP: ${coordinator}"
	shift 3
	scidb_install ${packages_path} ${coordinator} 1
	for hostname in $@; do
	    scidb_install ${packages_path} ${hostname} 0
	done;
	;;
    scidb_remove)
	if [ $# -lt 3 ]; then
	    print_help
	fi
	packages_path=${2}
	shift 2
	hostname
	for hostname in $@; do
	    scidb_remove ${packages_path} ${hostname}
	done;
	;;
    scidb_prepare)
	if [ $# -lt 7 ]; then
	    print_help
	fi
	username=${2}
	password=${3}
	database=${4}
	base_path=${5}
	instance_count=${6}
	shift 6
	scidb_prepare ${username} "${password}" ${database} ${base_path} ${instance_count} $@
	;;
    scidb_start)
	if [ $# -lt 3 ]; then
	    print_help
	fi
	username=${2}
	coordinator=${3}
	scidb_start ${username} ${coordinator}
	;;
    prepare_httpd_cdash)
	if [ $# -lt 3 ]; then
	    print_help
	fi;
	username=${2}
	shift 2
	for hostname in $@; do
	    prepare_httpd_cdash ${username} ${hostname}
	done;
	;;
    *)
	print_help
	;;
esac

