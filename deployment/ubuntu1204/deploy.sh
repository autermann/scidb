echo "Args #: $#"
expected=3
USAGE="Usage: deploy.sh <install_type=toolchain|runtime|pg|pkgs|stop-virbr0> <target_host> <target_user> [network] [scidb-pkgs]"
if [ $# -lt ${expected} ]; then
   echo ${USAGE}
   exit 1
fi

binpath=`dirname $0`
if [ -z "${binpath}" ] ; then
    binpath=.
fi
echo "Script dir: ${binpath}"
install=$1
target=$2
user=$3

echo "Target host: $target"
echo "Target user: $user"
echo "Install: $install"

echo "Make sure $target has ssh server installed (sudo apt-get install openssh-server)"

if [ ${install} = "toolchain" ]; then
   chmod 400 ${binpath}/toolchain.sh #read-only
   scp ${binpath}/toolchain.sh ${user}@${target}:/tmp/ &&
   # The user that can sudo without a password has to be used.
   # If such a user is not available, modify the command as follows and pipe in the password on stdin.
   # ssh ${user}@${target} 'sudo -S /bin/sh +x /tmp/toolchain.sh && exec 0>&- && rm /tmp/toolchain.sh'
   # For example, sh +x deploy.sh ... < passwordfile
   # WARNING: DO NOT give the [sudo] password on the command line or by typing it in.
   ssh ${user}@${target} 'sudo -n /bin/sh +x /tmp/toolchain.sh && rm /tmp/toolchain.sh' ||
   ssh ${user}@${target} 'sudo -n rm -f /tmp/toolchain.sh'
   exit 0
fi

if [ ${install} = "runtime" ]; then
   chmod 400 ${binpath}/runtime.sh #read-only
   scp ${binpath}/runtime.sh ${user}@${target}:/tmp/ &&
   ssh ${user}@${target} 'sudo -n /bin/sh +x /tmp/runtime.sh && rm -f /tmp/runtime.sh' ||
   ssh ${user}@${target} 'sudo -n rm -f /tmp/runtime.sh'
   exit 0
fi

if [ ${install} = "pg" ]; then
   shift 3
   NET=$@
   CMD="sudo -n /bin/sh +x /tmp/pg.sh ${NET} && rm -f /tmp/pg.sh"
   chmod 400 ${binpath}/pg.sh #read-only
   scp ${binpath}/pg.sh ${user}@${target}:/tmp/ &&
   ssh ${user}@${target} ${CMD} ||
   ssh ${user}@${target} 'sudo -n rm -f /tmp/pg.sh'
   exit 0
fi

if [ ${install} = "pkgs" ]; then
   shift 3
   PKGS=$@
   CMD="sudo -n /bin/sh +x /tmp/scidb_pkgs.sh ${PKGS} && rm -f /tmp/scidb_pkgs.sh"
   chmod 400 ${PKGS} ${binpath}/scidb_pkgs.sh
   scp ${binpath}/scidb_pkgs.sh ${PKGS} ${user}@${target}:/tmp/ &&

   ssh ${user}@${target} ${CMD} ||
   ssh ${user}@${target} 'sudo -n rm -f /tmp/scidb_pkgs.sh'
   exit 0
fi

if [ ${install} = "stop-virbr0" ]; then
   chmod 400 ${binpath}/stop_virbr0.sh #read-only
   scp ${binpath}/stop_virbr0.sh ${user}@${target}:/tmp/ &&
   ssh ${user}@${target} 'sudo -n /bin/sh +x /tmp/stop_virbr0.sh && rm -f /tmp/stop_virbr0.sh' ||
   ssh ${user}@${target} 'sudo -n rm -f /tmp/stop_virbr0.sh'
   exit 0
fi

echo ${USAGE}
exit 1
