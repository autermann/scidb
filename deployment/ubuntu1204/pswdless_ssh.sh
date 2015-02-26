echo "Args #: $#"
ID=`id -un`
min=1
if [ $# -lt ${min} ]; then
   echo "WARNING: This utility generates an RSA key for user ${ID} and distributes it to specified hosts"
   echo "Usage: pswdless_ssh.sh [ <host>]+"
   exit 1
fi

ROOT=root
echo "User: ${ID}"

if  [ "${ID}" == "${ROOT}" ] ; then
   echo "This utility is too dangerous to be used for root"
   exit 1
fi

ssh-keygen -t rsa &&

for host in $@; do
    ssh-copy-id ${ID}@${host}
    ssh ${ID}@${host} 'echo "If this did not require a password, it worked!"'
done

exit 0
