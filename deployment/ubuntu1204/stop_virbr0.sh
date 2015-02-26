# Requires root privs
id

DIR=/tmp/stage
rm -rf ${DIR}
mkdir ${DIR}

# Stop VM NAT
echo "Attempting to shut down virbr0 ..."
virsh net-destroy default 1>/dev/null 2>/dev/null
virsh net-autostart default --disable 1>/dev/null 2>/dev/null
# virsh net-undefine default #this will remove the default config
service libvirtd restart 1>/dev/null 2>/dev/null
service libvirt-bin restart 1>/dev/null 2>/dev/null
# ifconfig virbr0 down

if (ifconfig virbr0 1>/dev/null 2>/dev/null); then
   echo "ERROR: network interface virbr0 appears to be running."
   exit 1
fi

cd /tmp
rm -rf ${DIR}
exit 0
