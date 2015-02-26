# Requires root privs
id
min=1
if [ $# -ne ${min} ]; then
   echo "Usage: pg.sh network_ip (where network_ip=W.X.Y.Z/N) "
   exit 1
fi
network=$1
if !(echo "${network}" | grep / 1>/dev/null); then
   echo "Invalid network format in ${network}"
   echo "Usage: pg.sh network_ip (where network_ip=W.X.Y.Z/N) "
   exit 1;
fi

DIR=/tmp/stage
rm -rf ${DIR}
mkdir ${DIR}
cd ${DIR}

PTRN='^[ri]i '
if !(dpkg -l postgresql-8.4  | grep ${PTRN}); then
   echo "Cannot find postgresql-8.4"
   exit 1;
fi

if !(dpkg -l postgresql-contrib-8.4  | grep ${PTRN}); then
   echo "Cannot find postgresql-contrib-8.4"
   exit 1;
fi

#Become postgres user and
#Edit /etc/postgresql/8.4/main/pg_hba.conf
#Append the following configuration lines to give access to 10.X.Y.Z/N network:
#host all all 10.X.Y.Z/N trust
# For example: 10.0.20.0/24
#
#Edit /etc/postgresql/8.4/main/postgresql.conf
#Set IP address(es) to listen on; you can use comma-separated list of addresses;
#defaults to 'localhost', and '*' is all ip address:
#listen_addresses='*'

echo "su postgres"
su postgres < /dev/null
id
echo "host    all    all    ${network}     md5" >> /etc/postgresql/8.4/main/pg_hba.conf &&
echo "listen_addresses='*'" >> /etc/postgresql/8.4/main/postgresql.conf 
echo "Starting postgres ..."

if !(/etc/init.d/postgresql restart && /etc/init.d/postgresql status | grep 8.4); then
   echo "Could not configure postgresql-8.4"
   exit 1;
fi

cd /tmp
rm -rf ${DIR}
