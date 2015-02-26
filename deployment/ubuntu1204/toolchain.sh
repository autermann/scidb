# Requires root privs
id

DIR=/tmp/stage
rm -rf ${DIR}
mkdir ${DIR}

# from the wiki page
PKGS="build-essential\
 cmake\
 libboost1.46-all-dev\
 libpqxx-3.1\
 libpqxx3-dev\
 libprotobuf7\
 libprotobuf-dev\
 protobuf-compiler\
 doxygen\
 flex\
 bison\
 liblog4cxx10\
 liblog4cxx10-dev\
 libcppunit-1.12-1\
 libcppunit-dev\
 libbz2-dev\
 zlib1g-dev\
 subversion\
 libreadline6-dev\
 libreadline6\
 python-paramiko\
 python-crypto\
 xsltproc\
 gfortran\
 libscalapack-mpi1\
 liblapack-dev\
 libopenmpi-dev"

# Postgres should not be needed (?)
# postgresql-8.4
# postgresql-contrib-8.4
# XXX TODO: do we need SWIG & R connector dependencies ?

cd ${DIR}

# INSTALL
# apt-get update &&
apt-get install -y ${PKGS}

# VERIFY
PTRN='^[ri]i '
if !(dpkg -l cmake | grep ${PTRN} | grep 2.8.7); then
   echo "Cannot find cmake 2.8.7"
   exit 1;
fi

# XXX TODO: verify versions as well
for p in ${PKGS}; do
    if !(dpkg -l ${p} | tail -n +6 | grep ${PTRN}); then
       echo "Cannot find ${p}"
       exit 1;
    fi
done

cd /tmp
rm -rf ${DIR}


