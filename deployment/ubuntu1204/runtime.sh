# Requires root privs
id

DIR=/tmp/stage
rm -rf ${DIR}
mkdir ${DIR}

PKGS="python2.7\
  python-paramiko\
  python-crypto\
  postgresql-8.4\
  postgresql-contrib-8.4\
  libpqxx-3.1\
  libprotobuf7\
  liblog4cxx10\
  libboost-filesystem1.46.1\
  libboost-system1.46.1\
  libboost-regex1.46.1\
  libboost-serialization1.46.1\
  libboost-program-options1.46.1\
  libbz2-1.0\
  libreadline6\
  libgfortran3\
  libscalapack-mpi1\
  liblapack3gf\
  libopenmpi1.3\
  openmpi-common\
  openmpi-bin"

cd ${DIR}

echo "Installing ${PKGS}"

apt-get install -y ${PKGS}

PTRN='^[ri]i '

if !(dpkg -l python | grep ${PTRN} | grep 2.7); then
   echo "Cannot find python 2.7"
   exit 1;
fi

if !(dpkg -l python-crypto | grep ${PTRN} | grep 2.4); then
   echo "Cannot find python-crypto 2.4"
   exit 1;
fi

if !(dpkg -l python-paramiko | grep ${PTRN} | grep 1.7); then
   echo "Cannot find python-paramiko 1.7"
   exit 1;
fi

if !(dpkg -l libpqxx-3.1 | grep ${PTRN} | grep 3.1); then
   echo "Cannot find libpqxx-3.1"
   exit 1;
fi

if !(dpkg -l libprotobuf7 | grep ${PTRN} | grep 2.4); then
   echo "Cannot find libprotobuf7"
   exit 1;
fi

if !(dpkg -l liblog4cxx10 | grep ${PTRN} | grep 0.10); then
   echo "Cannot find liblog4cxx10"
   exit 1;
fi

# Boost
if !(dpkg -l libboost-filesystem1.46.1 | grep ${PTRN} | grep 1.46); then
   echo "Cannot find libboost-filesystem1.46.1"
   exit 1;
fi

if !(dpkg -l libboost-system1.46.1 | grep ${PTRN} | grep 1.46); then
   echo "Cannot find libboost-system1.46.1"
   exit 1;
fi

if !(dpkg -l libboost-regex1.46.1 | grep ${PTRN} | grep 1.46); then
   echo "Cannot find libboost-regex1.46.1"
   exit 1;
fi

if !(dpkg -l libboost-serialization1.46.1 | grep ${PTRN} | grep 1.46); then
   echo "Cannot find libboost-serialization1.46.1"
   exit 1;
fi

if !(dpkg -l libboost-program-options1.46.1 | grep ${PTRN} | grep 1.46); then
   echo "Cannot find libboost-program-options1.46.1"
   exit 1;
fi

# ZIP
if !(dpkg -l libbz2-1.0 | grep ${PTRN} | grep 1.0); then
   echo "Cannot find libbz2-1.0"
   exit 1;
fi

if !(dpkg -l libreadline6 | grep ${PTRN} | grep 6.2); then
   echo "Cannot find libreadline6"
   exit 1;
fi

if !(dpkg -l libgfortran3 | grep ${PTRN} | grep 4.6); then
   echo "Cannot find libgfortran3 4.6"
   exit 1;
fi

# Scalapack
if !(dpkg -l libscalapack-mpi1 | grep ${PTRN} | grep 1.8); then
   echo "Cannot find libscalapack-mpi1 1.8"
   exit 1;
fi

# BLACS
if !(dpkg -l libblacs-mpi1 | grep ${PTRN} | grep 1.1); then
   echo "Cannot find libblacs-mpi1 1.1"
   exit 1;
fi

# Lapack
if !(dpkg -l liblapack3gf | grep ${PTRN} | grep 3.3); then
   echo "Cannot find liblapack3gf 3.3"
   exit 1;
fi

# BLAS
if !(dpkg -l libblas3gf | grep ${PTRN} | grep 1.2); then
   echo "Cannot find libblas3gf 1.2"
   exit 1;
fi

if !(dpkg -l libopenmpi1.3 | grep ${PTRN} | grep 1.4); then
   echo "Cannot find libopenmpi1.3 v1.4"
   exit 1;
fi

if !(dpkg -l openmpi-bin | grep ${PTRN} | grep 1.4); then
   echo "Cannot find openmpi-bin v1.4"
   exit 1;
fi

if !(dpkg -l openmpi-common | grep ${PTRN} | grep 1.4); then
   echo "Cannot find openmpi-common v1.4"
   exit 1;
fi

if !(dpkg -l postgresql-contrib-8.4 | grep ${PTRN} | grep 8.4); then
   echo "Cannot find postgresql-contrib-8.4"
   exit 1;
fi

if !(dpkg -l postgresql-8.4  | grep ${PTRN}); then
   echo "Cannot find postgresql-8.4"
   exit 1;
fi

# stop Postgres
if !(/etc/init.d/postgresql stop); then
   echo "WARNING: Cannot stop postgresql-8.4"
fi

if !(/etc/init.d/postgresql status | grep -v 8.4); then
   echo "WARNING: postgresql-8.4 appears to be running"
fi

if (ifconfig virbr0 1>/dev/null 2>/dev/null); then
    echo "WARNING: network interface virbr0 appears to be running. \n\
         It may interfere with SciDB/MPI traffic.\n\
         To shutdown virbr0, use stop_virbr0.sh"
fi

cd /tmp
rm -rf ${DIR}
