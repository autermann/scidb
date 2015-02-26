# Requires root privs
id
min=1
if [ $# -lt ${min} ]; then
   echo "Usage: pkgs.sh [<pkg>.deb]+"
   exit 1
fi

DIR=/tmp/stage
rm -rf ${DIR}
mkdir ${DIR}
cd ${DIR}

FILES=$@
PKGS=
for f in ${FILES} ; do
  b=`basename ${f}`
  mv "/tmp/${b}" ${DIR}
  PKGS="${PKGS} ${DIR}/${b}"
done

echo "Removing old scidb packages"
dpkg -r '*scidb*'

echo "Installing new scidb packages: ${PKGS}"
dpkg -i ${PKGS}

echo "Checking installation status of scidb packages"
VER=12.10
PTRN='^[ri]i '
for pkg in ${PKGS}; do
  name=`dpkg --info ${pkg} | grep Package: | awk '{print $2}'`
  if !(dpkg -l ${name} | grep ${PTRN} | grep ${VER}); then
     echo "Cannot find ${name} ${VER}"
     rm -rf ${DIR}
     exit 1;
  fi
done

cd /tmp
rm -rf ${DIR}
