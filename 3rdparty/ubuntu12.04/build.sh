#!/bin/bash

set -eu

#[ -f /usr/sbin/mock ] && HAS_MOCK=1 || HAS_MOCK=0

#if [ ! $HAS_MOCK ]; then
#    echo You dont have mock utility
#    echo Connect SciDB repository and install mock
#fi

function usage()
{
    echo "Usage: $0 <result dir>"
    exit 1
}

function die()
{
    echo $1
    exit 1
}

[ ! "$#" -eq 1 ] && usage

export SCIDB_VERSION_MAJOR=`awk -F . '{print $1}' $(dirname "$0")/../../version`
export SCIDB_VERSION_MINOR=`awk -F . '{print $2}' $(dirname "$0")/../../version`
export SCIDB_VERSION_PATCH=`awk -F . '{print $3}' $(dirname "$0")/../../version`

build_dir="`mktemp -d /tmp/scidb_packaging.XXXXX`"
chroot_result_dir="$1"
source_dir=~/scidb_3rdparty_sources

dsc_list_source="mpich2_1.2.1.1-4.dsc"
dir_list="mpich2-1.2.1.1"
dsc_list_result="scidb-${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}-mpich2_1.2.1.1-4.dsc"

pushd "$(dirname "$0")"
script_dir="`pwd`"
popd

baseurl="http://downloads.paradigm4.com/ubuntu12.04/3rdparty_sources"

# original URLs stored in ${baseurl}/original.txt
sources="
mpich2_1.2.1.1-4.diff.gz
mpich2_1.2.1.1-4.dsc
mpich2_1.2.1.1.orig.tar.gz
"

echo Preparing dirs
mkdir -p "${build_dir}" "${source_dir}" "${chroot_result_dir}" || die

echo Downloading sources to "${source_dir}"
pushd "${source_dir}"
    for filename in $sources; do
        [ ! -f $filename ] && wget "${baseurl}/${filename}" -O tmp && mv tmp $filename
    done
popd

echo Copying sources to "${build_dir}"
cp "${source_dir}"/* "${build_dir}"

for patch in $(ls "${script_dir}"/patches/*.patch); do
    cat ${patch} | sed -e "s/SCIDB_VERSION_MAJOR/${SCIDB_VERSION_MAJOR}/g" | sed -e "s/SCIDB_VERSION_MINOR/${SCIDB_VERSION_MINOR}/g" | sed -e "s/SCIDB_VERSION_PATCH/${SCIDB_VERSION_PATCH}/g" > "${build_dir}"/$(basename ${patch})
done;

# build source packages
echo "Build source packages"
pushd ${build_dir}

for dscfile in ${dsc_list_source}; do
    dpkg-source -x ${dscfile}
done;

for dirname in ${dir_list}; do
    pushd ${dirname}
    for patch in $(ls ../${dirname}*.patch); do
	patch -p1 < ${patch}
    done
    dpkg-buildpackage -rfakeroot -S -us -uc
    popd
done

popd

# build binary packages
echo "Build dependencies in chroot"
pushd ${build_dir}

for dscfile in ${dsc_list_result}; do
    python ${script_dir}/../../utils/chroot_build.py -b -d ubuntu-precise-amd64 -s ${dscfile} -r "${chroot_result_dir}" || die Can not build ${dscfile}
done;

popd

echo Removing build dir "${build_dir}"
sudo rm -rf "${build_dir}"

echo Done. Take result from "${chroot_result_dir}"
