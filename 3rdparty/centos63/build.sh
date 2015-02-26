#!/bin/bash

[ -f /usr/sbin/mock ] && HAS_MOCK=1 || HAS_MOCK=0

if [ ! $HAS_MOCK ]; then
    echo You dont have mock utility
    echo Connect SciDB repository and install mock
fi

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

build_dir="`mktemp -d /tmp/scidb_packaging.XXXXX`"
chroot_result_dir="$1"
source_dir=~/scidb_3rd_party_sources

pushd "$(dirname "$0")"
script_dir="`pwd`"
popd

sources="
http://netcologne.dl.sourceforge.net/project/boost/boost/1.46.1/boost_1_46_1.tar.bz2
http://pqxx.org/download/software/libpqxx/libpqxx-3.1.tar.gz
http://www.cmake.org/files/v2.8/cmake-2.8.10.tar.gz
http://prdownloads.sourceforge.net/swig/swig-2.0.8.tar.gz
http://zlib.net/pigz/pigz-2.2.5.tar.gz
https://fedorahosted.org/mock/raw-attachment/wiki/MockTarballs/mock-1.1.24.tar.xz
http://www.sai.msu.su/apache/logging/log4cxx/0.10.0/apache-log4cxx-0.10.0.tar.gz
http://protobuf.googlecode.com/files/protobuf-2.4.1.tar.bz2
http://argparse.googlecode.com/files/argparse-1.2.1.tar.gz
"

echo Preparing dirs
mkdir -p "${build_dir}"/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS} "${source_dir}" "${chroot_result_dir}" || die

echo Downloading sources to "${source_dir}"
pushd "${source_dir}"
    for url in $sources; do
        filename="`basename $url`"
        [ ! -f $filename ] && wget "$url" -O tmp && mv tmp $filename
    done
popd

echo Copying sources to "${build_dir}/SOURCES"
cp "${source_dir}"/*  "${script_dir}"/centos-6.3-x86_64.cfg "${script_dir}"/log4cxx-cstring.patch "${build_dir}/SOURCES"

echo Copying specs to "${build_dir}/SOURCES"
cp "${script_dir}"/*.spec "${build_dir}"/SPECS

echo Building source packages
pushd "${build_dir}"/SPECS
    for f in *.spec; do
        echo Building $f source package
        rpmbuild -D"_topdir ${build_dir}" -bs $f || die "Can't build $f"
    done
popd

echo Building dependencies in chroot
pushd "${build_dir}"/SRPMS
    for f in *.src.rpm; do
        echo Building binary package from $f
        python ${script_dir}/../../utils/chroot_build.py -b -d centos-6.3-x86_64 -s $f -r "${chroot_result_dir}" || die Can not build $f
    done
popd

echo Removing build dir "${build_dir}"
sudo rm -rf "${build_dir}"

echo Done. Take result from "${chroot_result_dir}"
