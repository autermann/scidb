#!/bin/bash

function usage()
{
    echo "Usage: $0 <rpm|deb> <local|chroot|insource> <result dir> [chroot distro]"
    exit 1
}

function die()
{
    echo $*
    exit 1
}

[ "$#" -lt 3 ] && usage

type=$1
target=$2
result_dir=$3
distro=$4
jobs=$[`getconf _NPROCESSORS_ONLN`+1]

case $type in
    deb|rpm);;
    *) usage;;
esac

case $target in
    local|chroot|insource);;
    *) usage;;
esac

if [ "$target" == "chroot" ]; then
    [ "$#" -lt 4 ] && echo Looks like you forgot chroot distro! Try: centos-6.3-x86_64 or ubuntu-precise-amd64 && usage
fi

if [ $target != "insource" ]; then
    build_dir="`mktemp -d /tmp/scidb_packaging.XXXXX`"
    build_src_dir="${build_dir}"/scidb-sources
fi

echo Extracting version
VERSION="`cat version|sed 's/\(.*\..*\..*\)\..*/\1/'`"

if [ -d .git ]; then
    echo Extracting revision from git
    REVISION="`git svn find-rev master`"
elif [ -d .svn ]; then
    echo Extracting revision from svn
    REVISION="`svn info|grep Revision|awk '{print $2}'|perl -p -e 's/\n//'`"
fi

echo Preparing result dir
mkdir "${result_dir}"

if [ $target != "insource" ]; then
    echo Preparing building dir ${build_dir}
    mkdir -p "${build_dir}" "${build_src_dir}"

    if [ -d .git ]; then
        echo Extracting sources from git
        git archive HEAD | tar -xC "${build_src_dir}"  || die git archive
        git diff HEAD > "${build_src_dir}"/local.patch || die git diff
        pushd "${build_src_dir}"
            (git apply local.patch && rm local.patch) > /dev/null 2>&1
        popd
    elif [ -d .svn ]; then
        echo Extracting sources from svn
        svn export --force . "${build_src_dir}" || die svn export
    else
        die Can not extract revision. This is nor svn nor git working copy!
    fi

    echo -n $REVISION > "${build_src_dir}"/revision
fi

if [ "$type" == "deb" ]; then
    [ ! -d debian ] && die Can not find debian directory in current dir

    if [ $target != "insource" ]; then
        echo Preparing sources
        m4 -DVERSION=$VERSION -DBUILD=$REVISION debian/changelog.in > "${build_src_dir}"/debian/changelog

        pushd "${build_src_dir}"
            echo Building source packages in ${build_src_dir}
            dpkg-buildpackage -rfakeroot -S -uc -us
        popd

        if [ "$target" == "local" ]; then
            echo Building binary packages locally
            pushd "${build_dir}"
                dpkg-source -x scidb_$VERSION-$REVISION.dsc scidb-build || die dpkg-source failed
            popd
            pushd "${build_dir}"/scidb-build
                dpkg-buildpackage -rfakeroot -uc -us -j${jobs} || die dpkg-buildpackage failed
            popd
            pushd "${build_dir}"
                echo Moving result from `pwd` to ${result_dir}
                mv *.deb *.dsc *.changes *.tar.gz "${result_dir}"
            popd
        elif [ "$target" == "chroot" ]; then
            echo Building binary packages in chroot
            python utils/chroot_build.py -b -d "${distro}" -r "${result_dir}" -s "${build_dir}"/scidb_$VERSION-$REVISION.dsc -j${jobs} || die chroot_build.py failed
        fi
    else
        echo Cleaning old packages
        pushd ..
            rm -f *.deb
            rm -f *.changes
        popd

        echo Preparing sources
        m4 -DVERSION=$VERSION -DBUILD=$REVISION debian/changelog.in > debian/changelog

        echo Building binary packages locally
        BUILD_DIR="`pwd`" INSOURCE=1 dpkg-buildpackage -rfakeroot -uc -us -b -j${jobs} || die dpkg-buildpackage failed
        pushd ..
            echo Moving result from `pwd` to ${result_dir}
            mv *.deb *.changes "${result_dir}"
        popd
    fi
elif [ "$type" == "rpm" ]; then
    [ ! -f ./scidb.spec.in ] && die Can not find scidb.spec.in file in current dir

    if [ $target != "insource" ]; then
        echo Preparing rpmbuild dirs
        mkdir -p "${build_dir}"/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}

        echo Preparing sources
        m4 -DVERSION=$VERSION -DBUILD=$REVISION scidb.spec.in > "${build_dir}"/SPECS/scidb.spec
        pushd "${build_src_dir}"
            tar czf ${build_dir}/SOURCES/scidb.tar.gz *
        popd

        echo Building SRPM
        pushd "${build_dir}"/SPECS/
            rpmbuild -D"_topdir ${build_dir}" -bs ./scidb.spec || die rpmbuild failed
        popd

        if [ "$target" == "local" ]; then
            echo Building RPM locally
            pushd ${build_dir}/SRPMS
                rpmbuild -D"_topdir ${build_dir}" --rebuild scidb-$VERSION-$REVISION.src.rpm || die rpmbuild failed
            popd
            echo Moving result from "${build_dir}"/SRPMS and "${build_dir}"/RPMS and to ${result_dir}
            mv "${build_dir}"/SRPMS/*.rpm "${build_dir}"/RPMS/*/*.rpm "${result_dir}"
        elif [ "$target" == "chroot" ]; then
            echo Building RPM in chroot
            python utils/chroot_build.py -b -d "${distro}" -r "${result_dir}" -s ${build_dir}/SRPMS/scidb-$VERSION-$REVISION.src.rpm  || die chroot_build.py failed
        fi
    else
        echo Cleaning old files
        rm -rf rpmbuild

        echo Preparing sources
        m4 -DVERSION=$VERSION -DBUILD=$REVISION scidb.spec.in > scidb.spec

        echo Building binary packages insource
        rpmbuild --with insource -D"_topdir `pwd`/rpmbuild" -D"_builddir `pwd`" -bb ./scidb.spec  || die rpmbuild failed

        pushd rpmbuild/RPMS
            echo Moving result from `pwd` to ${result_dir}
            mv */*.rpm "${result_dir}"
        popd
    fi
fi

if [ $target != "insource" ]; then
    echo Removing ${build_dir}
    sudo rm -rf "${build_dir}"
fi

echo Done. Take result packages in ${result_dir}
