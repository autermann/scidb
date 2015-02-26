#!/bin/bash

case `./os_detect.sh` in
    "CentOS 6.3")
	./chroot_build.py -i -d centos-6.3-x86_64
	;;
    "RedHat 6.3")
	echo "We do not support build SciDB under RedHat 6.3. Please use CentOS 6.3 instead"
	exit 1
	;;
    "Ubuntu 12.04")
	./chroot_build.py -i -d ubuntu-precise-amd64
	;;
    *)
	echo "Not supported OS"
	exit 1
	;;
esac
