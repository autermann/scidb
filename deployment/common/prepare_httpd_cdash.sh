#!/bin/bash

username=${1}

case `./os_detect.sh` in
    "CentOS 6.3")
	service iptables stop
	chkconfig iptables off
	yum install -y httpd
	chkconfig httpd on
	service httpd start
	CONFIG=/etc/httpd/conf/httpd.conf
	cat ${CONFIG} | sed -e "s/\/var\/www\/html/\/var\/www/g" > ${CONFIG}.new
	cat ${CONFIG}.new > ${CONFIG}
	rm -f ${CONFIG}.new
	usermod -G apache -a ${username}
	mkdir -p /var/www/cdash_logs
	chmod g+wx -R /var/www/cdash_logs
	chown apache:apache -R /var/www
	CONFIG=/etc/sysconfig/selinux
	cat ${CONFIG} | sed -e "s/enforcing/disabled/g" > ${CONFIG}.new
	cat ${CONFIG}.new > ${CONFIG}
	rm -f ${CONFIG}.new
	setenforce 0	
	;;
    "RedHat 6.3")
	echo "We do not support build SciDB under RedHat 6.3. Please use CentOS 6.3 instead"
	exit 1
	;;
    "Ubuntu 12.04")
	apt-get install -y apache2
	usermod -G www-data -a ${username}
	mkdir -p /var/www/cdash_logs
	chmod g+wxr -R /var/www/cdash_logs
	chown www-data:www-data -R /var/www
	;;
    *)
	echo "Not supported OS"
	exit 1
	;;
esac
