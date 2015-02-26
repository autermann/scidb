#!/bin/bash
#
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2008-2013 SciDB, Inc.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
#

OS="not supported"
FILE=/etc/issue
if [ $# -eq 1 ]; then
    FILE=`readlink -f ${1}`
fi;
if [ `cat ${FILE} | grep "CentOS" | grep "6." | wc -l` = "1" ]; then
    OS="CentOS 6"
fi

if [ `cat ${FILE} | grep "Ubuntu" | grep "12.04" | wc -l` = "1" ]; then
    OS="Ubuntu 12.04"
fi

if [ `cat ${FILE} | grep "Red Hat" | grep "6." | wc -l` = "1" ]; then
    OS="RedHat 6"
fi

if [ "${OS}" == "not supported" ]; then
    echo "Not supported: "`cat ${FILE} | head -n1`
    exit 1
fi

echo ${OS}

exit 0
