#!/bin/bash
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2008-2012 SciDB, Inc.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation version 3 of the License.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the GNU General Public License for the complete license terms.
#
# You should have received a copy of the GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/>.
# END_COPYRIGHT
#

# This script is used within the SciDB test harness to exercise 
# iquery aborts/recovery. 

query="$4"
signal="" 
command="iquery"
# seconds after running query to wait before issuing kill
tmpfile="/tmp/killquery.$$.tmp"
delay=""
pid=""

#################################################################
# Process command line arguments
#################################################################
usage="\
usage: $0 <-aql|-afl> <#signal 2|9|15> <#seconds> \"query in quotes\" \n\n \
      -aql|-afl .............. specify the type of query to be used as either -afl or -aql \n \
      \#signal ................ specify the signal (2, 9, or 15) to kill iquery with (just the integer) \n \
      \#seconds ............... specify the number of seconds to wait before issuing the kill (just the integer) \n \
      query .................. specify the query to execute (in quotes)\n"

# check all arguments.
if [ "$1" == "" -o "$2" == "" -o "$3" == "" -o "$4" == "" ]
then
        echo -e "$usage"
        exit 1
fi

# delay must be numeric
if [ `expr $3 + 1 2> /dev/null` ]
then
	delay=$3
else
	echo -e "$usage"
	echo "ERROR: number of seconds must be an integer"
	exit 1
fi

# is the query going to be afl or aql
if [ "$1" != "-aql" -a "$1" != "-afl" ]
then
	echo -e "$usage"
	echo "ERROR: first argument must be -afl or -aql"
	exit 1
else
	if [ "$1" == "-aql" ]
	then
		command="$command -f $tmpfile "
	fi
	
	if [ "$1" == "-afl" ]
	then 
		command="$command -a -f $tmpfile "
	fi
fi

# We should only allow signals 2, 9, or 15 (for now).
if [ "$2" == "2" -o "$2" == "9" -o "$2" == "15" ]
then
	signal=$2
else
	echo -e "$usage"
	echo "ERROR: the signal argument must be either 2, 9, or 15."
	exit 1
fi

##################################################################
# MAIN - Do work. 
##################################################################

# store the query in a temp file because bash is being a pain about processing it through the command line
echo "$query" > $tmpfile

if [ ! -f $tmpfile ]
then
	echo "Error writing to $tmpfile"
	exit 1
fi


echo "Attempting  to kill (-$signal) iquery command after $delay seconds..."
#echo "    - running iquery command..."
 
#$command > /dev/null 2>&1 &
$command &
pid=$!
# disown the command so that we don't see unneeded stderr output
disown

#echo "    - waiting for $delay seconds..."
sleep $delay

#echo "    - sending signal $signal to iquery..."

kill -$signal $pid > /dev/null 2>&1

if [ $? -eq 0 ]
then
	echo "SUCCESS: kill returned code: $?"
	rm -f $tmpfile
	exit 0
else
	echo "WARNING: kill returned code: $? ... iquery may have had an error or completed before the $delay second delay."
	rm -f $tmpfile
	exit 1
fi

