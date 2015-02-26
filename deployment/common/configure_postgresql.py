#!/usr/bin/env python

#Become postgres user and
#Edit /etc/postgresql/8.4/main/pg_hba.conf
#Append the following configuration lines to give access to 10.X.Y.Z/N network:
#host all all 10.X.Y.Z/N trust
# For example: 10.0.20.0/24
#
#Edit /etc/postgresql/8.4/main/postgresql.conf
#Set IP address(es) to listen on; you can use comma-separated list of addresses;
#defaults to 'localhost', and '*' is all ip address:
#listen_addresses='*'

import sys
import os

OS=sys.argv[1]
username=sys.argv[2]
password=sys.argv[3]
network=sys.argv[4]

if OS == "CentOS 6.3" or OS == "RedHat 6.3":
    pg_hba_conf="/var/lib/pgsql/data/pg_hba.conf"
    postgresql_conf="/var/lib/pgsql/data/postgresql.conf"
    default=[
'# "local" is for Unix domain socket connections only',
'local   all         all                               ident',
'# IPv4 local connections:',
'host    all         all         127.0.0.1/32          ident',
'# IPv6 local connections:',
'host    all         all         ::1/128               ident'
]
elif OS == "Ubuntu 12.04":
    pg_hba_conf="/etc/postgresql/8.4/main/pg_hba.conf"
    postgresql_conf="/etc/postgresql/8.4/main/postgresql.conf"
    default=[
'# "local" is for Unix domain socket connections only',
'local   all         all                               ident',
'# IPv4 local connections:',
'host    all         all         127.0.0.1/32          md5',
'# IPv6 local connections:',
'host    all         all         ::1/128               md5',
]    
else:
    sys.stderr.write("Does not support %s\n" % OS)
    sys.exit(1)

actual=open(pg_hba_conf, 'r').readlines()
found=-1
if len(actual) < len(default):
    sys.stderr.write("%s: empty\n" % pg_hba_conf)
    sys.exit(1)
else:
    for line in reversed(default):
        if line == actual[found][:-1]:
            if not actual[found].startswith('#'):
                actual[found] = "%s" % actual[found].replace('ident', 'trust')
                actual[found] = "%s" % actual[found].replace('md5', 'trust')
            found -= 1
        else:
            break

if found > -6:
    sys.stderr.write("Can't update %s\n" % pg_hba_conf)
    sys.exit(1)
actual.append('host    all    all    %s    trust' % network)
open(pg_hba_conf, 'w').write(''.join(actual))

actual=open(postgresql_conf, 'r').readlines()
extra="listen_addresses='*'\n"
if actual[-1] != extra:
    actual.append(extra)
    open(postgresql_conf,'w').write(''.join(actual))
