#!/usr/bin/python

# Initialize, start and stop scidb. 
# Supports single instance and cluster configurations. 
#
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2008-2011 SciDB, Inc.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation version 3 of the License, or
# (at your option) any later version.
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

import sys
import time
import os
import string
import errno
import socket
from ConfigParser import *
from ConfigParser import RawConfigParser

def usage():
  print ""
  print "\t Usage: mktestsymlink.py <db> <conffile>"
  print ""
  print " Use this script to build a symlink for testing procedures"
  print "$ mktestsymlink.py <db> <conffile>"
  sys.exit(2)

# Very basic check.
if len(sys.argv) != 3:
  usage()

# Defaults
# Set the rootpath to the parent directory of the bin directory
d = {}
db = sys.argv[1]
configfile = sys.argv[2]
baseDataPath = None

# Parse a ini file
def parse_global_options(filename):
    config = RawConfigParser()
    config.read(filename)
    section_name = db

    # First process the "global" section. 
    try:
      #print "Parsing %s section." % (section_name)
      for (key, value) in config.items(section_name):
        d[key] = value
            
    except ParsingError:
        print "Error"
        sys.exit(1)
    d['db_name'] = db
    print d


if __name__ == "__main__":
  parse_global_options(configfile)
  baseDataPath = d.get('base-path')

  #print "os.getcwd()",os.getcwd()
  #print "os.getcwd()+'/..'",os.getcwd()+'/..'
  #print "os.path.normpath(os.getcwd()+'/..')",os.path.normpath(os.getcwd()+'/..')
  #print "basepath+/000",baseDataPath+'/000'
  slink = baseDataPath+"/000/tests"
  try:
    os.remove(slink)
  except OSError, detail: 
    if detail.errno != errno.ENOENT:
        print "Cannot remove symlink %s OSError:"%slink, detail
        sys.exit(detail.errno)

  os.symlink(os.path.normpath(os.getcwd()+'/..'), slink)
      
sys.exit(0)
