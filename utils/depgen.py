#!/usr/bin/python

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

import sys
try:
    import json
except ImportError:
    import simplejson as json
from optparse import OptionParser
import subprocess
import re
from string import Template

def execcmd(cmd):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env={'LANG':'C'})
    p.wait()
    stdout, stderr = p.communicate()
    return (p.returncode, stdout, stderr)

def dpkg_get_package(package):
    (ret, out, err) = execcmd(('dpkg', '-l', package))
    if ret == 0:       
        out = [i for i in out.split('\n')[5:-1][0].split(' ') if i != ''][0:3]
        if out[0][1] != 'i':
            print "Package '" + package + "' not installed!"
            sys.exit(1)
        return (out[1], out[2])
    else:
        print "Can not find package '" + package +"'. dpkg error: " + err
        print "Check if you have it installed"
        sys.exit(1)       

def rpm_get_package(package):
    (ret, out, err) = execcmd(('rpm', '-q', '-i', package))
    if ret == 0:
        prog = re.compile('Name\s*:\s*([0-9a-zA-Z]*).*Version\s*:\s*([0-9\.]*).*', re.MULTILINE|re.DOTALL)
        res = prog.search(out)
        return (res.group(1),res.group(2))
    else:
        print "Can not find package '" + package +"'. rpm error: " + out.strip()
        print "Check if you have it installed"
        sys.exit(1)       

def get_package(package):
    if PACKAGE_TOOL == 'dpkg':
        return dpkg_get_package(package)
    elif PACKAGE_TOOL == 'rpm':
        return rpm_get_package(package)
    else:
        print("Can not gather package info with tool '" + PACKAGE_TOOL + "'")
        sys.exit(1)
        
def makedep(package, version, use_package_manager=True):
    p = None
    package = package.strip()
    if package == "?":
        for distrover in (DISTROVER, '*'):
            if distrover in version:
                for k,v in version[distrover].items():
                    for dep in makedep(k, v):
                        yield dep
                break
    elif package == "#":
        for k,v in version.items():
            for dep in makedep(k, v, False):
                yield dep
    else:
        p = None
        if use_package_manager:
            p = get_package(package)
        else:
            p = (None, None)
        if type(version).__name__ != 'list':
            version = [version]
        for ver in version:
            s = Template(ver)
            ver = s.safe_substitute(targetver=p[1], scidbver=SCIDBVER)
            yield (package, ver)
    
def main():
    parser = OptionParser()
    parser.add_option("-f", "--file", dest="filename", help="dependencies file")
    parser.add_option("-d", "--distro", dest="distroname", help="distro name")
    parser.add_option("-v", "--distrover", dest="distrover", help="distro version")
    parser.add_option("-V", "--scidbver", dest="scidbver", help="SciDB version (X.Y.Z)")
    parser.add_option("-p", "--package", dest="package", help="package name")
    parser.add_option("-P", "--packaging-utility", dest="pkgutil", help="packaging utility (rpm/dpkg)")
    (options, args) = parser.parse_args()

    if not options.filename:
        print("Dependencies filename is required")
        sys.exit(1)
    if not options.distroname:
        print("Distro name is required")
        sys.exit(1)
    if not options.distrover:
        print("Distro version is required")
        sys.exit(1)
    if not options.package:
        print("Package name is required")
        sys.exit(1)
    if not options.scidbver:
        print("SciDB version is required")
        sys.exit(1)

    deps_tree = None

    try:
        f = open(options.filename)
        deps_tree = json.load(f)
        f.close()        
    except IOError, (errno, strerror):
        print("Can not open file '" + options.filename + "': " + strerror)
        sys.exit(1)
    except ValueError, e:
        print("Can not parse file '" + options.filename + "': " + str(e))
        sys.exit(1)
        
    if not options.distroname in deps_tree:
        print "Unknown distro '" + options.distroname + "'"
        sys.exit(1)
    else:
        deps_tree = deps_tree[options.distroname]

    if not 'packaging_tool' in deps_tree:
        print "packaging_tool not defined for distro '" + options.distroname + "'"
        sys.exit(1)
    else:
        global PACKAGE_TOOL
        PACKAGE_TOOL=deps_tree['packaging_tool']        
        
    if not options.package in deps_tree:
        print "Unknown package '" + options.package + "'"
        sys.exit(1)
    else:
        deps_tree = deps_tree[options.package]

    global DISTROVER
    DISTROVER=options.distrover
    
    global SCIDBVER
    SCIDBVER=options.scidbver
    
    res = ''
    first = True
    for dep in deps_tree:
        for newdep in makedep(dep, deps_tree[dep]):
            if not first:
                res += ', '
            first = False
            res += newdep[0] + ' ' + newdep[1]
    print res
if __name__ == '__main__':
    main()
