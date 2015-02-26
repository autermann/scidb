#!/usr/bin/python

# Initialize, start and stop scidb in a cluster.
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

import collections
import copy
import datetime
import errno
import exceptions
import os
import subprocess
import sys
import string
import time
import traceback
import copy
import argparse

_DBG = os.environ.get("SCIDB_DBG",False)
 
# not order preserving
# should be O(n log n) or O(n) depending on
# whether set uses hashing or trees
def noDupes(seq):
    # simple implementation:
    # there are faster ways, but significant amounts of
    # dictionary code is involved.
    return list(set(seq))

# bad style to use from, removes the namespace colission-avoidance mechanism
import ConfigParser

def printDebug(string, force=False):
   if _DBG or force:
      print >> sys.stderr, "%s: DBG: %s" % (sys.argv[0], string)
      sys.stderr.flush()

def printDebugForce(string):
    printDebug(string, force=True)

def printInfo(string):
   sys.stdout.flush()
   print >> sys.stdout, "%s" % (string)
   sys.stdout.flush()

def printError(string):
   print >> sys.stderr, "%s: ERROR: %s" % (sys.argv[0], string)
   sys.stderr.flush()

# borrowed from http://code.activestate.com/recipes/541096/
def confirm(prompt=None, resp=False):
    """prompts for yes or no response from the user. Returns True for yes and
    False for no.
    """
    if prompt is None:
        prompt = 'Confirm'

    if resp:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')

    while True:
        ans = raw_input(prompt)
        if not ans:
            return resp

        if ans not in ['y', 'Y', 'n', 'N']:
            print 'please enter y or n.'
            continue

        if ans == 'y' or ans == 'Y':
            return True

        if ans == 'n' or ans == 'N':
            return False
    return False

# Parse a config file
def parseOptions(filename, section_name):
   config = ConfigParser.RawConfigParser()
   config.read(filename)
   options={}
   try:
       for (key, value) in config.items(section_name):
           options[str(key)] = value
   except Exception, e:
      printError("config file parser error in file: %s, reason: %s" % (filename, e))
      sys.exit(1)
   return options
#
# Execute OS command
# This is a wrapper method for subprocess.Popen()
# If waitFlag=True and raiseOnBadExitCode=True and the exit code of the child process != 0,
# an exception will be raised.
def executeIt(cmdList,
              env=None,
              cwd=None,
              useShell=False,
              cmd=None,
              stdinFile=None,
              stdoutFile=None,
              stderrFile=None,
              waitFlag=True,
              collectOutput=False,
              raiseOnBadExitCode=True):
    ret = 0
    out = ''
    err = ''

    if not cwd:
        cwd = os.getcwd()

    my_env = copy.copy(os.environ)

    if env:
        for (key,value) in env.items():
            my_env[key] = value

    if useShell:
       cmdList=[" ".join(cmdList)]

    try:
       stdIn = None
       if stdinFile:
          stdIn=open(stdinFile,"r")

       stdOut = None
       if stdoutFile:
          # print "local - about to open stdoutFile log file:", stdoutFile
          stdOut=open(stdoutFile,"w")
       elif not waitFlag:
          stdOut=open("/dev/null","w")

       stdErr = None
       if stderrFile:
          #print "local - about to open stderrFile log file:", stderrFile
          stdErr=open(stderrFile,"w")
       elif not waitFlag:
          stdErr=open("/dev/null","w")

       if collectOutput:
           if not waitFlag:
               raise Exception("Inconsistent arguments: waitFlag=%s and collectOutput=%s" % (str(waitFlag), str(collectOutput)))
           if not stdErr:
               stdErr = subprocess.PIPE
           if not stdOut:
               stdOut = subprocess.PIPE

       printDebug("Executing: "+str(cmdList))

       p = subprocess.Popen(cmdList,
                            env=my_env, cwd=cwd,
                            stdin=stdIn, stderr=stdErr, stdout=stdOut,
                            shell=useShell, executable=cmd)
       if collectOutput:
           out, err = p.communicate() # collect stdout,stderr, wait

       if waitFlag:
          p.wait()
          ret = p.returncode
          if ret != 0 and raiseOnBadExitCode:
             raise Exception("Abnormal return code: %s on command %s" % (ret, cmdList))
    finally:
       if stdIn:
          stdIn.close()
       if stdOut and not isinstance(stdOut,int):
          stdOut.close()
       if stdErr and not isinstance(stdErr,int):
          stdErr.close()

    return (ret,out,err)

def getOS (scidbEnv):
    bin_path=os.path.join(scidbEnv.source_path,"deployment/common/os_detect.sh")
    curr_dir=os.getcwd()

    cmdList=[bin_path]
    ret,out,err = executeIt(cmdList,
                            useShell=True,
                            collectOutput=True,
                            cwd=scidbEnv.build_path)
    os.chdir(curr_dir)
    printDebug(out)
    return out

def rm_rf(path,force=False,throw=True):
    if not force and not confirm("WARNING: about to delete *all* contents of "+path, True):
        if throw:
            raise Exception("Cannot continue without removing the contents of "+path)

    cmdList=["/bin/rm", "-rf", path]
    ret = executeIt(cmdList,
                    useShell=True,
                    stdoutFile="/dev/null",
                    stderrFile="/dev/null")
    return ret

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST or not os.path.isdir(path):
            raise

def setup(scidbEnv):
    oldCmakeCache = os.path.join(scidbEnv.source_path,"CMakeCache.txt")
    if os.access(oldCmakeCache, os.R_OK):
        printInfo("WARNING: Deleting old CMakeCache file:"+oldCmakeCache)
        os.remove(oldCmakeCache)

    oldCmakeCache = os.path.join(scidbEnv.build_path,"CMakeCache.txt")
    if os.access(oldCmakeCache, os.R_OK):
        printInfo("WARNING: Deleting old CMakeCache file:"+oldCmakeCache)
        os.remove(oldCmakeCache)

    build_type=os.environ.get("SCIDB_BUILD_TYPE","Debug")
    rm_rf(scidbEnv.build_path+"/*",scidbEnv.args.force, throw=False)
    mkdir_p(scidbEnv.build_path)
    curr_dir=os.getcwd()
    cmdList=["cmake",
             "-DCMAKE_BUILD_TYPE=%s"%build_type,
             "-DCMAKE_INSTALL_PREFIX=%s"%scidbEnv.install_path,
             scidbEnv.source_path]
    ret = executeIt(cmdList,
                    cwd=scidbEnv.build_path)
    os.chdir(curr_dir)

def make(scidbEnv):
    jobs=os.environ.get("SCIDB_MAKE_JOBS","1")
    if scidbEnv.args.jobs:
       jobs = scidbEnv.args.jobs

    curr_dir=os.getcwd()

    cmdList=["/usr/bin/make", "-j%s"%jobs]
    if scidbEnv.args.target:
        cmdList.append(scidbEnv.args.target)

    ret = executeIt(cmdList,
                    cwd=scidbEnv.build_path)
    os.chdir(curr_dir)

def make_packages (scidbEnv):
    bin_path=os.path.join(scidbEnv.source_path,"deployment/deploy.sh")
    curr_dir=os.getcwd()

    extra_env={}
    extra_env["SCIDB_BUILD_PATH"]=scidbEnv.build_path
    extra_env["SCIDB_SOURCE_PATH"]=scidbEnv.source_path
    cmdList=[bin_path,
             "build_fast",
             scidbEnv.args.package_path]
    ret = executeIt(cmdList,
                    env=extra_env,
                    cwd=scidbEnv.build_path)
    os.chdir(curr_dir)

def confirmRecordedInstallPath (scidbEnv):
    cmakeConfFile=os.path.join(scidbEnv.build_path,"CMakeCache.txt")
    curr_dir=os.getcwd()

    cmdList=["grep", "CMAKE_INSTALL_PREFIX", cmakeConfFile]
    ret,out,err = executeIt(cmdList, collectOutput=True)

    os.chdir(curr_dir)
    printDebug(out+" (install path raw)")
    installPath = out.split('=')[1]
    printDebug(installPath+" (install path parsed)")
    if installPath.strip() != scidbEnv.install_path:
        raise Exception("Inconsistent install path: recorded by setup=%s vs default/environment=%s" %
                        (installPath, scidbEnv.install_path))

def getScidbVersion (scidbEnv):
    bin_path=os.path.join(scidbEnv.source_path,"deployment/deploy.sh")
    curr_dir=os.getcwd()

    cmdList=[bin_path,
             "usage", "|", "grep", "\"SciDB version:\""]
    ret,out,err = executeIt(cmdList,
                            useShell=True,
                            collectOutput=True,
                            cwd=scidbEnv.build_path)
    os.chdir(curr_dir)
    printDebug(out+" (raw)")
    version = out.split(':')[1]
    printDebug(version+" (parsed)")
    return version.strip()

def version(scidbEnv):
    print getScidbVersion(scidbEnv)

def make_src_package(scidbEnv):
    jobs=os.environ.get("SCIDB_MAKE_JOBS","1")
    if scidbEnv.args.jobs:
        jobs = scidbEnv.args.jobs

    mkdir_p(scidbEnv.args.package_path)

    curr_dir=os.getcwd()

    cmdList=["/usr/bin/make", "-j%s"%jobs , "src_package"]
    ret = executeIt(cmdList,
                    cwd=scidbEnv.build_path)
    os.chdir(curr_dir)

    tar = os.path.join(scidbEnv.build_path,"scidb-*.tgz")
    cmdList=[ "mv", tar, scidbEnv.args.package_path ]
    ret = executeIt(cmdList,
                    useShell=True)
    os.chdir(curr_dir)

def cleanup(scidbEnv):
    curr_dir=os.getcwd()

    configFile = os.path.join(scidbEnv.install_path,"etc/config.ini")
    if os.access(configFile, os.R_OK):
        stop(scidbEnv)
        config={}
        db_name=os.environ.get("SCIDB_NAME","mydb")
        config=parseOptions(configFile, db_name)
        data_path = config["base-path"]
        rm_rf(data_path+"/*", scidbEnv.args.force)

    rm_rf(scidbEnv.install_path+"/*", scidbEnv.args.force)
    rm_rf(scidbEnv.build_path+"/*", scidbEnv.args.force)
    os.chdir(curr_dir)

def generateConfigFile(scidbEnv, db_name, data_path, instance_num, configFile):
    if os.access(configFile, os.R_OK):
        os.remove(configFile)
    fd = open(configFile,"w")
    print >>fd, "[%s]"%(db_name)
    print >>fd, "server-0=localhost,%d"%(instance_num-1)
    print >>fd, "db_user=%s"  %(db_name)
    print >>fd, "db_passwd=%s"%(db_name)
    print >>fd, "install_root=%s" %(scidbEnv.install_path)
    print >>fd, "pluginsdir=%s"   %(os.path.join(scidbEnv.install_path,"lib/scidb/plugins"))
    print >>fd, "logconf=%s"      %(os.path.join(scidbEnv.install_path,"share/scidb/log1.properties"))
    print >>fd, "base-path=%s"    %(data_path)
    print >>fd, "base-port=1239"
    print >>fd, "interface=eth0"
    print >>fd, "redundancy=1"
    fd.close()

def install(scidbEnv):
    configFile = os.path.join(scidbEnv.install_path,"etc/config.ini")
    if os.access(configFile, os.R_OK):
        stop(scidbEnv)

    db_name=os.environ.get("SCIDB_NAME","mydb")
    pg_user=os.environ.get("SCIDB_PG_USER","postgres")

    curr_dir=os.getcwd()
    os.chdir(scidbEnv.build_path)

    rm_rf(scidbEnv.install_path+"/*", scidbEnv.args.force)
    mkdir_p(scidbEnv.install_path)

    cmdList=["/usr/bin/make", "install"]
    ret = executeIt(cmdList)

    # Generate config.ini or allow for a custom one

    data_path=""
    if scidbEnv.args.config:
        cmdList=[ "cp", scidbEnv.args.config, configFile]
        ret = executeIt(cmdList)
        configOpts=parseOptions(configFile, db_name)
        data_path = configOpts["base-path"]
    else:
        data_path=os.path.join(scidbEnv.stage_path, "DB-"+db_name)
        data_path=os.environ.get("SCIDB_DATA_PATH", data_path)
        instance_num=int(os.environ.get("SCIDB_INSTANCE_NUM","4"))
        generateConfigFile(scidbEnv, db_name, data_path, instance_num, configFile)

    # Create log4j config files

    log4jFileSrc = os.path.join(scidbEnv.build_path,"bin/log1.properties")
    log4jFileTgt = os.path.join(scidbEnv.install_path,"share/scidb/log1.properties")

    cmdList=[ "cp", log4jFileSrc, log4jFileTgt]
    ret = executeIt(cmdList)

    version = getScidbVersion(scidbEnv)
    platform = getOS(scidbEnv).strip()
    printDebug("platform="+platform)
    if platform.startswith("CentOS 6"):
        # boost dependencies should be installed here

        boostLibs  = os.path.join("/opt/scidb",version,"lib","libboost*.so.*")
        libPathTgt = os.path.join(scidbEnv.install_path,"lib")

        # Move boost libs into the install location

        cmdList=[ "cp", boostLibs, libPathTgt]
        ret = executeIt(cmdList,useShell=True)

    # Create PG user/role

    cmdList=[ "sudo","-u", pg_user, os.path.join(scidbEnv.install_path,"bin/scidb.py"),
              "init_syscat", db_name,
              os.path.join(scidbEnv.install_path,"etc/config.ini")]
    ret = executeIt(cmdList)

    # Initialize SciDB

    cmdList=[ os.path.join(scidbEnv.install_path,"bin/scidb.py"),
              "initall-force", db_name, os.path.join(scidbEnv.install_path,"etc/config.ini")]
    ret = executeIt(cmdList)

    # Setup test links

    testLinkPath = os.path.join(data_path,"000","tests")
    if os.access(testLinkPath, os.R_OK):
        os.remove(testLinkPath)
    os.symlink(os.path.join(scidbEnv.build_path,"tests"), testLinkPath)

    testLinkPath = os.path.join(data_path,"000","examples")
    if os.access(testLinkPath, os.R_OK):
        os.remove(testLinkPath)
    os.symlink(os.path.join(scidbEnv.build_path,"tests/examples"), testLinkPath)

    os.chdir(curr_dir)

def start(scidbEnv):
    db_name=os.environ.get("SCIDB_NAME","mydb")
    cmdList=[ os.path.join(scidbEnv.install_path,"bin/scidb.py"),
              "startall", db_name, os.path.join(scidbEnv.install_path,"etc/config.ini")]
    ret = executeIt(cmdList)

def stop(scidbEnv):
    db_name=os.environ.get("SCIDB_NAME","mydb")
    cmdList=[ os.path.join(scidbEnv.install_path,"bin/scidb.py"),
              "stopall", db_name, os.path.join(scidbEnv.install_path,"etc/config.ini")]
    ret = executeIt(cmdList)

def getScidbPidsCmd(dbName=None):
    cmd = "ps --no-headers -e -o pid,cmd | awk \'{print $1 \" \" $2}\' | grep SciDB-000"
    if dbName:
        cmd = cmd + " | grep \'%s\'"%(dbName)
    cmd = cmd + " | awk \'{print $1}\'"
    return cmd

def forceStop(scidbEnv):
    db_name=os.environ.get("SCIDB_NAME","mydb")

    cmdList = [getScidbPidsCmd(db_name) + ' | xargs kill -9']
    executeIt(cmdList,
              useShell=True,
              cwd=scidbEnv.build_path,
              stdoutFile="/dev/null",
              stderrFile="/dev/null")

def tests(scidbEnv):
    curr_dir=os.getcwd()

    os.chdir(os.path.join(scidbEnv.build_path, "tests/harness"))

    version = getScidbVersion(scidbEnv)

    libPath = os.path.join(scidbEnv.install_path,"lib")
    binPath = os.path.join(scidbEnv.install_path,"bin")
    testEnv = os.path.join(scidbEnv.source_path,"tests/harness/scidbtestharness.env")
    testBin = os.path.join(scidbEnv.install_path,"bin/scidbtestharness ")

    cmdList=[".", testEnv, ";",
             "PATH=%s:${PATH}"%(binPath),
             "LD_LIBRARY_PATH=%s:${LD_LIBRARY_PATH}"%(libPath),
             testBin,
             "--port=${IQUERY_PORT}",
             "--connect=${IQUERY_HOST}",
             "--root-dir=./testcases"]

    if scidbEnv.args.all:
        pass  # nothing to add
    elif  scidbEnv.args.test_id:
        cmdList.append("--test-id="+ scidbEnv.args.test_id)
    elif  scidbEnv.args.suite_id:
        cmdList.append("--suite-id="+ scidbEnv.args.suite_id)
    else:
        raise Exception("Cannot figure out which tests to run")

    if scidbEnv.args.record:
        cmdList.append("--record")

    cmdList.extend([ "|", "tee", "run.tests.log" ])
    ret = executeIt(cmdList,
                    useShell=True)
    os.chdir(curr_dir)

# Environment setup (variables with '_' are affected by environment)

#XXX TODO: support optional CMAKE -D

class SciDBEnv:
    def __init__(self, bin_path, source_path, stage_path, build_path, install_path, args):
        self.bin_path = bin_path
        self.source_path = source_path
        self.stage_path = stage_path
        self.build_path = build_path
        self.install_path = install_path
        self.args = args

def getScidbEnv(args):
    bin_path=os.path.abspath(os.path.dirname(sys.argv[0]))
    source_path=bin_path
    stage_path=os.path.join(source_path, "stage")
    build_path=os.path.join(stage_path, "build")
    install_path=os.path.join(stage_path, "install")

    printDebug("Source path: "+source_path)

    build_path=os.environ.get("SCIDB_BUILD_PATH", build_path)
    printDebug("Build path: "+build_path)

    install_path=os.environ.get("SCIDB_INSTALL_PATH",install_path)
    printDebug("Install path: "+install_path)

    return SciDBEnv(bin_path, source_path, stage_path, build_path, install_path, args)

# The main entry routine that does command line parsing
def main():
    scidbEnv=getScidbEnv(None)
    parser = argparse.ArgumentParser()
    parser.add_argument('-v','--verbose', action='store_true', help="display verbose output")
    subparsers = parser.add_subparsers(dest='subparser_name',
                                       title="Environment variables affecting all subcommands:"+
                                       "\nSCIDB_BUILD_PATH - build products location, default = %s"%(os.path.join(scidbEnv.bin_path,"stage","build"))+
                                       "\nSCIDB_INSTALL_PATH - SciDB installation directory, default = %s\n\nSubcommands"%(os.path.join(scidbEnv.bin_path,"stage","install")),
                                       description="Use -h/--help with a particular subcommand from the list below to learn its usage")

    subParser = subparsers.add_parser('setup', usage=
    """%(prog)s [-h | options]\n
Creates a new build directory for an out-of-tree build and runs cmake there.
Environment variables:
SCIDB_BUILD_TYPE - [RelWithDebInfo | Debug | Release], default = Debug""")

    subParser.add_argument('-f','--force', action='store_true', help=
                           "automatically confirm any old state/directory cleanup")
    subParser.set_defaults(func=setup)

    subParser = subparsers.add_parser('make', usage=
    """%(prog)s [-h | options]\n
Builds the sources
Environment variables:
SCIDB_MAKE_JOBS - number of make jobs to spawn (the -j parameter of make)""")
    subParser.add_argument('target', nargs='?', default=None, help=
                           "make target, default is no target")
    subParser.add_argument('-j','--jobs', type=int, help=
                           "number of make jobs to spawn (the -j parameter of make)")
    subParser.set_defaults(func=make)

    subParser = subparsers.add_parser('make_packages', usage=
    """%(prog)s [-h | options]\n
Builds deployable SciDB packages""")
    subParser.add_argument('package_path', default=os.path.join(scidbEnv.build_path,"packages"),
                           nargs='?', help=
                           "directory path for newly generated_packages, default is $SCIDB_BUILD_PATH/packages")
    subParser.set_defaults(func=make_packages)

    subParser = subparsers.add_parser('make_src_package', usage=
    """%(prog)s [-h | options]\n
Builds SciDB source tar file
Environment variables:
SCIDB_MAKE_JOBS - number of make jobs to spawn (the -j parameter of make)""")
    subParser.add_argument('package_path', default=os.path.join(scidbEnv.build_path,"packages"),
                           nargs='?', help=
                           "directory path for newly generated tar file, default is $SCIDB_BUILD_PATH/packages")
    subParser.add_argument('-j','--jobs', type=int, help=
                           "number of make jobs to spawn (the -j parameter of make)")
    subParser.set_defaults(func=make_src_package)

    subParser = subparsers.add_parser('install', usage=
    """%(prog)s [-h | options]\n
Re-create SciDB Postgres user. Install and initialize SciDB.
Environment variables:
SCIDB_NAME      - the name of the SciDB database to be installed, default = mydb
SCIDB_PG_USER   - OS user under which the Postgres DB is running, default = postgres
SCIDB_DATA_PATH - the common directory path prefix used to create SciDB instance directories (aka base-path).
                  It is overidden by the command arguments, default is $SCIDB_BUILD_PATH/DB-$SCIDB_NAME
SCIDB_INSTANCE_NUM - the number of SciDB instances to initialize."
                  It is overidden by the command arguments, default is 4.""")
    subParser.add_argument('config', default=None, nargs='?', help=
                           "config.ini file to use with scidb.py, default is generated")
    subParser.add_argument('-f','--force', action='store_true', help=
                           "automatically confirm any old state/directory cleanup")
    subParser.set_defaults(func=install)

    subParser = subparsers.add_parser('start', usage=
    """%(prog)s [-h]\n
Start SciDB (previously installed by \'install\')
Environment variables:
SCIDB_NAME - the name of the SciDB database to start, default = mydb""")
    subParser.set_defaults(func=start)

    subParser = subparsers.add_parser('stop', usage=
    """%(prog)s [-h]\n
Stop SciDB (previously installed by \'install\')
Environment variables:
SCIDB_NAME - the name of the SciDB database to stop, default = mydb""")
    subParser.set_defaults(func=stop)

    subParser = subparsers.add_parser('forceStop', usage=
    """%(prog)s [-h]\n
Stop SciDB instances with \'kill -9\'
Environment variables:
SCIDB_NAME - the name of the SciDB database to stop, default = mydb""")
    subParser.set_defaults(func=forceStop)

    subParser = subparsers.add_parser('tests', usage=
    """%(prog)s [-h | options]\n
Run scidbtestharness for a given set of tests
The results are stored in $SCIDB_BUILD_PATH/tests/harness/run.tests.log
Environment variables:"""+
"\n%s/tests/harness/scidbtestharness.env is source'd to create the environment for scidbtestharness"%(scidbEnv.bin_path))
    group = subParser.add_mutually_exclusive_group()
    group.add_argument('--all', action='store_true', help="run all scidbtestharness tests")
    group.add_argument('--test-id', help="run a specific scidbtestharness test")
    group.add_argument('--suite-id', default='checkin', help="run a specific scidbtestharness test suite, default is \'checkin\'")
    subParser.add_argument('--record', action='store_true', help="record the expected output")
    subParser.set_defaults(func=tests)

    subParser = subparsers.add_parser('cleanup', usage=
    """%(prog)s [-h | options]\n
Remove build, install, SciDB data directory trees.
It will execute stop() if config.ini is present in the install directory.""")
    subParser.add_argument('-f','--force', action='store_true',
                           help="automatically confirm any old state/directory cleanup")
    subParser.set_defaults(func=cleanup)

    subParser = subparsers.add_parser('version', usage=
    """%(prog)s\n
 Print SciDB version (in short form)""")
    subParser.set_defaults(func=version)

    args = parser.parse_args()
    scidbEnv.args=args

    global _DBG
    if args.verbose:
       _DBG=args.verbose

    printDebug("cmd="+args.subparser_name)

    curr_dir=os.getcwd()
    try:
        if args.subparser_name != "setup" and args.subparser_name != "cleanup" and args.subparser_name != "forceStop" :
            confirmRecordedInstallPath(scidbEnv)
        args.func(scidbEnv)
    except Exception, e:
        printError("Command %s failed: %s\n"% (args.subparser_name,e) +
                   "Make sure commands setup,make,install,start "+
                   "are performed (in that order) before stop,stopForce,tests" )
        if _DBG:
            traceback.print_exc()
        sys.stderr.flush()
        os.chdir(curr_dir)
        sys.exit(1)
    os.chdir(curr_dir)
    sys.exit(0)

### MAIN
if __name__ == "__main__":
   main()
### end MAIN
