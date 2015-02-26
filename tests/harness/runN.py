#!/usr/bin/python
#
# Load the data and run all the test scripts
#

from glob import glob
import subprocess
import sys
import time
import os
import string

# Just for sanity. You could probably run more if you wanted to
MAX_SUPPORTED_NODES = 20

MODE_INIT_START = 0
MODE_START = 1
MODE_CREATE_LOAD = 2
MODE_INIT_START_TEST = 3

mode = MODE_INIT_START_TEST

# Number of nodes
numNodes = 3
# Name of this scidb instance.
instanceName = "mydb"
# Tests to run
# Running all checkin tests
# Port number (currently used for iquery)
portNumber = "1239"
harnessArgs = []


if (len(sys.argv) > 1):
  if (sys.argv[1] =="-h" or sys.argv[1]=="--help"):
    print "Usage: runN.py <numNodes> <instanceName> [port-number] [harness options]"
    print "Example runN.py 1 pavel --record"
    print "Example runN.py 1 pavel --istart"
    print "Example runN.py 1 pavel --start"
    print "Example runN.py 1 pavel --create-load"
    sys.exit(0)
  numNodes = int(sys.argv[1])

if (len(sys.argv) > 2):
  instanceName = sys.argv[2]

if (len(sys.argv) > 3):
  if (sys.argv[3] == "--start"):
    mode = MODE_START
  elif (sys.argv[3] == "--istart"):
    mode = MODE_INIT_START
  elif sys.argv[3] == "--create-load":
    mode = MODE_CREATE_LOAD
  else:
      portNumber = sys.argv[3]

if (len(sys.argv) > 4):
  harnessArgs = sys.argv[4:]

if ( numNodes < 1 or numNodes > MAX_SUPPORTED_NODES ):
  print "Invalid <numNodes>:", numNodes, "must be between 1 and", MAX_SUPPORTED_NODES
  sys.exit(1)

if numNodes > 1:
  redundancyArgs = ["--redundancy", "1"]
else:
  redundancyArgs = []

numNodes = numNodes - 1
basepath = os.path.realpath(os.path.dirname(sys.argv[0])) 
binpath = basepath + "/../../bin"
meta_file = binpath + "/data/meta.sql"
plugins_dir = binpath + "/plugins"
pidfile = "/tmp/runN.pids"

nodePaths = []
for i in range (0, numNodes):
  nodePaths.append(binpath + "/node" + str(i+1))

print "DB instance name:", instanceName, "with root +", numNodes, "nodes."

# Connection string, for now derive it from the instanceName. 
connstr="host=localhost port=5432 dbname=" + instanceName +" user=" + instanceName + " password=" + instanceName

# Cleanup and initialize
def cleanup(path): 
  print "Cleaning up old logs and storage files."
  subprocess.Popen(["rm", "-f", "scidb.log"], cwd=path).wait()
  subprocess.Popen(["rm", "-f", "init.log"], cwd=path).wait()
  subprocess.Popen(["rm", "-f", "storage.header", "storage.cfg", "storage.scidb", "storage.log_1", "storage.log_2", "storage.data1"], cwd=path).wait()  

def master_init():
  cleanup(binpath)
  print "Initializing catalog..."
  subprocess.Popen(["sudo", "-u", "postgres", "./init-db.sh", instanceName, instanceName, instanceName, meta_file], 
                   cwd=binpath, stdout=open("init-stdout.log","w"), stderr=open("init-stderr.log","w")).wait()
  subprocess.Popen([binpath + "/scidb", "--metadata", meta_file, "--plugins", plugins_dir, "-rc", connstr, "--initialize"], cwd=binpath).wait()
  time.sleep(2)
  
def nodes_init():
  for i in range (0, numNodes):
    print "Initializing node ", i + 1, "..."
    subprocess.Popen(["rm", "-rf", nodePaths[i]]).wait()
    subprocess.Popen(["mkdir", nodePaths[i]]).wait()
    subprocess.Popen([binpath + "/scidb", "--metadata", meta_file, "--plugins", plugins_dir, "-rc", connstr], cwd=nodePaths[i]).wait()
    time.sleep(2)
   
# Start the single node server. 
def start(path,options,port):
  valgrindArgList = ["valgrind", "--num-callers=50", "--track-origins=yes", "-v", "--log-file=" + path + "/valgrind.log"]
  argList = [binpath + "/scidb", "-p", port,
             "-m", "64",
             "-l", binpath + "/log1.properties",
             "--metadata", meta_file,
             "--merge-sort-buffer", "64",
             "--plugins", plugins_dir,
             "-" + options + "c", connstr]
  argList.extend(redundancyArgs)

  rle_arg = []
  if os.environ.get('USE_RLE', '0') == '1':
    rle_arg = ["--rle-chunk-format", "1"]
    
  argList.extend(rle_arg);

  if os.environ.get('USE_VALGRIND', '0') == '1':
    print "Starting SciDB server under valgrind..."
    arglist.extend(["--no-watchdog", "True"])
    valgrindArgList.extend(argList)
    argList = valgrindArgList
  else:
    print "Starting SciDB server..."
  p = subprocess.Popen(argList, cwd=path, stdout=open(path + "/scidb-stdout.log","w"),
                                          stderr=open(path + "/scidb-stderr.log","w"))
  return p

def shutdown(p):
  p.terminate()

def check_pidfile():
  if (os.path.exists(pidfile)):
    f = open(pidfile, 'r')
    for line in f:
      pid = line.strip()
      procfile = "/proc/" + pid + "/stat"
      if (os.path.exists(procfile)):
        f1 = open(procfile, 'r')
        progname = string.split(f1.readline())[1]
        f1.close()
        if ( progname == "(scidb)" ):
          print "NOTE: killing old scidb instance at PID " + pid
          subprocess.Popen(["kill", "-9", pid], cwd=".").wait()
          while (os.path.exists(procfile)):
            time.sleep(1)
    
def savepids(master_pid, nodePids):
  subprocess.Popen(["rm", "-f", pidfile], cwd=".").wait()
  f = open(pidfile, 'w')
  f.write(str(master_pid.pid))
  print "Master pid: " + str(master_pid.pid) 
  f.write("\n")
  for popen in nodePids:
    f.write(str(popen.pid))
    print "node pid: " + str(popen.pid)
    f.write("\n")
  f.close()

def run_unit_tests():
  ut_path = basepath + "/../unit"
  res = subprocess.Popen([ut_path + "/unit_tests", "-c", connstr], cwd=".", stderr=open("/dev/null","w"), stdout=open("/dev/null","w")).wait()
  if (res != 0):
    bad_results.append ( "UNIT_TESTS" )
        
if __name__ == "__main__":

  os.chdir(basepath);

  check_pidfile()
   
  if ( mode == MODE_INIT_START or mode == MODE_INIT_START_TEST or mode == MODE_CREATE_LOAD ): 
    master_init()
    nodes_init()
  master_pid = start(binpath, "k", portNumber)
  
  nodePids =[]
  for i in range (0, numNodes):
      nodePids.append(start(nodePaths[i], "", "0"));
  if os.environ.get('USE_VALGRIND', '0') == '1':
      time.sleep(10*(numNodes+1))
  else:
    time.sleep(5*(numNodes+1))
  savepids(master_pid, nodePids)
  
  if ( mode != MODE_INIT_START_TEST ):
    if mode == MODE_CREATE_LOAD:
	os.environ["PATH"] = binpath + ":" + os.environ["PATH"]
	os.environ["IQUERY_HOST"] = 'localhost'
	os.environ["IQUERY_PORT"] = portNumber
	subprocess.Popen([binpath + "/scidbtestharness", "--root-dir", "createload", "--test-name", "createload.test", "--debug", "5", "--port", portNumber] + harnessArgs, cwd=".", env=os.environ).wait()
    sys.exit(0)

        
  # Add environment variables
  os.environ["PATH"] = binpath + ":" + os.environ["PATH"]
  os.environ["IQUERY_HOST"] = 'localhost'
  os.environ["IQUERY_PORT"] = portNumber
  subprocess.Popen([binpath + "/scidbtestharness", "--root-dir", "testcases", "--suite-id", "checkin", "--debug", "5", "--port", portNumber] + harnessArgs, cwd=".", env=os.environ).wait()
  sys.exit(0)
