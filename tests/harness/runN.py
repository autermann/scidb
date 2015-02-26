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
MAX_SUPPORTED_INSTANCES = 20

MODE_INIT_START = 0
MODE_START = 1
MODE_CREATE_LOAD = 2
MODE_INIT_START_TEST = 3

mode = MODE_INIT_START_TEST

# Number of instances
numInstances = 3
# Name of this scidb instance.
instanceName = "mydb"
# Tests to run
# Running all checkin tests
# Port number (currently used for iquery)
portNumber = "1239"
harnessArgs = []


if (len(sys.argv) > 1):
  if (sys.argv[1] =="-h" or sys.argv[1]=="--help"):
    print "Usage: runN.py <numInstances> <instanceName> [port-number] [harness options]"
    print "Example runN.py 1 pavel --record"
    print "Example runN.py 1 pavel --istart"
    print "Example runN.py 1 pavel --start"
    print "Example runN.py 1 pavel --create-load"
    sys.exit(0)
  numInstances = int(sys.argv[1])

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

if ( numInstances < 1 or numInstances > MAX_SUPPORTED_INSTANCES ):
  print "Invalid <numInstances>:", numInstances, "must be between 1 and", MAX_SUPPORTED_INSTANCES
  sys.exit(1)

if numInstances > 1:
  redundancyArgs = ["--redundancy", "1"]
else:
  redundancyArgs = []

numInstances = numInstances - 1
basepath = os.path.realpath(os.path.dirname(sys.argv[0])) 
binpath = basepath + "/../../bin"
meta_file = binpath + "/data/meta.sql"
plugins_dir = binpath + "/plugins"
storage_dir = os.environ.get('SCIDB_STORAGE', binpath)
pidfile = "/tmp/runN.pids"

storage_file = storage_dir + '/storage.scidb'
storage_files = []
for i in range (0, numInstances):
  storage_files.append(storage_dir + "/instance" + str(i+1) + "/storage.scidb")

instancePaths = []
for i in range (0, numInstances):
  instancePaths.append(binpath + "/instance" + str(i+1))

print "DB instance name:", instanceName, "with root +", numInstances, "instances."

# Connection string, for now derive it from the instanceName. 
connstr="host=localhost port=5432 dbname=" + instanceName +" user=" + instanceName + " password=" + instanceName

# Cleanup and initialize
def cleanup(path, storage_path): 
  print "Cleaning up old logs and storage files."
  subprocess.Popen(["rm", "-f", "scidb.log"], cwd=path).wait()
  subprocess.Popen(["rm", "-f", "init.log"], cwd=path).wait()
  subprocess.Popen(["rm", "-f", storage_path + "/storage.header", storage_path + "/storage.cfg", storage_path + "/storage.scidb", storage_path + "/storage.log_1",
            storage_path + "/storage.log_2", storage_path + "/storage.data1"], cwd=path).wait()  

def master_init():
  cleanup(binpath, storage_dir)
  subprocess.Popen(["mkdir", storage_dir]).wait()
  print "Initializing catalog..."
  subprocess.Popen(["sudo", "-u", "postgres", "./init-db.sh", instanceName, instanceName, instanceName, meta_file], 
                   cwd=binpath, stdout=open("init-stdout.log","w"), stderr=open("init-stderr.log","w")).wait()
  subprocess.Popen([binpath + "/scidb", "--storage", storage_file, "--metadata", meta_file, "--plugins", plugins_dir, "-rc", connstr, "--initialize"], cwd=binpath).wait()
  time.sleep(2)
  
def instances_init():
  for i in range (0, numInstances):
    print "Initializing instance ", i + 1, "..."
    subprocess.Popen(["rm", "-rf", instancePaths[i]]).wait()
    subprocess.Popen(["mkdir", instancePaths[i]]).wait()
    if storage_dir != binpath:
        instance_storage = storage_dir + "/instance" + str(i+1)
        subprocess.Popen(["rm", "-rf", instance_storage]).wait()
        subprocess.Popen(["mkdir", instance_storage]).wait()
    subprocess.Popen([binpath + "/scidb", "--storage", storage_files[i], "--metadata", meta_file, "--plugins", plugins_dir, "-rc", connstr], cwd=instancePaths[i]).wait()
    time.sleep(2)
   
# Start the single instance server. 
def start(path,options,storage,port):
  valgrindArgList = ["valgrind", "--num-callers=50", "--track-origins=yes", "-v", "--log-file=" + path + "/valgrind.log"]
  argList = [binpath + "/scidb", "-p", port,
             "-m", "64",
             "--storage", storage,
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
    
def savepids(master_pid, instancePids):
  subprocess.Popen(["rm", "-f", pidfile], cwd=".").wait()
  f = open(pidfile, 'w')
  f.write(str(master_pid.pid))
  print "Master pid: " + str(master_pid.pid) 
  f.write("\n")
  for popen in instancePids:
    f.write(str(popen.pid))
    print "instance pid: " + str(popen.pid)
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
    instances_init()
  master_pid = start(binpath, "k", storage_file, portNumber)
  
  instancePids =[]
  for i in range (0, numInstances):
      instancePids.append(start(instancePaths[i], "", storage_files[i], str(int(portNumber)+i+1)));
  if os.environ.get('USE_VALGRIND', '0') == '1':
      time.sleep(10*(numInstances+1))
  else:
    time.sleep(5*(numInstances+1))
  savepids(master_pid, instancePids)
  
  if ( mode != MODE_INIT_START_TEST ):
    if mode == MODE_CREATE_LOAD:
	os.environ["PATH"] = binpath + ":" + os.environ["PATH"]
	os.environ["IQUERY_HOST"] = 'localhost'
	os.environ["IQUERY_PORT"] = portNumber
	subprocess.Popen([binpath + "/scidbtestharness", "--plugins", plugins_dir, "--root-dir", "createload", "--test-name", "createload.test", "--debug", "5", "--port", portNumber] + harnessArgs, cwd=".", env=os.environ).wait()
    sys.exit(0)

        
  # Add environment variables
  os.environ["PATH"] = binpath + ":" + os.environ["PATH"]
  os.environ["IQUERY_HOST"] = 'localhost'
  os.environ["IQUERY_PORT"] = portNumber
  subprocess.Popen([binpath + "/scidbtestharness", "--plugins", plugins_dir, "--root-dir", "testcases", "--suite-id", "checkin", "--debug", "5", "--port", portNumber] + harnessArgs, cwd=".", env=os.environ).wait()
  sys.exit(0)
