#!/usr/bin/env python
#
#  This scripts prints the no. of times each operator is tested in the test harness.

import subprocess
import sys


SCIDB_PORT=1239


# Initialize variables
OP_LIST=[]
OP_COUNT={}
DIR_LIST=[]


def usage():
  print 'Usage: ',sys.argv[0],'<dir1> [<dir2>] ... [<dirn>]'
  print ''
  exit(1)


# Run the shell command and return the output
def run_cmd(cmd):
#  print cmd
  ret = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
  res = ret.stdout.read()
  if res.find('Error id') >= 0:
    print res
    exit(1)
  return res


# Get list of operators
def get_op_list():
  global OP_LIST
  cmd = 'iquery -aq "project(list(\'operators\'),name)" -ocsv -p '+str(SCIDB_PORT)
  op_str = run_cmd(cmd)
  op_str = op_str.partition('\n')[2]    # skip the first row which is attribute name
#  print op_str
  while(op_str != ''):
    OP_LIST.append(op_str.partition('\n')[0].replace('"',''))
    op_str = op_str.partition('\n')[2]
#  print OP_LIST
  print 'Total no. of operators =',len(OP_LIST)


# Get count of operators
def get_op_count():
  global OP_COUNT
  global DIR_LIST
  print 'Calculating count of operators ...'
  for SPATH in sys.argv:
    DIR_LIST.append(SPATH)
  DIR_LIST.remove(sys.argv[0])
  i = 0
  offset = len(OP_LIST) % 4   # to show time remaining
  j = len(OP_LIST) + 4 - offset          # to show time remaining
  for op in OP_LIST:
    tcount = 0
    for SPATH in DIR_LIST:
      cmd = 'grep -ir "'+op+'(" '+SPATH+' --exclude-dir=".svn" --include="*.test" | wc -l'
      tcount = tcount + int(run_cmd(cmd))
      cmd = 'grep -ir "'+op+' (" '+SPATH+' --exclude-dir=".svn" --include="*.test" | wc -l'
      tcount = tcount + int(run_cmd(cmd))
    OP_COUNT[op]=tcount
#    print '%3d) %-25s: %10d' % (i+1,op,tcount)
    i = i + 1
    if i == j/4:              # to show time remaining
      print '  25% done'
    elif i == j/2:
      print '  50% done'
    elif i == (j/4 + j/2):
      print '  75% done'
  print '  100% done',''
  print ''


# Adjust counts based on pattern matching
# For example, count(window) = count(window) - count(variable_window)
def adjust_count():
  global OP_COUNT
  OP_STR = OP_COUNT.__str__()
  i = 0
  for op in OP_LIST:
    if OP_STR.count(op) > 1:    # skip if operator name is not repeated
      for k in OP_COUNT.keys():
        if k.endswith(op) and k != op:
          OP_COUNT[op] = OP_COUNT[op] - OP_COUNT[k]
          print '%3d) %-25s: %10d' % (i+1,op,OP_COUNT[op])
          break
    else:
      print '%3d) %-25s: %10d' % (i+1,op,OP_COUNT[op])
    i = i+1



if __name__ == "__main__":
  if len(sys.argv) < 2 or sys.argv.count('--help') > 0:
    usage()
  get_op_list()
  get_op_count()
  adjust_count()
  print 'done.'


