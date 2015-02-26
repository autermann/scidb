#!/usr/bin/python

# Initialize, start and stop scidb in a cluster.
#
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

import subprocess
import sys
import traceback
import os
import string
import errno
import struct
from datetime import datetime, date, time
import array
from ConfigParser import RawConfigParser

def printDebug(string, force=False):
   if _DBG or force:
      print >> sys.stderr, "%s: DBG: %s" % (sys.argv[0], string)
      sys.stderr.flush()

def printInfo(string):
   sys.stdout.flush()
   print >> sys.stdout, "%s" % (string)
   sys.stdout.flush()

def printError(string):
   print >> sys.stderr, "%s: ERROR: %s" % (sys.argv[0], string)
   sys.stderr.flush()

def usage():
   print ""
   print "Usage: %s [<config_file>]" % sys.argv[0]
   print "Commands:"
   print "\t all"

# Parse a config file
def parseGlobalOptions(filename, section_name):
   config = RawConfigParser()
   config.read(filename)

   # First process the "global" section.
   try:
      #print "Parsing %s section." % (section_name)
      for (key, value) in config.items(section_name):
         _configOptions[key] = value
   except Exception, e:
      printError("config file parser error in file: %s, reason: %s" % (filename, e))
      sys.exit(1)
#
# Execute OS command
# This is a wrapper method for subprocess.Popen()
# If waitFlag=True and raiseOnBadExitCode=True and the exit code of the child process != 0,
# an exception will be raised.
def executeIt(cmdList,
              nocwd=False,
              useShell=False,
              cmd=None,
              stdoutFile=None, stderrFile=None,
              waitFlag=True,
              raiseOnBadExitCode=True):
    ret = 0
    out = ''
    err = ''

    dataDir = "./"
    if nocwd:
       currentDir = None
    else:
       currentDir = dataDir
    # print currentDir

    my_env = os.environ

    if useShell:
       cmdList=[" ".join(cmdList)]

    try:
       sout = None
       if stdoutFile != None:
          # print "local - about to open stdoutFile log file:", stdoutFile
          sout=open(stdoutFile,"w")
       elif not waitFlag:
          sout=open("/dev/null","w")

       serr = None
       if stderrFile != None:
          #print "local - about to open stderrFile log file:", stderrFile
          serr=open(stderrFile,"w")
       elif not waitFlag:
          serr=open("/dev/null","w")

       p = subprocess.Popen(cmdList,
                            env=my_env, cwd=currentDir,
                            stderr=serr, stdout=sout,
                            shell=useShell, executable=cmd)
       if waitFlag:
          p.wait()
          ret = p.returncode
          if ret != 0 and raiseOnBadExitCode:
             raise Exception("Abnormal return code: %d" % ret)
    finally:
       if (sout != None):
          sout.close()
       if (serr != None):
          serr.close()

    return ret

#
# Remove a given array from SciDB
# if record=True, name is added into usedMatrices
def eliminate(name, record=True):
    if record:
       _usedMatrices[name]=1
    cmdList=[_iqueryBin, "-p", _basePort, "-c", _targetHost, "-naq", "remove(%s)"%name]
    ret = executeIt(cmdList,
                    useShell=False,
                    raiseOnBadExitCode=False,
                    nocwd=True,
                    stdoutFile="/dev/null",
                    stderrFile="/dev/null")
    return ret
#
# Run a given AFL without fetching the result
def nafl(input, timeIt=False):
    cmdList=[_iqueryBin, "-p", _basePort, "-c", _targetHost, "-naq", input]
    if _timePrefix != None and timeIt:
       cmdList.insert(0,_timePrefix)
    ret = executeIt(cmdList,
                    useShell=False,
                    nocwd=True,
                    stdoutFile="/dev/null",
                    stderrFile=None)
    return ret

def store(what, name):
   _usedMatrices[name]=1
   nafl("store(%s, %s)" % (what,name))

#
# Run a given AFL with printing the result to stdout
#format:
#"-ocsv+"   # for explicit row,column printing
#"-osparse" # enable this when debugging distribution issues

def afl(input, format="-ocsv", timeIt=False):
    cmdList=[_iqueryBin, "-p", _basePort, "-c", _targetHost, format, "-aq", input]
    if _timePrefix != None and timeIt:
       cmdList.insert(0,_timePrefix)
    ret = executeIt(cmdList,
                    useShell=False,
                    nocwd=True,
                    stdoutFile=None,
                    stderrFile=None)
    return ret
#
# Run a given AFL with collecting the result into a buffer, which is returned
def aflResult(input,timeIt=False):
    # XXX TODO: get rid of the file altogether ?
    outFile = "/tmp/afl.%s.out" % os.getpid()
    cmdList=[_iqueryBin, "-p", _basePort, "-c", _targetHost, "-ocsv", "-aq", input]
    if _timePrefix != None and timeIt:
       cmdList.insert(0,_timePrefix)
    ret = executeIt(cmdList,
                    useShell=False,
                    nocwd=True,
                    stdoutFile=outFile,
                    stderrFile=None)
    sout=None
    try:
       sout = open(outFile, "r")
       result = sout.read()
    finally:
       if sout!=None:
          sout.close()
    return (ret,result)
#
# Create a 2D SciDB array schema
def createMatrix(name, rows, cols, rchunk_size, cchunk_size):
   _usedMatrices[name]=1
   aflStr = "create array %s <v:double>[r=0:%d,%d,0, c=0:%d,%d,0]" % (name, rows-1, rchunk_size, cols-1, cchunk_size)
   eliminate(name)
   nafl(aflStr)
#
# Populate (i.e. build) a 2D SciDB array
def populateMatrix(name, expression):
   aflStr = "build(%s,%s)" % (name,expression)
   store(aflStr,name)

#
# Return a number generator in the specified range.
# If range is invalid, None is returned.
# Format: '+'|'*':begin#:end#:step#
def getRangeGenerator(rangeStr):
   # Try to parse the range 
   limits = string.split(rangeStr,':')
   if len(limits) == 4:
      intLimits = map((lambda v: int(v)), limits[1:])
      if limits[0]=='+':
         return (lambda v: iterateRange(intLimits, v, (lambda v1,v2: add(v1,v2))))
      elif limits[0]=='*':
         return (lambda v: iterateRange(intLimits, v, (lambda v1,v2: mult(v1,v2))))
   return None

#
# Return a list of numbers
# If list is invalid, None is returned.
# Format: (#,#)+
# Note: a single number is not an accepted format (use range)
def parseList(listStr):
   tmp = string.split(listStr,',')
   if len(tmp) > 1 : #list of 1 is not acceptable, use range
      return map((lambda v: int(v)), tmp)
   return None

#
# Return a chunk size generator based on the listName.
# listName can be either in range/list format or a name of an internally-stored list,
# e.g. DEFAULT_SIZE_LIST
def getChunkSizeGenerator(order, listName):
   chunkSizeLists = {
      "DEFAULT_SIZE_LIST" :[32]
   }

   chunkSizeList=None
   if listName in chunkSizeLists:
      chunkSizeList=chunkSizeLists[listName]
   else:
     chunkSizeList = parseList(listName)

   if chunkSizeList!=None:
      divs=getOrderDivisorList()
      if len(divs)>0:  #variation in chunk sizes is requested
         chunkSizeList = varyChunkSizes(order, chunkSizeList, divs)
      return (lambda v: iterateList(chunkSizeList, v))

   func = getRangeGenerator(listName)
   if func != None:
      return func

   raise Exception("Invalid chunk size list range/name %s " % (listName))
#
# Generate chunk size for testing lots of edge conditions
# by setting the chunksize to make edge conditions
# and to ensure a minimum amount of multi-instance
# distribution is involved, by dividing into
# roughly 2,3,4, .. etc pieces
def varyChunkSizes(mat_order, scalapack_block_sizes, divs):
    chunksizes={}
    max_scalapack_block = scalapack_block_sizes[len(scalapack_block_sizes)-1]
    for scalapack_block_size in scalapack_block_sizes:
       chunksizes[scalapack_block_size]=1
       # white box test: also test chunksizes near the matrix size
       #
       # _ separates the numbers,... easier to tr to newlines for sorting with sort -nu
       # for now, we are not going to go above chunksize 64 ... that may be
       # the limit I set in the operator
       if mat_order < scalapack_block_size:
          # add on order-1, order, and order+1 sizes (boundary conditions)
          chunksizes[mat_order-1]=1
          chunksizes[mat_order]=1
          chunksizes[mat_order+1]=1
       elif mat_order <= scalapack_block_size:
          # test is same as "equal"
          # add on order-1 andl order ... order+1 is over the limit
          chunksizes[mat_order-1]=1
          chunksizes[mat_order]=1
       elif mat_order<=max_scalapack_block: # XXX: svd errors out on chunk sizes > max_scalapack_block 
          # just order itself
          chunksizes[mat_order]=1

    # XXX TODO JHM: verify logic
    # black box test: divide the size into N chunks ..
    # this is away from the edge conditions, but without exhaustive testing
    for div in divs: #XXX must get above the number of instances to see full parallelism
        tmpChunkSize = mat_order / div
        if tmpChunkSize > 1 and tmpChunkSize <= max_scalapack_block:
           chunksizes[tmpChunkSize]=1

    # remove duplicate chunk sizes only to save testing time
    result = sorted(chunksizes.keys())
    for i in range(len(result)):
       if result[i] > mat_order:
          return result[:i+1] #XXX test only one chunk size greater than the order

    return result

def getMatrixTypeMap():
    # XXX TODO:
    # here are some handy input matrices. keep these until we have full
    # extensive regression tests written, using R and SciDB compare ops, for example
    # XXX TODO: better names ?
    matrixDefs = {
            "zero"    :"0",
            "identity":"iif(r=c, 1, 0)",
            "int_nz"  :"iif(r=c,1,r+c)",
#            "1_2_3_4" :"iif(r=0, iif(c=0,1,2), iif(c=0,3,4))",
#            "r*k+c"   :"(r-1)*2+c",
#            "c*k+r"   :"double(r+1)+0.1*double(c+1)",
#            "r+c+2"   :"r+c+2",
#            "diag_1"  :"iif(r=c, iif(r=1, 3, 1), 0.0)",
#            "diag_2"  :"iif(r=c, sqrt(2)/2, iif(r=0,sqrt(2)/2,-sqrt(2)/2))",
#            "diag_3"  :"iif(r=c, sqrt(3)/2, iif(c=2, -0.5, 0.5))",
#            "r/c"     :"double(r+1)/double(c+1)",
#            "iid"     :"instanceid()",
            "random"  :"abs(random()/2147483648.0)+1.0e-6"   # want range (0,1] approximate with [1e-6,1.000001]
            }
    return matrixDefs

def getMatrixDef(matrixType):
    matrixDefs = getMatrixTypeMap()
    return matrixDefs[matrixType]

#
# Return a list of matrix size for a given list name
def getMatrixOrderList(listName):
    # useful ORDER_LISTs:
    #
    # sizes, thoroughness
    # -------,----------
    matrixOrderLists = {
            "XLARGE_SIZE_LIST"  :[16384, 32768, 65536],   # requires an ILP64 BLAS
            "LARGE_SIZE_LIST"   :[128, 256, 512, 1024, 2048, 4096, 8192],
            "MEDIUM_SIZE_LIST"  :[2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 17, 20, 32, 64, 128, 256, 512],
            "SMALL_SIZE_LIST"   :[2, 3, 4, 8, 9, 10, 32, 128, 512],
            "SMALLER_SIZE_LIST" :[3, 7, 17, 32, 128, 512],
            "SMALLER2_SIZE_LIST":[3, 17, 64, 512],
            "SMALLER3_SIZE_LIST":[2, 3, 17, 64],
            "SMALLEST_SIZE_LIST":[4, 8, 17, 64],
            "XSMALLEST_SIZE_LIST":[5]
            }
    return getOrderGenerator(listName, matrixOrderLists)

#
# Return a matrix order generator based on the listName.
# listName can be either in range/list format or a name of an internally-stored list,
# e.g. XSMALLEST_SIZE_LIST
def getOrderGenerator(listName, listMap):
   orderList = None
   if listName in listMap:
      orderList = listMap[listName]
   else:
      orderList = parseList(listName)

   if orderList != None:
      return (lambda v: iterateList(orderList, v))

   func = getRangeGenerator(listName)
   if func != None:
      return func
   raise Exception("Invalid size list range/name %s " % (listName))

# generate the next value based on the list of values and the last value
def iterateList(values, lastVal):
   for v in values:
       if v > lastVal:
          return v
   return 0

# generate the next value in the given range of values and the last value
def iterateRange(rangeSpec, lastVal, opFunc):
   if lastVal==0:
      return rangeSpec[0]
   if lastVal>0:
      if opFunc(lastVal,rangeSpec[2]) <= rangeSpec[1]:
         return opFunc(lastVal,rangeSpec[2])
   return 0

def add(v1,v2):
   return v1+v2
def mult(v1,v2):
   return v1*v2

#
# Execute func for each/specified type of test matrix
def iterateOverMatrixTypes(func,types=None):
    matrixDefs = getMatrixTypeMap()
    if types!=None:
       for type in types:
          if type in matrixDefs:
             func(type, matrixDefs[type])
          else:
             printError("Unknown matrix type"+type)
    else:
       for key,value in matrixDefs.items():
          func(key, value)

def getChunkSizeList():
    return _chunkSizeList

def getOrderDivisorList():
    return _divisorList
#
# Execute the test specified by func
def doTest(matrixTypeName, matrixType, order, chunksize, error_limit, func):
   try:
      func(matrixTypeName, matrixType, order, chunksize, error_limit)
      sys.stdout.flush()
   except Exception, e:
      mesg = "Testing matrix type %s, size %d, chunk_size %d, error_limit %d PASS: FALSE" % \
          (matrixTypeName, order, chunksize, error_limit)
      printDebug(mesg,True)
      traceback.print_exc()
      sys.stderr.flush()
      printInfo(mesg)
      printInfo("Reason: %s" % e)

#
# Run tests specified by func for all matrix sizes specified by orderList,
def runTests(orderList, errorLimit, func, matrixTypes=None):
   orderListFunc=getMatrixOrderList(orderList) # for checkin test
   printDebug("Iterating over the following matrix orders:")
   printDebug(str(orderList))

   chunkSizeList = getChunkSizeList()
   printDebug("TEST CHUNK SIZES %s"% str(chunkSizeList))

   #LOOP: matrix size (order)
   order = orderListFunc(0)
   while order!=0:
      #LOOP: Chunk Size
      chunkSizeGen = getChunkSizeGenerator(order, chunkSizeList)
      chunkSize = chunkSizeGen(0)
      while chunkSize!=0:
         printDebug("TEST CHUNK SIZE %s"% str(chunkSize))
         iterateOverMatrixTypes((lambda key,matrixType: doTest(key, matrixType, order, chunkSize, errorLimit, func)),\
                                   matrixTypes)
         chunkSize = chunkSizeGen(chunkSize)
      order = orderListFunc(order)

# Generate a diagonal matrix from the vector vector and store in the array called result
def generateDiagonal(vector,result):
   printDebug("Generating diagonal matrix from %s" % vector)

   # get length of vector
   dims = getDims(vector)
   cols = dims[0]

   intervalAfl = "project(dimensions(%s),chunk_interval)" % vector
   (ret,chunkInterval) = aflResult(intervalAfl)

   printDebug("DEBUG: chunkInterval=%s" % chunkInterval)

   chunkSize = int(string.split(chunkInterval)[1])

   # generate a column matrix of equal to the singular values, S (since S is output as a vector,
   # and the linear aglebra library does not yet know that a vector is a column)
   # and a row vector of ones
   # and multiply them together.  They will RLE nicely.
   #   [ a ]                      [ a a a ]
   #   [ b ]       x  [ 1 1 1 ] = [ b b b ]
   #   [ c ]                      [ c c c ]
   #
   #   t(addim(VEC)) * VEC_1 -> OUTER_PRODUCT
   #

   VEC_1="VEC_1"
   createMatrix(VEC_1,1,cols,1,chunkSize)
   populateMatrix(VEC_1, "1") #matrix of ones

   if False:
      printDebug("debug diag transpose(adddim(%s)) @@@@@@@@@@@@@@@@" % vector)
      afl("transpose(adddim(%s,c))" % vector)
      eliminate("DEBUG_1")
      afl("store(transpose(adddim(%s,c)),DEBUG_1)" % vector)
      afl("show(DEBUG_1)")
      printDebug("debug diag transpose %s @@@@@@@@@@@@@@@@" % vector)
      afl("show(%s)" % VEC_1)

   OUTER_PRODUCT="OUTER_PRODUCT"
   outerProductAfl="multiply(transpose(adddim(%s,c)),%s)" % (vector,VEC_1)
   eliminate(OUTER_PRODUCT)
   store(outerProductAfl,OUTER_PRODUCT)

   # then we just use iif to set off-diagonal values to 0
   #
   #
   #    [ a 0 0 ]
   # -> [ 0 b 0 ] which is our result
   #    [ 0 0 c ]
   resultAfl = "project(apply(%s,s,iif(i=c,multiply,0.0)),s)" % (OUTER_PRODUCT)
   eliminate(result)
   store(resultAfl,result)

#
# Frobenius norm of a matrix
def norm(MATRIX, attr, attr_norm, MAT_NORM=None):
    printDebug("Computing norm(%s)"%MATRIX)

    normAfl="project(apply(sum(project(apply(%s,square,%s*%s),square)), %s, sqrt(square_sum)), %s)" % \
        (MATRIX,attr,attr,attr_norm,attr_norm)

    if MAT_NORM != None:
       eliminate(MAT_NORM)
       store(normAfl,MAT_NORM)
    return normAfl

#
# Scale (i.e. multiply) the singular values in MATRIX by 1/LSB/order
def scaleError(order, MATRIX, nelsb, ebpc, MAT_ERROR):
   # 2^55= 36028797018963968 ~= reciprocal of 1 LSB
   errorAfl="project(apply(%s,%s,%s/%d*36028797018963968.0),%s)" % (MATRIX,ebpc,nelsb,order,ebpc)
   if MAT_ERROR != None:
      eliminate(MAT_ERROR)
      store(errorAfl,MAT_ERROR)
   return errorAfl

def divMat(MAT_1, attr_1, MAT_2, attr_2, attr_res, MAT_RES=None):
   divAfl = "project(apply(join(%s,%s),%s,%s/%s), %s)" % (MAT_1, MAT_2, attr_res, attr_1, attr_2, attr_res)
   if MAT_RES != None:
      eliminate(MAT_RES)
      store(divAfl,MAT_RES)
   return divAfl

def addMat(MAT_1, attr_1, MAT_2, attr_2, attr_res, MAT_RES=None):
   divAfl = "project(apply(join(%s,%s),%s,%s+%s), %s)" % (MAT_1, MAT_2, attr_res, attr_1, attr_2, attr_res)
   if MAT_RES != None:
      eliminate(MAT_RES)
      store(divAfl,MAT_RES)
   return divAfl

def getDims(M):
   dimsAfl="project(dimensions(%s),length)" % M
   (ret,lengths)=aflResult(dimsAfl)

   printDebug("getDims: lengths=%s" % str(lengths))

   results = string.split(lengths)
   return map((lambda v: int(v)), results[1:])


# Compute residual measures: (MAT_1-MAT_2)
def computeResidualMeasures(MAT_1, attr_1, MAT_2, attr_2, attr_res, MAT_RES=None):
    printDebug("computeResidualMeasures start")
    printDebug("MAT_1 is %s" % MAT_1)
    printDebug("attr_1 is %s" % attr_1)
    printDebug("MAT_2 is %s"  % MAT_2)
    printDebug("attr_2 is %s" % attr_2)
    printDebug("attr_res is %s" % attr_res)
    printDebug("MAT_RES is %s" % MAT_RES)

    diffAfl = "apply(join(%s,%s),%s,(%s-%s))" % (MAT_1, MAT_2, attr_res, attr_1, attr_2)

    if MAT_RES != None:
       printDebug("computeResidualMeasures output is in array %s" % MAT_RES)
       eliminate(MAT_RES)
       store(diffAfl, MAT_RES)
    printDebug("computeResidualMeasures finish")
    return diffAfl

# Print TRUE (or FALSE) to stdout if the RMS_EBPC values in RMS_ERROR are "less than" ERROR_LIMIT
# where "less than" is the default compareOp 
def testForError(testName, RMS_ERROR, RMS_EBPC, ERROR_LIMIT, compareOp="<"):
   passTestAfl = "apply(%s,PASS,%s%s%d)" % (RMS_ERROR,RMS_EBPC,compareOp,ERROR_LIMIT)
   (ret,res)=aflResult(passTestAfl)
   vals = string.split(res)[1]
   pair = string.split(vals,',')
   if _DBG:
      afl(passTestAfl)
   printDebug("%s PASS (error(%s) %s limit(%d)): %s" % \
                (testName,pair[0],compareOp,ERROR_LIMIT, str.upper(pair[1])), True)
   printInfo("%s PASS : %s" % \
                (testName,str.upper(pair[1])))

# Compute  norm( INPUT - U S VT ) / [ norm(INPUT) order ulp ]
# order = max(M,N)
def doSvdMetric1(NROW,NCOL,MAT_INPUT, VEC_S, MAT_U, MAT_VT, MAT_ERROR, EBPC, func):
    printDebug("doSvdMetric1 start")
    printDebug("MAT_INPUT is %s" % MAT_INPUT)
    printDebug("VEC_S is %s"  % VEC_S)
    printDebug("MAT_U is %s"  % MAT_U)
    printDebug("MAT_VT is %s" % MAT_VT)
    printDebug("MAT_ERROR is %s"% MAT_ERROR)

    # multiply the factors back together
    # first must turn the S vector into a diagonal matrix

    METRIC_TMP_DIAG_SS="METRIC_TMP_DIAG_SS"
    generateDiagonal(VEC_S, METRIC_TMP_DIAG_SS)
    if _DBG:
       printDebug("%s:"%METRIC_TMP_DIAG_SS)
       afl("show(%s)"%METRIC_TMP_DIAG_SS)

    inverseSVDAfl = "multiply(%s,multiply(%s,%s))" % (MAT_U,METRIC_TMP_DIAG_SS,MAT_VT)

    # difference of |original| and the |product of the svd matrices|
    diffAfl = computeResidualMeasures(MAT_INPUT, "v", inverseSVDAfl, "multiply", "res_measure")

    inputNormAfl = norm(MAT_INPUT, "v", "input_norm")

    residualNormAfl = norm(diffAfl, "res_measure", "res_norm")

    divAfl = func(residualNormAfl, "res_norm", inputNormAfl, "input_norm", "res_error")

    order = max(NROW, NCOL)
    errAfl = scaleError(order, divAfl, "res_error", EBPC, MAT_ERROR)

    printDebug("doSvdMetric1 output is in array %s" % MAT_ERROR)
    printDebug("doSvdMetric1 finish")
    return errAfl

# Compute norm(I - Ut*U) / [ M ulp ]
# M=NROW
def doSvdMetric2(MAT_U, NROW, CHUNKSIZE, MAT_ERROR, EBPC):
    printDebug("doSvdMetric2 start")
    printDebug("MAT_U is %s"  % MAT_U)
    printDebug("NROW is %d"  % NROW)
    printDebug("MAT_ERROR is %s"% MAT_ERROR)
    printDebug("EBPC is %s"% EBPC)

    I="IDENTITY"
    createMatrix(I, NROW, NROW, CHUNKSIZE, CHUNKSIZE)
    populateMatrix(I,getMatrixDef("identity"))

    UTxUAfl = "multiply(transpose(%s),%s)" % (MAT_U,MAT_U)

    diffAfl = computeResidualMeasures(I, "v", UTxUAfl, "multiply", "res_measure")

    residualNormAfl = norm(diffAfl, "res_measure", "res_norm")

    errAfl = scaleError(NROW, residualNormAfl, "res_norm", EBPC, MAT_ERROR)

    printDebug("doSvdMetric2 output is in array %s" % MAT_ERROR)
    printDebug("doSvdMetric2 finish")
    return errAfl

# Compute norm(I - VT*VTt) / [ N ulp ]
# N=NCOL
def doSvdMetric3(MAT_VT, NCOL, CHUNKSIZE, MAT_ERROR, EBPC):
    printDebug("doSvdMetric3 start")
    printDebug("MAT_VT is %s"  % MAT_VT)
    printDebug("NCOL is %d"  % NCOL)
    printDebug("MAT_ERROR is %s"% MAT_ERROR)
    printDebug("EBPC is %s"% EBPC)

    I="IDENTITY"
    createMatrix(I, NCOL, NCOL, CHUNKSIZE, CHUNKSIZE)
    populateMatrix(I,getMatrixDef("identity"))

    VTTxVTAfl = "multiply(%s, transpose(%s))" % (MAT_VT,MAT_VT)

    diffAfl = computeResidualMeasures(I, "v", VTTxVTAfl, "multiply", "res_measure")

    residualNormAfl = norm(diffAfl, "res_measure", "res_norm")

    errAfl = scaleError(NCOL, residualNormAfl, "res_norm", EBPC, MAT_ERROR)

    printDebug("doSvdMetric3 output is in array %s" % MAT_ERROR)
    printDebug("doSvdMetric3 finish")
    return errAfl

def doSvdMetric4(NROW, VEC_S, MAT_ERROR, EBPC):
   printDebug("doSvdMetric4 start")
   printDebug("VEC_S is %s"  % VEC_S)
   printDebug("MAT_ERROR is %s"% MAT_ERROR)
   printDebug("NROW is %d"  % NROW)

   if _DBG:
      printDebug("doSvdMetric4 VEC_S: ")
      afl("show(%s)"%VEC_S)
      afl("scan(%s)"%VEC_S)

   cols = NROW
   if _DBG:
      # get length of vector
      dims = getDims(VEC_S)
      cols = dims[0]
      if cols != NROW:
         raise Exception("Mismatched dimensions: %d != %d" % (cols,NROW))

   # make sure the singular values, sigma, (in S) are strictly decreasing
   # TODO: we should note why we do this with joining the shift, rather than sorting and comparing for equality
   #       since that would be the naive way.
   compareAfl = "project(apply( \
                           sum(apply(join (subarray(%s, 0, (%d-2)) as L, \
                                           subarray(%s, 1, (%d-1)) as R), \
                               BLAH, iif(L.sigma<R.sigma, 1, 0) ), BLAH), \
                         %s, BLAH_sum), %s)" %  (VEC_S, cols, VEC_S, cols, EBPC, EBPC)

   if MAT_ERROR != None:
      eliminate(MAT_ERROR)
      store(compareAfl, MAT_ERROR)

   printDebug("doSvdMetric4 output is in array %s" % MAT_ERROR)
   printDebug("doSvdMetric4 finish")
   return compareAfl

#
# Test the SVD operator.
# Implements an appropriate interface to be used by iterateOverTests()
def testSVD(matrixTypeName, matrixTypeDef, ORDER, CHUNKSIZE, ERROR_LIMIT):
###################
# Notes from James:
# Though I get tests 1-4 right away (and so commented what they are below, I don't get the metrics in 5-9 right away...
# I would understand if they were, e.g. U1 dot U2, (checking orthogonality of singular vectors) but I don't get U1 minus U2.
# --> Bryan may recognize right away what 5-9 are for more quickly than I will.

# Anyway, this will give you an idea of how we could run these tests at very large sizes in reasonable amounts of time.

# Let A be an matrix of size M,N and SVD(A) -> U, S, VT
# Let I be the identity matrix
# Let ulp be the machine precision (stands for unit of least precision, also called machine epsilon or epsilon-sub-m)
# Let SIZE be the number of singular values requested

# The 9 metrics are:

#     Product of factors: norm( A - U S VT ) / [ norm(A) max(M,N) ulp ]
#     Check length of the singular vectors: norm(I - Ui' Ui) / [ M ulp ]
#     ..... Similarly for VT, N
#     Check that S contains requested number of values in decreasing order
#     norm(S1 - S2) / [ SIZE ulp |S| ]
#     norm(U1 - U2) / [ M ulp ]
#     norm(S1 - S3) / [ SIZE ulp |S| ]
#     norm(VT1-VT3) / [ N ulp ]
#     norm(S1 - S4) / [ SIZE ulp |S|  ]

# The test matrices are of 6 types:

#     0 matrix
#     I (identity)
#     diag(1, ...., ULP) "evenly spaced"
#     U*D*VT where U,VT are orthogonal and D is "evenly spaced" as in 3
#     matrix of 4, multiplied by the sqrt of the overflow threshold   (larger values)
#     matrix of 4, multiplied by the sqrt of the underflow threshold (smaller values


# From this we can see that these can efficiently constructed from the following:

#     norm of matrix  -- exists in SciDB, IIRC, it was not super-fast
#     norm of matrix difference -- "SSVD norm" is very close to what is wanted here
#     multiplication of a few scalars ... apply will be fine
#     array(scalar) ... direct construction of run-lengths might be appropriate
#     diagonal(vector) .... matrix from vector -- doSvdL.sh does this inefficiently
#     matrix multiplication ... we will have this soon.

# Don't yet know how the U and VT for test matrix 4 is synthesized yet ... we can get to that later
# Ref: http://www.netlib.org/scalapack/lawn93/node45.html
###################
  
   NROW=ORDER
   if ORDER>1:
      orderList=[ORDER-1, ORDER, ORDER+1]
   else:
      orderList=[ORDER, ORDER+1]
   orderList=[ORDER] # XXX: make a (ORDER, ORDER) matrix for now
   for NCOL in orderList:
      testSVDByDim(matrixTypeName, matrixTypeDef, NROW, NCOL, CHUNKSIZE, ERROR_LIMIT)

#
# XXX
def testSVDByDim(matrixTypeName, matrixTypeDef, NROW, NCOL, CHUNKSIZE, ERROR_LIMIT):
   # want to use "nafl" from iqfuncs.bash, but haven't resolved quoting problems yet
   INPUT="INPUT"
   createMatrix(INPUT, NROW, NCOL, CHUNKSIZE, CHUNKSIZE)

   if False:
      # don't bother saving and running SvdMetric,
      # plain svd is too slow to compare to anyway
      populateMatrix(INPUT,getMatrixDef("random"))

      printDebug("Computing [only] the SVD matrices")

      nafl("gesvd(%s,'values')" % INPUT)
      nafl("gesvd(%s,'left')" % INPUT)
      nafl("gesvd(%s,'right')" % INPUT)
   else:
      populateMatrix(INPUT,matrixTypeDef)

      printDebug("Generating SVD matrices")
      S="S"
      SAfl = "gesvd(%s,'values')" % INPUT
      eliminate(S)
      store(SAfl,S) #always materialized until the generation of a diagonal matrix is improved
      if _DBG:
         printDebug("%s:"%S)
         afl("show(%s)"%S)

      UAfl = "gesvd(%s,'left')" % INPUT
      U="U"
      eliminate(U)
      store(UAfl, U)
      if _DBG:
         printDebug("%s:"%U)
         afl("show(%s)"%U)

      VTAfl = "gesvd(%s,'right')" % INPUT
      VT="VT"
      eliminate(VT)
      store(VTAfl, VT)
      if _DBG:
         printDebug("%s:"%VT)
         afl("show(%s)"%VT)

      printDebug("Computing the error metric 1")

      RMS_ERROR="RMS_ERROR"
      RMS_EBPC="rms_error_bits_per_cell"

      resultMesg = " matrix type %s, RxC %dx%d, chunk_size %d, error_limit %d" % \
          (matrixTypeName, NROW,NCOL, CHUNKSIZE, ERROR_LIMIT)

      # matrix of residual measures: U*S*VT(from svd) - INPUT (the original matrix)
      errorLimit = ERROR_LIMIT
      compareOp="<"
      if matrixTypeName == "zero":
         # all values should be zero
         errorLimit = 0
         compareOp="="
         doSvdMetric1(NROW,NCOL, INPUT, S, U, VT, RMS_ERROR, RMS_EBPC, \
                         (lambda MAT_1, attr_1, MAT_2, attr_2, attr_res: addMat(MAT_1, attr_1, MAT_2, attr_2, attr_res)))
      else:
         doSvdMetric1(NROW,NCOL, INPUT, S, U, VT, RMS_ERROR, RMS_EBPC, \
                         (lambda MAT_1, attr_1, MAT_2, attr_2, attr_res: divMat(MAT_1, attr_1, MAT_2, attr_2, attr_res)))
      testForError("SVD_METRIC_1"+resultMesg, RMS_ERROR, RMS_EBPC, errorLimit, compareOp)

      # TODO: also need to check for orthoginality of U,V, what else?

      # orthoginality of U
      printDebug("Computing the error metric 2")
      doSvdMetric2(U, NROW, CHUNKSIZE, RMS_ERROR, RMS_EBPC)
      testForError("SVD_METRIC_2"+resultMesg, RMS_ERROR, RMS_EBPC, ERROR_LIMIT)

      # orthoginality of VT
      printDebug("Computing the error metric 3")
      doSvdMetric3(VT, NCOL, CHUNKSIZE, RMS_ERROR, RMS_EBPC)
      testForError("SVD_METRIC_3"+resultMesg, RMS_ERROR, RMS_EBPC, ERROR_LIMIT)

      printDebug("Computing the error metric 4")
      doSvdMetric4(NROW, S, RMS_ERROR, RMS_EBPC)
      errorLimit=0 #force error limit
      testForError("SVD_METRIC_4"+resultMesg,
                   RMS_ERROR, RMS_EBPC, errorLimit, compareOp="=")

#
# compute: norm(MULT-GEMM)/ORDER/LBS
def doGemmMetric(ORDER, MULT, GEMM, MAT_ERROR, EBPC):
   printDebug("doGemmMetric start")

   diffAfl = computeResidualMeasures(MULT, "multiply", GEMM, "gemm",  "res_measure")

   residualNormAfl = norm(diffAfl, "res_measure", "res_norm")

   referenceNormAfl = norm(MULT, "multiply", "ref_norm")

   divAfl = divMat(residualNormAfl, "res_norm", referenceNormAfl, "ref_norm", "res_error")

   errAfl = scaleError(ORDER, divAfl, "res_error", EBPC, MAT_ERROR)

   printDebug("doGemmMetric output is in array %s" % MAT_ERROR)
   printDebug("doGemmMetric finish")
   return errAfl

#
# Test the GEMM operator.
# Implements an appropriate interface to be used by iterateOverTests()
def testGEMM(matrixTypeName, matrixTypeDef, ORDER, CHUNKSIZE, ERROR_LIMIT):
   # make a (ORDER, ORDER) matrix
   NROW=ORDER
   NCOL=ORDER

   printInfo("GEMM TEST")

   # convert to integers
   matrixTypeDef = "iif(floor(%s)=0, 1, floor(%s))" % (matrixTypeDef,matrixTypeDef)
   INPUT="INPUT"
   createMatrix(INPUT, NROW, NCOL, CHUNKSIZE, CHUNKSIZE)
   populateMatrix(INPUT,matrixTypeDef)

   printDebug("GEMM(A,B) vs multiply(A,B) size %s, chunksize %s" % (ORDER, CHUNKSIZE))

   dims = getDims(INPUT)

   printDebug("GEMM matrix dims: %s"%str(dims))

   GEMM_RESULT_SCHEMA=INPUT
   TRANS = INPUT
   if dims[0]!=dims[1]: #not square matrix ?
      TRANS = "transpose(%s)"%INPUT
      GEMM_RESULT_SCHEMA=INPUT+"_GEMM_SCHEMA"
      eliminate(GEMM_RESULT_SCHEMA)
      createMatrix(GEMM_RESULT_SCHEMA, dims[0], dims[0], CHUNKSIZE, CHUNKSIZE)

   gemmAfl = "gemm(%s, %s, build(%s,0))" % (INPUT, TRANS, GEMM_RESULT_SCHEMA)
   if _DBG:
      GEMM="GEMM"
      eliminate(GEMM)
      printDebug("Saving %s"%GEMM)
      store(gemmAfl, GEMM)
      afl("show(%s)" % GEMM)

   multAfl = "multiply(%s,%s)" % (INPUT,TRANS)
   if _DBG:
      MULT="MULT"
      eliminate(MULT)
      store(multAfl,MULT)
      afl("show(%s)" % MULT)

   printDebug("Computing GEMM error metric")
   RMS_ERROR="RMS_ERROR"
   RMS_EBPC="rms_error_bits_per_cell"

   doGemmMetric(ORDER, multAfl, gemmAfl, RMS_ERROR, RMS_EBPC)

   resultMesg = " matrix type %s, size %d, chunk_size %d, error_limit %d" % \
          (matrixTypeName, ORDER, CHUNKSIZE, ERROR_LIMIT)
   testForError("GEMM"+resultMesg, RMS_ERROR, RMS_EBPC, ERROR_LIMIT)

#
# Test the MPICOPY operator.
# Implements an appropriate interface to be used by iterateOverTests()
def testMPICopy(matrixTypeName, matrixTypeDef, order, chunkSize, errorLimit):
   # make a (order, order) matrix
   nrow=order
   ncol=order
   errorLimit=0 # copy should not incur any errors

   printInfo("MPICOPY TEST, force error_limit=%d"%errorLimit)

   INPUT="INPUT"
   createMatrix(INPUT, nrow, ncol, chunkSize, chunkSize)
   populateMatrix(INPUT,matrixTypeDef)

   mpicopyAfl = "mpicopy(%s)" % (INPUT)
   if _DBG:
      MPICOPY="MPICOPY"
      eliminate(MPICOPY)
      store(mpicopyAfl, MPICOPY)
      afl("show(%s)" % MPICOPY)

   printDebug("Computing MPICOPY error metric")
   error = "res_measure"
   metricAfl = computeResidualMeasures(INPUT, "v", mpicopyAfl, "copy", error)

   maxAfl = "max(project(%s, %s))" % (metricAfl, error)
   error = error+"_max"

   testForError("MPICOPY", maxAfl, error, errorLimit, compareOp="=" )

#
# Execute all or a subset of tests registered with the system
# To register a test, it has to be added to the allTests table
def iterateOverTests(sizeList, errorLimit, tests=None, matrixTypes=None):
   allTests = {
      "SVD" : lambda mName, mType, order, chsize, errLimit: testSVD(mName, mType, order, chsize, errLimit),
      "GEMM" : lambda mName, mType, order, chsize, errLimit: testGEMM(mName, mType, order, chsize, errLimit),
      "MPICOPY" : lambda mName, mType, order, chsize, errLimit: testMPICopy(mName, mType, order, chsize, errLimit)
   }
   if tests!=None:
      for test in tests:
         if test in allTests:
            func = allTests[test]
            runTests(sizeList, errorLimit, func, matrixTypes)
         else:
            printError("Unknown test:"+test)
   else:
      for func in allTests.values():
         runTests(sizeList, errorLimit, func, matrixTypes)

#config file options
_configOptions = {}

#
# SciDB connection port
_basePort="1239"

#
# SciDB connection target host
_targetHost="localhost"

# The name of the iquery executable
_iqueryBin="iquery"

# Time iquery executions
_timePrefix=None

# Chunk sizes
_chunkSizeList="DEFAULT_SIZE_LIST"

# Chunk size divisors
#_divisorList=[2,3] # e.g. 7,3 or 7,4,3
_divisorList=[]

# A list of array names used during the test runs.
# It is used to clean up the SciDB state on exit.
_usedMatrices={}

_DBG = False

# The main entry routine that does command line parsing
def main():
   # Very basic check.
   if len(sys.argv)>2:
      usage()
      sys.exit(2)

   if len(sys.argv)==2:
      arg = sys.argv[1]
      if len(arg) <= 0:
         usage()
         sys.exit(2)
   else:
      arg=""

   if arg == "--help" or arg == "-h":
      usage()
      sys.exit(0)

   if len(arg) > 0:
      configfile=arg
      parseGlobalOptions(configfile,"dense_linear_algebra")

   global _basePort
   global _targetHost
   global _iqueryBin
   global _timePrefix
   global _divisorList
   global _chunkSizeList

   errorLimit=10
   sizeList="SMALLEST_SIZE_LIST"
   testsToRun = None # means all
   matriciesToUse = None # means all

   if "install-path" in _configOptions:
      installPath = _configOptions.get("install-path")
      if len(installPath) > 0:
         _iqueryBin = installPath+"/bin/"+_iqueryBin

   if "base-port" in _configOptions:
      _basePort = _configOptions.get("base-port")

   if "time-queries" in _configOptions:
      _timePrefix="time"

   if "target-host" in _configOptions:
      _targetHost = _configOptions.get("target-host")

   if "error-limit" in _configOptions:
      errorLimit = int(_configOptions.get("error-limit"))

   if "size-list" in _configOptions:
      sizeList = _configOptions.get("size-list")

   if "chunk-size-list" in _configOptions:
      _chunkSizeList = _configOptions.get("chunk-size-list")

   if "divisor-list" in _configOptions:
       tmp = parseList(_configOptions.get("divisor-list"))
       if tmp != None :
          _divisorList = tmp

   if "tests" in _configOptions:
      tests = _configOptions.get("tests")
      testsToRun=tests.split(',')
      for t in testsToRun:
         if t == "all":
            testsToRun = None
            break

   if "matrix-types" in _configOptions:
      matriciesToUse = _configOptions.get("matrix-types")
      matriciesToUse = matriciesToUse.split(',')
      for m in matriciesToUse:
         if m == "all":
            matriciesToUse = None
            break

   nafl("load_library(\'dense_linear_algebra\')");
   printDebug("Start %s: sizeList %s, errorLimit %d, testsToRun %s on matricies %s"%\
                (str(datetime.utcnow()),sizeList,errorLimit,str(testsToRun),str(matriciesToUse)),True)

   iterateOverTests(sizeList, errorLimit, testsToRun, matriciesToUse)

   for name in _usedMatrices.keys():
      eliminate(name,False)

   printDebug("Finish %s: sizeList %s, errorLimit %d, testsToRun %s on matricies %s"%\
                (str(datetime.utcnow()),sizeList,errorLimit,str(testsToRun),str(matriciesToUse)),True)
   sys.exit(0)

### MAIN
if __name__ == "__main__":
   main()
### end MAIN
