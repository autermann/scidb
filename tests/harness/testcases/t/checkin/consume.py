#!/usr/bin/python
from scidbTestCase import testCase
from pushPopCleaners import ArrayCleaner
from pushPopCleaners import DataDirCleaner
from scidbIqueryLib import scidbIquery
from queryMakerLib import defaultBuildQuery
import functools
import sys
import time
import os

# Consume query template.
CONSUME_QUERY = 'consume(<query>)'

# Message that is returned upon successful completion of a consume query.
SUCCESS_MSG = 'Query was executed successfully'

def loadLibQueries():
    
    libs = [
        'dense_linear_algebra',
        ]
    queries = ['load_library(\'' + x + '\'' + ')' for x in libs]
    return queries
    
def getPluginOpTestQueries(dbq):
    matchBuild = dbq.build(t=['float'],c=[1000,1000],o=[10,10],i='1.0')
    gemmBuild = dbq.build(r=[(0,9)],c=[32,32])
    gesvdBuild = dbq.build(r=[(0,9)],c=[32,32],i='random()%1.0')
    mpiRankBuild = dbq.build(r=[(0,63)],c=[32,32,],i='random()%1.0') #mpirank(build(<v:double>[i=0:64-1,32,0, j=0:64-1,32,0],0))
    
    queries = [
    #bestmatch - no test yet (causes problems because test array is large)
    #bestmatch - no test yet (causes problems because test array is large)
    #fits - seems to be broken (causes network errors)
    #fits_show - afraid to run it for the same reason as above
    'gemm(' + gemmBuild + ',' + gemmBuild + ',' + gemmBuild + ')',
    'gesvd(' + gesvdBuild + ',\'values\')'
    ]
    
    return queries

# Auxiliary queries which create arrays for the operators which require stored array
# names.
def getArrayCreateQueries(dbq):
    # dbq - object for "rolling" schema and build query statements.
    
    # Default build query and schema "roller": makes the strings.
    # The default build query (called as is - dbq.build()), will produce this:
    #  'build(<attr1:double>[i=0:9,4,0,j=0:9,4,0],random())'
    # The default schema (if called as is - dbq.schema()), will produce this:
    # '<attr1:double>[i=0:9,4,0,j=0:9,4,0]'
    # See definition of the class on explanation of parameters.
    
    a1a2a3_0_3 = dbq.schema(a=['attr3'],d=['attr1','attr2'],r=[(0,3)])
    reshapeSch = dbq.schema(r=[(0,4),(0,19)],c=[6,6])
    inputSch = dbq.schema(a=['a','b'],d=['x','y'],t=['int32'],r=[(0,3)])
    defSchema = dbq.schema()
    int64_i_0_11 = functools.partial(dbq.build, t=['int64'],d=['i'],r=[(0,11)])
    dbl_i_0_11 = functools.partial(dbq.build, t=['double'],d=['i'],r=[(0,11)])
    
    ARRAY_CREATE_QUERIES = [
        'create array array_for_redimension_test ' + a1a2a3_0_3,
        'create array array_for_remove_test '      + a1a2a3_0_3,
        'create array array_for_rename_test '      + a1a2a3_0_3,
        'create array array_for_repart_test '      + defSchema,
        'create array array_for_reshape_test '     + reshapeSch,
        'create array array_for_input_test '       + inputSch,
        'consume(store(join(join(transpose(' + int64_i_0_11(i='i%4') + '),transpose(' + int64_i_0_11(a=['attr2'],i='i%4') + ')),transpose(' + dbl_i_0_11(a=['attr3'],i='i') + ')),redim1))'
        ]
    return ARRAY_CREATE_QUERIES

def getOpTestQueries(dbq):
    # dbq - object for "rolling" schema and build query statements.
    
    # Default build query and schema "roller": makes the strings.
    # The default build query (called as is - dbq.build()), will produce this:
    #  'build(<attr1:double>[i=0:9,4,0,j=0:9,4,0],random())'
    # The default schema (if called as is - dbq.schema()), will produce this:
    # '<attr1:double>[i=0:9,4,0,j=0:9,4,0]'
    # See definition of the class on explanation of parameters.
    
    # Skipped operators:
    # 1) cancel: returns DDL error when passed to consume
    # 2) consume: seems to error out when passed to itself
    # 3) create array: also throws some error when passed to consume
    # 4) load_library: did not attempt to implement (likely not needed)
    # 5) redimension_store: returns error when passed to consume
    # 6) remove: throws DDL errors when passed to consume
    # 7) rename: throws DDL errors when passed to consume
    # 8) unload_library: did not attempt to implement (likely not needed)
    
    # Default build statement: build(<attr1:double>[i=0:99,4,0,j=0:99,4,0],random())
    defBuild = dbq.build()
    
    # Default array schema: <attr1:double>[i=0:99,4,0,j=0:99,4,0]
    defSchema = dbq.schema()
    
    int64_i_0_11 = functools.partial(dbq.build, t=['int64'],d=['i'],r=[(0,11)])
    dbl_i_0_11 = functools.partial(dbq.build, t=['double'],d=['i'],r=[(0,11)])
    
    # Main set of queries of all operators which will be "fed" to consume operator.
    OP_TEST_QUERIES = [
         'adddim('                 + defBuild + ',attr2)',
         'aggregate('              + dbq.build(t=['int64'],r=[(0,9)],i='random()%5') + ',sum(attr1))',
         'allversions(array_for_redimension_test)',
         'attributes(array_for_redimension_test)',
         'analyze('                + defBuild + ')',
         'apply('                  + defBuild + ',val2,0.5*attr1)',
         'approxdc('               + dbq.build(t=['int64'],r=[(0,9)],i='random()%16') + ',attr1)',
         'attribute_rename('       + defBuild + ',attr1,new_val)',
         'avg('                    + defBuild + ',attr1)',
         'avg_rank('               + defBuild + ')',
         'bernoulli('              + defBuild + ',0.31459,12345)',
         'between('                + defBuild + ',1,1,88,88)',
         defBuild, # simple build command (so, default string is ok)
         'build_sparse('           + dbq.schema(r=[(0,199)])+ ',1,i=j)',
         'cast('                   + dbq.build(t=['int64'],r=[(0,199)]) + ',' + dbq.schema(a=['attr2'],t=['int64'],d=['i','jj'],r=[(0,199)])+ ')',
         'concat('                 + dbq.build(r=[(0,199)]) + ',' + dbq.build(r=[(0,199)])+ ')',
         'count('                  + dbq.build(r=[(0,199)])+ ')',
         'cross('                  + dbq.build(r=[(0,19)])+ ',' + dbq.build(a=['attr2'],d=['x','y'],r=[(0,19)]) + ')',
         'cross_join('             + dbq.build(r=[(0,19)]) + ',' + dbq.build(a=['attr2'],d=['x','y'],r=[(0,19)]) + ',j,y)',
         'cumulate('               + dbq.build(r=[(0,49)]) + ',sum(attr1),j)',
         'deldim('                 + dbq.build(r=[(0,0),(0,199)]) + ')',
         'dimensions(array_for_repart_test),1', # test consume operator second option
         'diskinfo()',
         'echo(\'test,test,test\')',
         'explain_logical(\''      + defBuild + '\',\'afl\')',
         'explain_physical(\''     + defBuild + '\',\'afl\')',
         'filter('                 + dbq.build(i='random()/2147483647.0') + ',attr1<0.5)',
         'help()',
         'input(array_for_input_test,\'../tests/harness/testcases/data/M4x4_1.txt\')',
         'insert('                 + defBuild + ',array_for_repart_test)',
         'join('                   + dbq.build(r=[(0,19)]) + ',' + dbq.build(r=[(0,19)]) + ')',
         'list(\'instances\')',
         'load(array_for_input_test,\'../tests/harness/testcases/data/M4x4_1.txt\',-2,\'(int32, int32)\',99,shadow_array)',
         'lookup(join('            + dbq.build(a=['v1'],d=['i'],r=[(0,3)],i=1) + ',' + dbq.build(a=['v2'],d=['i'],r=[(0,3)],i='i') + '),' + defBuild + ')',
         'materialize('            + defBuild + ',1)',
         'max('                    + defBuild + ')',
         'merge(build_sparse('     + defSchema + ',1,i=j),build_sparse(' + dbq.schema(a=['attr2'])+ ',1,i=0))',
         'min('                    + defBuild + ')',
         'mstat()',
         'normalize('              + dbq.build(d=['i']) + ')',
         'old_unpack('             + defBuild + ',x)',
         'project(join(normalize(' + dbq.build(d=['i']) + '),normalize(' + dbq.build(a=['attr2'],d=['i']) + ')),attr1)',
         'quantile('               + dbq.build(d=['i'],r=[(0,999)]) + ',2)',
         'rank('                   + dbq.build(i='random()%5') + ',attr1,j)',
         'redimension(redim1, array_for_redimension_test),3', # test consume operator second option
         'reduce_distro(array_for_repart_test,3)',
         'regrid('                 + defBuild + ',2,2,avg(attr1))',
         'repart('                 + defBuild + ',array_for_repart_test)',
         'reshape('                + dbq.build(r=[(0,9)]) + ',array_for_reshape_test)',
         'reverse('                + defBuild + ')',
         'sample('                 + defBuild + ',0.33)',
         'save('                   + defBuild + ',\'test1.txt\')',
         'scan(array_for_reshape_test)',
         'setopt(\'mem-array-threshold\')',
         'sg('                     + defBuild + ',3)',
         'show(\''                 + defBuild + '\',\'afl\')',
         'slice('                  + defBuild + ',j,22)',
         'sort('                   + dbq.build(t=['int64'],i='random()%10') + ',attr1)',
         'splitarraytest('         + dbq.build(i='random()%10') + ')',
         'stdev('                  + dbq.build(i='random()%10') + ',attr1,j)',
         'store('                  + dbq.build(i='random()%10') + ',array_for_store_test)',
         'subarray('               + dbq.build(i='random()%10') + ',10,10,2,32)',
         'substitute('             + dbq.build(r=[(0,9)],i='iif(i=j,null,1)',n=True) + ',' + dbq.build(d=['i'],r=[(0,0)],c=[1],i=0) + ')',
         'sum('                    + dbq.build(t=['int64'],r=[(0,9)],i='random()%5') + ')',
         'thin('                   + dbq.build(i='random()%10')+ ',0,2,0,2)',
         'transpose('              + dbq.build(r=[(0,19),(0,29)],i='random()%10') + ')',
         'unpack('                 + dbq.build(r=[(0,9)],i=1)+ ',j)',
         'var('                    + defBuild + ',attr1)',
         'variable_window('        + defBuild + ',i,2,6,sum(attr1))',
         'versions(array_for_store_test)',
         'window('                 + defBuild + ',2,11,4,13,min(attr1))',
         'xgrid('                  + dbq.build(r=[(0,3)]) + ',2,2)'
         ]
    return OP_TEST_QUERIES
# Test case class.
class igor1TestCase(testCase):
    #-------------------------------------------------------------------------
    # Constructor:
    def __init__(self):
        super(igor1TestCase,self).__init__() # Important: call the superclass constructor.
        self.registerCleaner(ArrayCleaner()) # Register the cleaner class to remove arrays
                                             # created by this test.
        try:
            dataPath = os.path.join(
                os.environ['SCIDB_DATA_PATH'],
                '000',
                '0'
                )
            # Register the cleaner that will remove any data files created by this test.
            self.registerCleaner(DataDirCleaner(dataPath))
        except:
            pass # Somehow, we could not get to the scidb data folder: 
                 # we will leave some junk files in there.
            
        self.__iquery = scidbIquery() # Iquery wrapper class.
        self.exitCode = 0 # Exit code for test harness
        
    #-------------------------------------------------------------------------
    # runQueries: executes a list of queries
    def runQueries(self,queries,stopOnError=False):
        exitStatus = 0
        for q in queries:
            queryException = False
            queryOk = False
            startTime = time.time()
            
            try:
                exitCode,\
                stdoutData,\
                stderrData = self.__iquery.runQuery(q)
                queryOk = True
            except Exception as e:
                queryException = True
                queryOk = False
            endTime = time.time()
            queryTime = time.strftime('%H:%M:%S',time.gmtime(endTime-startTime))

            if ((exitCode != 0) or not (SUCCESS_MSG in stdoutData)):
                msg =  'Error during query: {0}'.format(q)
                if (queryException):
                    msg =  'Exception during query: {0}'.format(q)
                print msg
                print stdoutData
                print stderrData
                exitStatus = 1
                queryOk = False
                
            if ((not queryOk) and stopOnError):
                break
                
            if (queryOk):
                print 'Time = {0}, sucess: {1}'.format(queryTime,q)
        return exitStatus
    #-------------------------------------------------------------------------
    # test: main entry point into the test.
    # Important: this function gets called by the superclass function runTest.
    # RunTest wraps this function in a "try: except: finally:" clause to
    # ensure the cleanup function is called.  Cleanup performs all of the 
    # actions of the registered cleaner classes (see constructor).
    def test(self):
        
        # Default build query and schema "roller": makes the strings.
        # The default build query (called as is - dbq.build()), will produce this:
        #  'build(<attr1:double>[i=0:9,4,0,j=0:9,4,0],random())'
        # The default schema (if called as is - dbq.schema()), will produce this:
        # '<attr1:double>[i=0:9,4,0,j=0:9,4,0]'
        # See definition of the class on explanation of parameters.
        dbq = defaultBuildQuery()
        
        # Execute all of the array creation queries first.
        self.exitCode = self.runQueries(getArrayCreateQueries(dbq),stopOnError=True)
        
        if (self.exitCode != 0):
            print 'Cannot proceed: array creation queries did not complete successfully!'
            return
        
        # Now, run the main list of consume queries.
        self.exitCode = self.runQueries([CONSUME_QUERY.replace('<query>',x) for x in getOpTestQueries(dbq)])
        
        # Load some libraries.
        self.exitCode += self.runQueries(loadLibQueries(),stopOnError=True)

        # Run a few extra queries from the additional libraries.
        self.exitCode += self.runQueries([CONSUME_QUERY.replace('<query>',x) for x in getPluginOpTestQueries(dbq)])

#-----------------------------------------------------------------------------
# Main entry point into the script.
if __name__ == '__main__':
    it = igor1TestCase() # Create the test case object
    it.runTest() # Call the method that wraps the test with setup and cleanup.
    print 'All done.'
    if (it.exitCode != 0):
        sys.exit(it.exitCode) # Signal to test harness pass or failure.
    
