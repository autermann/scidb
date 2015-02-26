#import os
import sys
#import functools
#import arrayCleaner
#import subprocess

class testCase(object):
    def __init__(self):
        self.__cleaners = []
        self.__cleanerClasses = set([])
    def registerCleaner(self,cleaner):
        if (not (cleaner.__class__.__name__ in self.__cleanerClasses)):
            self.__cleaners.append(cleaner)
            self.__cleanerClasses.add(cleaner.__class__.__name__)
        else:
            msg = 'Cleaner instance of class {0} is already registered!'
            print msg.format(cleaner.__class__.__name__)
    def cleanup(self):
        for cleaner in self.__cleaners:
            cleaner.pop()
    def setup(self):
        for cleaner in self.__cleaners:
            cleaner.push()
    def runTest(self):
        result = None
        try:
            self.setup()
            result = self.test()
        except:
            print 'got exception'
            raise
        finally:
            self.cleanup()
    def test(self):
        pass
    def disable(self):
        print 'Disabled.'
        sys.exit(0)
"""
class myTest(scidbTestCase):
    def test(self):
        iquery = os.environ['IQUERY']
        myQuery = 'create array igor_a2 <x:int64,y:string>[i=0:2,3,0];'
        cmd = [iquery, '-aq']
        cmd.append(myQuery)
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
            )
            
        # Wait for the query completion and grab the iquery "raw" output data:
        process.wait()
        stdoutData,stderrData = process.communicate(None)
        print '----------------------'
        print stdoutData
        print '----------------------'
        zz
def main():
    iquery = os.environ['IQUERY']
    mt = myTest()
    mt.registerCleaner(arrayCleaner.ArrayCleaner(iquery))
    mt.runTest()
    print 'All done!'
    
if __name__ == '__main__':
    main()
"""
