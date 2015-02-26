#!/usr/bin/python
import sys
import CommandRunner
import BackupHelper
import BasicRestoreTests
import unittest
import os
import pwd

def main():
    q = [
        'iquery',
        '-ocsv',
        '-q',
        'SELECT * FROM sort(list(\'instances\'),instance_id)'
        ]
    cr = CommandRunner.CommandRunner()
    exits,outputs = cr.waitForProcesses(
        [cr.runSubProcess(q)],
        True
        )

    if (any(exits)):
        print 'Error: could not get scidb instance data!'
        errors = outputs[0][1]
        print errors
        sys.exit(1)
    lines = [line.strip() for line in outputs[0][0].split('\n')]
    lines = [line.replace('\'','') for line in lines if len(line) > 0]
    lines = lines[1:]
    hostList = sorted(
        [line.split(',') for line in lines],
        key=lambda x: int(x[2])
        )
    hosts = reduce( # Scidb hosts (machines)
        lambda x,y: x + y if y[0] not in x else x,
        [[x[0]] for x in hostList]
        )

    nInst = len(lines) / len(hosts)

    user = pwd.getpwuid(os.getuid())[0]

    cq = BackupHelper.CREATE_200x200_ARRAYS
    
    bkpFolder = r'/tmp/bkpTest'

    arrays = [a + str(i+1) for a in ['A','B','C'] for i in range(3)]

    bh = BackupHelper.BkpTestHelper(
        user,
        hosts,
        nInst,
        bkpFolder,
        arrays
        )

    bh.createArrays(arrays,[5,6,7,8],True)

    S1 = BasicRestoreTests.getParallelTestSuite(user,hosts,nInst,bh,cq)

    runner = unittest.TextTestRunner(verbosity=2)
    ret = map(runner.run,[S1])

    bh.removeArrays(arrays,True)

    if (
        any([len(x.errors) > 0 for x in ret]) or
        any([len(x.failures) > 0 for x in ret]) or
        any([x.testsRun <= 0 for x in ret])
            ):
        print 'FAIL'
        sys.exit(1)
    
if __name__ == '__main__':
    main()
