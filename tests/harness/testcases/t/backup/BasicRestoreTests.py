#!/usr/bin/python
#-------------------------------------------------------------------------------
# Imports:
import argparse
import sys
import os
import re
import types
import subprocess
import time
import itertools
import functools
import unittest

from CommandRunner import CommandRunner
import BackupHelper

BACKUP=os.path.join(os.environ['SCIDB_INSTALL_PATH'],'bin','scidb_backup.py')

allTests = [
    'test_text_parallel',
    'test_text_allVersions',
    'test_text_zip',
    'test_text_parallel_allVersions',
    'test_text_parallel_zip',
    'test_text_allVersions_zip',
    'test_text_parallel_allVersions_zip',
    'test_binary_parallel',
    'test_binary_allVersions',
    'test_binary_zip',
    'test_binary_parallel_allVersions',
    'test_binary_parallel_zip',
    'test_binary_allVersions_zip',
    'test_binary_parallel_allVersions_zip',
    'test_opaque_parallel',
    'test_opaque_allVersions',
    'test_opaque_zip',
    'test_opaque_parallel_allVersions',
    'test_opaque_parallel_zip',
    'test_opaque_allVersions_zip',
    'test_opaque_parallel_allVersions_zip',
    'test_text',
    'test_binary',
    'test_opaque'
    ]
parallelTests = [
    'test_text_parallel',
    'test_text_parallel_allVersions',
    'test_text_parallel_zip',
    'test_text_parallel_allVersions_zip',
    'test_binary_parallel',
    'test_binary_parallel_allVersions',
    'test_binary_parallel_zip',
    'test_binary_parallel_allVersions_zip',
    'test_opaque_parallel',
    'test_opaque_parallel_allVersions',
    'test_opaque_parallel_zip',
    'test_opaque_parallel_allVersions_zip'
    ]
def testWatchdog(testFunc):
    @functools.wraps(testFunc)
    def wrapper(obj):
        try:
            testFunc(obj)
        except AssertionError as e:
            print e
            obj.fail('Test failed!')
            raise e
    return wrapper
    
class RestoreTest(unittest.TestCase):
    _cmdRunner = CommandRunner()

    def __init__(self, *args, **kwargs):
        super(RestoreTest, self).__init__(*args, **kwargs)

    def setBackupHelper(self,bh):
        self.bkpHelper = bh

    def setUp(self):
        self.bkpHelper.removeBackup(BACKUP)
        self.bkpHelper.removeArrays(self.bkpHelper.getAllArrayNames())
        self.bkpHelper.reCreateArrays(self.bkpHelper.getAllArrayNames(),4)

    def tearDown(self):
        self.bkpHelper.removeBackup(BACKUP)
        self.bkpHelper.removeArrays(self.bkpHelper.getAllArrayNames())

    def setBackupHelper(self,bh):
        self.bkpHelper = bh

    def commonTestBody(
        self,
        toRestore,
        saveCmd,
        restoreCmd,
        versions=False
        ):
        sys.stderr.write('\n' + ' '.join(restoreCmd) + '\n')
        exits,outs=self._cmdRunner.waitForProcesses(
            [self._cmdRunner.runSubProcess(saveCmd,useShell=True)],
            True
            )
        self.bkpHelper.removeArrays(self.bkpHelper.getAllArrayNames())
        exits,outs = self._cmdRunner.waitForProcesses(
            [self._cmdRunner.runSubProcess(restoreCmd,useShell=True)],
            True
            )
        restored = self.bkpHelper.getDBArrays(versions)

        self.assertTrue(
            set(toRestore) == set(restored),
            'Restored arrays do not match initial conditions!'
            )
        self.assertTrue(
            self.bkpHelper.checkArrayData(toRestore),
            'Restored array data does not match initial conditions!'
            )

    @testWatchdog
    def test_text_parallel(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'text',
            '--parallel',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'text',
            '--parallel',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)
    @testWatchdog
    def test_text_allVersions(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored.
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'text',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'text',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_text_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'text',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'text',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

    @testWatchdog
    def test_text_parallel_allVersions(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored.
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'text',
            '--parallel',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'text',
            '--parallel',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_text_parallel_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'text',
            '--parallel',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'text',
            '--parallel',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

    @testWatchdog
    def test_text_allVersions_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored.
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'text',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'text',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_text_parallel_allVersions_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored.
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'text',
            '--parallel',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'text',
            '--parallel',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_binary_parallel(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'binary',
            '--parallel',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'binary',
            '--parallel',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

    @testWatchdog
    def test_binary_allVersions(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored.
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save','binary',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'binary',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_binary_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'binary',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'binary',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

    @testWatchdog
    def test_binary_parallel_allVersions(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored.
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'binary',
            '--parallel',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'binary',
            '--parallel',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_binary_parallel_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'binary',
            '--parallel',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'binary',
            '--parallel',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

    @testWatchdog
    def test_binary_allVersions_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored.
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'binary',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'binary',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_binary_parallel_allVersions_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored.
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'binary',
            '--parallel',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'binary',
            '--parallel',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_opaque_parallel(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'opaque',
            '--parallel',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'opaque',
            '--parallel',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

    @testWatchdog
    def test_opaque_allVersions(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored.
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save','opaque',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'opaque',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_opaque_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'opaque',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'opaque',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

    @testWatchdog
    def test_opaque_parallel_allVersions(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored.
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'opaque',
            '--parallel',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'opaque',
            '--parallel',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_opaque_parallel_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'opaque',
            '--parallel',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'opaque',
            '--parallel',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

    @testWatchdog
    def test_opaque_allVersions_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'opaque',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'opaque',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_opaque_parallel_allVersions_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'opaque',
            '--parallel',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'opaque',
            '--parallel',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_text(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'text',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'text',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

    @testWatchdog
    def test_binary(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'binary',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'binary',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

    @testWatchdog
    def test_opaque(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'opaque',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'opaque',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

def getTestSuite(tests,user,hosts,ninst,bh,q=None):

    suite = unittest.TestSuite()
    testList = map(RestoreTest,tests)
    map(lambda x: x.setBackupHelper(bh),testList)
    if (q is not None):
        map(lambda x: x.bkpHelper.setCreateQuery(q),testList)
    
    suite.addTests(testList)
    return suite

def getFullTestSuite(user,hosts,ninst,bh,q=None):
    return getTestSuite(allTests,user,hosts,ninst,bh,q=None)

def getParallelTestSuite(user,hosts,ninst,bh,q=None):
    return getTestSuite(parallelTests,user,hosts,ninst,bh,q=None)

def getNonParallelTestSuite(user,hosts,ninst,bh,q=None):
    return getTestSuite(
        set(allTests) - set(parallelTests),
        user,
        hosts,
        ninst,
        bh,
        q=None
        )
