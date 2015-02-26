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

##############################################################################
class CommandRunner:

    def runSubProcess(self,cmd,si=None,so=subprocess.PIPE,se=subprocess.PIPE,useShell=False):
        localCmd = list(cmd)
        if (useShell):
            localCmd = ' '.join(localCmd)

        proc = subprocess.Popen(
            localCmd,
            stdin=si,
            stdout=so,
            stderr=se,
            shell=useShell
            )
        return proc
        
    def runSshCommand(self,user,host,cmd,si=subprocess.PIPE,so=subprocess.PIPE,se=subprocess.PIPE):       
        sshCmd = ['ssh', user + '@' + host]
        proc = self.runSubProcess(sshCmd,si,so,se)
        proc.stdin.write(' '.join(cmd) + '\n')
        proc.stdin.close()
        proc.stdin = None
        return proc
        
    def waitForProcesses(self,procs,outputs=False):
        outs = []
        outs = map(lambda p: p.communicate(),procs)
        exits = map(lambda p: p.returncode,procs)
        return exits,outs
        
    def runSubProcesses(self,cmds,si=None,so=subprocess.PIPE,se=subprocess.PIPE,useShell=False):
        return map(
            lambda cmd: self.runSubProcess(cmd,si,so,se,useShell),cmds
            )
            
    def runSshCommands(self,user,hosts,cmds,si=subprocess.PIPE,so=subprocess.PIPE,se=subprocess.PIPE):
        return map(
            lambda i: self.runSshCommand(user,hosts[i],cmds[i],si,so,se),range(len(hosts))
            )

