#!/usr/bin/python

import sys
import argparse
import subprocess

CMD_BUILD = 0
CMD_INIT = 1
CMD_UPDATE = 2
CMD_LOGIN = 3

DISTROS=['precise-amd64', 'oneiric-amd64', 'natty-amd64']

TGZ_DIR='/var/cache/pbuilder'

MIRROR='deb http://archive.ubuntu.com/ubuntu/ %s restricted main multiverse universe'

BUILD_RESULT='/tmp/scidb_build'

def main():
    parser = argparse.ArgumentParser()
    groupCmd = parser.add_mutually_exclusive_group()
    groupCmd.add_argument('--build', dest='command', action='store_const', const=CMD_BUILD, help='Build SciDB on selected platforms (by default)')
    groupCmd.add_argument('--init', dest='command', action='store_const', const=CMD_INIT, help='Initialize pbuilder tgzs')
    groupCmd.add_argument('--update', dest='command', action='store_const', const=CMD_UPDATE, help='Update pbuilder tgzs')
    groupCmd.add_argument('--login', dest='command', action='store_const', const=CMD_LOGIN, help='Login to pbuilder chroot')
    parser.add_argument('--distro', dest='distro', type=str, nargs='+', help='Target distro names')
    parser.add_argument('--dsc', dest='dsc', type=str, help='Dsc file for building')
    parser.add_argument('-j', '--jobs', dest='build_jobs', type=int, help='Number of build jobs')

    parser.set_defaults(
      command=CMD_BUILD,
      distro=DISTROS,
      build_jobs=1)
    args = vars(parser.parse_args())

    CMD = args['command']
    DISTRO = args['distro']
    DSC = args['dsc']
    BUILD_OPTS = ' -j%i' % args['build_jobs']

    print("Will " + ['build SciDB for', 'init chroot for', 'update chroot for', 'login into '][CMD] + " next target(s): " + str(DISTRO))

    if CMD == CMD_LOGIN:
        if len(DISTRO) != 1:
            print 'Exactly one target distro should be specified'
            exit(1)
        login(DISTRO[0])
        exit(0)

    for distro in DISTRO:
        if CMD == CMD_INIT:
            init(distro)
        elif CMD == CMD_UPDATE:
            update(distro)
        elif CMD == CMD_BUILD:
            if not DSC:
                print '.dsc file is not specified'
                exit(1)
            build(distro, DSC, BUILD_OPTS)

def init(distro):
    tgz = TGZ_DIR+'/'+distro+'.tgz'
    (dist, arch) = distro.split('-')
    mirror = MIRROR % dist
    pbargs = ['pbuilder', '--create',
        '--basetgz', tgz,
        '--architecture', arch,
        '--othermirror', mirror,
        '--distribution', dist,
        '--override-config']
    print "Initializing %s from %s" % (tgz, mirror)
    subprocess.Popen(pbargs).wait()

def update(distro):
    tgz = TGZ_DIR+'/'+distro+'.tgz'
    (dist, arch) = distro.split('-')
    mirror = MIRROR % dist
    pbargs = ['pbuilder', '--update',
        '--basetgz', tgz,
        '--distribution', dist,
        '--othermirror', mirror,
        '--architecture', arch,
        '--override-config']
    print "Updating %s from %s" % (tgz, mirror)
    subprocess.Popen(pbargs).wait()

def build(distro, dsc, buildopts):
    tgz = TGZ_DIR+'/'+distro+'.tgz'
    (dist, arch) = distro.split('-')
    mirror = MIRROR % dist
    buildresult = BUILD_RESULT+'/'+distro
    pbargs = ['pbuilder', '--build',
        '--basetgz', tgz,
        '--distribution', dist,
        '--othermirror', mirror,
        '--architecture', arch,
        '--buildresult', buildresult,
        '--debbuildopts', buildopts,
        '--override-config',
        dsc]
    print "Building %s in %s" % (dsc, tgz)
    subprocess.Popen(pbargs).wait()

def login(distro):
    tgz = TGZ_DIR+'/'+distro+'.tgz'
    (dist, arch) = distro.split('-')
    mirror = MIRROR % dist
    pbargs = ['pbuilder', '--login',
        '--basetgz', tgz,
        '--distribution', dist,
        '--othermirror', mirror,
        '--architecture', arch,
        '--override-config']
    print "Logging into %s" % tgz
    subprocess.Popen(pbargs).wait()

if __name__ == '__main__':
    main()
