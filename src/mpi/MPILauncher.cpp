/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2012 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation version 3 of the License.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the GNU General Public License for the complete license terms.
*
* You should have received a copy of the GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/>.
*
* END_COPYRIGHT
*/

#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
#include <map>
#include <vector>
#include <sstream>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_array.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <log4cxx/logger.h>
#include <util/Thread.h>
#include <util/Network.h>
#include <util/FileIO.h>
#include <system/Cluster.h>
#include <query/Query.h>
#include <mpi/MPILauncher.h>
#include <mpi/MPIManager.h>
#include <mpi/MPIUtils.h>
#include <util/shm/SharedMemoryIpc.h>

using namespace std;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.mpi"));

#if defined(NDEBUG)
static const bool DBG = false;
#else
static const bool DBG = true;
#endif

MpiLauncher::MpiLauncher(uint64_t launchId, const boost::shared_ptr<Query>& q)
  : _pid(0),
    _status(0),
    _queryId(q->getQueryID()),
    _launchId(launchId),
    _query(q),
    _waiting(false),
    _inError(false),
    _MPI_LAUNCHER_KILL_TIMEOUT(scidb::getLivenessTimeout())
{
}

MpiLauncher::MpiLauncher(uint64_t launchId, const boost::shared_ptr<Query>& q, uint32_t timeout)
  : _pid(0),
    _status(0),
    _queryId(q->getQueryID()),
    _launchId(launchId),
    _query(q),
    _waiting(false),
    _inError(false),
    _MPI_LAUNCHER_KILL_TIMEOUT(timeout)
{
}

void MpiLauncher::getPids(vector<pid_t>& pids)
{
    ScopedMutexLock lock(_mutex);
    if (_pid <= 1) {
        throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__)
            << " MPI launcher is not running";
    }
    pids.push_back(_pid);
}

void MpiLauncher::launch(const vector<string>& slaveArgs,
                         const boost::shared_ptr<const InstanceMembership>& membership,
                         const size_t maxSlaves)
{
    vector<string> args;
    {
        ScopedMutexLock lock(_mutex);
        if (_pid != 0 || _waiting) {
            throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__)
                << " MPI launcher is already running";
        }
        boost::shared_ptr<Query> query = _query.lock();
        Query::validateQueryPtr(query);

        buildArgs(args, slaveArgs, membership, query, maxSlaves);
    }
    pid_t pid = fork();

    if (pid < 0) {
        // error
        int err = errno;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
               << "fork" << pid << err <<"");

    } else if (pid > 0) {
        // parent
        ScopedMutexLock lock(_mutex);
        if (_pid != 0 || _waiting) {
            throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__)
                << " MPI launcher is corrupted after launch";
        }
        _pid = pid;
        LOG4CXX_DEBUG(logger, "MPI launcher process spawned, pid="<<_pid);
        return;

    }  else {
        // child
        becomeProcGroupLeader();
        recordPids();
        setupLogging();

        if (DBG) {
            std::cerr << "LAUNCHER pid="<<getpid()
             << ", pgid="<< ::getpgid(0)
             << ", ppid="<< ::getppid()<<std::endl;
        }

        closeFds();
        boost::scoped_array<const char*> argv(new const char*[args.size()+1]);
        initExecArgs(args, argv);
        const char *path = argv[0];

        if (DBG) {
            std::cerr << "LAUNCHER pid="<<::getpid()<<" args for "<<path<<" are ready" << std::endl;
            for (size_t i=0; i<args.size(); ++i) {
                const char * arg = argv[i];
                if (!arg) break;
                cerr << "LAUNCHER arg["<<i<<"] = "<< argv[i] << std::endl;
            }
        }

        int rc = ::execv(path, const_cast<char* const*>(argv.get()));

        assert(rc == -1);
        rc=rc; // avoid compiler warning

        perror("LAUNCHER execv");
        _exit(1);
    }
    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE);
}

bool MpiLauncher::isRunning()
{
    pid_t pid=0;
    int status=0;
    {
        ScopedMutexLock lock(_mutex);
        if (_pid<=0) { return false; }
        pid = _pid;
    }

    const bool doNotWait=true;
    bool rc = waitForExit(pid, &status, doNotWait);

    if (!rc) {
        return true;
    }

    ScopedMutexLock lock(_mutex);
    _pid = -pid;
    _status = status;

    return false;
}

void MpiLauncher::destroy(bool force)
{
    pid_t pid=0;
    int status=0;
    string pidFile;
    {
        ScopedMutexLock lock(_mutex);
        if (_pid == 0 || _waiting) {
            throw (InvalidStateException(REL_FILE, __FUNCTION__, __LINE__)
                   << " MPI launcher already destroyed");
        }
        _waiting = true;
        pid = _pid;
        status = _status;
        pidFile = mpi::getLauncherPidFile(_installPath, _queryId, _launchId);

        if (pid > 0) {
            if (!force) {
                scheduleKillTimer();
            } else { // kill right away
                boost::shared_ptr<boost::asio::deadline_timer> dummyTimer;
                boost::system::error_code dummyErr;
                handleKillTimeout(dummyTimer, dummyErr);
            }
        }
        if (force) {
            _inError=true;
        }
    }
    if (pid < 0) {
        completeLaunch(-pid, pidFile, status);
        return;
    }
    bool rc = waitForExit(pid,&status);
    assert(rc); rc=rc;
    {
        ScopedMutexLock lock(_mutex);
        if (!_waiting || pid != _pid) {
             throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__)
                 << " MPI launcher is corrupted after collecting process exit code";
         }

        _pid = -pid;
        _status = status;

        if (_killTimer) {
            size_t n = _killTimer->cancel();
            assert(n<2); n=n;
        }
    }
    completeLaunch(pid, pidFile, status);
}

void MpiLauncher::completeLaunch(pid_t pid, const std::string& pidFile, int status)
{
    // rm args file
    boost::scoped_ptr<SharedMemoryIpc> shmIpc(mpi::newSharedMemoryIpc(_ipcName));
    shmIpc->remove();
    shmIpc.reset();

    // rm pid file
    scidb::File::remove(pidFile.c_str(), false);

    // rm log file
    if (!logger->isTraceEnabled() && !_inError) {
        string logFileName = mpi::getLauncherLogFile(_installPath, _queryId, _launchId);
        scidb::File::remove(logFileName.c_str(), false);
    }

    if (WIFSIGNALED(status)) {
        LOG4CXX_ERROR(logger, "SciDB MPI launcher (pid="<<pid<<") terminated by signal = "
                      << WTERMSIG(status) << (WCOREDUMP(status)? ", core dumped" : ""));
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "MPI launcher process";

    } else if (WIFEXITED(status)) {
        int rc = WEXITSTATUS(status);
        if (rc != 0) {
            LOG4CXX_ERROR(logger, "SciDB MPI launcher (pid="<<_pid<<") exited with status = " << rc);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "MPI launcher process";

        } else {
            LOG4CXX_DEBUG(logger, "SciDB MPI launcher (pid="<<_pid<<") exited with status = " << rc);
            return;
        }
    }
    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE);
}

 void MpiLauncher::handleKillTimeout(boost::shared_ptr<boost::asio::deadline_timer>& killTimer,
                                     const boost::system::error_code& error)
 {
     ScopedMutexLock lock(_mutex);

     if (error == boost::asio::error::operation_aborted) {
         assert(_pid < 0);
         LOG4CXX_TRACE(logger, " MPI launcher kill timer cancelled");
         return;
     }
     if (error) {
         assert(false);
         LOG4CXX_WARN(logger, "MPI launcher kill timer encountered error"<<error);
     }
     if (_pid <= 0) {
         LOG4CXX_WARN(logger, "MPI launcher kill timer cannot kill pid="<<_pid);
         return;
     }
     if (!_waiting) {
         assert(false);
         LOG4CXX_ERROR(logger, "MPI launcher kill timer cannot kill pid="<<_pid);
         throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__)
             << " MPI launcher process cannot be killed";
     }
     LOG4CXX_WARN(logger, "MPI launcher is about to kill group pid="<<_pid);

     // kill launcher's proc group
     MpiErrorHandler::killProc(_installPath, -_pid);
 }

static void validateLauncherArg(const std::string& arg)
{
    const char *notAllowed = " \n";
    if (arg.find_first_of(notAllowed) != string::npos) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT)
               << (string("MPI launcher argument with whitespace: ")+arg));
    }
}

/// @todo XXX tigor: move command args into a file
void MpiLauncher::buildArgs(vector<string>& args,
                            const vector<string>& slaveArgs,
                            const boost::shared_ptr<const InstanceMembership>& membership,
                            const boost::shared_ptr<Query>& query,
                            const size_t maxSlaves)
{
    for (vector<string>::const_iterator iter=slaveArgs.begin();
         iter!=slaveArgs.end(); ++iter) {
        validateLauncherArg(*iter);
    }

    const Instances& instances = membership->getInstanceConfigs();

    map<InstanceID,const InstanceDesc*> sortedInstances;
    getSortedInstances(sortedInstances, instances, query);

    ostringstream buf;
    const string clusterUuid = Cluster::getInstance()->getUuid();
    buf << _queryId;
    const string queryId  = buf.str();
    buf.str("");
    buf << _launchId;
    const string launchId = buf.str();

    // preallocate memory
    const size_t ARGS_PER_INSTANCE = 16;
    const size_t ARGS_PER_LAUNCH = 4;
    const size_t MPI_PREFIX_CORRECTION = 2;
    size_t totalArgsNum = ARGS_PER_LAUNCH +
       (ARGS_PER_INSTANCE+slaveArgs.size()) * std::min(maxSlaves, sortedInstances.size()) -
       MPI_PREFIX_CORRECTION;
    args.clear();
    args.reserve(totalArgsNum);
    InstanceID myId = Cluster::getInstance()->getLocalInstanceId();

    args.push_back(string("")); //place holder for the binary
    args.push_back(string("--verbose"));
    args.push_back(string("--tag-output"));
    args.push_back(string("--timestamp-output"));

    // first, find my own install path, and add coordinator arguments
    for (map<InstanceID,const InstanceDesc*>::const_iterator i = sortedInstances.begin();
         i !=  sortedInstances.end(); ++i) {

        assert(i->first<sortedInstances.size());
        const InstanceDesc* desc = i->second;
        assert(desc);
        InstanceID currId = desc->getInstanceId();
        assert(currId < instances.size());

        if (currId != myId) {
            continue;
        }
        assert(args[0].empty());
        const string& installPath = desc->getPath();
        _installPath = installPath;
        args[0] = MpiManager::getLauncherBinFile(installPath);

        addPerInstanceArgs(myId, desc, clusterUuid, queryId,
                           launchId, slaveArgs, args);
    }

    assert(!args[0].empty());

    // second, loop again to actually start all the instances
    size_t count = 1;
    for (map<InstanceID,const InstanceDesc*>::const_iterator i = sortedInstances.begin();
         i !=  sortedInstances.end() && count<maxSlaves; ++i,++count) {

        const InstanceDesc* desc = i->second;
        InstanceID currId = desc->getInstanceId();

        if (currId == myId) {
            --count;
            continue;
        }
        addPerInstanceArgs(myId, desc, clusterUuid, queryId,
                           launchId, slaveArgs, args);
    }
    int64_t shmSize(0);
    vector<string>::iterator iter=args.begin();
    iter += ARGS_PER_LAUNCH;

    // compute arguments size
    const size_t DELIM_SIZE=sizeof('\n');
    for (; iter!=args.end(); ++iter) {
        string& arg = (*iter);
        shmSize += (arg.size()+DELIM_SIZE);
    }

    LOG4CXX_TRACE(logger, "MPI launcher arguments size = " << shmSize);

    // Create shared memory to pass the arguments to the launcher
    _ipcName = mpi::getIpcName(_installPath, clusterUuid, queryId, myId, launchId) + ".launch_args";

    LOG4CXX_TRACE(logger, "MPI launcher arguments ipc = " << _ipcName);

    boost::scoped_ptr<SharedMemoryIpc> shmIpc(mpi::newSharedMemoryIpc(_ipcName));
    char* ptr(NULL);
    try {
        shmIpc->create(SharedMemoryIpc::RDWR);
        shmIpc->truncate(shmSize);
        ptr = reinterpret_cast<char*>(shmIpc->get());
    } catch(scidb::SharedMemoryIpc::SystemErrorException& e) {
        LOG4CXX_ERROR(logger, "Cannot map shared memory: " << e.what());
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "shared_memory_mmap");
    } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
        LOG4CXX_ERROR(logger, "Unexpected error while mapping shared memory: " << e.what());
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << e.what());
    }
    assert(ptr);

    size_t off = 0;
    iter=args.begin();
    iter += ARGS_PER_LAUNCH;
    for (; iter!=args.end(); ++iter) {
        string& arg = (*iter);

        if (off == 0) {
        } else if (arg == "-H") {
            *(ptr+off) = '\n';
            ++off;
        } else {
            *(ptr+off) = ' ';
            ++off;
        }
         memcpy((ptr+off), arg.data(), arg.size());
         off += arg.size();
         arg.clear();
    }
    *(ptr+off) = '\n';
    ++off;
    assert(static_cast<int64_t>(off) <= shmSize);
    shmIpc->close();
    shmIpc->flush();

    assert(args.size() >= ARGS_PER_LAUNCH+2);
    args[ARGS_PER_LAUNCH+0] = "--app";
    args[ARGS_PER_LAUNCH+1] = mpi::getIpcFile(_installPath,_ipcName);
    args.resize(ARGS_PER_LAUNCH+2);
}

void MpiLauncher::addPerInstanceArgs(const InstanceID myId, const InstanceDesc* desc,
                                     const string& clusterUuid,
                                     const string& queryId,
                                     const string& launchId,
                                     const vector<string>& slaveArgs, vector<string>& args)
{
    InstanceID currId = desc->getInstanceId();

    ostringstream instanceIdStr;
    instanceIdStr << currId;

    const string& host = desc->getHost();
    const string& installPath = desc->getPath();

    ostringstream portStr;
    portStr << desc->getPort();

    // mpirun command line:
    // [":", "-H", <IP>, "-np", <#>, "-wd", <path>, "--prefix", <path>, "-x", "LD_LIBRARY_PATH"]*
    validateLauncherArg(host);
    args.push_back("-H");
    args.push_back(host);

    args.push_back("-np");
    args.push_back("1");

    validateLauncherArg(installPath);
    args.push_back("-wd");
    args.push_back(installPath);

    if (currId != myId) {
        // XXX NOTE: --prefix is not appended for this instance (the coordinator)
        // and this instance's arguments go first in the argument list because of
        // of an apparent bug in mpirun handling of --prefix
        const string mpiDir = MpiManager::getMpiDir(installPath);
        validateLauncherArg(mpiDir);
        args.push_back("--prefix");
        args.push_back(mpiDir);
    }
    args.push_back("-x");
    args.push_back("LD_LIBRARY_PATH");

    const string slaveBinFile = mpi::getSlaveBinFile(installPath);
    validateLauncherArg(slaveBinFile);
    args.push_back(slaveBinFile);

    // slave args
    args.push_back(clusterUuid);
    args.push_back(queryId);
    args.push_back(instanceIdStr.str());
    args.push_back(launchId);
    args.push_back(portStr.str());

    args.insert(args.end(), slaveArgs.begin(), slaveArgs.end());
}

void MpiLauncher::getSortedInstances(map<InstanceID,const InstanceDesc*>& sortedInstances,
                                     const Instances& instances,
                                     const boost::shared_ptr<Query>& query)
{
    for (Instances::const_iterator i = instances.begin(); i != instances.end(); ++i) {
        InstanceID id = i->getInstanceId();
        try {
            // lid should be equal mpi rank
            InstanceID lid = query->mapPhysicalToLogical(id);
            sortedInstances[lid] = &(*i);
        } catch(SystemException& e) {
            if (e.getLongErrorCode() != SCIDB_LE_INSTANCE_OFFLINE) {
                throw;
            }
        }
    }
    assert(sortedInstances.size() == query->getInstancesCount());
}

void MpiLauncher::closeFds()
{
    //XXX TODO: move to Sysinfo
    long maxfd = ::sysconf(_SC_OPEN_MAX);
    if (maxfd<2) {
        maxfd = 1024;
    }

    cerr << "LAUNCHER: maxfd = " << maxfd << endl;

    // close all fds except for stderr,stdout
    int rc = scidb::File::closeFd(0); //stdin
    rc=rc; // avoid compiler warning
    for (long fd=3; fd <= maxfd ; ++fd) {
        rc = scidb::File::closeFd(fd);
        rc=rc; // avoid compiler warning
    }
}

void MpiLauncher::becomeProcGroupLeader()
{
    if (setpgid(0,0) != 0) {
        perror("setpgid");
        _exit(1);
    }
}

void MpiLauncher::setupLogging()
{
    std::string path =
        mpi::getLauncherLogFile(_installPath, _queryId, _launchId);
    mpi::connectStdIoToLog(path);
}

void MpiLauncher::recordPids()
{
    assert(!_installPath.empty());
    string path =
        mpi::getLauncherPidFile(_installPath, _queryId, _launchId);
    mpi::recordPids(path);
}

void MpiLauncher::initExecArgs(const vector<string>& args,
                               boost::scoped_array<const char*>& argv)
{
     size_t argsSize = args.size();
     if (argsSize<1) {
         cerr << "LAUNCHER: initExecArgs failed to get args:" << argsSize << endl;
         _exit(1);
     }
     for (size_t i=0; i < argsSize; ++i) {
         argv[i] = args[i].c_str();
     }
     argv[argsSize] = NULL;
 }

void MpiLauncher::scheduleKillTimer()
{
    // this->_mutex must be locked
    assert (_pid > 1);
    assert(!_killTimer);
    _killTimer = shared_ptr<boost::asio::deadline_timer>(new boost::asio::deadline_timer(getIOService()));
    int rc = _killTimer->expires_from_now(posix_time::seconds(_MPI_LAUNCHER_KILL_TIMEOUT));
    if (rc != 0) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
               << "boost::asio::expires_from_now" << rc << rc << _MPI_LAUNCHER_KILL_TIMEOUT);
    }
    _killTimer->async_wait(boost::bind(&MpiLauncher::handleKillTimeout,
                                      shared_from_this(), _killTimer,
                                      boost::asio::placeholders::error));
}

bool MpiLauncher::waitForExit(pid_t pid, int *status, bool noWait)
{
    int opts = 0;
    if (noWait) {
        opts = WNOHANG;
    }
    while(true) {

        pid_t rc = ::waitpid(pid,status,opts);

        if ((rc == -1) && (errno==EINTR)) {
            continue;
        }
        if (rc == 0 && noWait) {
            return false;
        }
        if ((rc <= 0) || (rc != pid)) {
            int err = errno;
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
                   << "wait" << rc << err << pid);
        }
        return true;
    }
    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE);
    return false;
}

} //namespace
