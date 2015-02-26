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

#include <sys/types.h>
#include <signal.h>
#include <ostream>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/bind.hpp>
#include <boost/make_shared.hpp>
#include <log4cxx/logger.h>
#include <system/Cluster.h>
#include <system/Utils.h>
#include <system/Config.h>
#include <util/Network.h>
#include <util/FileIO.h>
#include <mpi/MPIManager.h>
#include <mpi/MPISlaveProxy.h>
#include <mpi/MPIUtils.h>

using namespace std;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.mpi"));
MpiManager::MpiManager() : _isReady(false)
{
    const time_t MIN_CLEANUP_PERIOD = 5; //sec
    scidb::Scheduler::Work workItem = boost::bind(&MpiManager::initiateCleanup);
    _cleanupScheduler = scidb::getScheduler(workItem, MIN_CLEANUP_PERIOD);
}

void MpiManager::initiateCleanup()
{
    scidb::WorkQueue::WorkItem item = boost::bind(&MpiErrorHandler::cleanAll);
    boost::shared_ptr<scidb::WorkQueue> q = scidb::getWorkQueue();
    try {
        q->enqueue(item);
    } catch (const scidb::WorkQueue::OverflowException& e) {
        LOG4CXX_ERROR(logger, "Overflow exception from the work queue: "<<e.what());
        assert(false);
        // it is not fatal, try again
        MpiManager::getInstance()->cleanup();
    }
}
void MpiManager::cleanup()
{
    _cleanupScheduler->schedule();
}
void MpiManager::init()
{
    shared_ptr<scidb::NetworkMessageFactory> factory = scidb::getNetworkMessageFactory();

    shared_ptr<MpiMessageHandler> msgHandler(new MpiMessageHandler());
    factory->addMessageType(scidb::mtMpiSlaveHandshake,
                            boost::bind(&MpiMessageHandler::createMpiSlaveHandshake, msgHandler,_1),
                            boost::bind(&MpiMessageHandler::handleMpiSlaveHandshake, msgHandler,_1));

    factory->addMessageType(scidb::mtMpiSlaveResult,
                            boost::bind(&MpiMessageHandler::createMpiSlaveResult, msgHandler,_1),
                            boost::bind(&MpiMessageHandler::handleMpiSlaveResult, msgHandler,_1));

    scidb::NetworkMessageFactory::MessageHandler emptyHandler;
    factory->addMessageType(scidb::mtMpiSlaveCommand,
                            boost::bind(&MpiMessageHandler::createMpiSlaveCommand, msgHandler,_1),
                            emptyHandler);
}

/**
 * Creates symbolic links to mpirun, orted, and scidb_mpi_slave
 * @return true if links are created (or have already existed), false otherwise
 */
void MpiManager::initMpiLinks(const std::string& installPath,
                              const std::string& mpiPath,
                              const std::string& pluginPath)
{
    // mpi
    string target = getMpiDir(installPath);

    int rc = ::symlink(mpiPath.c_str(), target.c_str());
    if (rc !=0 && errno != EEXIST) {
        int err = errno;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
                       << "symlink" << rc << err << target);
    }

    // scidb_mpi_slave
    target = mpi::getSlaveBinFile(installPath);
    rc = ::symlink(mpi::getSlaveSourceBinFile(pluginPath).c_str(), target.c_str());
    if (rc !=0 && errno != EEXIST) {
        int err = errno;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
               << "symlink" << rc << err << target);
    }
}

void MpiManager::initMpi()
{
    const boost::shared_ptr<const InstanceMembership> membership =
        Cluster::getInstance()->getInstanceMembership();
    const std::string& installPath = MpiManager::getInstallPath(membership);

    std::string dir = mpi::getLogDir(installPath);
    if (!scidb::File::createDir(dir)) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_CREATE_DIRECTORY)
               << dir);
    }
    dir = mpi::getPidDir(installPath);
    if (!scidb::File::createDir(dir)) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_CREATE_DIRECTORY)
               << dir);
    }
    if (mpi::getShmIpcType() == SharedMemoryIpc::FILE_TYPE) {
        dir = mpi::getIpcDir(installPath);
        if (!scidb::File::createDir(dir)) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_CREATE_DIRECTORY)
                   << dir);
        }
    }
    initMpiLinks(installPath,
                 scidb::Config::getInstance()->getOption<string>(CONFIG_MPI_DIR),
                 scidb::Config::getInstance()->getOption<string>(CONFIG_PLUGINS));
    _isReady = true;
    // read the proc table and kill all the mpi procs started by the old process of this instance
    MpiErrorHandler::killAllMpiProcs();
    cleanup();
}

boost::shared_ptr<MpiOperatorContext> MpiManager::checkAndSetCtx(scidb::QueryID queryId,
                                                     const boost::shared_ptr<MpiOperatorContext>& ctx)
{
    LOG4CXX_TRACE(logger, "MpiManager::checkAndSetCtx: queryID="<<queryId << ", ctx=" << ctx.get());
    ScopedMutexLock lock(_mutex);
    if (!_isReady) {
        initMpi();
    }
    std::pair<ContextMap::iterator, bool> res = _ctxMap.insert(ContextMap::value_type(queryId, ctx));
    if (res.second) {
        cleanup();
    }
    return (*res.first).second;
}

bool MpiManager::removeCtx(scidb::QueryID queryId)
{
    LOG4CXX_DEBUG(logger, "MpiManager::removeCtx: queryID="<<queryId);
    ScopedMutexLock lock(_mutex);
    return (_ctxMap.erase(queryId) > 0);
}

void MpiMessageHandler::handleMpiSlaveHandshake(const boost::shared_ptr<scidb::MessageDescription>& messageDesc)
{
    handleMpiSlaveMessage<scidb_msg::MpiSlaveHandshake, scidb::mtMpiSlaveHandshake>(messageDesc);
}
void MpiMessageHandler::handleMpiSlaveResult(const boost::shared_ptr<scidb::MessageDescription>& messageDesc)
{
    handleMpiSlaveMessage<scidb_msg::MpiSlaveResult, scidb::mtMpiSlaveResult>(messageDesc);
}

void MpiMessageHandler::handleMpiSlaveDisconnect(uint64_t launchId,
                                                 const boost::shared_ptr<scidb::Query>& query)
{
    if (!query) {
        return;
    }
    boost::shared_ptr< scidb::ClientMessageDescription >
        cliMsg(new MpiOperatorContext::EofMessageDescription());
    try {
        MpiMessageHandler::processMessage(launchId, cliMsg, query);
    } catch (const scidb::Exception& e) {
        query->handleError(e.copy());
    }
}

void MpiMessageHandler::processMessage(uint64_t launchId,
                                       const boost::shared_ptr<scidb::ClientMessageDescription>& cliMsg,
                                       const shared_ptr<scidb::Query>& query)
{
    try {
        boost::shared_ptr<MpiOperatorContext> ctx =
            MpiManager::getInstance()->checkAndSetCtx(query->getQueryID(),
                                                      boost::shared_ptr<MpiOperatorContext>());
        query->validate();

        if (!ctx) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "MPI-based operator context not found");
        }
        LOG4CXX_DEBUG(logger, "MpiManager::processMessage: queryID="<< query->getQueryID()
                      << ", ctx = " << ctx.get()
                      << ", launchId = " << launchId
                      << ", messageType = " << cliMsg->getMessageType()
                      << ", queryID = " << cliMsg->getQueryId());
        ctx->pushMsg(launchId, cliMsg);
    } catch (const scidb::Exception& e) {
        LOG4CXX_ERROR(logger, "MpiManager::processMessage:"
                      " Error occurred in slave message handler: "
                      << e.what()
                      << ", messageType = " << cliMsg->getMessageType()
                      << ", queryID = " << cliMsg->getQueryId());
        throw;
    }
}

MpiErrorHandler::~MpiErrorHandler()
{}

void MpiErrorHandler::handleError(const boost::shared_ptr<Query>& query)
{
    MpiManager::getInstance()->removeCtx(query->getQueryID());

    MpiOperatorContext::LaunchCleaner cleaner =
        boost::bind(&MpiErrorHandler::clean, query->getQueryID(), _1, _2);
    _ctx->clear(cleaner);
}

void MpiErrorHandler::finalize(const boost::shared_ptr<Query>& query)
{
    MpiManager::getInstance()->removeCtx(query->getQueryID());

    uint64_t launchId = _ctx->getNextLaunchId()-1;
    boost::shared_ptr<MpiSlaveProxy> slave = _ctx->getSlave(launchId);
    if (slave) {
        slave->destroy();
    }
}

void MpiErrorHandler::clean(scidb::QueryID queryId, uint64_t launchId,
                            MpiOperatorContext::LaunchInfo* info)
{
    MpiOperatorContext::LaunchInfo::ShmIpcSet& ipcs = info->_shmIpcs;
    for (MpiOperatorContext::LaunchInfo::ShmIpcSet::const_iterator ipcIter = ipcs.begin();
         ipcIter != ipcs.end(); ++ipcIter) {
        const boost::shared_ptr<SharedMemoryIpc>& ipc = *ipcIter;

        ipc->close();
        if (!ipc->remove()) {
            LOG4CXX_ERROR(logger, "Failed to remove shared memory IPC = "
                          << ipc->getName());
        }
    }
    if (info->_slave) {
        try {
            info->_slave->destroy(true);
            assert(launchId == info->_slave->getLaunchId());
        } catch (std::exception& e) {
            LOG4CXX_ERROR(logger, "Failed to destroy slave for launch = "
                                  << info->_slave->getLaunchId()
                          << " ("<< queryId << ")"
                          << " because: "<<e.what());
        }
    }
    if (info->_launcher) {
        try {
            info->_launcher->destroy(true);
            assert(launchId == info->_launcher->getLaunchId());
        } catch (std::exception& e) {
            LOG4CXX_ERROR(logger, "Failed to destroy launcher for launch = "
                          << info->_launcher->getLaunchId()
                          << " ("<< queryId << ")"
                          << " because: "<<e.what());
        }
    }
}

static void getQueryId(std::set<scidb::QueryID>* queryIds,
                       const boost::shared_ptr<scidb::Query>& q)
{
    queryIds->insert(q->getQueryID());
    LOG4CXX_DEBUG(logger, "Next query ID: "<<q->getQueryID());
}

void MpiErrorHandler::processLauncherPidFile(const std::string& installPath,
                                             const std::string& fileName)
{
    const char *myFuncName = "MpiErrorHandler::processLauncherPidFile";
    std::vector<pid_t> pids;
    if (!mpi::readPids(fileName, pids)) {
        LOG4CXX_WARN(logger, myFuncName << ": cannot read pids for: "<<fileName);
        return;
    }
    assert(pids.size()==2);
    LOG4CXX_DEBUG(logger,  myFuncName << ": killing launcher pid group = " << pids[0]);

    // kill launcher's proc group
    if (!killProc(installPath, -pids[0])) {
        LOG4CXX_DEBUG(logger,  myFuncName << ": removing file: "<<fileName);
        scidb::File::remove(fileName.c_str(), false);
    }
}

void MpiErrorHandler::processSlavePidFile(const std::string& installPath,
                                          const std::string& fileName)
{
    const char *myFuncName = "MpiErrorHandler::processSlavePidFile";
    std::vector<pid_t> pids;
    if (!mpi::readPids(fileName, pids)) {
        LOG4CXX_WARN(logger, myFuncName << ": cannot read pids for: "<<fileName);
        return;
    }
    bool removeFile = true;

    // kill slave
    LOG4CXX_DEBUG(logger, myFuncName << ": killing slave pid = "<<pids[0]);
    if (killProc(installPath, pids[0])) {
        removeFile = false;
    }

    // kill slave parent (orted)
    LOG4CXX_DEBUG(logger, myFuncName << ": killing slave ppid ="<<pids[1]);
    if (killProc(installPath, pids[1])) {
        removeFile = false;
    }
    if (removeFile) {
        LOG4CXX_DEBUG(logger, myFuncName << ": removing file: "<<fileName);
        scidb::File::remove(fileName.c_str(), false);
    }
}

void MpiErrorHandler::cleanAll()
{
    const char *myFuncName = "MpiErrorHandler::cleanAll";
    // read ipc files
    const boost::shared_ptr<const InstanceMembership> membership =
        Cluster::getInstance()->getInstanceMembership();
    const std::string& installPath = MpiManager::getInstallPath(membership);

    std::string uuid = Cluster::getInstance()->getUuid();
    InstanceID myInstanceId = Cluster::getInstance()->getLocalInstanceId();

    std::string ipcDirName = mpi::getIpcDir(installPath);
    LOG4CXX_TRACE(logger, myFuncName << ": SHM directory: "<< ipcDirName);

    // read shared memory ipc files
    std::list<std::string> ipcFiles;
    try {
        scidb::File::readDir(ipcDirName.c_str(), ipcFiles);
    } catch (const scidb::SystemException& e) {
        LOG4CXX_WARN(logger, myFuncName << ": unable to read: "
                     << ipcDirName <<" because: "<<e.what());
        if (e.getLongErrorCode() != scidb::SCIDB_LE_SYSCALL_ERROR) {
            throw;
        }
        ipcFiles.clear();
    }

    // read pid files
    std::string pidDirName = mpi::getPidDir(installPath);
    LOG4CXX_TRACE(logger, myFuncName << ": PID directory: "<< pidDirName);

    std::list<std::string> pidFiles;
    try {
        scidb::File::readDir(pidDirName.c_str(), pidFiles);
    } catch (const scidb::SystemException& e) {
        LOG4CXX_WARN(logger, myFuncName << ": unable to read: "<< pidDirName
                     <<" because: "<<e.what());
        if (e.getLongErrorCode() != scidb::SCIDB_LE_SYSCALL_ERROR) {
            throw;
        }
        pidFiles.clear();
    }

    // collect query IDs AFTER reading the files
    std::set<QueryID> queryIds;
    boost::function<void (const boost::shared_ptr<scidb::Query>&)> f;
    f = boost::bind(&getQueryId, &queryIds, _1);
    scidb::Query::listQueries(f);

    // identify dead shm objects
    for (std::list<std::string>::iterator iter = ipcFiles.begin();
         iter != ipcFiles.end(); ++iter) {
        const std::string& fileName = *iter;
        LOG4CXX_DEBUG(logger, myFuncName << ": next SHM object: "<<fileName);
        uint64_t queryId=0;
        uint64_t launchId=0;
        uint64_t instanceId=myInstanceId;

        if (!mpi::parseSharedMemoryIpcName(fileName,
                                           uuid,
                                           instanceId,
                                           queryId,
                                           launchId)) {
            // ignore file with unknown name
            continue;
        }

        if (instanceId != myInstanceId
            || queryIds.find(queryId) != queryIds.end()) {
            // ignore live file
            continue;
        }

        const string ipcFileName = ipcDirName+"/"+fileName;

        LOG4CXX_DEBUG(logger, myFuncName << ": removing SHM object: "<< ipcFileName);

        if (File::remove(ipcFileName.c_str(), false) != 0) {
            LOG4CXX_ERROR(logger, myFuncName << ": failed to remove SHM object: "<< ipcFileName);
        }
    }

    // identify dead pid files
    for (std::list<std::string>::iterator iter = pidFiles.begin();
         iter != pidFiles.end(); ++iter) {
        const std::string& fileName = *iter;
        LOG4CXX_DEBUG(logger, myFuncName << ": next pid object: "<<fileName);
        QueryID queryId=0;
        uint64_t launchId=0;
        int n=0;
        int rc = ::sscanf(fileName.c_str(), "%"PRIu64".%"PRIu64".%n", &queryId, &launchId, &n);
        if (rc == EOF || rc < 2) {
            // ignore file with unknown name
            continue;
        }
        if (queryIds.find(queryId) != queryIds.end()) {
            // ignore live file
            continue;
        }
        assert(n >= 0);
        size_t len = fileName.size();
        assert(static_cast<size_t>(n) <= len);

        if (fileName.compare(n, len-n, mpi::LAUNCHER_BIN)==0) {
            processLauncherPidFile(installPath, pidDirName+"/"+fileName);
        } else if (fileName.compare(n, len-n, mpi::SLAVE_BIN)==0) {
            processSlavePidFile(installPath, pidDirName+"/"+fileName);
        }
    }
}

void MpiErrorHandler::killAllMpiProcs()
{
    const char *myFuncName = "MpiErrorHandler::killAll";
    // read ipc files
    const boost::shared_ptr<const InstanceMembership> membership =
        Cluster::getInstance()->getInstanceMembership();
    const std::string& installPath = MpiManager::getInstallPath(membership);

    // read pid files
    std::string procDirName = mpi::getProcDirName();

    std::list<std::string> pidFiles;
    try {
        scidb::File::readDir(procDirName.c_str(), pidFiles);
    } catch (const scidb::SystemException& e) {
        LOG4CXX_WARN(logger, myFuncName << ": unable to read: "
                     << procDirName <<" because: "<<e.what());
        if (e.getLongErrorCode() != scidb::SCIDB_LE_SYSCALL_ERROR) {
            throw;
        }
        pidFiles.clear();
    }
    // identify dead pid files
    for (std::list<std::string>::iterator iter = pidFiles.begin();
         iter != pidFiles.end(); ++iter) {
        const std::string& fileName = *iter;
        LOG4CXX_TRACE(logger, myFuncName << ": next pid object: "<<fileName);
        pid_t pid=0;
        int rc = ::sscanf(fileName.c_str(), "%d", &pid);
        if (rc == EOF || rc < 1) {
            // ignore file with unknown name
            continue;
        }
        assert(pid >= 1);
        killProc(installPath, pid);
    }
}

bool MpiErrorHandler::killProc(const std::string& installPath, pid_t pid)
{
    const char *myFuncName = "MpiErrorHandler::processProcFile";
    bool doesProcExist = false;
    if (pid > 0) {
        stringstream ss;
        ss<<pid;
        const std::string pidFileName = ss.str();
        std::string procName;
        if (!mpi::readProcName(pidFileName, procName)) {
            LOG4CXX_WARN(logger, myFuncName << ": cannot read:" << pidFileName);
            return doesProcExist;
        }
        if (!MpiManager::canRecognizeProc(installPath, procName)) {
            return doesProcExist;
        }
        LOG4CXX_DEBUG(logger, myFuncName << ": killing "<< procName << ", pid ="<<pid);
    } else {
        LOG4CXX_DEBUG(logger, myFuncName << ": killing process group pid ="<<pid);
    }
    assert(pid != ::getpid());

    // kill proc
    int rc = ::kill(pid, SIGKILL);
    int err = errno;
    if (rc!=0) {
        if (err==EINVAL) {
            assert(false);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
                   << "kill" << rc << err << pid);
        }
        if (err!=ESRCH) {
            LOG4CXX_ERROR(logger, myFuncName << ": failed to kill pid ="
                          << pid << "err=" << err);
        } else {
            LOG4CXX_DEBUG(logger, myFuncName << ": failed to kill pid ="
                          << pid << "err=" << err);
        }
    }
    doesProcExist = (rc==0 || err!=ESRCH);
    return doesProcExist;
}

///XXX TODO tigor: find a better place and implementation
const std::string& MpiManager::getInstallPath(const boost::shared_ptr<const InstanceMembership>& membership)
{
    const Instances& instances = membership->getInstanceConfigs();
    InstanceID myId = Cluster::getInstance()->getLocalInstanceId();

    for (Instances::const_iterator i = instances.begin();
         i != instances.end(); ++i) {

        InstanceID currId = i->getInstanceId();

        if (currId == myId) {
            const std::string& path = i->getPath();
            if (!scidb::isFullyQualified(path)) {
                throw (USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NON_FQ_PATH_ERROR) << path);
            }
            return path;
        }
    }
    assert(false);
    throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
           << "Cluster membership is missing my instance ID");

    // to make compiler happy
    std::string* p(NULL);
    return *p;
}

}
