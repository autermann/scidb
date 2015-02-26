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

/**
 * @file MPILauncher.h
 *      MpiLauncher class provides an interface to launche MPI jobs.
 */

#ifndef MPILAUNCHER_H_
#define MPILAUNCHER_H_

// standards
#include <stdio.h>
#include <sys/types.h>
#include <map>
#include <vector>
#include <sstream>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/scoped_array.hpp>
#include <boost/asio.hpp>
#include <util/WorkQueue.h>

// scidb
#include <array/Metadata.h>
#include <system/Cluster.h>
#include <system/ErrorCodes.h>
#include <system/Exceptions.h>
#include <util/Mutex.h>

#include <mpi/MPIUtils.h>

namespace scidb
{

/// used to start mpi slaves
class MpiLauncher : public boost::enable_shared_from_this<MpiLauncher>
{
 public:
    class InvalidStateException: public scidb::SystemException
    {
    public:
      InvalidStateException(const char* file, const char* function, int32_t line)
      : SystemException(file, function, line, "scidb",
                        scidb::SCIDB_SE_INTERNAL, scidb::SCIDB_LE_UNKNOWN_ERROR,
                        "SCIDB_SE_INTERNAL", "SCIDB_LE_UNKNOWN_ERROR", uint64_t(0))
      {
      }
       ~InvalidStateException() throw () {}
       void raise() const { throw *this; }
       template <class T>
       InvalidStateException& operator <<(const T &param)
       {
           return static_cast<InvalidStateException&>(scidb::SystemException::operator<<(param));
       }
    };

    virtual ~MpiLauncher() {}

    /**
     * Get pid & ppid of the MPI launcher process
     */
    void getPids(std::vector<pid_t>& pids);

    /**
     * Launch MPI jobs
     * @param slaveArgs arguments to pass to the MPI slaves
     * @param membership the current cluster membership
     */
    void launch(const std::vector<std::string>& slaveArgs,
                const boost::shared_ptr<const scidb::InstanceMembership>& membership,
                const size_t maxSlaves);
    /**
     * Check if the launcher is running
     * @return false if the launcher has either not been started or already exited
     * @note this function must not be called after or concurrently with destroy()
     */
    bool isRunning();

    /**
     * Wait for exit and if necessary kill this launcher (mpirun) after system wide liveness timeout
     * @param force if true, kill with the launcher immediately and preserve MPI-related logs
     */
    void destroy(bool force=false);

    uint64_t getLaunchId() { return _launchId; /* no need to lock because never changes */ }

    /// MPI implementation type 
    enum { OMPI=0, MPICH };

    static size_t getMPIType() { return size_t(MPICH); }

    static std::string getLauncherPath(const std::string& mpiPath)
    {
        assert(!mpiPath.empty());
        return mpiPath + "/bin/" + ((getMPIType() == OMPI) ?
                                    mpi::OMPI_LAUNCHER_BIN :
                                    mpi::MPICH_LAUNCHER_BIN);
    }

    static std::string getDaemonPath(const std::string& mpiPath)
    {
        assert(!mpiPath.empty());
        return mpiPath+"/bin/"+ ((getMPIType() == OMPI) ?
                                 mpi::OMPI_DAEMON_BIN :
                                 mpi::MPICH_DAEMON_BIN);
    }

 protected:

    MpiLauncher(uint64_t launchId, const boost::shared_ptr<scidb::Query>& q);
    MpiLauncher(uint64_t launchId, const boost::shared_ptr<scidb::Query>& q, uint32_t timeout);

    virtual  void buildArgs(std::vector<std::string>& envVars,
                            std::vector<std::string>& args,
                            const std::vector<std::string>& slaveArgs,
                            const boost::shared_ptr<const scidb::InstanceMembership>& membership,
                            const boost::shared_ptr<scidb::Query>& query,
                            const size_t maxSlaves) = 0;

    static void getSortedInstances(std::map<scidb::InstanceID,
                                   const scidb::InstanceDesc*>& sortedInstances,
                                   const scidb::Instances& instances,
                                   const boost::shared_ptr<scidb::Query>& query);
    const std::string& getInstallPath()
    {
        return _installPath;
    }
    void setInstallPath(const std::string& path)
    {
        assert(_installPath.empty());
        _installPath = path;
    }

    bool addIpcName(const std::string& name)
    {
        assert(!name.empty());
        return _ipcNames.insert(name).second;
    }

    static char* initIpcForWrite(SharedMemoryIpc* shmIpc, int64_t shmSize);

    
    static void resolveHostNames(boost::shared_ptr<std::vector<std::string> >& hosts);
    static void handleHostNameResolve(const boost::shared_ptr<WorkQueue>& workQueue,
                                      boost::shared_ptr<std::vector<std::string> >& hosts,
                                      size_t indx,
                                      const boost::system::error_code& error,
                                      boost::asio::ip::tcp::resolver::iterator endpoint_iterator);
    static void processHostNameResolve(boost::shared_ptr<std::vector<std::string> >& hosts,
                                       size_t indx,
                                       const boost::system::error_code& error,
                                       boost::asio::ip::tcp::resolver::iterator endpoint_iterator);
 private:
    void handleKillTimeout(boost::shared_ptr<boost::asio::deadline_timer>& killTimer,
                           const boost::system::error_code& error);
    void buildArgsOMPI(std::vector<std::string>& args,
                       const std::vector<std::string>& slaveArgs,
                       const boost::shared_ptr<const scidb::InstanceMembership>& membership,
                       const boost::shared_ptr<scidb::Query>& query,
                       const size_t maxSlaves);
    void buildArgsMPICH(std::vector<std::string>& args,
                        const std::vector<std::string>& slaveArgs,
                        const boost::shared_ptr<const scidb::InstanceMembership>& membership,
                        const boost::shared_ptr<scidb::Query>& query,
                        const size_t maxSlaves);
    void closeFds();
    void becomeProcGroupLeader();
    void setupLogging();
    void recordPids();
    void initExecArgs(const std::vector<std::string>& args,
                      boost::scoped_array<const char*>& argv);
    void scheduleKillTimer();
    bool waitForExit(pid_t pid, int *status, bool noWait=false);
    void completeLaunch(pid_t pid, const std::string& pidFile, int status);
 private:
    MpiLauncher();
    MpiLauncher(const MpiLauncher&);
    MpiLauncher& operator=(const MpiLauncher&);

    pid_t _pid;
    int _status;
    scidb::QueryID _queryId;
    uint64_t _launchId;
    boost::weak_ptr<scidb::Query> _query;
    bool _waiting;
    bool _inError;
    boost::shared_ptr<boost::asio::deadline_timer> _killTimer;
    std::string _installPath;
    std::set<std::string> _ipcNames;
    scidb::Mutex _mutex;
    const uint32_t _MPI_LAUNCHER_KILL_TIMEOUT;
};

class MpiLauncherOMPI : public MpiLauncher
{
 public:
    MpiLauncherOMPI(uint64_t launchId, const boost::shared_ptr<scidb::Query>& q)
    : MpiLauncher(launchId, q) {}
    MpiLauncherOMPI(uint64_t launchId, const boost::shared_ptr<scidb::Query>& q, uint32_t timeout)
    : MpiLauncher(launchId, q, timeout) {}
    virtual ~MpiLauncherOMPI() {}
 protected:
    virtual  void buildArgs(std::vector<std::string>& envVars,
                            std::vector<std::string>& args,
                            const std::vector<std::string>& slaveArgs,
                            const boost::shared_ptr<const scidb::InstanceMembership>& membership,
                            const boost::shared_ptr<scidb::Query>& query,
                            const size_t maxSlaves);
 private:

    void addPerInstanceArgsOMPI(const InstanceID myId,
                                const InstanceDesc* desc,
                                const std::string& clusterUuid,
                                const std::string& queryId,
                                const std::string& launchId,
                                const std::vector<std::string>& slaveArgs,
                                std::vector<std::string>& args);
};

/**
 *  A class to launch an MPICH job.
 *  @note There are several questions that need to be addressed wrt MPICH
 *  1. There is no way (yet?) to identify run-away hydra_pmi_proxy processes after a restart,
 *      or the ones whose children failed to handshake with SciDB.
 *  2. MPICH uses /dev/shm for local communication, which is not cleaned up anyhow by SciDB.
 */
class MpiLauncherMPICH : public MpiLauncher
{
 public:
    MpiLauncherMPICH(uint64_t launchId, const boost::shared_ptr<scidb::Query>& q)
    : MpiLauncher(launchId, q) {}
    MpiLauncherMPICH(uint64_t launchId, const boost::shared_ptr<scidb::Query>& q, uint32_t timeout)
    : MpiLauncher(launchId, q, timeout) {}
    virtual ~MpiLauncherMPICH() {}

 protected:
    virtual void buildArgs(std::vector<std::string>& envVars,
                           std::vector<std::string>& args,
                           const std::vector<std::string>& slaveArgs,
                           const boost::shared_ptr<const scidb::InstanceMembership>& membership,
                           const boost::shared_ptr<scidb::Query>& query,
                           const size_t maxSlaves);
 private:
    void addPerInstanceArgsMPICH(const InstanceID myId,
                                 const InstanceDesc* desc,
                                 const std::string& clusterUuid,
                                 const std::string& queryId,
                                 const std::string& launchId,
                                 const std::vector<std::string>& slaveArgs,
                                 std::vector<std::string>& args,
                                 std::vector<std::string>& hosts);
    /**
     * Generate a script to be invoked by MPICH as an SSH launcher (i.e. /usr/bin/ssh)
     * The script insert an environment variable into hydra_pmi_proxy process to make
     * it identifiable and becomes /usr/bin/ssh (via exec)
     */
    static std::string
    getLauncherSSHExecContent(const std::string& clusterUuid, const std::string& queryId,
                              const std::string& launchId,    const std::string& daemonBinPath);
};
MpiLauncher* newMPILauncher(uint64_t launchId, const boost::shared_ptr<scidb::Query>& q);
MpiLauncher* newMPILauncher(uint64_t launchId, const boost::shared_ptr<scidb::Query>& q, uint32_t timeout);
}
#endif
