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
 * @file MpiUtils.h
 *      A collection of common routines used by the MPI slave process and SciDB
 */

#ifndef MPIUTILS_H_
#define MPIUTILS_H_

#include <stdint.h>
#include <assert.h>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <stdexcept>
#include <util/shm/SharedMemoryIpc.h>

namespace scidb
{
namespace mpi
{
    const std::string SLAVE_BIN    = "scidb_mpi_slave";
    const std::string LAUNCHER_BIN = "mpirun";
    const std::string DAEMON_BIN   = "orted";
    const std::string MPI_DIR      = "mpi";
    const std::string MPI_PID_DIR = "mpi_pid";
    const std::string MPI_LOG_DIR = "mpi_log";
    const std::string MPI_IPC_DIR = "mpi_ipc";

    /// @return the type of shared memory in use by MPI-based operators
    SharedMemoryIpc::SharedMemoryIpcType_t getShmIpcType();

    /// @return a new shared memory IPC object corresponding to the shared memory type in use
    SharedMemoryIpc* newSharedMemoryIpc(const std::string& name);

    /**
     * @return filename to store the pid(s) of MPI launcher
     * Format: sprintf(...,"%"PRIu64"."PRIu64"."LAUNCHER_BIN, queryId, launchId)
     * @note  Expected Streamable_<>_tt interface:
     * friend ostream& operator<<(ostream&, Streamable&)
     */
    template <typename Streamable_Q_tt, typename Streamable_L_tt>
    std::string getLauncherPidFile(const std::string& installPath,
                                   Streamable_Q_tt queryId,
                                   Streamable_L_tt launchId)
    {
        assert(!installPath.empty());
        std::ostringstream fileName;
        fileName << installPath << "/" << MPI_PID_DIR
                 << "/" << queryId
                 << "." << launchId
                 << "." << LAUNCHER_BIN;
        return fileName.str();
    }

    std::string getLogDir(const std::string& installPath);

    /**
     * @return filename with stderr+stdout of MPI launcher
     * Format: sprintf(...,"%"PRIu64"."PRIu64"."LAUNCHER_BIN".log", queryId, launchId)
     * @note  Expected Streamable_<>_tt interface:
     * friend ostream& operator<<(ostream&, Streamable&)
     */
    template <typename Streamable_Q_tt, typename Streamable_L_tt>
    std::string getLauncherLogFile(const std::string& installPath,
                                   Streamable_Q_tt queryId,
                                   Streamable_L_tt launchId)
    {
        assert(!installPath.empty());
        std::ostringstream fileName;
        fileName << getLogDir(installPath)
                 <<  "/" << queryId
                 << "." << launchId
                 << "." << LAUNCHER_BIN << ".log";
        return fileName.str();
    }

    /**
     * @param installPath this instance install/data path
     * @return filename of the MPI slave relative to installPath
     */
    std::string getSlaveBinFile(const std::string& installPath);

    /**
     * @param pluginPath SciDB plugin installation directory (i.e. /opt/scidb/<ver#>/lib/scidb/plugins)
     * @return filename of the MPI slave relative pluginPath
     */
    std::string getSlaveSourceBinFile(const std::string& pluginPath);

    /**
     * @return filename to store the pid(s) of MPI slave
     * Format: sprintf(...,"%s/%"PRIu64"."PRIu64"."SLAVE_BIN, queryId, launchId)
     * @note Expected Streamable_<>_tt interface:
     * friend ostream& operator<<(ostream&, Streamable&)
     */
    template <typename Streamable_Q_tt, typename Streamable_L_tt>
    std::string getSlavePidFile(const std::string& installPath,
                                Streamable_Q_tt queryId,
                                Streamable_L_tt launchId)
    {
        assert(!installPath.empty());
        std::ostringstream fileName;
        fileName << installPath << "/" << MPI_PID_DIR
                 << "/" << queryId
                 << "." << launchId
                 << "." << SLAVE_BIN;
        return fileName.str();
    }

    /**
     * @return filename with stderr+stdout of MPI slave
     * Format: sprintf(...,"%"PRIu64"."PRIu64"."SLAVE_BIN".log, queryId, launchId)
     * @note  Expected Streamable_<>_tt interface:
     * friend ostream& operator<<(ostream&, Streamable&)
     */
    template <typename Streamable_Q_tt, typename Streamable_L_tt>
    std::string getSlaveLogFile(const std::string& installPath,
                                Streamable_Q_tt queryId,
                                Streamable_L_tt launchId)
    {
        assert(!installPath.empty());
        std::ostringstream fileName;
        fileName << getLogDir(installPath)
                 <<  "/" << queryId
                 << "." << launchId
                 << "." << SLAVE_BIN << ".log";
        return fileName.str();
    }

    std::string getPidDir(const std::string& installPath);
    std::string getIpcDir(const std::string& installPath);
    std::string getProcDirName();

    /**
     * @return name of an IPC object for communicating with an MPI slave
     * @note Expected Streamable_<>_tt interface:
     * friend ostream& operator<<(ostream&, Streamable&)
     */
    template <typename Streamable_Q_tt,
              typename Streamable_I_tt,
              typename Streamable_L_tt>
    std::string getIpcName(const std::string& installPath,
                           const std::string& clusterUuid,
                           Streamable_Q_tt queryId,
                           Streamable_I_tt instanceId,
                           Streamable_L_tt launchId)
    {
        assert(!clusterUuid.empty());
        std::ostringstream name;
        if (getShmIpcType() == SharedMemoryIpc::SHM_TYPE) {
            name << "SciDB-"
                 << clusterUuid
                 << "-" << queryId
                 << "-" << instanceId
                 << "-" << launchId;
        } else if (getShmIpcType() == SharedMemoryIpc::FILE_TYPE) {
            name << getIpcDir(installPath)
                 <<  "/" << queryId
                 << "." << launchId;
        } else {
            assert(false);
            throw std::logic_error("Unknown IPC mode");
        }
        return name.str();
    }

    /**
     * @return name of an IPC object as it appears in the filesystem
     * @note Expected Streamable_<>_tt interface:
     * friend ostream& operator<<(ostream&, Streamable&)
     */
    std::string getIpcFile(const std::string& installPath,
                           const std::string& ipcName);

    /**
     * Parse the SharedMemoryIpc name which must be relative to getIpcDir()
     * @param ipcName [in]
     * @param clusterUuid [in]
     * @param instanceId [in/out]
     * @param queryId [out]
     * @param launchId [out]
     * @return true if the ipcName is in the correct format and the parsed clusterUuid, instanceId match the parsed values
     */
    bool parseSharedMemoryIpcName(const std::string& ipcName,
                                  const std::string& clusterUuid,
                                  uint64_t& instanceId,
                                  uint64_t& queryId,
                                  uint64_t& launchId);

    /// redirect stderr,stdout to logFile, close stdin
    void connectStdIoToLog(const std::string& logFile);
    /**
     * Record getpid() and getppid() in a given file
     */
    void recordPids(const std::string& fileName);

    /**
     * Read pid and ppid from a given file
     * @return true if the pids are successfully read
     */
    bool readPids(const std::string& fileName, std::vector<pid_t>& pids);

    /**
     * Read command line name of the process specified by a stringnified pid
     * @return true if the process name is successfully read
     */
    bool readProcName(const std::string& pid, std::string& procName);

    class Command
    {
    public:
        const static std::string EXIT;
        Command(){}
        virtual ~Command() {}
        const std::string& getCmd() { return _cmd; }
        void setCmd(const std::string& cmd) { _cmd=cmd; }
        void addArg(const std::string& arg) { _args.push_back(arg); }
        const std::vector<std::string>& getArgs() { return _args; }
        void clear() { _cmd=""; _args.clear(); }
        std::string toString();
    private:
        Command(const Command&);
        Command& operator=(const Command&);
        std::string _cmd;
        std::vector<std::string> _args;
    };

    std::ostream& operator<<(std::ostream& os, scidb::mpi::Command& cmd);
}
}

#endif
