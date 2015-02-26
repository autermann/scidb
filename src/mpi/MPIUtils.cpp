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
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <iostream>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <inttypes.h>
#include <util/FileIO.h>
#include <util/shm/SharedMemoryIpc.h>
#include <mpi/MPIUtils.h>

using namespace std;

namespace scidb
{
namespace mpi
{

/// @return the type of shared memory in use
SharedMemoryIpc::SharedMemoryIpcType_t getShmIpcType()
{
    return SharedMemoryIpc::SHM_TYPE;
}

SharedMemoryIpc* newSharedMemoryIpc(const std::string& name)
{
    if (getShmIpcType() == scidb::SharedMemoryIpc::SHM_TYPE) {
        return new SharedMemory(name);
    } else if (getShmIpcType() == scidb::SharedMemoryIpc::FILE_TYPE) {
        return new SharedFile(name);
    }
    return NULL;
}

std::string getSlaveBinFile(const std::string& installPath)
{
    std::string fileName = installPath+"/"+SLAVE_BIN;
    return fileName;
}
std::string getPidDir(const std::string& installPath)
{
    assert(!installPath.empty());
    std::string dir = installPath+"/"+MPI_PID_DIR;
    return dir;
}
std::string getLogDir(const std::string& installPath)
{
    assert(!installPath.empty());
    std::string dir = installPath+"/"+MPI_LOG_DIR;
    return dir;
}
std::string getIpcDir(const std::string& installPath)
{
    assert(!installPath.empty());
    std::string dir("/dev/shm");

    if (getShmIpcType() == SharedMemoryIpc::FILE_TYPE) {
        dir = installPath+"/"+MPI_IPC_DIR;
    } else if (getShmIpcType() != SharedMemoryIpc::SHM_TYPE) {
        assert(false);
        throw std::logic_error("Unknown IPC mode");
    }
    return dir;
}
std::string getIpcFile(const std::string& installPath,
                       const std::string& ipcName)
{
    if (getShmIpcType() == SharedMemoryIpc::SHM_TYPE) {
        return (getIpcDir(installPath) + "/" + ipcName);
    }
    if (getShmIpcType() != SharedMemoryIpc::FILE_TYPE) {
        assert(false);
        throw std::logic_error("Unknown IPC mode");
    }
    return ipcName;
}
bool parseSharedMemoryIpcName(const std::string& fileName,
                                     const std::string& clusterUuid,
                                     uint64_t& instanceId,
                                     uint64_t& queryId,
                                     uint64_t& launchId)
{
    if (getShmIpcType()==SharedMemoryIpc::SHM_TYPE) {

        string format("SciDB-");
        format += clusterUuid;
        format += "-%"PRIu64"-%"PRIu64"-%"PRIu64"%n";
        int n=0;
        int rc = ::sscanf(fileName.c_str(), format.c_str(), &queryId, &instanceId, &launchId, &n);
        if (rc == EOF || rc < 3) {
            // ignore file with unknown name
            return false;
        }
        return true;
    }
    if (getShmIpcType()!=SharedMemoryIpc::FILE_TYPE) {
        assert(false);
        throw std::logic_error("Unknown IPC mode");
    }
    int n=0;
    int rc = ::sscanf(fileName.c_str(), "%"PRIu64".%"PRIu64"%n", &queryId, &launchId, &n);
    if (rc == EOF || rc < 2) {
        // ignore file with unknown name
        return false;
    }
    return true;
}

std::string getProcDirName()
{
    std::string dir("/proc");
    return dir;
}

struct FdCleaner
{
    FdCleaner(int fd) : _fd(fd) {}
    ~FdCleaner() { scidb::File::closeFd(_fd); }
    int _fd;
};

void connectStdIoToLog(const std::string& logFile, bool closeStdin)
{
    int fd = scidb::File::openFile(logFile, (O_WRONLY|O_CREAT|O_EXCL));
    if (fd < 0) {
        perror("open");
        _exit(1);
    }
    {
        struct FdCleaner fdCleaner(fd);

        if (::dup2(fd, STDERR_FILENO) != STDERR_FILENO) {
            perror("dup2(stderr)");
            _exit(1);
        }
        if (::dup2(fd, STDOUT_FILENO) != STDOUT_FILENO) {
            perror("dup2(stdout)");
            _exit(1);
        }
    }

    if (closeStdin) {
        scidb::File::closeFd(STDIN_FILENO);
        return;
    }

    fd = scidb::File::openFile(std::string("/dev/null"), (O_RDONLY));
    if (fd < 0) {
        perror("open");
        _exit(1);
    }
    struct FdCleaner stdinCleaner(fd);

    if (::dup2(fd, STDIN_FILENO) != STDIN_FILENO) {
        perror("dup2(stdin)");
        _exit(1);
    }
}

void recordPids(const std::string& fileName)
{
    int fd = scidb::File::openFile(fileName, (O_WRONLY|O_CREAT|O_EXCL|O_SYNC));
    if (fd < 0) {
        perror("open");
        _exit(1);
    }
    struct FdCleaner fdCleaner(fd);

    // XXX TODO tigor: consider using std::stringstream
    char outBuf[128];
    int n = snprintf(outBuf, sizeof(outBuf), "%d %d", ::getpid(), ::getppid());
    if (n<1 || static_cast<size_t>(n) >= sizeof(outBuf)) {
        cerr << "snprintf failed with: " << n << endl;
        _exit(1);
    }
    for (ssize_t off = 0; n>off; ) {
        ssize_t nwritten = ::write(fd, outBuf+off, n-off);
        if (nwritten >= 0) {
            off += nwritten;
            continue;
        }
        if (errno == EINTR) {
            continue;
        }
        perror("write");
        _exit(1);
    }
    if (scidb::File::closeFd(fd) != 0) {
        perror("close");
        _exit(1);
    }
}

bool readPids(const std::string& fileName, std::vector<pid_t>& pids)
{
    ::FILE* fp = ::fopen(fileName.c_str(), "r");

    if (fp == NULL) {
        assert(errno!=EINVAL);
        return false;
    }
    struct FileCleaner
    {
        FileCleaner(::FILE* fp) : _fp(fp) {}
        ~FileCleaner() { ::fclose(_fp); /* ignore the return code */ }
        ::FILE *_fp;
    } fileCleaner(fp);

    pids.resize(2);
    int rc = ::fscanf(fp,"%d %d", &pids[0], &pids[1]);
    if (rc == EOF || rc < 2
        // sanity check for pids:
        || pids[0] < 2 || pids[1] < 2) {
        return false;
    }
    return true;
}

std::string getSlaveSourceBinFile(const std::string& pluginPath)
{
    assert(!pluginPath.empty());
    std::string fileName = pluginPath+"/"+SLAVE_BIN;
    return fileName;
}

bool readProcName(const std::string& pid, std::string& procName)
{
    string fileName = getProcDirName()+"/"+pid+"/cmdline";

    int fd = scidb::File::openFile(fileName, (O_RDONLY));
    if (fd < 0) {
        return false;
    }
    struct FdCleaner fdCleaner(fd);

    const size_t readSize(1024);
    std::vector<char> buf(readSize);
    size_t off(0);

    while(true) {
        buf.resize(off+readSize);
        char *start = &buf[off];
        ssize_t n = read(fd, start, readSize);
        if (n < 0) {
            return false;
        }
        if (n == 0) {
            break;
        }
        char *end = reinterpret_cast<char*>(memchr(&buf[off], '\0', n));
        if (end != NULL) {
            assert(end>start);
            const ssize_t tail = (end-start);
            assert(tail < n);
            off += tail;
            break;
        } else {
            off += n;
        }
    }
    buf.resize(off);
    procName.replace(procName.begin(), procName.end(),
                     buf.begin(), buf.end());
    return true;
}


std::string
getScidbMPIEnvVar(const string& clusterUuid,
                  const string& queryId,
                  const string& launchId)
{
    stringstream var;
    var << SCIDBMPI_ENV_VAR <<"="<< queryId <<"."<< launchId <<"." << clusterUuid;
    return var.str();
}

bool
parseScidbMPIEnvVar(const string& envVarValue,
                    const string& clusterUuid,
                    uint64_t& queryId,
                    uint64_t& launchId)
{
  string uuid;
  bool rc = parseScidbMPIEnvVar(envVarValue,
				queryId,
				launchId,
				uuid);
  return (rc && (uuid==clusterUuid));
}

bool
parseScidbMPIEnvVar(const string& envVarValue,
                    uint64_t& queryId,
                    uint64_t& launchId,
		    string& clusterUuid)
{
  std::vector<char> buf(envVarValue.size()+1);
  string format("%"PRIu64".%"PRIu64".%s""%n");
  int n=0;
  int rc = ::sscanf(envVarValue.c_str(), format.c_str(), &queryId, &launchId, &buf[0], &n);
  if (rc == EOF || rc < 3) {
    return false;
  }
  buf[buf.size()-1]='\0';
  clusterUuid = string(&buf[0]);
  return true;
}
 
bool matchEnvVar(const std::string& varName,
                 char* varPair,
                 std::string& varValue)
{
    char * delim = strchr(varPair, '=');
    if (delim == NULL) {
        return false;
    }
    *delim = '\0';
    if (varName == varPair) {
        varValue = string(delim+1);
        return true;
    }
    return false;
}

bool readProcEnvVar(const std::string& pid,
                    const std::string& varName,
                    std::string& varValue)
{
    string fileName = getProcDirName()+"/"+pid+"/environ";

    int fd = scidb::File::openFile(fileName, (O_RDONLY));
    if (fd < 0) {
        return false;
    }
    struct FdCleaner fdCleaner(fd);

    char buf[8196];
    const size_t readSize(8196);
    std::vector<char> prefix;

    while(true) {
        ssize_t n = read(fd, buf, readSize);
        if (n < 0) {
            return false;
        }
        if (n == 0) {
            if (!prefix.empty() && matchEnvVar(varName, &prefix[0], varValue)) {
                return true;
            }
            return false;
        }
        size_t off(0);
        while (off<static_cast<size_t>(n)) {
            assert(off < readSize);
            size_t len = n-off;
            char *start = buf + off;
            char *end = reinterpret_cast<char*>(memchr(start, '\0', len));
            if (end == NULL) {
                prefix.resize(prefix.size()+len);
                memcpy(&prefix[prefix.size()-len],start,len);
                assert(prefix.at(prefix.size()-1)=='\0');
                break;
            }
            assert(end > start);
            assert(*end=='\0');
            len = end-start+1;
            if (prefix.size() >0) {
                prefix.resize(prefix.size()+len);
                memcpy(&prefix[prefix.size()-len],start,len);
                start = &prefix[0];
                assert(prefix.at(prefix.size()-1)=='\0');
            }
            if (matchEnvVar(varName, start, varValue)) {
                return true;
            }
            prefix.resize(0);
            off += len;
        }
    }
    assert(false);
    return false;
}


const std::string Command::EXIT("EXIT");
std::string Command::toString()
{
    stringstream ss;
    ss << *this;
    return ss.str();
}

std::ostream& operator<<(std::ostream& os, scidb::mpi::Command& cmd)
{
    os << "<" << cmd.getCmd() << ">[";
    const std::vector<std::string>& args = cmd.getArgs();
    std::vector<std::string>::const_iterator i = args.begin();
    if (i != args.end()) {
        os << *(i++);
    }
    for (; i != args.end(); ++i) {
        os << "," << *i;
    }
    os << "]";
    return os;
}

} //namespace
} //namespace
