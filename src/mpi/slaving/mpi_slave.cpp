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

// std C++
#include <string>
#include <iostream>

// std C
#include <stdlib.h>

// de-facto standards
#include <mpi.h>

// scidb public (include/)
#include <mpi/MPIUtils.h>
#include <SciDBAPI.h>
#include <util/Network.h>
#include <util/NetworkMessage.h>
#include <util/shm/SharedMemoryIpc.h>


// scidb internals
#include <linear_algebra/dlaScaLA/slaving/pdgesvdSlave.hpp>
#include <linear_algebra/dlaScaLA/slaving/pdgemmSlave.hpp>
#include <linear_algebra/scalapackUtil/test/slaving/mpiCopySlave.hpp>
#include <linear_algebra/scalapackUtil/test/slaving/mpiRankSlave.hpp>
#include <network/BaseConnection.h>
#include <network/proto/scidb_msg.pb.h>

// usings
using namespace std;

// types
typedef uint64_t QueryID;
typedef uint64_t InstanceID;

// forward decls
uint64_t str2uint64(const char *str);
uint32_t str2uint32(const char *str);
int initMpi();
void runScidbCommands(uint32_t port,
                      const std::string& clusterUuid,
                      QueryID queryId,
                      InstanceID instanceId,
                      uint64_t launchId);

void mpiErrorHandler(MPI::Comm& comm, int *a1, ...)
{
    ::abort();
}

/**
 * Implementation of scidb::MessageDesc which is aware of DLA specific messages
 */
class MpiMessageDesc : public scidb::MessageDesc
{
 public:
    MpiMessageDesc() : scidb::MessageDesc() {}
    MpiMessageDesc(boost::shared_ptr<scidb::SharedBuffer> binary)
    : scidb::MessageDesc(binary) {}
    virtual ~MpiMessageDesc() {}
    virtual bool validate();
    protected:
    virtual scidb::MessagePtr createRecord(scidb::MessageID messageType);
 private:
    MpiMessageDesc(const MpiMessageDesc&);
    MpiMessageDesc& operator=(const MpiMessageDesc&);
};

scidb::MessagePtr
MpiMessageDesc::createRecord(scidb::MessageID messageType)
{
    if (messageType < scidb::mtSystemMax) {
        return scidb::MessageDesc::createRecord(messageType);
    }

    boost::shared_ptr<scidb::NetworkMessageFactory> msgFactory;
    if (messageType == scidb::mtMpiSlaveResult) {
        return scidb::MessagePtr(new scidb_msg::MpiSlaveResult());
    }
    if (messageType == scidb::mtMpiSlaveHandshake) {
        return scidb::MessagePtr(new scidb_msg::MpiSlaveHandshake());
    }
    if (messageType == scidb::mtMpiSlaveCommand) {
        return scidb::MessagePtr(new scidb_msg::MpiSlaveCommand());
    }
    cerr <<  "SLAVE: unknown message type " << messageType << std::endl;
    exit(6);
    return scidb::MessagePtr();
}

bool MpiMessageDesc::validate()
{
    if (MessageDesc::validate()) {
        return true;
    }
    scidb::MessageID msgId = getMessageType();

    return (msgId == scidb::mtMpiSlaveResult ||
            msgId == scidb::mtMpiSlaveHandshake ||
            msgId == scidb::mtMpiSlaveCommand);
}

/**
 * Slave's interface to SciDB
 *
 */
class MpiMasterProxy
{
    public:
    /**
     * Destructor
     */
    virtual ~MpiMasterProxy()
    {
        if (_connection) {
            try {
                const scidb::SciDB& sciDB = scidb::getSciDB();
                sciDB.disconnect(_connection);
            } catch (const std::exception& e) {
                cerr << "SLAVE: failure in disconnect: "<<e.what()<<std::endl;
            }
            _connection = NULL;
        }
    }

    /// Constructor
    MpiMasterProxy(uint32_t port, const std::string& clusterUuid, uint64_t queryId,
                   uint64_t instanceId, uint64_t rank,
                   uint64_t launchId)
    : _port(port), _clusterUuid(clusterUuid), _queryId(queryId), _instanceId(instanceId), _rank(rank),
      _launchId(launchId), _connection(NULL)
    {
    }

    /// internal use only
    scidb::BaseConnection* getConnection()
    {
        return _connection;
    }

    /**
     * Send the initial handshake message to SciDB and get the next command from SciDB
     * @param [out] nextCmd
     * @return shared memory ipc name
     * @throw scidb::Exception
     */
    void sendHandshake(scidb::mpi::Command& nextCmd)
    {
        if (_connection) {
            cerr << "SLAVE: connection to SciDB already open "<<std::endl;
            exit(99);
        }
        const scidb::SciDB& sciDB = scidb::getSciDB();
        _connection = reinterpret_cast<scidb::BaseConnection*>(sciDB.connect("localhost", _port));
        if (!_connection) {
            cerr << "SLAVE: cannot connect to SciDB "<<std::endl;
            exit(7);
        }
        boost::shared_ptr<scidb::MessageDesc> handshakeMessage(new MpiMessageDesc());
        handshakeMessage->initRecord(scidb::mtMpiSlaveHandshake);
        handshakeMessage->setQueryID(_queryId);
        boost::shared_ptr<scidb_msg::MpiSlaveHandshake> record = handshakeMessage->getRecord<scidb_msg::MpiSlaveHandshake>();

        record->set_cluster_uuid(_clusterUuid);
        record->set_instance_id(_instanceId);
        record->set_launch_id(_launchId);
        record->set_rank(_rank);
        record->set_pid(::getpid());
        record->set_ppid(::getppid());

        sendReceive(handshakeMessage, &nextCmd);
    }

    /**
     * Send the status of the previous command to SciDB and get the next command from SciDB
     * @param [out] nextCmd
     * @param [in] status command status
     * @throw scidb::Exception
     */
    void sendResult(int64_t status, scidb::mpi::Command& nextCmd)
    {
        sendResult(status, &nextCmd);
    }

    /**
     * Send the status of the previous command to SciDB
     * @param [in] status command status
     * @throw scidb::Exception
     */
    void sendResult(int64_t status)
    {
        sendResult(status, NULL);
    }
    private:

    void sendResult(int64_t status, scidb::mpi::Command* nextCmd)
    {
        boost::shared_ptr<scidb::MessageDesc> resultMessage(new MpiMessageDesc());
        resultMessage->initRecord(scidb::mtMpiSlaveResult);
        resultMessage->setQueryID(_queryId);
        boost::shared_ptr<scidb_msg::MpiSlaveResult> record = resultMessage->getRecord<scidb_msg::MpiSlaveResult>();
        record->set_status(status);
        record->set_launch_id(_launchId);

        sendReceive(resultMessage, nextCmd);
    }

    void sendReceive(boost::shared_ptr<scidb::MessageDesc>& resultMessage, scidb::mpi::Command* nextCmd)
    {
        if (nextCmd == NULL) {
            _connection->send(resultMessage);
            return;
        }
        boost::shared_ptr<MpiMessageDesc> commandMessage =
            _connection->sendAndReadMessage<MpiMessageDesc>(resultMessage);

        boost::shared_ptr<scidb_msg::MpiSlaveCommand> cmdMsg =
            commandMessage->getRecord<scidb_msg::MpiSlaveCommand>();
        const string commandStr = cmdMsg->command();
        nextCmd->setCmd(commandStr);

        typedef google::protobuf::RepeatedPtrField<std::string> ArgsType;
        const ArgsType& args = cmdMsg->args();

        for(ArgsType::const_iterator iter = args.begin();
            iter != args.end(); ++iter) {

            const std::string& arg = *iter;
            nextCmd->addArg(arg);
        }
    }

    private:
    MpiMasterProxy(const MpiMasterProxy&);
    MpiMasterProxy& operator=(const MpiMasterProxy&);

    friend void handleBadHandshake(QueryID queryId,
                                   InstanceID instanceId,
                                   uint64_t launchId,
                                   MpiMasterProxy& scidbProxy);

    uint32_t _port;
    std::string _clusterUuid;
    uint64_t _queryId;
    uint64_t _instanceId;
    uint64_t _rank;
    uint64_t _launchId;
    scidb::BaseConnection* _connection;
};

/// test routines
void handleSlowStart(const char *timeoutStr);
void handleSlowSlave(const std::vector<std::string>& args,
                      MpiMasterProxy& scidbProxy);
void handleEchoCommand(const std::vector<std::string>& args,
                       int64_t& result);
void handleBadMessageFlood(QueryID queryId,
                           InstanceID instanceId,
                           uint64_t launchId,
                           MpiMasterProxy& scidbProxy);
void handleBadHandshake(QueryID queryId,
                        InstanceID instanceId,
                        uint64_t launchId,
                        MpiMasterProxy& scidbProxy);
void handleBadStatus(QueryID queryId,
                     InstanceID instanceId,
                     uint64_t launchId,
                     MpiMasterProxy& scidbProxy);
void handleAbnormalExit(const std::vector<std::string>& args);

void setupLogging(const std::string& installPath,
                  const char *queryId, const char *launchId)
{
    std::string logFile = scidb::mpi::getSlaveLogFile(installPath, queryId, launchId);
    scidb::mpi::connectStdIoToLog(logFile);
}

/**
 * DLA (MPI) Slave process entry
 * @param argc >=6
 * @param argv:
 * [1] - cluster UUID
 * [2] - query ID
 * [3] - instance ID (XXXX logical ? physical ?)
 * [4] - launch ID
 * [5] - SciDB instance port
 */

int main(int argc, char* argv[])
{
    if(false) {
        // allow for attachng gdb before a fault occurs
        // because not getting a core file after mpi prints stack trace.
        // this is a useful debugging method, so its good to leave code for it.
        char hostname[256];
        ::gethostname(hostname, sizeof(hostname));
        std::cerr << "DLA_RUN read for attach at pid " << ::getpid() << std::endl ;
        int i=0 ;
        while(i==0) {
            ::sleep(5);
        }
    }

    if (argc < 6) {
        cerr << "SLAVE: Invalid args" << std::endl;
        exit(1);
    }
    const string installPath(".");
    const char* clusterUuid   = argv[1];
    const char* queryIdStr    = argv[2];
    const char* instanceIdStr = argv[3];
    const char* launchIdStr   = argv[4];
    const char* portStr       = argv[5];

    string path =
        scidb::mpi::getSlavePidFile(installPath, queryIdStr, launchIdStr);
    scidb::mpi::recordPids(path);

    setupLogging(installPath, queryIdStr, launchIdStr);

    std::cerr << "SLAVE pid="<< ::getpid() <<":" << std::endl;
    for (int i=0; i < argc; ++i) {
        std::cerr << "arg["<<i<<"]="<<argv[i] << std::endl;
    }

    uint32_t port = str2uint32(portStr);
    if (port == 0) {
        cerr << "SLAVE: Invalid port arg: " << portStr << std::endl;
        exit(3);
    }
    QueryID queryId = str2uint64(queryIdStr);
    InstanceID instanceId = str2uint64(instanceIdStr);

    const int START_DELAY_INDEX = 6;
    if (argc>START_DELAY_INDEX) {
        handleSlowStart(argv[START_DELAY_INDEX]);
    }
    try {
        runScidbCommands(port, clusterUuid, queryId,
                         instanceId, str2uint64(launchIdStr));
    }
    catch (const scidb::SystemException &e)
    {
        if (e.getShortErrorCode() == scidb::SCIDB_SE_NETWORK) {
            cerr << "SLAVE: Connection with SciDB error" << std::endl;
            exit(10);
        }
        throw;
    }
    exit(EXIT_SUCCESS);
}

/// Convert ascii to uint64_t
uint64_t str2uint64(const char *str)
{
    char *ptr=0;
    errno = 0;
    int64_t num = strtoll(str,&ptr,10);
    if (errno !=0 || str == 0 || (*str) == 0 || (*ptr) != 0 || num<0) {
        cerr << "SLAVE: Invalid numeric string for uint64_t: " << str << std::endl;
        exit(8);
    }
    return num;
}

/// Convert ascii to uint32_t
uint32_t str2uint32(const char *str)
{
    char *ptr=0;
    errno = 0;
    int32_t num = strtol(str,&ptr,10);
    if (errno !=0 || str == 0 || (*str) == 0 || (*ptr) != 0 || num<0) {
        cerr << "SLAVE: Invalid numeric string for uint32_t: " << str << std::endl;
        exit(9);
    }
    return num;
}

int initMpi()
{
    MPI::Init();

    MPI::Errhandler eh = 
       MPI::Comm::Create_errhandler((MPI::Comm::Errhandler_fn*)  &mpiErrorHandler);

    MPI::COMM_WORLD.Set_errhandler(eh);

    cerr << "SLAVE: error handler set" << endl;

    //
    //  Get the number of processes.
    //
    int size = MPI::COMM_WORLD.Get_size ();
    assert(size > 0);
    //
    //  Determine this processes's rank.
    //
    int rank = MPI::COMM_WORLD.Get_rank();

    assert(rank < size);
    assert(rank >= 0);
    cout << "SLAVE: rank: "<< rank  << " is ready (stdout)" << endl;
    cerr << "SLAVE: rank: "<< rank  << " is ready (stderr)" << endl;
    cerr << "SLAVE: size: "<< size  << endl;
    return rank;
}

void runScidbCommands(uint32_t port,
                      const std::string& clusterUuid,
                      QueryID queryId,
                      InstanceID instanceId,
                      uint64_t launchId)
{
    MpiMasterProxy scidbProxy(port, clusterUuid, queryId,
                              instanceId, instanceId, launchId);
    // Handshake
    scidb::mpi::Command scidbCommand;
    scidbProxy.sendHandshake(scidbCommand);

    /// MPI -- NB: only needs to be inited for DLA commands?
    //             might have other commands that won't use it
    //             even though we are running under MPI ?
    int rank = initMpi();
    assert(rank >=0);
    assert(static_cast<uint64_t>(rank) == instanceId);
    rank=rank; // avoid compiler warning

    int64_t INFO=0;  // all slave proxys expect 0 for success
                     // TODO: change this to fail and add explicit success overwrites

    while (scidbCommand.getCmd() != scidb::mpi::Command::EXIT) {
        cerr << "SLAVE: command: "<< scidbCommand << std::endl;

        if(scidbCommand.getCmd() == "DLAOP") {
            enum dummy {ARG_IPCNAME=0, ARG_NBUFS, ARG_DLAOP};
            const std::vector<std::string>& args = scidbCommand.getArgs();
            // TODO JHM; eliminate cerr debug messages
            cerr << "SLAVE: NUMARGS = " << args.size() << std::endl;

            const string ipcName(args[ARG_IPCNAME]);
            const string dlaOp(args[ARG_DLAOP]);
            const unsigned nBufs = atoi(args[ARG_NBUFS].c_str());

            cerr << "SLAVE: ARG_IPCNAME = " << ipcName << std::endl;
            cerr << "SLAVE: ARG_DLAOP = " << dlaOp << std::endl;
            cerr << "SLAVE: ARG_NBUFS = " << nBufs << std::endl;

            const size_t MAX_BUFS = 20;
            if(nBufs > MAX_BUFS) {
                cerr << "SLAVE: ARG_NBUFS is invalid " << std::endl;
                ::exit(99); // IGOR what is the right way?
            }

            // now get the buffers sent by the master
            void* bufs[MAX_BUFS];
            size_t sizes[MAX_BUFS];
            boost::scoped_ptr<scidb::SharedMemoryIpc> shMems[MAX_BUFS];

            for (size_t i=0; (i < nBufs && i < MAX_BUFS); i++) {
                shMems[i].reset();
                bufs[i] = NULL;
                sizes[i] = 0;

                std::stringstream shMemName;          // name Nth buffer
                shMemName << ipcName << "." << i ;

                try {
                    shMems[i].reset(scidb::mpi::newSharedMemoryIpc(shMemName.str()));

                    scidb::SharedMemoryIpc::AccessMode mode =
                        i < 1 ? scidb::SharedMemoryIpc::RDONLY :
                                scidb::SharedMemoryIpc::RDWR ;
                    shMems[i]->open(mode);
                    // it would be nice to give the name in the .open() call
                    // so the constructor can be no-arg, which would simplify the code
                    bufs[i] = reinterpret_cast<char*>(shMems[i]->get());
                    sizes[i] = static_cast<uint64_t>(shMems[i]->getSize());
                } catch(scidb::SharedMemoryIpc::SystemErrorException& e) {
                    cerr << "SLAVE: Cannot map shared memory: " << e.what() << std::endl;
                    exit(4);
                } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
                    cerr << "SLAVE: Bug in mapping shared memory: " << e.what() << std::endl;
                    exit(5);
                }
                assert(bufs[i]);

                cerr << "SLAVE: IPC BUF at:"<< bufs[i] << std::endl;
                cerr << "SLAVE: IPC size = " << sizes[i] << std::endl;
            }

            // dispatch on the dla operator
            if(dlaOp == "pdgesvd_") {
                INFO = scidb::pdgesvdSlave(bufs, sizes, nBufs);
            } else if (dlaOp == "pdgemm_") {
                INFO = scidb::pdgemmSlave(bufs, sizes, nBufs);
            } else if (dlaOp == "mpirank") {
                INFO = scidb::mpirankSlave(bufs, sizes, nBufs);
            } else if (dlaOp == "mpicopy") {
                cerr << "runScidbCommands: calling mpiCopySlave()" << std::endl;
                INFO = scidb::mpiCopySlave(bufs, sizes, nBufs);
            } else {
                cerr << "runScidbCommands: DLAOP '" << dlaOp << "' not implemented" << std::endl;
                handleAbnormalExit(scidbCommand.getArgs());
            }
        }
        else if(scidbCommand.getCmd() == "ECHO") {
            handleEchoCommand(scidbCommand.getArgs(), INFO);
        }
        else if(scidbCommand.getCmd() == "SLOW_SLAVE") {
            handleSlowSlave(scidbCommand.getArgs(), scidbProxy);
        }
        else if(scidbCommand.getCmd() == "ABNORMAL_EXIT") {
            handleAbnormalExit(scidbCommand.getArgs());
        }
        else if(scidbCommand.getCmd() == "BAD_MSG_FLOOD") {
            handleBadMessageFlood(queryId, instanceId, launchId, scidbProxy);
        }
        else if(scidbCommand.getCmd() == "BAD_HANDSHAKE") {
            handleBadHandshake(queryId, instanceId, launchId, scidbProxy);
        }
        else if(scidbCommand.getCmd() == "BAD_STATUS") {
            handleBadStatus(queryId, instanceId, launchId, scidbProxy);
        }

        scidbCommand.clear();

        // no cleanup needed, destructors and process exit do it all
        scidbProxy.sendResult(INFO, scidbCommand);
    }

    cerr << "SLAVE: MPI Finalize" << std::endl;
    MPI::Finalize();
    cerr << "SLAVE: MPI Finalize done!!!" << std::endl;
}

void handleEchoCommand(const std::vector<std::string>& args,
                       int64_t& result)
{
    cerr << "SLAVE: NUMARGS = " << args.size() << std::endl;

    if (args.size() != 2) {
        cerr << "SLAVE: NUMARGS for ECHO is invalid" << std::endl;
        exit(99);
    }

    const string ipcInName(args[0]);
    const string ipcOutName(args[1]);

    // now get the buffers sent by the master

    boost::scoped_ptr<scidb::SharedMemoryIpc> shmIn(scidb::mpi::newSharedMemoryIpc(ipcInName));
    char* bufIn(NULL);
    size_t sizeIn(0);
    boost::scoped_ptr<scidb::SharedMemoryIpc> shmOut(scidb::mpi::newSharedMemoryIpc(ipcOutName));
    char* bufOut(NULL);
    size_t sizeOut(0);

    try {
        shmIn->open(scidb::SharedMemoryIpc::RDONLY);
        bufIn  = reinterpret_cast<char*>(shmIn->get());
        sizeIn = shmIn->getSize();
    } catch(scidb::SharedMemoryIpc::SystemErrorException& e) {
        cerr << "SLAVE: Cannot map shared memory: " << e.what() << std::endl;
        exit(4);
    } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
        cerr << "SLAVE: Bug in mapping shared memory: " << e.what() << std::endl;
        exit(5);
    }
    if (!bufIn) {
        cerr << "SLAVE: Cannot map input shared memory buffer" << std::endl;
        exit(99);
    }
    try {
        shmOut->open(scidb::SharedMemoryIpc::RDWR);
        bufOut = reinterpret_cast<char*>(shmOut->get());
        sizeOut = shmOut->getSize();
    } catch(scidb::SharedMemoryIpc::SystemErrorException& e) {
        cerr << "SLAVE: Cannot map shared memory: " << e.what() << std::endl;
        exit(4);
    } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
        cerr << "SLAVE: Bug in mapping shared memory: " << e.what() << std::endl;
        exit(5);
    }
    if (!bufOut) {
        cerr << "SLAVE: Cannot map output shared memory buffer" << std::endl;
        exit(99);
    }
    if (sizeIn != sizeOut) {
        cerr << "SLAVE: Input and output shared memory buffer differ in size" << std::endl;
        exit(99);
    }
    memcpy(bufOut, bufIn, sizeOut);
    result = 1;
}

void handleBadMessageFlood(QueryID queryId,
                           InstanceID instanceId,
                           uint64_t launchId,
                           MpiMasterProxy& scidbProxy)
{
    const size_t MSG_NUM = 10000;
    cerr << "SLAVE: sending "<< MSG_NUM <<" wrong messages from BAD_MSG_FLOOD" << std::endl;
    // SciDB is not waiting for messages with launch_id=0, so it should not queue up these messages
    assert(launchId>0);
    for (size_t i=0; i < MSG_NUM; ++i)
    {
        boost::shared_ptr<scidb::MessageDesc> wrongMessage(new MpiMessageDesc());
        wrongMessage->initRecord(scidb::mtMpiSlaveHandshake);
        wrongMessage->setQueryID(queryId);
        boost::shared_ptr<scidb_msg::MpiSlaveHandshake> wrongRecord = wrongMessage->getRecord<scidb_msg::MpiSlaveHandshake>();
        wrongRecord->set_cluster_uuid("");
        wrongRecord->set_instance_id(0);
        wrongRecord->set_launch_id(0);
        wrongRecord->set_rank(0);
        wrongRecord->set_pid(0);
        wrongRecord->set_ppid(0);
        scidbProxy.getConnection()->send(wrongMessage);
    }
 }


void handleBadHandshake(QueryID queryId,
                        InstanceID instanceId,
                        uint64_t launchId,
                        MpiMasterProxy& scidbProxy)
{
    cerr << "SLAVE: sending wrong message from BAD_HANDSHAKE" << std::endl;

    scidb::mpi::Command nextCmd;

    boost::shared_ptr<scidb::MessageDesc> wrongMessage(new MpiMessageDesc());
    wrongMessage->initRecord(scidb::mtMpiSlaveHandshake);
    wrongMessage->setQueryID(queryId);

    // SciDB is not expecting a handshake message at this time in the current launch
    boost::shared_ptr<scidb_msg::MpiSlaveHandshake> wrongRecord = wrongMessage->getRecord<scidb_msg::MpiSlaveHandshake>();

    wrongRecord->set_cluster_uuid("");
    wrongRecord->set_instance_id(0);
    wrongRecord->set_launch_id(launchId);
    wrongRecord->set_rank(0);
    wrongRecord->set_pid(::getpid());
    wrongRecord->set_ppid(::getppid());

    scidbProxy.sendReceive(wrongMessage, &nextCmd);

    if (nextCmd.getCmd() != scidb::mpi::Command::EXIT) {
        exit(99);
    }

    MPI::Finalize();
    exit(0);
}

void handleBadStatus(QueryID queryId,
                     InstanceID instanceId,
                     uint64_t launchId,
                     MpiMasterProxy& scidbProxy)
{
    cerr << "SLAVE: sending malformed status from BAD_MSG" << std::endl;
    char buf[1];
    boost::shared_ptr<scidb::SharedBuffer> binary(new scidb::MemoryBuffer(buf, 1));

    boost::shared_ptr<scidb::MessageDesc> wrongMessage(new MpiMessageDesc(binary));
    wrongMessage->initRecord(scidb::mtMpiSlaveResult);
    wrongMessage->setQueryID(queryId);
    boost::shared_ptr<scidb_msg::MpiSlaveResult> wrongRecord = wrongMessage->getRecord<scidb_msg::MpiSlaveResult>();

    wrongRecord->set_status(0);
    wrongRecord->set_launch_id(launchId);

    scidbProxy.getConnection()->send(wrongMessage);
    // SciDB should drop the connection after this message, causing this process to exit
}

void handleSlowSlave(const std::vector<std::string>& args,
                       MpiMasterProxy& scidbProxy)
{
    cerr << "SLAVE: NUMARGS = " << args.size() << std::endl;

    if (args.size() != 1) {
        cerr << "SLAVE: NUMARGS for SLOW_SLAVE is invalid" << std::endl;
        exit(99);
    }

    uint32_t timeout = str2uint32(args[0].c_str());

    cerr << "SLAVE: sleeping for " << timeout << " sec" << std::endl;
    sleep(timeout);

    scidb::mpi::Command nextCmd;

    cerr << "SLAVE: sending bogus result " << timeout << std::endl;
    scidbProxy.sendResult(static_cast<int64_t>(timeout), nextCmd);

    if (nextCmd.getCmd() != scidb::mpi::Command::EXIT) {
        exit(99);
    }

    cerr << "SLAVE: sleeping for " << timeout << " sec" << std::endl;
    sleep(timeout);

    MPI::Finalize();
    exit(EXIT_SUCCESS);
}

void handleAbnormalExit(const std::vector<std::string>& args)
{
    cerr << "SLAVE: NUMARGS = " << args.size() << std::endl;

    if (args.size() != 1) {
        cerr << "SLAVE: NUMARGS for ABNORMALEXIT is invalid" << std::endl;
        exit(99);
    }

    uint32_t exitCode = str2uint32(args[0].c_str());
    cerr << "SLAVE: exiting with " << exitCode << std::endl;
    exit(exitCode);
}

void handleSlowStart(const char *timeoutStr)
{
    uint32_t timeout = str2uint32(timeoutStr);
    cerr << "SLAVE: sleeping for " << timeout << " sec" << std::endl;
    sleep(timeout);
}

