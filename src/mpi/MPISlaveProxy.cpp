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
#include <boost/shared_ptr.hpp>
#include <util/Network.h>
#include <util/NetworkMessage.h>
#include <util/FileIO.h>
#include <query/Query.h>
#include <log4cxx/logger.h>
#include <../src/network/proto/scidb_msg.pb.h> //XXX TODO: move to public ?
#include <mpi/MPISlaveProxy.h>
#include <mpi/MPIUtils.h>
#include <mpi/MPIManager.h>
#include <sys/types.h>
#include <signal.h>
#include <time.h>

using namespace std;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.mpi"));

static double getTimeInSecs()
{
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
        assert(false);
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_GET_SYSTEM_TIME);
    }
    return (ts.tv_sec + ts.tv_nsec*1e-9);
}

static bool checkForTimeout(double startTime, double timeout,
                            uint64_t launchId, MpiOperatorContext* ctx)
{
    if ((getTimeInSecs() - startTime) > timeout) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
            << "MPI slave process failed to communicate in time";
    }
    return true;
}

static bool checkLauncher(double startTime, double timeout,
                          uint64_t launchId, MpiOperatorContext* ctx)
{
    boost::shared_ptr<MpiLauncher> launcher(ctx->getLauncher(launchId));
    if (launcher && !launcher->isRunning()) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                << "MPI launcher process";
    }
    checkForTimeout(startTime, timeout, launchId, ctx);

    return true;
}

void MpiSlaveProxy::waitForHandshake(boost::shared_ptr<MpiOperatorContext>& ctx)
{
    if (_connection) {
        throw (InvalidStateException(REL_FILE, __FUNCTION__, __LINE__)
               << "Connection to MPI slave already established");
    }

    MpiOperatorContext::LaunchErrorChecker errChecker =
        boost::bind(&checkLauncher, getTimeInSecs(),
                    static_cast<double>(_MPI_SLAVE_RESPONSE_TIMEOUT), _1, _2);
    boost::shared_ptr<scidb::ClientMessageDescription> msg = ctx->popMsg(_launchId, errChecker);
    assert(msg);

    _connection = msg->getClientContext();

    if (msg->getMessageType() != scidb::mtMpiSlaveHandshake) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
            << "MPI slave handshake is invalid");
    }

    boost::shared_ptr<scidb_msg::MpiSlaveHandshake> handshake =
        boost::dynamic_pointer_cast<scidb_msg::MpiSlaveHandshake>(msg->getRecord());
    assert(handshake);

    // parse the handshake
    if (!handshake->has_pid()) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
            << "MPI slave handshake has no PID");
    }
    if (!handshake->has_ppid()) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << "MPI slave handshake has no PPID");
    }

    _pids.push_back(handshake->pid());
    _pids.push_back(handshake->ppid());

    string clusterUuid = Cluster::getInstance()->getUuid();

    if (handshake->cluster_uuid() != clusterUuid) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << "MPI slave handshake has invalid clusterUuid");
    }
    InstanceID instanceId = Cluster::getInstance()->getLocalInstanceId();

    if (handshake->instance_id() != instanceId) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << "MPI slave handshake has invalid instanceId");
    }

    if (handshake->launch_id() != _launchId) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << "MPI slave handshake has invalid launchId");
    }

    boost::shared_ptr<scidb::Query> query( _query.lock());
    Query::validateQueryPtr(query);

    if (handshake->rank() != query->getInstanceID()) { // logical instance ID
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << "MPI slave handshake has invalid rank");
    }

    ClientContext::DisconnectHandler dh =
        boost::bind(&MpiMessageHandler::handleMpiSlaveDisconnect, _launchId, _1);
    _connection->attachQuery(query->getQueryID(), dh);

    LOG4CXX_DEBUG(logger, "MpiSlaveProxy::waitForHandshake: handshake: "
                  <<" pid="<<handshake->pid()
                  <<", ppid="<<handshake->ppid()
                  <<", cluster_uuid="<<handshake->cluster_uuid()
                  <<", instance_id="<<handshake->instance_id()
                  <<", launch_id="<<handshake->launch_id()
                  <<", rank="<<handshake->rank());
}

void MpiSlaveProxy::sendCommand(mpi::Command& cmd, boost::shared_ptr<MpiOperatorContext>& ctx)
{
    if (!_connection) {
        throw (InvalidStateException(REL_FILE, __FUNCTION__, __LINE__)
               << "No connection to MPI slave");
    }

    // set command
    boost::shared_ptr<scidb_msg::MpiSlaveCommand> cmdPtr(new scidb_msg::MpiSlaveCommand());
    cmdPtr->set_command(cmd.getCmd());

    // set args
    const std::vector<std::string>& args = cmd.getArgs();
    google::protobuf::RepeatedPtrField<std::string>* msgArgs = cmdPtr->mutable_args();
    assert(msgArgs);
    msgArgs->Reserve(args.size());
    for (std::vector<std::string>::const_iterator iter = args.begin(); iter != args.end(); ++iter) {
        cmdPtr->add_args(*iter);
    }
    // send to slave
    scidb::MessagePtr msgPtr(cmdPtr);
    boost::asio::const_buffer binary(NULL,0);
    try {
        scidb::sendAsync(_connection, scidb::mtMpiSlaveCommand, msgPtr, binary);

    } catch (const scidb::SystemException& e) {
        LOG4CXX_ERROR(logger, "MpiSlaveProxy::sendCommand: "
                      << "FAILED to send MpiSlaveCommand to slave because: "
                      << e.what());
        throw;
    }
    LOG4CXX_DEBUG(logger, "MpiSlaveProxy::sendCommand: MpiSlaveCommand "
                  << cmd.toString() << " sent to slave");
}

/// @todo XXX TODO tigor: make it timeout ? TBD
int64_t MpiSlaveProxy::waitForStatus(boost::shared_ptr<MpiOperatorContext>& ctx, bool raise)
{
    if (!_connection) {
        throw (InvalidStateException(REL_FILE, __FUNCTION__, __LINE__)
               << "No connection to MPI slave");
    }

    MpiOperatorContext::LaunchErrorChecker noopChecker;
    boost::shared_ptr<scidb::ClientMessageDescription> msg = ctx->popMsg(_launchId, noopChecker);
    assert(msg);

    LOG4CXX_DEBUG(logger, "MpiSlaveProxy::waitForStatus: message from client: "
                  <<" ctx = " << msg->getClientContext().get()
                  <<", msg type = "<< msg->getMessageType()
                  <<", queryID = "<<msg->getQueryId());

    if (_connection != msg->getClientContext()) {
        if (!msg->getClientContext() &&
            msg->getMessageType() == scidb::SYSTEM_NONE_MSG_ID) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "MPI slave disconnected prematurely");
        }
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << "MPI slave connection context mismatch");
    }

    if (msg->getMessageType() != scidb::mtMpiSlaveResult) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << "MPI slave returned invalid status");
    }

    boost::shared_ptr<scidb_msg::MpiSlaveResult> result =
        boost::dynamic_pointer_cast<scidb_msg::MpiSlaveResult>(msg->getRecord());
    assert(result);

    if (!result->has_status()) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << "MPI slave returned no status");
    }

    if (raise && result->status() != 0) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OPERATION_FAILED)
               << result->status());
    }
    return result->status();
}

void MpiSlaveProxy::waitForExit(boost::shared_ptr<MpiOperatorContext>& ctx)
{
    if (!_connection) {
        throw (InvalidStateException(REL_FILE, __FUNCTION__, __LINE__)
               << "No connection to MPI slave");
    }

    MpiOperatorContext::LaunchErrorChecker timeChecker =
       boost::bind(&checkForTimeout, getTimeInSecs(),
                   static_cast<double>(_MPI_SLAVE_RESPONSE_TIMEOUT), _1, _2);
    boost::shared_ptr<scidb::ClientMessageDescription> msg = ctx->popMsg(_launchId, timeChecker);
    assert(msg);

    LOG4CXX_DEBUG(logger, "MpiSlaveProxy::waitForExit: "
                  <<" ctx = " << msg->getClientContext().get()
                  <<", msg type = "<< msg->getMessageType()
                  <<", queryID = "<<msg->getQueryId());

    if (msg->getMessageType() != scidb::SYSTEM_NONE_MSG_ID) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << "MPI slave returned invalid status");
    }
    assert(!msg->getClientContext());
    _connection.reset();
}

void MpiSlaveProxy::destroy(bool error)
{
    if (error) {
        _inError=true;
    }
    // kill the slave proc and its parent orted
    for ( std::vector<pid_t>::const_iterator iter=_pids.begin();
          iter!=_pids.end(); ++iter) {
        pid_t pid = *iter;

        //XXX TODO tigor: kill proceess group (-pid) ?
        MpiErrorHandler::killProc(_installPath, pid);
    }

    // rm pid file
    std::string pidFile = mpi::getSlavePidFile(_installPath, _queryId, _launchId);
    scidb::File::remove(pidFile.c_str(), false);

    // rm log file
    if (!logger->isTraceEnabled() && !_inError) {
        string logFileName = mpi::getSlaveLogFile(_installPath, _queryId, _launchId);
        scidb::File::remove(logFileName.c_str(), false);
    }
}

} //namespace
