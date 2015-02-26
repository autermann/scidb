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

#include <query/Operator.h>
#include <array/Metadata.h>
#include <system/Cluster.h>
#include <query/Query.h>
#include <boost/make_shared.hpp>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <log4cxx/logger.h>
#include <util/NetworkMessage.h>
#include <../src/network/proto/scidb_msg.pb.h> //XXX TODO: move to public ?
#include <mpi/MPILauncher.h>
#include <mpi/MPIUtils.h>
#include <util/shm/SharedMemoryIpc.h>
#include <mpi/MPIManager.h>
#include <mpi/MPISlaveProxy.h>

namespace scidb
{
using namespace std;
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.mpi.test"));

class PhysicalMpiTest: public PhysicalOperator
{
  public:

    PhysicalMpiTest(const string& logicalName, const string& physicalName,
                    const Parameters& parameters, const ArrayDesc& schema)
    : PhysicalOperator(logicalName, physicalName, parameters, schema),
      _mustLaunch(false), NUM_LAUNCH_TESTS(3)
    {
    }
    virtual void setQuery(const boost::shared_ptr<Query>& query)
    {
        boost::shared_ptr<Query> myQuery = _query.lock();
        if (myQuery) {
            assert(query==myQuery);
            assert(_ctx);
            return;
        }
        PhysicalOperator::setQuery(query);
        _ctx = boost::shared_ptr<MpiOperatorContext>(new MpiOperatorContext(query));
        _ctx = MpiManager::getInstance()->checkAndSetCtx(query->getQueryID(),_ctx);
        shared_ptr<MpiErrorHandler> eh(new MpiErrorHandler(_ctx));
        query->pushErrorHandler(eh);
        Query::Finalizer f = boost::bind(&MpiErrorHandler::finalize, eh, _1);
        query->pushFinalizer(f);
    }
    void preSingleExecute(shared_ptr<Query> query)
    {
        _mustLaunch = true;
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays,
                                     boost::shared_ptr<Query> query)
    {
        MpiManager::getInstance()->cleanup();

        assert(_ctx);
        const boost::shared_ptr<const InstanceMembership> membership =
            Cluster::getInstance()->getInstanceMembership();

        if (membership->getViewId() != query->getCoordinatorLiveness()->getViewId()) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM2);
        }

        const string& installPath = MpiManager::getInstallPath(membership);

        testMultipleLaunches(installPath, membership, query);
        testEcho(installPath, membership,  query);
        testBadMessageFlood(installPath, membership,  query); // prereq: launchId>0
        testBadHandshake(installPath, membership,  query);
        testBadStatus(installPath, membership,  query);
        testSlowSlave(installPath, membership,  query);
        testSlaveExit(installPath, membership,  query);

        _ctx.reset();
        return shared_ptr<Array> (new MemArray(_schema));
    }

    void postSingleExecute(shared_ptr<Query> query)
    {
    }

    void launchMpiJob(boost::shared_ptr<MpiLauncher>& launcher,
                      std::vector<std::string>& args,
                      const boost::shared_ptr<const InstanceMembership>& membership,
                      const shared_ptr<Query>& query,
                      const int maxSlaves)
    {
        launcher->launch(args, membership, maxSlaves);

        vector<pid_t> pids;
        launcher->getPids(pids);
        for (vector<pid_t>::const_iterator i=pids.begin(); i != pids.end(); ++i) {
            LOG4CXX_DEBUG(logger,"XXXX Launched PID= "<<(*i));
        }

        if (!launcher->isRunning()) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "XXXX Bug in MPI launcher: MpiLauncher::isRunning returned false");
        }
    }

    void testMultipleLaunches(const std::string& installPath,
                              const boost::shared_ptr<const InstanceMembership>& membership,
                              boost::shared_ptr<Query>& query)
    {
        for ( size_t i=0; i < NUM_LAUNCH_TESTS; ++i)
        {
            LOG4CXX_DEBUG(logger, "XXXX MULTI-LAUNCH test "<<i);
            uint64_t launchId =_ctx->getNextLaunchId();

            boost::shared_ptr<MpiSlaveProxy> slave(new MpiSlaveProxy(launchId, query, installPath));
            _ctx->setSlave(launchId, slave);

            boost::shared_ptr<MpiLauncher> launcher;
            if (_mustLaunch) {
                launcher = boost::shared_ptr<MpiLauncher>(new MpiLauncher(launchId, query));
                // Perform some negative testing on the launcher object
                try {
                    vector<pid_t> pids;
                    launcher->getPids(pids);
                    throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                           << "XXXX Bug in MPI launcher: MpiLauncher::getPids did not fail");
                } catch(scidb::MpiLauncher::InvalidStateException& e) {
                    // expected
                }
                try {
                    launcher->destroy();
                    throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                           << "XXXX Bug in MPI launcher: MpiLauncher::destroy did not fail");
                } catch(scidb::MpiLauncher::InvalidStateException& e) {
                    // expected
                }
                if (launcher->isRunning()) {
                    throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                           << "XXXX Bug in MPI launcher: MpiLauncher::isRunning returned true");
                }

                _ctx->setLauncher(launchId, launcher);
                std::vector<std::string> args;
                launchMpiJob(launcher, args, membership, query, query->getInstancesCount());
            }

            // Create IPC
            string clusterUuid = Cluster::getInstance()->getUuid();
            InstanceID instanceId = Cluster::getInstance()->getLocalInstanceId();
            string ipcName = mpi::getIpcName(installPath, clusterUuid, query->getQueryID(), instanceId, launchId);

            // Construct slave command
            mpi::Command cmd;
            cmd.setCmd(string("DUMMY_COMMAND"));
            cmd.addArg(ipcName);

            try {
                slave->sendCommand(cmd, _ctx);
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "XXXX Bug in MPI slave state management: MpiSlaveProxy::sendCommand did not fail");
            } catch(scidb::MpiSlaveProxy::InvalidStateException& e) {
                // expected
            }
            try {
                slave->waitForStatus(_ctx);
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "XXXX Bug in MPI slave state management: MpiSlaveProxy::waitForStatus did not fail");
            } catch(scidb::MpiSlaveProxy::InvalidStateException& e) {
                // expected
            }
            try {
                slave->waitForExit(_ctx);
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "XXXX Bug in MPI slave state management: MpiSlaveProxy::waitForExit did not fail");
            } catch(scidb::MpiSlaveProxy::InvalidStateException& e) {
                // expected
            }

            //-------------------- Get the handshake
            slave->waitForHandshake(_ctx);

            // After the handshake the old slave must be gone
            boost::shared_ptr<MpiSlaveProxy> oldSlave = _ctx->getSlave(launchId-1);

            // cleanup the slave from the previous launch (if any)
            if (oldSlave) {
                oldSlave->destroy();
                oldSlave.reset();
            }
            _ctx->complete(launchId-1);

            boost::shared_ptr<SharedMemoryIpc> shmIpc(mpi::newSharedMemoryIpc(ipcName));
            _ctx->addSharedMemoryIpc(launchId, shmIpc);

            LOG4CXX_DEBUG(logger, "XXXX IPC name = " << ipcName);

            // Perform some negative testing on the shared memory object
            const uint64_t SMALL_SHM_SIZE = 777;
            try {
                shmIpc->truncate(SMALL_SHM_SIZE);
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "XXXX Bug in mapping shared memory: SharedMemoryIpc::truncate did not fail");
            } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
                // expected
            }
            try {
                shmIpc->getSize();
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "XXXX Bug in mapping shared memory: SharedMemoryIpc::getSize did not fail");
            } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
                // expected
            }
            try {
                shmIpc->get();
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "XXXX Bug in mapping shared memory: SharedMemoryIpc::get did not fail");
            } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
                // expected
            }

            char* ptr(NULL);

            try {
                shmIpc->create(SharedMemoryIpc::RDWR);
            } catch(scidb::SharedMemoryIpc::SystemErrorException& e) {
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                       << string("shared_memory_mmap ")+e.what());
            } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << string("XXXX Bug in creating shared memory: ") + e.what());
            }
            if (shmIpc->getName() != ipcName
                || shmIpc->getSize() != 0
                || shmIpc->getAccessMode() != SharedMemoryIpc::RDWR) {
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "XXXX Bug in creating shared memory object: SharedMemoryIpc::get* returned invalid value");
            }
            try {
                shmIpc->truncate(SMALL_SHM_SIZE);
            } catch(scidb::SharedMemoryIpc::SystemErrorException& e) {
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                       << string("shared_memory_mmap ")+e.what());
            } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << string("XXXX Bug in truncating shared memory: ")+e.what());
            }

            if (shmIpc->getName() != ipcName
                || shmIpc->getSize() != SMALL_SHM_SIZE
                || shmIpc->getAccessMode() != SharedMemoryIpc::RDWR) {
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "XXXX Bug in truncating shared memory: SharedMemoryIpc::get* returned invalid value");
            }
            try {
                ptr = reinterpret_cast<char*>(shmIpc->get());
            } catch(scidb::SharedMemoryIpc::SystemErrorException& e) {
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                       << string("shared_memory_mmap ")+e.what());
            } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << string("XXXX Bug in mapping shared memory: ") + e.what());
            }
            if (!ptr
                || shmIpc->getName() != ipcName
                || shmIpc->getSize() != SMALL_SHM_SIZE
                || shmIpc->getAccessMode() != SharedMemoryIpc::RDWR) {
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "XXXX Bug in mapping shared memory: SharedMemoryIpc::get* returned invalid value");
            }
            try {
                shmIpc->truncate(SMALL_SHM_SIZE);
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "XXXX Bug in mapping shared memory: SharedMemoryIpc::truncate did not fail");
            } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
                // expected
            }

            shmIpc->close();

            try {
                shmIpc->truncate(SMALL_SHM_SIZE, true);
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "XXXX Bug in mapping shared memory: SharedMemoryIpc::truncate did not fail");
            } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
                // expected
            }
            if (!ptr
                || shmIpc->getName() != ipcName
                || shmIpc->getSize() != SMALL_SHM_SIZE
                || shmIpc->getAccessMode() != SharedMemoryIpc::RDWR) {
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "XXXX Bug in mapping shared memory: SharedMemoryIpc::get* returned invalid value");
            }
            try {
                shmIpc->truncate(SMALL_SHM_SIZE);
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "XXXX Bug in mapping shared memory: SharedMemoryIpc::truncate did not fail");
            } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
                // expected
            }

            char digits[10] = { '0', '1', '2', '3','4','5','6','7','8','9' };
            ptr[SMALL_SHM_SIZE-1] = '\0';
            for (size_t i=0; i < (SMALL_SHM_SIZE-1); ++i) {
                ptr[i] = digits[instanceId%10];
            }

            LOG4CXX_TRACE(logger, "BUF:\n" << ptr );

            // Send command to slave
            slave->sendCommand(cmd, _ctx);

            // Get the command result
            slave->waitForStatus(_ctx);

            cmd.clear();
            cmd.setCmd(string("EXIT"));

            slave->sendCommand(cmd, _ctx);

            // Wait for the slave to disconnect
            slave->waitForExit(_ctx);

            try {
                slave->sendCommand(cmd, _ctx);
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "XXXX Bug in MPI slave state management: MpiSlaveProxy::sendCommand did not fail");
            } catch(scidb::MpiSlaveProxy::InvalidStateException& e) {
                // expected
            }

            try {
                slave->waitForStatus(_ctx);
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "XXXX Bug in MPI slave state management: MpiSlaveProxy::waitForStatus did not fail");
            } catch(scidb::MpiSlaveProxy::InvalidStateException& e) {
                // expected
            }
            try {
                slave->waitForExit(_ctx);
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "XXXX Bug in MPI slave state management: MpiSlaveProxy::waitForExit did not fail");
            } catch(scidb::MpiSlaveProxy::InvalidStateException& e) {
                // expected
            }

            shmIpc->close();
            if (!shmIpc->remove()) {
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "shared_memory_remove");
            }
            shmIpc->unmap();
            if (_mustLaunch) {
                launcher->destroy();
            }
        }
    }

    void testEcho(const std::string& installPath,
                  const boost::shared_ptr<const InstanceMembership>& membership,
                  boost::shared_ptr<Query>& query)
    {
        LOG4CXX_DEBUG(logger, "XXXX ECHO test");
	uint64_t launchId =_ctx->getNextLaunchId();

	boost::shared_ptr<MpiSlaveProxy> slave(new MpiSlaveProxy(launchId, query, installPath));
	_ctx->setSlave(launchId, slave);

	boost::shared_ptr<MpiLauncher> launcher;
	if (_mustLaunch) {
	    launcher = boost::shared_ptr<MpiLauncher>(new MpiLauncher(launchId, query));
	    _ctx->setLauncher(launchId, launcher);
            std::vector<std::string> args;
	    launchMpiJob(launcher, args, membership, query, query->getInstancesCount());
	}

	// Get the handshake
	slave->waitForHandshake(_ctx);

	// After the handshake the old slave must be gone
	boost::shared_ptr<MpiSlaveProxy> oldSlave = _ctx->getSlave(launchId-1);
	if (oldSlave) {
	    oldSlave->destroy();
	    oldSlave.reset();
	}
	_ctx->complete(launchId-1);

	//-------------------- Create IPCs
	string clusterUuid = Cluster::getInstance()->getUuid();
	InstanceID instanceId = Cluster::getInstance()->getLocalInstanceId();
	string ipcName = mpi::getIpcName(installPath, clusterUuid, query->getQueryID(), instanceId, launchId);

	string ipcNameIn = ipcName+".in";
	string ipcNameOut = ipcName+".out";

        LOG4CXX_DEBUG(logger, "XXXX IPC name.in = " << ipcNameIn);
        LOG4CXX_DEBUG(logger, "XXXX IPC name.out = " << ipcNameOut);

	boost::shared_ptr<SharedMemoryIpc> shmIpcIn(mpi::newSharedMemoryIpc(ipcNameIn));
	_ctx->addSharedMemoryIpc(launchId, shmIpcIn);

	boost::shared_ptr<SharedMemoryIpc> shmIpcOut(mpi::newSharedMemoryIpc(ipcNameOut));
	_ctx->addSharedMemoryIpc(launchId, shmIpcOut);

	const int64_t LARGE_SHM_SIZE = 64*1024*1024;

	char* ptrIn(NULL);
	char* ptrOut(NULL);
        try {
            shmIpcIn->create(SharedMemoryIpc::RDWR);
            shmIpcIn->truncate(LARGE_SHM_SIZE);
            ptrIn = reinterpret_cast<char*>(shmIpcIn->get());
            shmIpcOut->create(SharedMemoryIpc::RDWR);
            shmIpcOut->truncate(LARGE_SHM_SIZE);
            ptrOut = reinterpret_cast<char*>(shmIpcOut->get());
        } catch(scidb::SharedMemoryIpc::SystemErrorException& e) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                   << string("XXXX Cannot map shared memory: ") + e.what());
        } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << string("XXXX Bug in mapping shared memory: ")+ e.what());
        }

        char digits[10] = { '0', '1', '2', '3','4','5','6','7','8','9' };
        ptrIn[LARGE_SHM_SIZE-1] = '\0';
        ptrOut[LARGE_SHM_SIZE-1] = '\0';

        memset(ptrIn, digits[instanceId%10], (LARGE_SHM_SIZE-1));
        memset(ptrOut, ~digits[instanceId%10], (LARGE_SHM_SIZE-1));

        mpi::Command cmd;
        cmd.setCmd(string("ECHO"));
        cmd.addArg(ipcNameIn);
        cmd.addArg(ipcNameOut);

        slave->sendCommand(cmd, _ctx);

        LOG4CXX_DEBUG(logger, "XXXX Checking slave status");

        try {
            slave->waitForStatus(_ctx);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "XXXX Bug in reporting error from slave: MpiSlave::waitForStatus did not report failure");
        } catch(scidb::SystemException& e) {
            if (e.getLongErrorCode() != SCIDB_LE_OPERATION_FAILED) {
                throw;
            }
        }
        // Check the shared memory contents
        if (::memcmp(ptrIn, ptrOut, LARGE_SHM_SIZE) != 0) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "XXXX Bug in echoing data from MPI slave: corrupt data");
        }
        cmd.clear();
        cmd.setCmd(string("EXIT"));

        slave->sendCommand(cmd, _ctx);

        slave->waitForExit(_ctx);

        // Cleanup for object no longer in use
        try {
            shmIpcOut->truncate(0,true);
            shmIpcIn->truncate(0,true);
        } catch(scidb::SharedMemoryIpc::SystemErrorException& e) {
            LOG4CXX_WARN(logger, "XXXX Cannot truncate shared memory: " << e.what());
        } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << string("XXXX Bug in truncating shared memory: ") + e.what());
        }
        shmIpcIn->unmap();
        shmIpcOut->unmap();
        shmIpcIn->close();
        shmIpcOut->close();
        if (!shmIpcOut->remove()) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "shared_memory_remove");
        }
        if (!shmIpcIn->remove()) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "shared_memory_remove");
        }
        if (_mustLaunch) {
            launcher->destroy();
        }
    }

    static const uint32_t SLAVE_TIMEOUT_SEC = 5; // (minimum timeout supported by Event,Semaphore)/2
    static const uint32_t SLAVE_DELAY_SEC = 10 * 2; //(minimum timeout supported by Event,Semaphore)*2

    void testSlowSlave(const std::string& installPath,
                        const boost::shared_ptr<const InstanceMembership>& membership,
                        boost::shared_ptr<Query>& query)
    {
        LOG4CXX_DEBUG(logger, "XXXX SLOW_SLAVE test");
	uint64_t launchId =_ctx->getNextLaunchId();

	boost::shared_ptr<MpiSlaveProxy> slave(new MpiSlaveProxy(launchId, query, installPath, SLAVE_TIMEOUT_SEC));
	_ctx->setSlave(launchId, slave);

	boost::shared_ptr<MpiLauncher> launcher;
	if (_mustLaunch) {
	    launcher = boost::shared_ptr<MpiLauncher>(new MpiLauncher(launchId, query));
	    _ctx->setLauncher(launchId, launcher);
            stringstream ss;
            ss << SLAVE_DELAY_SEC;
            std::vector<std::string> args;
            args.push_back(ss.str());
	    launchMpiJob(launcher, args, membership, query, query->getInstancesCount());
	}

	// slave should delay sending a handshake
        try {
            slave->waitForHandshake(_ctx);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "XXXX Bug in MPI slave: MpiSlaveProxy::waitForHandshake did not timeout");
        } catch (scidb::SystemException& e) {
            if (e.getLongErrorCode() != SCIDB_LE_OPERATION_FAILED) {
                throw;
            }
            // expected
        }
        bool ok=false;
        const uint32_t maxTries = scidb::getLivenessTimeout() / SLAVE_TIMEOUT_SEC ;
        for (uint32_t i=0; i < maxTries; ++i) {
            try {
                slave->waitForHandshake(_ctx);
                ok = true;
                break;
            } catch (scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_OPERATION_FAILED) {
                    throw;
                }
                if (e.getErrorMessage().find("MPI slave process failed to communicate in time")
                    == std::string::npos) {
                    throw;
                }
                // expected
            }
        }

        if (!ok) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "MpiSlaveProxy::waitForHandshake timeout");
        }

	// After the handshake the old slave must be gone
	boost::shared_ptr<MpiSlaveProxy> oldSlave = _ctx->getSlave(launchId-1);
	if (oldSlave) {
	    oldSlave->destroy();
	    oldSlave.reset();
	}
	_ctx->complete(launchId-1);

        // Send command
        mpi::Command cmd;
        cmd.setCmd(string("SLOW_SLAVE"));
        stringstream ss;
        ss << SLAVE_DELAY_SEC;
        cmd.addArg(ss.str());

        slave->sendCommand(cmd, _ctx);

        // slave should send an unexpected response
        if (slave->waitForStatus(_ctx, false) != static_cast<uint64_t>(SLAVE_DELAY_SEC)) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "XXXX Bug in MPI slave: MpiSlaveProxy::waitForStatus did not fail on bad status");
        }
        cmd.clear();
        cmd.setCmd(string("EXIT"));

        slave->sendCommand(cmd, _ctx);

        try {
            // slave should delay exiting
            slave->waitForExit(_ctx);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "XXXX Bug in MPI slave: MpiSlaveProxy::waitForExit did not timeout");
        } catch (scidb::SystemException& e) {
            if (e.getLongErrorCode() != SCIDB_LE_OPERATION_FAILED) {
                throw;
            }
            if (e.getErrorMessage().find("MPI slave process failed to communicate in time")
                == std::string::npos) {
                throw;
            }
            // expected
        }
        ok=false;
        for (uint32_t i=0; i < maxTries; ++i) {
            try {
                slave->waitForExit(_ctx);
                ok = true;
                break;
            } catch (scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_OPERATION_FAILED) {
                    throw;
                }
                if (e.getErrorMessage().find("MPI slave process failed to communicate in time")
                    == std::string::npos) {
                    throw;
                }
                // expected
            }
        }
        if (!ok) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "MpiSlaveProxy::waitForExit timeout");
        }
        if (_mustLaunch) {
            launcher->destroy();
        }
    }

    void testSlaveExit(const std::string& installPath,
                          const boost::shared_ptr<const InstanceMembership>& membership,
                          boost::shared_ptr<Query>& query)
    {
        LOG4CXX_DEBUG(logger, "XXXX ABNORMAL_EXIT test");
	uint64_t launchId =_ctx->getNextLaunchId();

	boost::shared_ptr<MpiSlaveProxy> slave(new MpiSlaveProxy(launchId, query, installPath, SLAVE_TIMEOUT_SEC));
	_ctx->setSlave(launchId, slave);

	boost::shared_ptr<MpiLauncher> launcher;
	if (_mustLaunch) {
	    launcher = boost::shared_ptr<MpiLauncher>(new MpiLauncher(launchId, query));
	    _ctx->setLauncher(launchId, launcher);
            std::vector<std::string> args;
	    launchMpiJob(launcher, args, membership, query, query->getInstancesCount());
	}

	//Get the handshake
        bool ok=false;
        const uint32_t maxTries = 1 + scidb::getLivenessTimeout() / SLAVE_TIMEOUT_SEC ;
        for (uint32_t i=0; i < maxTries; ++i) {
            try {
                slave->waitForHandshake(_ctx);
                ok = true;
                break;
            } catch (scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_OPERATION_FAILED) {
                    throw;
                }
                if (e.getErrorMessage().find("MPI slave process failed to communicate in time")
                    == std::string::npos) {
                    throw;
                }
                // expected
            }
        }
        if (!ok) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "MpiSlaveProxy::waitForHandshake timeout");
        }


	// After the handshake the old slave must be gone
	boost::shared_ptr<MpiSlaveProxy> oldSlave = _ctx->getSlave(launchId-1);
	if (oldSlave) {
	    oldSlave->destroy();
	    oldSlave.reset();
	}
	_ctx->complete(launchId-1);

        // Send command
        mpi::Command cmd;
        cmd.setCmd(string("ABNORMAL_EXIT"));
        const int SLAVE_ERR_EXIT_CODE = 7;
        stringstream ss;
        ss << SLAVE_ERR_EXIT_CODE;
        cmd.addArg(ss.str());

        slave->sendCommand(cmd, _ctx);

        // slave should not respond
        try {
            slave->waitForStatus(_ctx);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "XXXX Bug in MPI slave: MpiSlaveProxy::waitForStatus did not fail");
        } catch (scidb::SystemException& e) {
            if (e.getLongErrorCode() != SCIDB_LE_UNKNOWN_ERROR) {
                throw;
            }
            if (e.getErrorMessage().find("disconnected prematurely")
                == std::string::npos) {
                throw;
            }
            // expected
        }
        // the EOF message should already be consumed by waitForStatus
        try {
            slave->waitForExit(_ctx);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "XXXX Bug in MPI slave: MpiSlaveProxy::waitForExit did not timeout");
        } catch (scidb::SystemException& e) {
            if (e.getLongErrorCode() != SCIDB_LE_OPERATION_FAILED) {
                throw;
            }
            // expected
        }

        // launcher should fail to complete cleanly
        if (_mustLaunch) {
            try {
                launcher->destroy();
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "XXXX Bug in MPI launcher: MpiLauncher did not fail to be destroyed");
            } catch (scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_OPERATION_FAILED) {
                    throw;
                }
                // expected
            }
        }
    }

    void testBadMessage(const std::string& installPath,
                        const boost::shared_ptr<const InstanceMembership>& membership,
                        boost::shared_ptr<Query>& query)
    {
        LOG4CXX_DEBUG(logger, "XXXX BAD_MSG from slave test");
	uint64_t launchId =_ctx->getNextLaunchId();

	boost::shared_ptr<MpiSlaveProxy> slave(new MpiSlaveProxy(launchId, query, installPath));
	_ctx->setSlave(launchId, slave);

	boost::shared_ptr<MpiLauncher> launcher;
	if (_mustLaunch) {
	    launcher = boost::shared_ptr<MpiLauncher>(new MpiLauncher(launchId, query));
	    _ctx->setLauncher(launchId, launcher);
            std::vector<std::string> args;
            launchMpiJob(launcher, args, membership, query, query->getInstancesCount());
	}

	//-------------------- Get the handshake
	slave->waitForHandshake(_ctx);

	// After the handshake the old slave must be gone
	boost::shared_ptr<MpiSlaveProxy> oldSlave = _ctx->getSlave(launchId-1);
	if (oldSlave) {
	    oldSlave->destroy();
	    oldSlave.reset();
	}
	_ctx->complete(launchId-1);

        mpi::Command cmd;
        cmd.setCmd(string("BAD_MSG"));

        slave->sendCommand(cmd, _ctx);

        LOG4CXX_DEBUG(logger, "XXXX Checking slave status");
        try {
            slave->waitForStatus(_ctx);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "XXXX Bug in detecting invalid status from slave: MpiSlave::waitForStatus did not report invalid status");
        } catch(scidb::SystemException& e) {
            if (e.getLongErrorCode() != SCIDB_LE_UNKNOWN_ERROR) {
                throw;
            }
            if (e.getErrorMessage().find("invalid status") == std::string::npos) {
                throw;
            }
        }

        // slave should be disconnected
        slave->waitForExit(_ctx);

        if (_mustLaunch) {
            try {
                launcher->destroy();
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "XXXX Bug in MPI launcher: MpiLauncher did not fail to be destroyed");
            } catch (scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_OPERATION_FAILED) {
                    throw;
                }
                // expected
            }
        }
        // clear all the bogus messages the slave has sent us
        _ctx->complete(0);
    }

    void testBadMessageFlood(const std::string& installPath,
                             const boost::shared_ptr<const InstanceMembership>& membership,
                             boost::shared_ptr<Query>& query)
    {
        LOG4CXX_DEBUG(logger, "XXXX BAD_MSG_FLOOD from slave test");
	uint64_t launchId =_ctx->getNextLaunchId();

	boost::shared_ptr<MpiSlaveProxy> slave(new MpiSlaveProxy(launchId, query, installPath));
	_ctx->setSlave(launchId, slave);

	boost::shared_ptr<MpiLauncher> launcher;
	if (_mustLaunch) {
	    launcher = boost::shared_ptr<MpiLauncher>(new MpiLauncher(launchId, query));
	    _ctx->setLauncher(launchId, launcher);
            std::vector<std::string> args;
            launchMpiJob(launcher, args, membership, query, query->getInstancesCount());
	}

	// Get the handshake
	slave->waitForHandshake(_ctx);

	// After the handshake the old slave must be gone
	boost::shared_ptr<MpiSlaveProxy> oldSlave = _ctx->getSlave(launchId-1);
	if (oldSlave) {
	    oldSlave->destroy();
	    oldSlave.reset();
	}
	_ctx->complete(launchId-1);

        mpi::Command cmd;
        cmd.setCmd(string("BAD_MSG_FLOOD"));

        slave->sendCommand(cmd, _ctx);

        LOG4CXX_DEBUG(logger, "XXXX Checking slave status");
        slave->waitForStatus(_ctx);

        cmd.clear();
        cmd.setCmd(string("EXIT"));

        slave->sendCommand(cmd, _ctx);

        // slave should be disconnected
        slave->waitForExit(_ctx);

        if (_mustLaunch) {
            launcher->destroy();
        }
        // clear all the bogus messages the slave has sent us
        _ctx->complete(0);
    }

    void testBadHandshake(const std::string& installPath,
                          const boost::shared_ptr<const InstanceMembership>& membership,
                          boost::shared_ptr<Query>& query)
    {
        LOG4CXX_DEBUG(logger, "XXXX BAD_HANDSHAKE from slave test");
	uint64_t launchId =_ctx->getNextLaunchId();

	boost::shared_ptr<MpiSlaveProxy> slave(new MpiSlaveProxy(launchId, query, installPath));
	_ctx->setSlave(launchId, slave);

	boost::shared_ptr<MpiLauncher> launcher;
	if (_mustLaunch) {
	    launcher = boost::shared_ptr<MpiLauncher>(new MpiLauncher(launchId, query));
	    _ctx->setLauncher(launchId, launcher);
            std::vector<std::string> args;
            launchMpiJob(launcher, args, membership, query, query->getInstancesCount());
	}

	// Get the handshake
	slave->waitForHandshake(_ctx);

	// After the handshake the old slave must be gone
	boost::shared_ptr<MpiSlaveProxy> oldSlave = _ctx->getSlave(launchId-1);
	if (oldSlave) {
	    oldSlave->destroy();
	    oldSlave.reset();
	}
	_ctx->complete(launchId-1);

        mpi::Command cmd;
        cmd.setCmd(string("BAD_HANDSHAKE"));

        slave->sendCommand(cmd, _ctx);

        LOG4CXX_DEBUG(logger, "XXXX Checking slave status");
        try {
            slave->waitForStatus(_ctx);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "XXXX Bug in detecting invalid status from slave: MpiSlave::waitForStatus did not report invalid status");
        } catch(scidb::SystemException& e) {
            if (e.getLongErrorCode() != SCIDB_LE_UNKNOWN_ERROR) {
                throw;
            }
            if (e.getErrorMessage().find("invalid status") == std::string::npos) {
                throw;
            }
        }

        cmd.clear();
        cmd.setCmd(string("EXIT"));

        slave->sendCommand(cmd, _ctx);

        // slave should be disconnected
        slave->waitForExit(_ctx);

        if (_mustLaunch) {
            launcher->destroy();
        }
        // clear all the bogus messages the slave has sent us
        _ctx->complete(0);
    }

    void testBadStatus(const std::string& installPath,
                       const boost::shared_ptr<const InstanceMembership>& membership,
                       boost::shared_ptr<Query>& query)
    {
        LOG4CXX_DEBUG(logger, "XXXX BAD_STATUS from slave test");
	uint64_t launchId =_ctx->getNextLaunchId();

	boost::shared_ptr<MpiSlaveProxy> slave(new MpiSlaveProxy(launchId, query, installPath));
	_ctx->setSlave(launchId, slave);

	boost::shared_ptr<MpiLauncher> launcher;
	if (_mustLaunch) {
	    launcher = boost::shared_ptr<MpiLauncher>(new MpiLauncher(launchId, query));
	    _ctx->setLauncher(launchId, launcher);
            std::vector<std::string> args;
            launchMpiJob(launcher, args, membership, query, query->getInstancesCount());
	}

	// Get the handshake
	slave->waitForHandshake(_ctx);

	// After the handshake the old slave must be gone
	boost::shared_ptr<MpiSlaveProxy> oldSlave = _ctx->getSlave(launchId-1);
	if (oldSlave) {
	    oldSlave->destroy();
	    oldSlave.reset();
	}
	_ctx->complete(launchId-1);

        mpi::Command cmd;
        cmd.setCmd(string("BAD_STATUS"));

        slave->sendCommand(cmd, _ctx);

        // slave should be disconnected
        slave->waitForExit(_ctx);

        if (_mustLaunch) {
            try {
                launcher->destroy();
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "XXXX Bug in MPI launcher: MpiLauncher did not fail to be destroyed");
            } catch (scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_OPERATION_FAILED) {
                    throw;
                }
                // expected
            }
        }
        // clear all the bogus messages the slave has sent us
        _ctx->complete(0);
    }

    private:
    bool _mustLaunch;
    boost::shared_ptr<MpiOperatorContext> _ctx;
    const uint64_t NUM_LAUNCH_TESTS;
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalMpiTest, "mpi_test", "PhysicalMpiTest");

}
