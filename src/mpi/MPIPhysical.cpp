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

///
/// @file MPIPhysical.cpp
///
///

// std C++
#include <sstream>
#include <string>

// std C
#include <time.h>

// de-facto standards
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_array.hpp>
#include <log4cxx/logger.h>

// SciDB
#include <array/Metadata.h>
#include <log4cxx/logger.h>
#include <query/Operator.h>

#include <system/Exceptions.h>
#include <system/Utils.h>

#include <mpi/MPIUtils.h>
#include <mpi/MPISlaveProxy.h>
#include <mpi/MPILauncher.h>
#include <mpi/MPIPhysical.hpp>
#include <util/shm/SharedMemoryIpc.h>

using namespace boost;

namespace scidb {
static const bool DBG = false;
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.mpi"));

///
/// some operators may not be able to work in degraded mode while they are being implemented
/// this call can make them exit if that is the case.
/// TODO: add a more explicit message of what is happening
void throwIfDegradedMode(shared_ptr<Query>& query) {
    const boost::shared_ptr<const InstanceMembership> membership =
    Cluster::getInstance()->getInstanceMembership();
    if ((membership->getViewId() != query->getCoordinatorLiveness()->getViewId()) ||
        (membership->getInstances().size() != query->getInstancesCount())) {
        // because we can't yet handle the extra data from
        // replicas that we would be fed in "degraded mode"
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM2);
    }
}

void MPIPhysical::setQuery(const boost::shared_ptr<Query>& query)
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

void MPIPhysical::launchMPISlaves(shared_ptr<Query>& query, const size_t maxSlaves)
{
    if(DBG) std::cerr << "launchNewMPISlave slave creation" << std::endl ;
    Cluster* cluster = Cluster::getInstance();

    const boost::shared_ptr<const InstanceMembership> membership =
       cluster->getInstanceMembership();

    const string& installPath = MpiManager::getInstallPath(membership);
    _launchId = _ctx->getNextLaunchId();

    boost::shared_ptr<MpiSlaveProxy> slave(new MpiSlaveProxy(_launchId, query, installPath));
    _ctx->setSlave(_launchId, slave);

    _mustLaunch = (query->getCoordinatorID() == COORDINATOR_INSTANCE);
    if (_mustLaunch) {
        _launcher = boost::shared_ptr<MpiLauncher>(new MpiLauncher(_launchId, query));
        _ctx->setLauncher(_launchId, _launcher);
        std::vector<std::string> args;
        _launcher->launch(args, membership, maxSlaves);
    }

    //-------------------- Get the handshake
    if(DBG) std::cerr << "launchNewMPISlave slave waitForHandshake 1" << std::endl ;
    if(DBG) std::cerr << "-------------------------------------" << std::endl ;
    slave->waitForHandshake(_ctx);
    if(DBG) std::cerr << "launchNewMPISlave slave waitForHandshake 1 done" << std::endl ;

    // After the handshake the old slave must be gone
    boost::shared_ptr<MpiSlaveProxy> oldSlave = _ctx->getSlave(_launchId-1);
    if (oldSlave) {
        if(DBG) std::cerr << "launchNewMPISlave oldSlave->destroy()" << std::endl ;
        oldSlave->destroy();
        oldSlave.reset();
    }
    _ctx->complete(_launchId-1);

    _ipcName = mpi::getIpcName(installPath, cluster->getUuid(), query->getQueryID(),
                               cluster->getLocalInstanceId(), _launchId);
}



// XXX TODO: consider returning std::vector<scidb::SharedMemoryPtr>
// XXX TODO: which would require supporting different types of memory (double, char etc.)
std::vector<MPIPhysical::SMIptr_t> MPIPhysical::allocateMPISharedMemory(size_t numBufs,
                                                                        size_t elemSizes[],
                                                                        size_t numElems[],
		                                                                string dbgNames[])
{
	if(DBG) {
		std::cerr << "SHM ALLOCATIONS:@@@@@@@@@@@@@@@@@@@" << std::endl ;
		for(size_t ii=0; ii< numBufs; ii++) {
		    std::cerr << "numElems["<<ii<<"] "<< dbgNames[ii] << " len = " << numElems[0] << std::endl;
		}
	}

	std::vector<SMIptr_t> shmIpc(numBufs);

	for(size_t ii=0; ii<numBufs; ii++) {
		std::stringstream suffix;
		suffix << "." << ii ;
		std::string ipcNameFull= _ipcName + suffix.str();
		LOG4CXX_TRACE(logger, "IPC name = " << ipcNameFull);
		shmIpc[ii] = SMIptr_t(mpi::newSharedMemoryIpc(ipcNameFull)); // can I get 'em off ctx instead?
		_ctx->addSharedMemoryIpc(_launchId, shmIpc[ii]);

		try {
			shmIpc[ii]->create(SharedMemoryIpc::RDWR);
			shmIpc[ii]->truncate(elemSizes[ii] * numElems[ii]);
		} catch(SharedMemoryIpc::SystemErrorException& e) {
			std::stringstream ss; ss << "shared_memory_mmap " << e.what();
			throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << ss.str()) ;
		} catch(SharedMemoryIpc::InvalidStateException& e) {
			throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << e.what());
		}
	}
    return shmIpc;
}



} // namespace
