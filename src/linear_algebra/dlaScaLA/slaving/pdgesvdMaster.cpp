/*
**
* BEGIN_COPYRIGHT
*
* PARADIGM4 INC.
* This file is part of the Paradigm4 Enterprise SciDB distribution kit
* and may only be used with a valid Paradigm4 contract and in accord
* with the terms and conditions specified by that contract.
*
* Copyright © 2010 - 2012 Paradigm4 Inc.
* All Rights Reserved.
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
#include <mpi/MPILauncher.h>
#include <mpi/MPIUtils.h>
#include <util/shm/SharedMemoryIpc.h>
#include <mpi/MPIManager.h>
#include <mpi/MPISlaveProxy.h>

#include "pdgesvdMaster.hpp"
#include "pdgesvdSlave.hpp"
#include "pdgesvdMasterSlave.hpp"

using namespace std;
using namespace scidb;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.libdense_linear_algebra"));


//
// the arguments to the prototype must be call-compatible with those for
// the FORTRAN pdgesvd_ subroutine, so that this can be substituted for
// calls to pdgesvd
//

// before calling this, you must have done (roughly):
// new MpiOperatorContext(query); // could be routine on query instead of setOperatorContext
// new MpiErrorHandler(ctx, NUM_LAUNCH_TESTS ???) ;
// query->pushErrorHandler(eh);
// query->pushFinalizer(boost::bind(&MpiErrorHandler::finalize, eh, _1);
// f.clear();
// eh.reset();
// query->setOperatorContext(ctx);
// membership check
// getInstallPath(membership, query); 
// new MpiSlaveProxy(...)
// ctx->setSlave()
// new MpiLauncher()
// ctx->setLauncher
// query->setOperatorContext(ctx);
// slave->waitForHandshae(ctx);
// cleanup oldSlave
void pdgesvdMaster(Query* query,  // or do I need only the ctx?
                   boost::shared_ptr<MpiOperatorContext>& ctx, // query->getOperatorCtxt returns superclass
                   boost::shared_ptr<MpiSlaveProxy>& slave,  // need ctx->getSlave();
                   const string& ipcName, // can this be in the ctx too?
                   void*  argsBuf,
                   const sl_int_t& NPROW, const sl_int_t& NPCOL,
                   const sl_int_t& MYPROW, const sl_int_t& MYPCOL, const sl_int_t& MYPNUM,
                   const char &jobU, const char &jobVT,
                   const sl_int_t& M, const sl_int_t &N, 
                   double* A, const sl_int_t& IA, const sl_int_t& JA, const sl_desc_t& DESC_A,
                   double* S, 
                   double* U,  const sl_int_t& IU,  const sl_int_t& JU,  const sl_desc_t& DESC_U,
                   double* VT, const sl_int_t& IVT, const sl_int_t& JVT, const sl_desc_t& DESC_VT,
                   sl_int_t& INFO)
{
    enum dummy { DBG=0 };
    static const char ARG_NUM_SHM_BUFFERS[] = "5";  // ARGS + A, S, U, and VT
    INFO = 1 ; 

    pdgesvdMarshallArgs(argsBuf, NPROW,  NPCOL, MYPROW, MYPCOL, MYPNUM,
                                 jobU, jobVT, M, N,
                                 NULL /*A*/,  IA,  JA,  DESC_A,
                                 NULL /*S*/,
                                 NULL /*U*/,  IU,  JU,  DESC_U,
                                 NULL /*VT*/, IVT, JVT, DESC_VT);

    //
    // ready to send stuff to the proxy
    //

    //-------------------- Send command
    mpi::Command cmd;
    cmd.setCmd(string("DLAOP")); // common command for all DLAOPS (TODO: to a header)
    cmd.addArg(ipcName);
    cmd.addArg(ARG_NUM_SHM_BUFFERS);
    cmd.addArg("pdgesvd_");             // sub-command name (TODO:factor to a header, replace in mpi_slave_scidb as well)
    slave->sendCommand(cmd, ctx);       // at this point the command and ipcName are sent
                                        // our slave finds and maps the buffers by name
                                        // based on ipcName
    if(DBG) std::cerr << "pdgesvdMaster: calling slave->waitForStatus(ctx)" << std::endl ;

    slave->waitForStatus(ctx);

    if(DBG) std::cerr << "pdgesvdMaster: slave->waitForStatus(ctx) complete" << std::endl ;

    //-------------------- Get the result
    cmd.clear();
    cmd.setCmd(string("EXIT"));         // command (TODO: factor to a header, fix all masters)
    slave->sendCommand(cmd, ctx);
    slave->waitForExit(ctx); // wait for the slave to disconnect

    INFO= 0; // success
}

} // namespace scidb
