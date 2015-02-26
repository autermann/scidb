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

// defacto std
#include <boost/make_shared.hpp>

// SciDB
#include <array/Metadata.h>
#include <log4cxx/logger.h>
#include <mpi/MPILauncher.h>
#include <mpi/MPIManager.h>
#include <mpi/MPISlaveProxy.h>
#include <mpi/MPIUtils.h>
#include <query/Operator.h>
#include <query/Query.h>
#include <system/Cluster.h>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <util/shm/SharedMemoryIpc.h>

// local
#include "mpiRankMaster.hpp"
#include "mpiRankSlave.hpp" // for argument structure


using namespace std;
using namespace scidb;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.libdense_linear_algebra"));

// Simple MPI operator(s) for testing which chunks are sent to a particular rank and which ones
// are returned by a particular rank.  This is helpful for unit testing the distribution functions
// needed to support ScaLAPACK.
//
// The operator accepts one matrix as input and takes one as output.
//
// The input matrix must be set to the rank of the process to which the caller
// (typically a unit test) expects the rank to be sent.  If there is a mismatch of any cell at any
// receiving rank, then an error status is returned.
//
// The ouput matrix may be set to any value, and the slave will return in it the rank
// of the slave process that returned the value.  This should equal the value sent in the input
//
// When the system is functioning correctly, then the output matrix will match the input matrix, and
// both the input and output arrays will match, no matter what the input and output distributions were.
//

void mpirankMaster(//general args
                Query* query,
                boost::shared_ptr<MpiOperatorContext>& ctx,
                boost::shared_ptr<MpiSlaveProxy>& slave,  // need ctx->getSlave();
                const string& ipcName, // can this be in the ctx too?
                void * argsBuf,
                const sl_int_t& NPROW, const sl_int_t& NPCOL,
                const sl_int_t& MYPROW, const sl_int_t& MYPCOL, const sl_int_t& MYPNUM,
                // mpirank operator args
                double *IN,  const sl_desc_t& DESC_IN,
                double *OUT, const sl_desc_t& DESC_OUT,
                sl_int_t &INFO)
{
    enum dummy {DBG=0};
    if (DBG) {
        std::cerr << "argsBuf:" << argsBuf << std::endl;
        std::cerr << "IN:" << (void*)(IN) << std::endl;
        std::cerr << "OUT:" << (void*)(OUT) << std::endl;
    }

    // marshall all arguments except the buffers IN & OUT into a struct:
    MPIRankArgs* args = reinterpret_cast<MPIRankArgs*>(argsBuf) ;
    args->NPROW = NPROW;
    args->NPCOL = NPCOL;
    args->MYPROW = MYPROW;
    args->MYPCOL = MYPCOL;
    args->MYPNUM = MYPNUM;

    args->IN.DESC = DESC_IN ;
    args->OUT.DESC = DESC_OUT ;

    if(DBG) {
        std::cerr << "argsBuf:  ----------------------------" << std::endl ;
        std::cerr << *args << std::endl;
        std::cerr << "argsBuf:  end-------------------------" << std::endl ;
    }
    // ready to send stuff to the proxy

    //-------------------- Send command
    mpi::Command cmd;
    cmd.setCmd(string("DLAOP")); // dummy command
    cmd.addArg(ipcName);
    cmd.addArg("3"); // 3 buffers: ARGS + IN, OUT arrays
    cmd.addArg("mpirank");
    slave->sendCommand(cmd, ctx);       // at this point the command and ipcName are sent
                                        // our slave finds and maps the buffers by name
                                        // based on ipcName
    if(DBG) std::cerr << "mpirankMaster: calling slave->waitForStatus(ctx)" << std::endl ;
    slave->waitForStatus(ctx);
    if(DBG) std::cerr << "mpirankMaster: slave->waitForStatus(ctx) complete" << std::endl ;

    //-------------------- Get the result
    cmd.clear();
    cmd.setCmd(string("EXIT"));
    slave->sendCommand(cmd, ctx);
    slave->waitForExit(ctx); // wait for the slave to disconnect
}

} // namespace scidb
