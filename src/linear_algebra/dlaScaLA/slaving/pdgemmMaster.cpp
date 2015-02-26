/*
**
* BEGIN_COPYRIGHT
*
* Copyright © 2012 SciDB
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

#include "pdgemmMaster.hpp"
#include "pdgemmSlave.hpp" // for argument structure

using namespace std;
using namespace scidb;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.libdense_linear_algebra"));


void pdgemmMaster(Query* query,  // or do I need only the ctx?
                   boost::shared_ptr<MpiOperatorContext>& ctx, // query->getOperatorCtxt returns superclass
                   boost::shared_ptr<MpiSlaveProxy>& slave,  // need ctx->getSlave();
                   const string& ipcName, // can this be in the ctx too?
                   void*  argsBuf,
                   const sl_int_t& NPROW, const sl_int_t& NPCOL,
                   const sl_int_t& MYPROW, const sl_int_t& MYPCOL, const sl_int_t& MYPNUM,
                   const char &TRANSA, const char &TRANSB,
                   const sl_int_t& M, const sl_int_t &N, const sl_int_t &K,
                   double* ALPHA,
                   double* A, const sl_int_t& IA, const sl_int_t& JA, const sl_desc_t& DESC_A,
                   double* B, const sl_int_t& IB,  const sl_int_t& JB,  const sl_desc_t& DESC_B,
                   double* BETA,
                   double* C, const sl_int_t& IC, const sl_int_t& JC, const sl_desc_t& DESC_C,
                   sl_int_t& INFO)  // real pdgemm has no info!!!
{
    enum dummy { DBG=0 };
    static const char ARG_NUM_SHM_BUFFERS[] = "4";  // ARGS + AA, BB, CC
    INFO = 1 ;

    if(DBG) {
        std::cerr << "argsBuf:" << argsBuf << std::endl;
        std::cerr << "A:" << (void*)(A) << std::endl;
        std::cerr << "B:" << (void*)(B) << std::endl;
        std::cerr << "C:" << (void*)(C) << std::endl;
    }

    // marshall all arguments except the buffers A,B,C into a struct:
    PdgemmArgs* args = reinterpret_cast<PdgemmArgs*>(argsBuf) ;
    args->NPROW = NPROW;
    args->NPCOL = NPCOL;
    args->MYPROW = MYPROW;
    args->MYPCOL = MYPCOL;
    args->MYPNUM = MYPNUM;

    args->TRANSA = TRANSA ;
    args->TRANSB = TRANSB ;

    args->ALPHA=*ALPHA ;
    args->BETA=*BETA ;
    args->M = M ;
    args->N = N ;
    args->K = K ;

    args->A.I = IA ;
    args->A.J = JA ;
    args->A.DESC = DESC_A ;

    args->B.I = IB ;
    args->B.J = JB ;
    args->B.DESC = DESC_B ;

    args->C.I = IC ;
    args->C.J = JC ;
    args->C.DESC = DESC_C ;

    if(DBG) {
        std::cerr << "argsBuf:  ----------------------------" << std::endl ;
        std::cerr << *args << std::endl;
        std::cerr << "argsBuf:  end-------------------------" << std::endl ;
    }

    //
    // ready to send stuff to the proxy
    //

    //-------------------- Send command
    mpi::Command cmd;
    cmd.setCmd(string("DLAOP")); // common command for all DLAOPS (TODO: to a header)
    cmd.addArg(ipcName);
    cmd.addArg(ARG_NUM_SHM_BUFFERS);
    cmd.addArg("pdgemm_");             // sub-command name (TODO:factor to a header, replace in mpi_slave as well)
    slave->sendCommand(cmd, ctx);       // at this point the command and ipcName are sent
                                        // our slave finds and maps the buffers by name
                                        // based on ipcName
    if(DBG) std::cerr << "pdgemmMaster: calling slave->waitForStatus(ctx)" << std::endl ;

    slave->waitForStatus(ctx);

    if(DBG) std::cerr << "pdgemmMaster: slave->waitForStatus(ctx) complete" << std::endl ;

    //-------------------- Get the result
    cmd.clear();
    cmd.setCmd(string("EXIT"));         // command (TODO: factor to a header, fix all masters)
    slave->sendCommand(cmd, ctx);
    slave->waitForExit(ctx); // wait for the slave to disconnect

    INFO= 0; // success
}

} // namespace scidb
