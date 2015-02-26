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

/*
 * entry.cpp
 *
 *  Created on: Dec 28, 2009
 *      Author: roman.simakov@gmail.com
 */

// include log4cxx header files.
#include <log4cxx/logger.h>
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/propertyconfigurator.h>
#include <log4cxx/helpers/exception.h>

#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/asio.hpp>

#include <dlfcn.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/resource.h>

#include "network/NetworkManager.h"
#include "system/SciDBConfigOptions.h"
#include "system/Config.h"
#include "util/JobQueue.h"
#include "util/ThreadPool.h"
#include "system/Constants.h"
#include "query/QueryProcessor.h"
#include "query/executor/SciDBExecutor.h"
#include "util/PluginManager.h"
#include "smgr/io/Storage.h"

// to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.entry"));

using namespace scidb;

boost::shared_ptr<ThreadPool> messagesThreadPool;

void scidb_termination_handler(int signum)
{
    NetworkManager::shutdown();
}

void runSciDB()
{
   struct sigaction action;
   action.sa_handler = scidb_termination_handler;
   sigemptyset(&action.sa_mask);
   action.sa_flags = 0;
   sigaction (SIGINT, &action, NULL);
   sigaction (SIGTERM, &action, NULL);

   Config *cfg = Config::getInstance();
   assert(cfg);

   // Configuring loggers
   const std::string& log4cxxProperties = cfg->getOption<string>(CONFIG_LOG4CXX_PROPERTIES);
   if (log4cxxProperties.empty()) {
      log4cxx::BasicConfigurator::configure();
      const std::string& log_level = cfg->getOption<string>(CONFIG_LOG_LEVEL);
      log4cxx::LoggerPtr rootLogger(log4cxx::Logger::getRootLogger());
      rootLogger->setLevel(log4cxx::Level::toLevel(log_level));
   }
   else {
      log4cxx::PropertyConfigurator::configure(log4cxxProperties.c_str());
   }

   LOG4CXX_INFO(logger, "Start SciDB instance (pid="<<getpid()<<"). " << SCIDB_BUILD_INFO_STRING(". "));
   LOG4CXX_INFO(logger, "Configuration:\n" << cfg->toString());   

   if (cfg->getOption<int>(CONFIG_MAX_MEMORY_LIMIT) > 0)
   {
       size_t maxMem = ((int64_t) cfg->getOption<int>(CONFIG_MAX_MEMORY_LIMIT)) * 1024 * 1024;
       LOG4CXX_DEBUG(logger, "Capping maximum memory:");

       struct rlimit rlim;
       if (getrlimit(RLIMIT_AS, &rlim) != 0)
       {
           LOG4CXX_DEBUG(logger, ">getrlimit call failed with errno "<<errno<<"; memory cap not set.");
       }
       else
       {
           if (rlim.rlim_cur == RLIM_INFINITY || rlim.rlim_cur > maxMem)
           {
               rlim.rlim_cur = maxMem;
               if (setrlimit(RLIMIT_AS, &rlim) != 0)
               {
                   LOG4CXX_DEBUG(logger, ">setrlimit call failed with errno "<<errno<<"; memory cap not set.");
               }
               else
               {
                   LOG4CXX_DEBUG(logger, ">memory cap set to " << rlim.rlim_cur  << " bytes.");
               }
           }
           else
           {
               LOG4CXX_DEBUG(logger, ">memory cap "<<rlim.rlim_cur<<" is already under "<<maxMem<<"; not changed.");
           }
       }
   }

   boost::shared_ptr<JobQueue> messagesJobQueue = boost::make_shared<JobQueue>();

   // Here we can play with thread number
   // TODO: For SG operations probably we should have separate thread pool
   messagesThreadPool = make_shared<ThreadPool>(cfg->getOption<int>(CONFIG_MAX_JOBS), messagesJobQueue);

   SystemCatalog* catalog = SystemCatalog::getInstance();
   try
   {
       catalog->connect(Config::getInstance()->getOption<string>(CONFIG_CATALOG_CONNECTION_STRING));
   }
   catch (const std::exception &e)
   {
       LOG4CXX_ERROR(logger, "System catalog connection failed: " << e.what());
       _exit(1);
   }
   int errorCode = 0;
   try
   {
       const bool initializeCluster = Config::getInstance()->getOption<bool>(CONFIG_INITIALIZE);
       if (!catalog->isInitialized() || initializeCluster)
       {
           catalog->initializeCluster();
       }

       TypeLibrary::registerBuiltInTypes();

       FunctionLibrary::getInstance()->registerBuiltInFunctions();

       // Force preloading builtin operators
       OperatorLibrary::getInstance();

       PluginManager::getInstance()->preLoadLibraries();

       messagesThreadPool->start();
       NetworkManager::getInstance()->run(messagesJobQueue);
   }
   catch (const std::exception &e)
   {
       LOG4CXX_ERROR(logger, "Error during SciDB execution: " << e.what());
       errorCode = 1;
   }
   try
   {
      Query::freeQueries();
      if (messagesThreadPool) {
         messagesThreadPool->stop();
      }
      StorageManager::getInstance().close();
   }
   catch (const std::exception &e)
   {
      LOG4CXX_ERROR(logger, "Error during SciDB exit: " << e.what());
      errorCode = 1;
   }
   LOG4CXX_INFO(logger, "SciDB instance. " << SCIDB_BUILD_INFO_STRING(". ") << " is exiting.");
   log4cxx::Logger::getRootLogger()->setLevel(log4cxx::Level::getOff());
   _exit(errorCode);
}

void printPrefix(const char * msg="")
{
   time_t t = time(NULL);
   assert(t!=(time_t)-1);
   struct tm *date = localtime(&t);
   assert(date);
   if (date) {
      cerr << date->tm_year+1900<<"-"
           << date->tm_mon+1<<"-"
           << date->tm_mday<<" "
           << date->tm_hour<<":"
           << date->tm_min<<":"
           << date->tm_sec   
           << " ";
   }
   cerr << "(ppid=" << getpid() << "): " << msg;
}
void handleFatalError(const int err, const char * msg)
{
   printPrefix(msg);
   cerr << ": "
        << err << ": "
        << strerror(err) << endl;
   exit(1);
}

int controlPipe[2];

void setupControlPipe()
{
   close(controlPipe[0]);
   close(controlPipe[1]);
   if (pipe(controlPipe)) {
      handleFatalError(errno,"pipe() failed");
   }
}

void checkPort()
{
    try {
       boost::asio::io_service ioService;
       boost::asio::ip::tcp::acceptor 
            testAcceptor(ioService,
                         boost::asio::ip::tcp::endpoint(
                              boost::asio::ip::tcp::v4(),
                              Config::getInstance()->getOption<int>(CONFIG_PORT)));
       testAcceptor.close();
       ioService.stop();
    } catch (const boost::system::system_error& e) {
       printPrefix();
       cerr << e.what()
            << ". Exiting."
            << endl;
       exit(1);
    }
}

void terminationHandler(int signum)
{
   unsigned char byte = 1;
   write(controlPipe[1], &byte, sizeof(byte));
   printPrefix("Terminated.\n");
   exit(0);
}

void setupTerminationHandler()
{
   controlPipe[0] = controlPipe[1] = -1;
   struct sigaction action;
   action.sa_handler = terminationHandler;
   sigemptyset(&action.sa_mask);
   action.sa_flags = 0;
   sigaction (SIGINT, &action, NULL);
   sigaction (SIGTERM, &action, NULL);
}

void handleExitStatus(int status, pid_t childPid)
{
   if (WIFSIGNALED(status)) {
      printPrefix();
      cerr << "SciDB child (pid="<<childPid<<") terminated by signal = "
           << WTERMSIG(status) << (WCOREDUMP(status)? ", core dumped" : "")
           << endl;
   }
   if (WIFEXITED(status)) {
      printPrefix();
      cerr << "SciDB child (pid="<<childPid<<") exited with status = "
           << WEXITSTATUS(status)
           << endl;
   }
}

void runWithWatchdog()
{
   setupTerminationHandler();

   uint32_t forkTimeout = 3; //sec
   uint32_t backOffFactor = 1;
   uint32_t maxBackOffFactor = 32;

   printPrefix("Started.\n");

   while (true)
   {
      checkPort();
      setupControlPipe();

      time_t forkTime = time(NULL);
      assert(forkTime > 0);

      pid_t pid = fork();

      if (pid < 0) { // error
         handleFatalError(errno,"fork() failed");
      } else if (pid > 0) { //parent

         // close the read end of the pipe
         close(controlPipe[0]);
         controlPipe[0] = -1;

         int status;
         pid_t p = wait(&status);
         if (p == -1) {
            handleFatalError(errno,"wait() failed");
         }

         handleExitStatus(status, pid);

         time_t exitTime = time(NULL);
         assert(exitTime > 0);

         if ((exitTime - forkTime) < forkTimeout) {
            sleep(backOffFactor*(forkTimeout - (exitTime - forkTime)));
            backOffFactor *= 2;
            backOffFactor = (backOffFactor < maxBackOffFactor) ? backOffFactor : maxBackOffFactor;
         } else {
            backOffFactor = 1;
         }

      }  else { //child

         //close the write end of the pipe
         close(controlPipe[1]);
         controlPipe[1] = -1;

         // connect stdin with the read end
         if (dup2(controlPipe[0], STDIN_FILENO) != STDIN_FILENO) {
            handleFatalError(errno,"dup2() failed");
         }
         if (controlPipe[0] != STDIN_FILENO) {
            close(controlPipe[0]);
            controlPipe[0] = -1;
         }

         runSciDB();
         assert(0);
      }
   }
   assert(0);
}

class LoggerControl {
  public:
    LoggerControl() {}
    ~LoggerControl() {
        log4cxx::Logger::getRootLogger()->setLevel(log4cxx::Level::getOff());
    }
};

int main(int argc, char* argv[])
{
    LoggerControl logCtx;

    // need to adjust sigaction SIGCHLD ?
   srandom(static_cast<unsigned int>(std::time(0)));
   try
   {
       initConfig(argc, argv);
   }
   catch (const std::exception &e)
   {
      printPrefix();
      cerr << "Failed to initialize server configuration: " << e.what() << endl;
      exit(1);
   }
   Config *cfg = Config::getInstance();

   if (cfg->getOption<bool>(CONFIG_DAEMONIZE))
   {
      if (daemon(1, 0) == -1) {
         handleFatalError(errno,"daemon() failed");
      }
   }

   if(cfg->getOption<bool>(CONFIG_REGISTER) ||
      cfg->getOption<bool>(CONFIG_NO_WATCHDOG)) {
      runSciDB();
      assert(0);
      exit(1);
   }
   runWithWatchdog();
   assert(0);
   exit(1);
}
