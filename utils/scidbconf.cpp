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
#include <stdlib.h>
#include <map>
#include <set>
#include <string>
#include <assert.h>

#include "system/Constants.h"

namespace scidb {


/**
 * Prints the usage and exits with 1.
 */
void printUsage()
{
   fprintf(stderr, "\tUsage: scidbconf [options]\n"
           "\tOptions:\n"
           "\t\t[-A|--all] all configuration information\n"
           "\t\t[-v|--version] version\n"
           "\t\t[-bt|--buildType] build type\n");
   exit(1);
}

void printHeader()
{
   const char *scidbHeader = "SciDB Configuration Information:";
   const char *copyright   = "Copyright (C) 2008-2012 SciDB, Inc.";
   printf("%s\n%s\n\n", scidbHeader, copyright);
}

void printVersion()
{
   printf("Version: %s\n", scidb::SCIDB_VERSION());
}

void printBuildType()
{
   printf("Build Type: %s\n", scidb::SCIDB_BUILD_TYPE());
}

typedef void (*Action)();
typedef std::map<std::string, Action> OptionDispatchMap;
/**
 * Build a dispatch table for all known options except for {-A,--all}
 * @param dispatchTable[in/out] a map of options to actions
 */
void initOptionDispatch(OptionDispatchMap& dispatchTable)
{
   Action f = &printVersion;
   dispatchTable["-v"] = f;
   dispatchTable["--version"] = f;
   f = &printBuildType;
   dispatchTable["-bt"] = f;
   dispatchTable["--buildType"] = f;
}

typedef std::set<Action> Actions;
/**
 * Convert the user supplied options to a set of actions,
 * the order is not preserved and duplicate options are ignored.
 * If an unknown option is found, the usage is printed and exit(1) is called.
 * If {-A|--all} is encountered all know actions are returned.
 * @param argc[in] number of command line arguments (including the executable name)
 * @param argv[in] the command line arguments
 * @param dispatchTable[in] the table of known options and actions
 * @param actions[out] the actions to be performed
 */
void parseOptions(int argc, char* argv[], OptionDispatchMap& dispatchTable, Actions& actions)
{
   assert(argc>1);
   assert(argv);

   const std::string strA("-A");
   const std::string strall("--all");

   bool doAll = false;
   for (int i = 1; i < argc; ++i) {
      if (strA.compare(argv[i]) == 0 ||
          strall.compare(argv[i]) == 0) {
         doAll = true;
         continue;
      }
      OptionDispatchMap::const_iterator iter =
         dispatchTable.find(std::string(argv[i]));
      if (iter == dispatchTable.end()) {
         printUsage();
         assert(0);
      }
      Action f = iter->second;
      actions.insert(f);
   }
   if (!doAll) {
      return; // what we have
   }
   for (OptionDispatchMap::const_iterator iter =
        dispatchTable.begin(); iter != dispatchTable.end(); ++iter) {
      Action f = iter->second;
      actions.insert(f);
   }
}

} //namespace scidb

using namespace scidb;

int main(int argc, char* argv[]) 
{
   if (argc < 2) {
      printUsage();
      // must exit
      assert(0);
   }
   OptionDispatchMap dispatchTable;
   initOptionDispatch(dispatchTable);
   Actions actions;
   parseOptions(argc, argv, dispatchTable, actions);
   assert(actions.size() > 0);

   printHeader();
   for (Actions::iterator iter = actions.begin();
        iter != actions.end(); ++iter) {
      (*iter)();
   }
   exit(0);
}
