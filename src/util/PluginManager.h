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
 * @file PluginManager.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief A manager of plugable modules.
 *
 * Loads modules, finds symbols, manages instances.
 * May have different implementations for different OSs.
 */

#ifndef PLUGINMANAGER_H_
#define PLUGINMANAGER_H_

#include <map>

#include "util/Singleton.h"

namespace scidb
{

struct PluginDesc
{
    void* handle;
    uint32_t major;
    uint32_t minor;
    uint32_t patch;
    uint32_t build;
};

// TODO: Serialize access by mutex
class PluginManager: public Singleton<PluginManager>
{
public:
    PluginManager();

    ~PluginManager();

    /**
     * The function finds module. Currently it's looking for a module in
     * plugins folder specified in config.
     * @param moduleName a name of module to load
     * @return a reference to loaded module descriptor
     */
    PluginDesc& findModule(const std::string& moduleName, bool* was = NULL);

    /**
     * The function finds symbol in the given module handle.
     * @param plugin a pointer of module to given by findModule method.
     * @param symbolName a name of symbol to load.
     * @return a pointer to loaded symbol.
     */
    void* openSymbol(void* plugin, const std::string& symbolName, bool throwException = false);

    /**
     * The function finds module and symbol in it. Currently it's looking for a module in
     * plugins folder specified in config.
     * @param moduleName a name of module to load
     * @param symbolName a name of symbol to load
     * @return a pointer to loaded symbol
     */
    void* findSymbol(const std::string& moduleName, const std::string& symbolName)
    {
        return openSymbol(findModule(moduleName).handle, symbolName, true);
    }

    /**
     * This method loads module and all user defined objects.
     * @param libraryName a name of library
     * @param registerInCatalog tells to register library in system catalog
     */
    void loadLibrary(const std::string& libraryName, bool registerInCatalog);

    /**
     * This method unloads module and all user defined objects.
     * @param libraryName a name of library
     */
    void unLoadLibrary(const std::string& libraryName);

    void preLoadLibraries();

    const std::map<std::string, PluginDesc>& getPlugins() {
        return _plugins;
    }

    const std::string& loadingLibrary() {
        return _loadingLibrary;
    }

    void setPluginsDirectory(const std::string &pluginsDirectory);
private:
    std::map<std::string, PluginDesc> _plugins;
    std::string _loadingLibrary;
    std::string _pluginsDirectory;
};


} // namespace

#endif /* PLUGINMANAGER_H_ */
