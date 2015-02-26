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

/**
 * @file QueryProcessor.cpp
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief Transparent interface for examining cluster physical resources
 */
#include <boost/filesystem/operations.hpp>
#include <boost/bind.hpp>

#include "system/Resources.h"
#include "network/NetworkManager.h"
#include "query/Query.h"
#include "util/Semaphore.h"
#include "util/Mutex.h"

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.system.resources"));

using namespace boost;
using namespace std;

namespace scidb
{

class BaseResourcesCollector
{
public:
    virtual ~BaseResourcesCollector(){};

protected:
    Semaphore _collectorSem;

    friend class Resources;
};

class FileExistsResourcesCollector: public BaseResourcesCollector
{
public:
    void collect(NodeID nodeId, bool exists, bool release = true)
    {
        LOG4CXX_TRACE(logger, "FileExistsResourcesCollector::collect." << " nodeId=" << nodeId
            << " exists=" << exists);
        {
            ScopedMutexLock lock(_lock);
            _nodesMap[nodeId] = exists;
            if (release)
                _collectorSem.release();
        }
    }

    ~FileExistsResourcesCollector()  {
        ScopedMutexLock lock(_lock);
    }    

    map<NodeID, bool> _nodesMap;

private:
    Mutex _lock;
    friend class Resources;
};

void Resources::fileExists(const string &path, map<NodeID, bool> &nodesMap, const shared_ptr<Query> &query)
{
    LOG4CXX_TRACE(logger, "Resources::fileExists. Checking file '" << path << "'");
    NetworkManager* networkManager = NetworkManager::getInstance();

    FileExistsResourcesCollector* collector = new FileExistsResourcesCollector();
    uint64_t id = 0;
    {
        ScopedMutexLock lock(_lock);
        id = ++_lastResourceCollectorId;
        _resourcesCollectors[id] = collector;

        collector->collect(query->getNodeID(), checkFileExists(path), false);
    }

    shared_ptr<MessageDesc> msg = make_shared<MessageDesc>(mtResourcesFileExistsRequest);
    shared_ptr<scidb_msg::ResourcesFileExistsRequest> request =
        msg->getRecord<scidb_msg::ResourcesFileExistsRequest>();
    msg->setQueryID(0);
    request->set_resource_request_id(id);
    request->set_file_path(path);
    networkManager->broadcast(msg);

    LOG4CXX_TRACE(logger, "Resources::fileExists. Waiting while nodes return result for collector " << id);

    try
    {
       Semaphore::ErrorChecker errorChecker = bind(&Query::validateQueryPtr, query);
       collector->_collectorSem.enter(query->getNodesCount() - 1, errorChecker);
    }
    catch (...)
    {
        LOG4CXX_TRACE(logger, "Resources::fileExists. Waiting for result of collector " << id <<
            " interrupter by error");
        {
            ScopedMutexLock lock(_lock);
            delete _resourcesCollectors[id];
            _resourcesCollectors.erase(id);
        }
        throw;
    }

    LOG4CXX_TRACE(logger, "Resources::fileExists. Returning result of collector " << id);

    {
        ScopedMutexLock lock(_lock);
        nodesMap = ((FileExistsResourcesCollector*) _resourcesCollectors[id])->_nodesMap;
        delete _resourcesCollectors[id];
        _resourcesCollectors.erase(id);
    }
}

bool Resources::fileExists(const string &path, NodeID nodeId, const shared_ptr<Query>& query)
{
    LOG4CXX_TRACE(logger, "Resources::fileExists. Checking file '" << path << "'");
    NetworkManager* networkManager = NetworkManager::getInstance();

    if (nodeId == query->getNodeID())
    {
        LOG4CXX_TRACE(logger, "Resources::fileExists. Node id " << nodeId << " is local node. Returning result.");
        return checkFileExists(path);
    }
    else
    {
        LOG4CXX_TRACE(logger, "Resources::fileExists. Node id " << nodeId << " is remote node. Requesting result.");
        FileExistsResourcesCollector* collector = new FileExistsResourcesCollector();
        uint64_t id = 0;
        {
            ScopedMutexLock lock(_lock);
            id = ++_lastResourceCollectorId;
            _resourcesCollectors[id] = collector;
        }

        shared_ptr<MessageDesc> msg = make_shared<MessageDesc>(mtResourcesFileExistsRequest);
        shared_ptr<scidb_msg::ResourcesFileExistsRequest> request =
            msg->getRecord<scidb_msg::ResourcesFileExistsRequest>();
        msg->setQueryID(0);
        request->set_resource_request_id(id);
        request->set_file_path(path);
        networkManager->sendMessage(nodeId, msg);

        LOG4CXX_TRACE(logger, "Resources::fileExists. Waiting while node return result for collector " << id);

        try
        {
           Semaphore::ErrorChecker errorChecker = bind(&Query::validateQueryPtr, query);
           collector->_collectorSem.enter(1, errorChecker);
        }
        catch (...)
        {
            LOG4CXX_TRACE(logger, "Resources::fileExists. Waiting for result of collector " << id <<
                " interrupter by error");
            {
                ScopedMutexLock lock(_lock);
                delete _resourcesCollectors[id];
                _resourcesCollectors.erase(id);
            }
            throw;
        }

        LOG4CXX_TRACE(logger, "Resources::fileExists. Returning result of collector " << id);

        bool result;
        {
            ScopedMutexLock lock(_lock);
            result = ((FileExistsResourcesCollector*) _resourcesCollectors[id])->_nodesMap[nodeId];
            delete _resourcesCollectors[id];
            _resourcesCollectors.erase(id);
        }
        return result;
    }
}

void Resources::handleFileExists(const shared_ptr<MessageDesc>& messageDesc)
{
    NetworkManager* networkManager = NetworkManager::getInstance();

    if (mtResourcesFileExistsRequest == messageDesc->getMessageType())
    {
        LOG4CXX_TRACE(logger, "Message is mtResourcesFileExistsRequest");
        shared_ptr<scidb_msg::ResourcesFileExistsRequest> inMsgRecord =
            messageDesc->getRecord<scidb_msg::ResourcesFileExistsRequest>();
        const string& file = inMsgRecord->file_path();
        LOG4CXX_TRACE(logger, "Checking file " << file);

        shared_ptr<MessageDesc> msg = make_shared<MessageDesc>(mtResourcesFileExistsResponse);
        msg->setQueryID(0);

        shared_ptr<scidb_msg::ResourcesFileExistsResponse> outMsgRecord =
            msg->getRecord<scidb_msg::ResourcesFileExistsResponse>();
        outMsgRecord->set_resource_request_id(inMsgRecord->resource_request_id());
        outMsgRecord->set_exits_flag(Resources::getInstance()->checkFileExists(file));

        networkManager->sendMessage(messageDesc->getSourceNodeID(), msg);
    }
    // mtResourcesFileExistsResponse
    else
    {
        LOG4CXX_TRACE(logger, "Message is mtResourcesFileExistsResponse");
        shared_ptr<scidb_msg::ResourcesFileExistsResponse> inMsgRecord =
            messageDesc->getRecord<scidb_msg::ResourcesFileExistsResponse>();

        LOG4CXX_TRACE(logger, "Marking file");
        Resources::getInstance()->markFileExists(
            inMsgRecord->resource_request_id(),
            messageDesc->getSourceNodeID(),
            inMsgRecord->exits_flag());
    }
}

bool Resources::checkFileExists(const std::string &path) const
{
    LOG4CXX_TRACE(logger, "Resources::checkFileExists. path=" << path);

    try
    {
        return boost::filesystem::exists(path);
    }
    catch (...)
    {
        return false;
    }
}

void Resources::markFileExists(uint64_t resourceCollectorId, NodeID nodeId, bool exists)
{
    LOG4CXX_TRACE(logger, "Resources::markFileExists. resourceCollectorId=" << resourceCollectorId
        << " nodeId=" << nodeId << " exists=" << exists);

    if (_resourcesCollectors.find(resourceCollectorId) != _resourcesCollectors.end())
    {
        FileExistsResourcesCollector *rc = (FileExistsResourcesCollector*) _resourcesCollectors[resourceCollectorId];
        rc->collect(nodeId, exists);
    }
    else
    {
        LOG4CXX_WARN(logger, "Resources::markFileExists. FileExistsResourcesCollector with resourceCollectorId="
            << resourceCollectorId << " not found!");
    }
}

}
