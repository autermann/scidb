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
 * MessageHandleJob.h
 *
 *  Created on: Jan 12, 2010
 *      Author: roman.simakov@gmail.com
 */

#ifndef MESSAGEHANDLEJOB_H_
#define MESSAGEHANDLEJOB_H_

#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include "stdint.h"

#include "util/Job.h"
#include <util/JobQueue.h>
#include <util/WorkQueue.h>
#include "network/proto/scidb_msg.pb.h"
#include "array/Metadata.h"
#include "network/Connection.h"

namespace scidb
{
class NodeLiveness;
 class MessageHandleJob: public Job, public boost::enable_shared_from_this<MessageHandleJob>
{
public:
        MessageHandleJob(const boost::shared_ptr<MessageDesc>& messageDesc);
        virtual ~MessageHandleJob();
        // Implementation of thread job
        void run();
        virtual void dispatch(boost::shared_ptr<scidb::JobQueue>& jobQueue);

private:
    boost::shared_ptr<MessageDesc> _messageDesc;
    NetworkManager& networkManager;
    size_t sourceId;
    bool _mustValidateQuery;

    typedef void(MessageHandleJob::*MsgHandler)();
    static MsgHandler _msgHandlers[scidb::mtSystemMax];

    void sgSync();
    void handlePreparePhysicalPlan();
    void handleExecutePhysicalPlan();
    void handleQueryResult();
    void handleChunk();
    void handleAggregateChunk();
    void handleRemoteChunk();
    void handleFetchChunk();
    void handleSyncRequest();
    void handleSyncResponse();
    void handleError();
    void handleNotify();
    void handleWait();
    void handleBarrier();
    void handleMPISend();
    void handleReplicaSyncRequest();
    void handleReplicaSyncResponse();
    void handleChunkReplica();
    void handleNodeStatus();
    void handleResourcesFileExists();
    void handleInvalidMessage();
    void handleAbortQuery();
    void handleCommitQuery();
    void enqueue(boost::shared_ptr<WorkQueue>& q);
};


} // namespace

#endif /* MESSAGEHANDLEJOB_H_ */
