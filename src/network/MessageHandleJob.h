/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2014 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
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
#include <stdint.h>
#include <util/Job.h>
#include <util/JobQueue.h>
#include <util/WorkQueue.h>
#include <query/Query.h>
#include "network/proto/scidb_msg.pb.h"
#include <array/Metadata.h>
#include "network/Connection.h"
#include "network/NetworkManager.h"

namespace scidb
{
    class InstanceLiveness;
    class MessageHandleJob: public Job, public boost::enable_shared_from_this<MessageHandleJob>
    {
    public:
        MessageHandleJob(const boost::shared_ptr<MessageDesc>& messageDesc);
        virtual ~MessageHandleJob();

        /**
         * Based on its contents this message is prepared and scheduled to run
         * on an appropriate queue.
         * @param requestQueue a system queue for running jobs that may block waiting for events from other jobs
         * @param workQueue a system queue for running jobs that are guaranteed to make progress
         */
        void dispatch(boost::shared_ptr<WorkQueue>& requestQueue,
                      boost::shared_ptr<WorkQueue>& workQueue);

    protected:
        /// Implementation of Job::run()
        /// @see Job::run()
        virtual void run();

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

        /**
         * Helper to avoid duplicate code.
         * When an empty-bitmap chunk is received, if RLE is true, the chunk is materialized, and getEmptyBitmap() is called on it.
         * The returned shared_ptr<ConstRLEEmptyBitmap> will be stored in the SG context and
         * will be used to process future chunks/aggregateChunks from the same sender.
         * Note that an empty-bitmap attribute can never be of aggregate type.
         */
        void _handleChunkOrAggregateChunk(bool isAggregateChunk);

        void handleChunk()
        {
            _handleChunkOrAggregateChunk(false);
        }

        void handleAggregateChunk()
        {
            _handleChunkOrAggregateChunk(true);
        }

        void handleRemoteChunk();
        void handleFetchChunk();
        void handleSyncRequest();
        void handleSyncResponse();
        void handleError();
        void handleNotify();
        void handleWait();
        void handleBarrier();
        void handleBufferSend();
        void handleReplicaSyncResponse();
        void handleReplicaChunk();
        void handleInstanceStatus();
        void handleResourcesFileExists();
        void handleInvalidMessage();
        void handleAbortQuery();
        void handleCommitQuery();
        /**
         * Helper method to enqueue this job to a given queue
         * @param queue the WorkQueue for running this job
         * @param handleOverflow inidicates whether to handle the queue overflow by setting this message's query to error;
         *        true by default
         * @throws WorkQueue::OverflowException if queue has no space (regardless of the handleOverflow flag)
         */
        void enqueue(boost::shared_ptr<WorkQueue>& queue, bool handleOverflow=true);
    };
} // namespace

#endif /* MESSAGEHANDLEJOB_H_ */
