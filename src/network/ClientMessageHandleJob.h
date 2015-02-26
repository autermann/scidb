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
 * @file ClientMessageHandleJob.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The job for handling messages from client.
 *
 * The main difference between handling of messages between instances is that we must send
 * response to this message and keep synchronous client connection to do that.
 */

#ifndef CLIENTMESSAGEHANDLEJOB_H_
#define CLIENTMESSAGEHANDLEJOB_H_

#include "boost/shared_ptr.hpp"
#include "stdint.h"

#include "util/Job.h"
#include "network/proto/scidb_msg.pb.h"
#include "array/Metadata.h"
#include "network/Connection.h"
#include "query/QueryProcessor.h"


namespace scidb
{

class Connection;


/**
 * The class created by network message handler for adding to queue to be processed
 * by thread pool and handle message from client.
 */
class ClientMessageHandleJob: public Job
{
 public:
    ClientMessageHandleJob(boost::shared_ptr< Connection > connection,
                           const boost::shared_ptr<MessageDesc>& messageDesc);

    // Implementation of thread job
    void run();

 private:
    boost::shared_ptr<Connection> _connection;
    boost::shared_ptr<MessageDesc> _messageDesc;

    std::string getProgramOptions(const string &programOptions) const;

    /**
     *  This method process message mtPrepareQuery containing client query string.
     *  At the end of work it send response to client by _connection.
     */
    void prepareClientQuery();

    /**
     *  This method process message mtExecuteQuery containing client query string or prepared query ID.
     *  At the end of work it send response to client by _connection.
     */
    void executeClientQuery();

    /**
     * This method sends next chunk to the client.
     */
    void fetchChunk();

    /**
     * This method cancels query execution and free context
     */
    void cancelQuery();

    /**
     * This method completes the query execution, persists the changes, and frees the context
     */
    void completeQuery();

    /// Helper to deal with exceptions in prepare/executeClientQuery()
    void handleExecuteOrPrepareError(const scidb::Exception& e,
                                     const scidb::QueryResult& queryResult,
                                     const scidb::SciDB& scidb);
};

} // namespace

#endif /* CLIENTMESSAGEHANDLEJOB_H_ */
