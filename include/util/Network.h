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
 * @file Network.h
 * @brief API allowing for sending and receiving network messages
 */

#ifndef NETWORK_H_
#define NETWORK_H_

#include <boost/asio.hpp>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>
#include <google/protobuf/message.h>
#include <util/Semaphore.h>
#include <util/WorkQueue.h>
#include <util/NetworkMessage.h>
#include <array/Metadata.h>

namespace scidb
{
   /**
    * Network message descriptor
    */
   class MessageDescription
   {
   public:
      virtual InstanceID getSourceInstanceID() const = 0;
      virtual MessagePtr getRecord() = 0;
      virtual MessageID getMessageType() const = 0;
      virtual boost::asio::const_buffer getBinary() = 0;
      virtual ~MessageDescription() {}
      virtual QueryID getQueryId() const = 0;
   };

   /**
    * Abstract client context
    */
   class ClientContext
   {
   public:
       typedef boost::shared_ptr<ClientContext> Ptr;

       /// Client connection disconnect handler
       typedef boost::function<void(const boost::shared_ptr<Query>&)> DisconnectHandler;

       /**
        * Attach a query specific handler for client's disconnect
        * @param queryID query ID
        * @param dh disconnect handler
        */
       virtual void attachQuery(QueryID queryID, DisconnectHandler& dh) = 0;

       /**
        * Detach a query specific handler for client's disconnect
        * @param queryID query ID
        */
       virtual void detachQuery(QueryID queryID) = 0;

       /**
        * Indicate that the context should no longer be used
        */
       virtual void disconnect() = 0;

       virtual ~ClientContext() {}
   };

   /**
    * Client message descriptor
    */
   class ClientMessageDescription : public MessageDescription
   {
   public:
       virtual ClientContext::Ptr getClientContext() = 0;
       virtual ~ClientMessageDescription() {}
   };

   /**
    * Network message factory allows for addition of network message handlers
    * on SciDB server (only).
    */
   class NetworkMessageFactory
   {
   public:
      typedef boost::function< MessagePtr(MessageID) > MessageCreator;
      typedef boost::function< void(const boost::shared_ptr<MessageDescription>& ) > MessageHandler;

      virtual bool isRegistered(const MessageID& msgID) = 0;
      virtual bool addMessageType(const MessageID& msgID,
                                  const MessageCreator& msgCreator,
                                  const MessageHandler& msgHandler) = 0;
      virtual MessagePtr createMessage(const MessageID& msgID) = 0;
      virtual MessageHandler getMessageHandler(const MessageID& msgID) = 0;
      virtual ~NetworkMessageFactory() {}
   };

   /**
    * Network message factory access method
    * @return an instance of the factory
    */
   boost::shared_ptr<NetworkMessageFactory> getNetworkMessageFactory();

   /**
    * Return a reference to the io service object to schedule timers
    * @note XXX:
    * it might be better not to expose the io service directly but rather
    * provide a speicific functionality API (e.g. scheduleTimer())
    */
   boost::asio::io_service& getIOService();

   /**
    * @return a queue suitable for running arbitrary tasks
    * @note No new threads are created as a result of adding work to the queue
    */
   boost::shared_ptr<WorkQueue> getWorkQueue();

   /**
    * A scheduler  that runs *at most* every specified period.
    * It schedules itself to run in max(0, ((lastTime + period) - currentTime)) seconds.
    * It is not recurring - every execution needs to be explicitly scheduled.
    * When scheduled for the first time, it is run immediately.
    */
   class Scheduler
   {
   public:
      typedef boost::function<void()> Work;
      virtual void schedule() = 0;
      virtual ~Scheduler() {}
   };

   /**
    * Get a scheduler for a given time period
    *
    * @param workItem  to execute
    * @param period limiting the max freaquency of execution
    * @throw scidb::UserException if period < 1 or !workItem
    */
   boost::shared_ptr<Scheduler> getScheduler(Scheduler::Work& workItem, time_t period);

   /**
    * Send message to an instance asynchrously, the delivery is not guaranteed
    * @param targetInstanceID the instance ID of the destination
    * @param msgID a message ID previously registered with NetworkMessageFactory
    * @param record the structured part of the message (derived from ::google::protobuf::Message),
                    the record type must match the one generated by the NetworkMessageFactory
    * @param binary the opaque payload of the message (which can be empty)
    */
   void sendAsync(InstanceID targetInstanceID,
                  MessageID msgID,
                  MessagePtr record,
                  boost::asio::const_buffer& binary);

   /**
    * Send message to a client asynchrously, the delivery is not guaranteed
    * @param clientCtx the client context of the destination
    * @param msgID a message ID previously registered with NetworkMessageFactory
    * @param record the structured part of the message (derived from ::google::protobuf::Message),
                    the record type must match the one generated by the NetworkMessageFactory
    * @param binary the opaque payload of the message (which can be empty)
    */
   void sendAsync(ClientContext::Ptr& clientCtx,
                  MessageID msgID,
                  MessagePtr record,
                  boost::asio::const_buffer& binary);
   /**
    * @return Time in seconds to wait before decalring a network-silent instance dead.
    */
   uint32_t getLivenessTimeout();

} // namespace scidb

#endif /* NETWORK_H_ */
