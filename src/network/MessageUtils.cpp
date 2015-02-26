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
 * @file MessageUtils.cpp
 *
 * @author roman.simakov@gmail.com
 */

#include <log4cxx/logger.h>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>

#include "network/MessageUtils.h"
#include "query/parser/ParsingContext.h"

using namespace boost;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network"));

#ifndef SCIDB_CLIENT

boost::shared_ptr<MessageDesc> makeErrorMessageFromException(const Exception& e, QueryID queryID)
{
    boost::shared_ptr<MessageDesc> errorMessage = boost::make_shared<MessageDesc>(mtError);
    boost::shared_ptr<scidb_msg::Error> errorRecord = errorMessage->getRecord<scidb_msg::Error>();
    errorMessage->setQueryID(queryID);
    
    errorRecord->set_file(e.getFile());
    errorRecord->set_function(e.getFunction());
    errorRecord->set_line(e.getLine());
    errorRecord->set_errors_namespace(e.getErrorsNamespace());
    errorRecord->set_short_error_code(e.getShortErrorCode());
    errorRecord->set_long_error_code(e.getLongErrorCode());
    errorRecord->set_stringified_short_error_code(e.getStringifiedShortErrorCode());
    errorRecord->set_stringified_long_error_code(e.getStringifiedLongErrorCode());
    errorRecord->set_what_str(e.getWhatStr());

    if (typeid(SystemException) == typeid(e))
    {
        errorRecord->set_type(1);
    }
    else if (typeid(UserException) == typeid(e))
    {
        errorRecord->set_type(2);
    }
    else if (typeid(UserQueryException) == typeid(e))
    {
        errorRecord->set_type(3);
        const shared_ptr<ParsingContext> &ctxt = ((const UserQueryException&) e).getParsingContext();
        ::scidb_msg::Error_ParsingContext *mCtxt = errorRecord->mutable_parsing_context(); 
        mCtxt->set_query_string(ctxt->getQueryString());
        mCtxt->set_line_start(ctxt->getLineStart());
        mCtxt->set_col_start(ctxt->getColStart());
        mCtxt->set_line_end(ctxt->getLineEnd());
        mCtxt->set_col_end(ctxt->getColEnd());
    }
    else
    {
        assert(0);
    }

    return errorMessage;
}

boost::shared_ptr<MessageDesc> makeOkMessage(QueryID queryID)
{
    boost::shared_ptr<MessageDesc> okMessage = boost::make_shared<MessageDesc>(mtError);
    okMessage->setQueryID(queryID);
    okMessage->getRecord<scidb_msg::Error>()->set_type(0);
    okMessage->getRecord<scidb_msg::Error>()->set_errors_namespace("scidb");
    okMessage->getRecord<scidb_msg::Error>()->set_short_error_code(SCIDB_E_NO_ERROR);
    okMessage->getRecord<scidb_msg::Error>()->set_long_error_code(SCIDB_E_NO_ERROR);

    return okMessage;
}

static bool parseNodeList(shared_ptr<NodeLiveness>& queryLiveness,
                          const scidb_msg::PhysicalPlan_NodeList& nodeList,
                          const bool isDeadList)
            
{
   assert(queryLiveness);

   const google::protobuf::RepeatedPtrField<scidb_msg::PhysicalPlan_NodeListEntry>&  nodes = nodeList.node_entry();
   for(  google::protobuf::RepeatedPtrField<scidb_msg::PhysicalPlan_NodeListEntry>::const_iterator nodeIter = nodes.begin();
         nodeIter != nodes.end(); ++nodeIter) {

      const scidb_msg::PhysicalPlan_NodeListEntry& entry = (*nodeIter);
      if(!entry.has_node_id()) {
         assert(false);
         return false;
      }
      if(!entry.has_gen_id()) {
         assert(false);
         return false;
      }
      NodeLiveness::NodePtr nodeEntry(new NodeLivenessEntry(entry.node_id(), entry.gen_id(), isDeadList));
      bool rc = queryLiveness->insert(nodeEntry);
      if (!rc) {
         assert(false);
         return false;
      }
   }
   return true;
}

boost::shared_ptr<MessageDesc> makeAbortMessage(QueryID queryID)
{
   boost::shared_ptr<MessageDesc> msg = boost::make_shared<MessageDesc>(mtAbort);
   boost::shared_ptr<scidb_msg::DummyQuery> record = msg->getRecord<scidb_msg::DummyQuery>();
   msg->setQueryID(queryID);

   return msg;
}

boost::shared_ptr<MessageDesc> makeCommitMessage(QueryID queryID)
{
   boost::shared_ptr<MessageDesc> msg = boost::make_shared<MessageDesc>(mtCommit);
   boost::shared_ptr<scidb_msg::DummyQuery> record = msg->getRecord<scidb_msg::DummyQuery>();
   msg->setQueryID(queryID);

   return msg;
}

bool parseQueryLiveness(shared_ptr<NodeLiveness>& queryLiveness,
                        shared_ptr<scidb_msg::PhysicalPlan>& ppMsg)
{
   assert(ppMsg);
   assert(ppMsg->IsInitialized());

   if (!ppMsg->has_view_id()) {
      assert(false);
      return false;
   }

   queryLiveness =
   shared_ptr<scidb::NodeLiveness>(new scidb::NodeLiveness(ppMsg->view_id(), 0));

   if (!ppMsg->has_dead_list()) {
      assert(false);
      return false;
   }
   const scidb_msg::PhysicalPlan_NodeList& deadList = ppMsg->dead_list();

   if (!ppMsg->has_live_list()) {
      assert(false);
      return false;
   }
   const scidb_msg::PhysicalPlan_NodeList& liveList = ppMsg->live_list();

   if (!parseNodeList(queryLiveness, deadList, true)) {
      assert(false);
      return false;
   }
   if (!parseNodeList(queryLiveness, liveList, false)) {
      assert(false);
      return false;
   }
   if (queryLiveness->getNumLive() < 1) {
      assert(false);
      return false;
   }
   return true;
}

bool serializeQueryLiveness(shared_ptr<const NodeLiveness>& queryLiveness,
                            shared_ptr<scidb_msg::PhysicalPlan>& ppMsg)
{
   assert(ppMsg);
   assert(queryLiveness);

   ppMsg->set_view_id(queryLiveness->getViewId());

   const NodeLiveness::DeadNodes& deadNodes = queryLiveness->getDeadNodes();
   scidb_msg::PhysicalPlan_NodeList* deadList = ppMsg->mutable_dead_list();
   assert(deadList);

   for ( NodeLiveness::DeadNodes::const_iterator iter = deadNodes.begin();
        iter != deadNodes.end(); ++iter) {
      google::protobuf::uint64 id = (*iter)->getNodeId();
      google::protobuf::uint64 genId = (*iter)->getGenerationId();
      scidb_msg::PhysicalPlan_NodeListEntry* nodeEntry = deadList->add_node_entry();
      assert(nodeEntry);
      nodeEntry->set_node_id(id);
      nodeEntry->set_gen_id(genId);
   }

   const NodeLiveness::LiveNodes& liveNodes = queryLiveness->getLiveNodes();
   assert(liveNodes.size() > 0);
   scidb_msg::PhysicalPlan_NodeList* liveList = ppMsg->mutable_live_list();
   assert(liveList);

   for ( NodeLiveness::LiveNodes::const_iterator iter = liveNodes.begin();
        iter != liveNodes.end(); ++iter) {
      google::protobuf::uint64 id = (*iter)->getNodeId();
      google::protobuf::uint64 genId = (*iter)->getGenerationId();
      scidb_msg::PhysicalPlan_NodeListEntry* nodeEntry = liveList->add_node_entry();
      assert(nodeEntry);
      nodeEntry->set_node_id(id);
      nodeEntry->set_gen_id(genId);
   }
   return true;
}

#endif //SCIDB_CLIENT

shared_ptr<Exception> makeExceptionFromErrorMessage(const boost::shared_ptr<MessageDesc> &msg)
{
    boost::shared_ptr<scidb_msg::Error> errorRecord = msg->getRecord<scidb_msg::Error>();

    assert(SCIDB_E_NO_ERROR != errorRecord->short_error_code());

    switch (errorRecord->type())
    {
        case 1:
            return shared_ptr<Exception>(new SystemException(errorRecord->file().c_str(), errorRecord->function().c_str(),
                errorRecord->line(), errorRecord->errors_namespace().c_str(), errorRecord->short_error_code(),
                errorRecord->long_error_code(),  errorRecord->what_str().c_str(),
                errorRecord->stringified_short_error_code().c_str(), errorRecord->stringified_long_error_code().c_str(),
                msg->getQueryID()));
        case 2:
            return shared_ptr<Exception>(new UserException(errorRecord->file().c_str(), errorRecord->function().c_str(),
                errorRecord->line(), errorRecord->errors_namespace().c_str(), errorRecord->short_error_code(),
                errorRecord->long_error_code(),  errorRecord->what_str().c_str(),
                errorRecord->stringified_short_error_code().c_str(), errorRecord->stringified_long_error_code().c_str(),
                msg->getQueryID()));
        case 3:
            return shared_ptr<Exception>(new UserQueryException(errorRecord->file().c_str(), errorRecord->function().c_str(),
                errorRecord->line(), errorRecord->errors_namespace().c_str(), errorRecord->short_error_code(),
                errorRecord->long_error_code(),  errorRecord->what_str().c_str(),
                errorRecord->stringified_short_error_code().c_str(), errorRecord->stringified_long_error_code().c_str(),
                make_shared<ParsingContext>(errorRecord->parsing_context().query_string(),
                        errorRecord->parsing_context().line_start(),
                        errorRecord->parsing_context().col_start(),
                        errorRecord->parsing_context().line_end(),
                        errorRecord->parsing_context().col_end()
                ),
                msg->getQueryID()));
        default:
            assert(0);
    }
}

void makeExceptionFromErrorMessageAndThrow(const boost::shared_ptr<MessageDesc> &msg)
{
    makeExceptionFromErrorMessage(msg)->raise();
}

} // namespace
