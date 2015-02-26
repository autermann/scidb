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
 * @author pavel.velikhov@gmail.com, roman.simakov@gmail.com
 *
 * @brief The interface to the Query Processor in SciDB
 *
 * The QueryProcessor provides the interface to create and execute queries
 * in SciDB.
 * The class that handles all major query processing tasks is QueryProcessor, which
 * is a stateless, reentrant class. The client of the QueryProcessor however uses the
 * Query and QueryResult interfaces instead of the QueryProcessor interface.
 */

#include <time.h>
#include <boost/make_shared.hpp>
#include <boost/serialization/string.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <log4cxx/logger.h>

#include "query/QueryProcessor.h"
#include "query/parser/QueryParser.h"
#include "query/parser/AST.h"
#include "query/parser/ALTranslator.h"
#include "smgr/io/Storage.h"
#include "network/NetworkManager.h"
#include "system/SciDBConfigOptions.h"
#include "system/SystemCatalog.h"
#include "array/ParallelAccumulatorArray.h"

using namespace std;
using namespace boost;
using namespace boost::archive;

namespace scidb
{

// Logger for query processor. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.processor"));

// Basic QueryProcessor implementation
class QueryProcessorImpl: public QueryProcessor
{
private:
    // Recursive method for executing physical plan
    boost::shared_ptr<Array> execute(boost::shared_ptr<PhysicalQueryPlanNode> node, boost::shared_ptr<Query> query, int depth);
    void preSingleExecute(boost::shared_ptr<PhysicalQueryPlanNode> node, boost::shared_ptr<Query> query);
    void postSingleExecute(boost::shared_ptr<PhysicalQueryPlanNode> node, boost::shared_ptr<Query> query);
    // Synchronization methods
    /**
     * Worker notifies coordinator about finishing work.
     * Coordinator wait notifications.
     */
    void notify(boost::shared_ptr<Query> query);

    /**
     * Worker waits notification.
     * Coordinator send out notifications.
     */
    void wait(boost::shared_ptr<Query> query);

public:
    boost::shared_ptr<Query> createQuery(string queryString, QueryID queryId);
    void parseLogical(boost::shared_ptr<Query> query, bool afl);
    void parsePhysical(const string& plan, boost::shared_ptr<Query> query);
    const ArrayDesc& inferTypes(boost::shared_ptr<Query> query);
    void setParameters(boost::shared_ptr<Query> query, QueryParamMap queryParams);
    bool optimize(boost::shared_ptr<Optimizer> optimizer, boost::shared_ptr<Query> query);
    void preSingleExecute(boost::shared_ptr<Query> query);
    void execute(boost::shared_ptr<Query> query);
    void postSingleExecute(boost::shared_ptr<Query> query);
    void inferArrayAccess(boost::shared_ptr<Query> query);
};


boost::shared_ptr<Query> QueryProcessorImpl::createQuery(string queryString, QueryID queryID)
{
    assert(queryID > 0);
    boost::shared_ptr<Query> query = Query::create(queryID);
    query->queryString = queryString;

    return query;
}


void QueryProcessorImpl::parseLogical(boost::shared_ptr<Query> query, bool afl)
{
    QueryParser queryParser(false);
    boost::shared_ptr<AstNode> root = queryParser.parse(query->queryString, !afl);
    // Infer AQL Types
    //if (!afl) root->inferAQLTypes();
    query->logicalPlan = boost::make_shared<LogicalPlan>(AstToLogicalPlan(root.get(), query));
}


void QueryProcessorImpl::parsePhysical(const std::string& plan, boost::shared_ptr<Query> query)
{
    assert(!plan.empty());

    boost::shared_ptr<PhysicalQueryPlanNode> node;

    stringstream ss;
    ss << plan;
    text_iarchive ia(ss);
    ia.register_type(static_cast<OperatorParam*>(NULL));
    ia.register_type(static_cast<OperatorParamReference*>(NULL));
    ia.register_type(static_cast<OperatorParamArrayReference*>(NULL));
    ia.register_type(static_cast<OperatorParamAttributeReference*>(NULL));
    ia.register_type(static_cast<OperatorParamDimensionReference*>(NULL));
    ia.register_type(static_cast<OperatorParamLogicalExpression*>(NULL));
    ia.register_type(static_cast<OperatorParamPhysicalExpression*>(NULL));
    ia.register_type(static_cast<OperatorParamSchema*>(NULL));
    ia.register_type(static_cast<OperatorParamAggregateCall*>(NULL));
    ia.register_type(static_cast<OperatorParamAsterisk*>(NULL));
    ia & node;

    query->addPhysicalPlan(boost::make_shared<PhysicalPlan>(node));
}


const ArrayDesc& QueryProcessorImpl::inferTypes(boost::shared_ptr<Query> query)
{
    return query->logicalPlan->inferTypes(query);
}

void QueryProcessorImpl::inferArrayAccess(boost::shared_ptr<Query> query)
{
    return query->logicalPlan->inferArrayAccess(query);
}


bool QueryProcessorImpl::optimize(boost::shared_ptr<Optimizer> optimizer, boost::shared_ptr<Query> query)
{
   query->addPhysicalPlan(optimizer->optimize(query, query->logicalPlan));

    return !query->getCurrentPhysicalPlan()->empty();
}


void QueryProcessorImpl::setParameters(boost::shared_ptr<Query> query, QueryParamMap queryParams)
{
}


// Recursive method for single executing physical plan
void QueryProcessorImpl::preSingleExecute(boost::shared_ptr<PhysicalQueryPlanNode> node, boost::shared_ptr<Query> query)
{
    Query::validateQueryPtr(query);

    boost::shared_ptr<PhysicalOperator> physicalOperator = node->getPhysicalOperator();

    vector<boost::shared_ptr<PhysicalQueryPlanNode> >& childs = node->getChildren();
    for (size_t i = 0; i < childs.size(); i++) {
        preSingleExecute(childs[i], query);
    }

    StatisticsScope sScope(&physicalOperator->getStatistics());
    physicalOperator->preSingleExecute(query);
}


void QueryProcessorImpl::preSingleExecute(boost::shared_ptr<Query> query)
{
    LOG4CXX_DEBUG(logger, "(Pre)Single executing queryID: " << query->getQueryID())

    preSingleExecute(query->getCurrentPhysicalPlan()->getRoot(), query);
}

void QueryProcessorImpl::postSingleExecute(boost::shared_ptr<PhysicalQueryPlanNode> node, boost::shared_ptr<Query> query)
{
   Query::validateQueryPtr(query);

    boost::shared_ptr<PhysicalOperator> physicalOperator = node->getPhysicalOperator();

    vector<boost::shared_ptr<PhysicalQueryPlanNode> >& childs = node->getChildren();
    for (size_t i = 0; i < childs.size(); i++) {
        postSingleExecute(childs[i], query);
    }

    StatisticsScope sScope(&physicalOperator->getStatistics());
    physicalOperator->postSingleExecute(query);
}


void QueryProcessorImpl::postSingleExecute(boost::shared_ptr<Query> query)
{
    LOG4CXX_DEBUG(logger, "(Post)Single executing queryID: " << query->getQueryID())

    postSingleExecute(query->getCurrentPhysicalPlan()->getRoot(), query);
}

// Recursive method for executing physical plan
boost::shared_ptr<Array> QueryProcessorImpl::execute(boost::shared_ptr<PhysicalQueryPlanNode> node, boost::shared_ptr<Query> query, int depth)
{
    Query::validateQueryPtr(query);

    boost::shared_ptr<PhysicalOperator> physicalOperator = node->getPhysicalOperator();
    physicalOperator->setQuery(query);

    vector<boost::shared_ptr<Array> > operatorArguments;
    vector<boost::shared_ptr<PhysicalQueryPlanNode> >& childs = node->getChildren();

    StatisticsScope sScope(&physicalOperator->getStatistics());
    if (node->isAgg())
    {
        // This assert should be provided by optimizer
        assert(childs.size() == 1);

        boost::shared_ptr<Array> currentResultArray = execute(childs[0], query, depth+1);
        assert(currentResultArray);

        if (query->getCoordinatorID() != COORDINATOR_INSTANCE)
        {
            if (Config::getInstance()->getOption<int>(CONFIG_PREFETCHED_CHUNKS) > 1 && currentResultArray->getSupportedAccess() == Array::RANDOM) {
                boost::shared_ptr<ParallelAccumulatorArray> paa = boost::make_shared<ParallelAccumulatorArray>(currentResultArray);
                currentResultArray = paa;
                paa->start(query);
            } else {
                currentResultArray = boost::make_shared<AccumulatorArray>(currentResultArray);
            }
        }
        query->setCurrentResultArray(currentResultArray);

        notify(query); 

        if (query->getCoordinatorID() == COORDINATOR_INSTANCE)
        {
            for (size_t i = 0; i < query->getInstancesCount(); i++)
            {
                boost::shared_ptr<Array> arg;
                if (i != query->getInstanceID()) {
                    arg = RemoteArray::create(query->getCurrentResultArray()->getArrayDesc(), query->getQueryID(), i);
                } else {
                    arg = query->getCurrentResultArray();
                }
                if (!arg)
                    throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_OPERATOR_RESULT);
                operatorArguments.push_back(arg);
            }

            /**
             * TODO: we need to get whole result on this instance before we call wait(query)
             * because we hope on currentResultArray of remote instances but it lives until we send wait notification
             */
            boost::shared_ptr<Array> res = physicalOperator->execute(operatorArguments, query);
            wait(query);

            return res;
        }
        else
        {
            wait(query);
            /**
             * TODO: This is temporary. 2-nd phase is performed only on coordinator but
             * other instances can continue execution with empty array with the same schema.
             * All data should be on coordinator.
             * But we also can't do it if coordinator will request data as pipeline.
             * For example count2 uses MergeArray.
             */
            return depth != 0
                ? boost::shared_ptr<Array>(new MemArray(physicalOperator->getSchema()))
                : query->getCurrentResultArray();
        }
    }
    else if (node->isDdl())
    {
        physicalOperator->execute(operatorArguments, query);
        return boost::shared_ptr<Array>();
    }
    else
    {        
        for (size_t i = 0; i < childs.size(); i++)
        {
            boost::shared_ptr<Array> arg = execute(childs[i], query, depth+1);
            if (!arg)
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_OPERATOR_RESULT);
            operatorArguments.push_back(arg);
        }
        return physicalOperator->execute(operatorArguments, query);
    }
}

void QueryProcessorImpl::execute(boost::shared_ptr<Query> query)
{
    LOG4CXX_INFO(logger, "Executing query(" << query->getQueryID() << "): " << query->queryString <<
                 "; from program: " << query->programOptions << ";")
    Query::validateQueryPtr(query);

    boost::shared_ptr<PhysicalQueryPlanNode> rootNode = query->getCurrentPhysicalPlan()->getRoot();
    boost::shared_ptr<Array> currentResultArray = execute(rootNode, query, 0);

    Query::validateQueryPtr(query);

    if (currentResultArray)
    {
        if (Config::getInstance()->getOption<int>(CONFIG_PREFETCHED_CHUNKS) > 1 && currentResultArray->getSupportedAccess() == Array::RANDOM) {
            if (typeid(*currentResultArray) != typeid(ParallelAccumulatorArray)) {
               boost::shared_ptr<ParallelAccumulatorArray> paa = boost::make_shared<ParallelAccumulatorArray>(currentResultArray);
               currentResultArray = paa;
               paa->start(query);
            }
        } else {
            if (typeid(*currentResultArray) != typeid(AccumulatorArray)) {
                currentResultArray = boost::make_shared<AccumulatorArray>(currentResultArray);
            }
        }
        if (query->getInstancesCount() > 1 &&
            query->getCoordinatorID() == COORDINATOR_INSTANCE &&
            !rootNode->isAgg() && !rootNode->isDdl())
        {
            // RemoteMergedArray uses the Query::_currentResultArray
            // so make sure to set it in advance
            query->setCurrentResultArray(currentResultArray);
            currentResultArray = RemoteMergedArray::create(currentResultArray->getArrayDesc(),
                    query->getQueryID(), query->statistics);
        }
        const ArrayDesc& arrayDesc = currentResultArray->getArrayDesc();
        for (size_t i = 0; i < arrayDesc.getDimensions().size(); i++) {
            const string& mappingArrayName = arrayDesc.getDimensions()[i].getMappingArrayName();
            if (arrayDesc.getDimensions()[i].getType() != TID_INT64 && !mappingArrayName.empty()) {
                query->_mappingArrays[mappingArrayName] = make_shared<AccumulatorArray>(query->getArray(mappingArrayName));
            }
        }
    }
    query->setCurrentResultArray(currentResultArray);
}


void QueryProcessorImpl::notify(boost::shared_ptr<Query> query)
{
   if (query->getCoordinatorID() != COORDINATOR_INSTANCE)
    {
        QueryID queryID = query->getQueryID();
        LOG4CXX_DEBUG(logger, "Sending notification in queryID: " << queryID << " to instance #" << query->getCoordinatorID())
        boost::shared_ptr<MessageDesc> messageDesc = boost::make_shared<MessageDesc>(mtNotify);
        messageDesc->setQueryID(queryID);
        NetworkManager::getInstance()->send(query->getCoordinatorID(), messageDesc);
    }
    else
    {
        const size_t instancesCount = query->getInstancesCount() - 1;
        LOG4CXX_DEBUG(logger, "Waiting notification in queryID from " << instancesCount << " instances")
        Semaphore::ErrorChecker errorChecker = bind(&Query::validate, query);
        query->results.enter(instancesCount, errorChecker);
    }
}


void QueryProcessorImpl::wait(boost::shared_ptr<Query> query)
{
   if (query->getCoordinatorID() == COORDINATOR_INSTANCE)
    {
        QueryID queryID = query->getQueryID();
        LOG4CXX_DEBUG(logger, "Send message from coordinator for waiting instances in queryID: " << query->getQueryID())
        boost::shared_ptr<MessageDesc> messageDesc = boost::make_shared<MessageDesc>(mtWait);
        messageDesc->setQueryID(queryID);
        NetworkManager::getInstance()->sendOutMessage(messageDesc);
    }
    else
    {
        LOG4CXX_DEBUG(logger, "Waiting notification in queryID from coordinator")
        Semaphore::ErrorChecker errorChecker = bind(&Query::validate, query);
        query->results.enter(errorChecker);
    }
}


/**
 * QueryProcessor static method implementation
 */

boost::shared_ptr<QueryProcessor> QueryProcessor::create()
{
    return boost::shared_ptr<QueryProcessor>(new QueryProcessorImpl());
}


} // namespace
