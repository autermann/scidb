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
 * @file QueryTree.cpp
 *
 * @author roman.simakov@gmail.com
 */

#include <boost/foreach.hpp>
#include <boost/make_shared.hpp>

#include "log4cxx/logger.h"
#include "query/QueryPlan.h"
#include "query/LogicalExpression.h"

using namespace boost;
using namespace std;

namespace scidb
{

// Logger for query processor. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.processor"));

// LogicalQueryPlanNode
LogicalQueryPlanNode::LogicalQueryPlanNode(
	const boost::shared_ptr<ParsingContext>& parsingContext,
	const boost::shared_ptr<LogicalOperator>& logicalOperator):
	_logicalOperator(logicalOperator),
	_parsingContext(parsingContext)
{

}

LogicalQueryPlanNode::LogicalQueryPlanNode(
	const boost::shared_ptr<ParsingContext>& parsingContext,
	const boost::shared_ptr<LogicalOperator>& logicalOperator,
	const std::vector<boost::shared_ptr<LogicalQueryPlanNode> > &childNodes):
	_logicalOperator(logicalOperator),
	_childNodes(childNodes),
	_parsingContext(parsingContext)
{
}

const ArrayDesc& LogicalQueryPlanNode::inferTypes(boost::shared_ptr< Query> query)
{
    std::vector<ArrayDesc> inputSchemas;
    ArrayDesc outputSchema;
	for (size_t i=0, end=_childNodes.size(); i<end; i++)
	{
        inputSchemas.push_back(_childNodes[i]->inferTypes(query));
	}
    outputSchema = _logicalOperator->inferSchema(inputSchemas, query);
    //FIXME: May be cover inferSchema method with another one and assign alias there?
    if (_logicalOperator->getAliasName() != "")
        outputSchema.addAlias(_logicalOperator->getAliasName());
	_logicalOperator->setSchema(outputSchema);
	LOG4CXX_DEBUG(logger, "Inferred schema for operator " << _logicalOperator->getLogicalName() << ": " << outputSchema);
	return _logicalOperator->getSchema();
}

void LogicalQueryPlanNode::inferArrayAccess(boost::shared_ptr<Query>& query)
{
    //XXX TODO: consider non-recursive implementation
    for (size_t i=0, end=_childNodes.size(); i<end; i++)
    {
        _childNodes[i]->inferArrayAccess(query);
    }
    assert(_logicalOperator);
    _logicalOperator->inferArrayAccess(query);
}

// PhysicalQueryPlanNode
PhysicalQueryPlanNode::PhysicalQueryPlanNode(const boost::shared_ptr<PhysicalOperator>& physicalOperator,
                                             bool agg, bool ddl, bool tile)
: _physicalOperator(physicalOperator),
  _parent(), _agg(agg), _ddl(ddl), _tile(tile), _isSgMovable(true), _isSgOffsetable(true), _distribution()
{
}

PhysicalQueryPlanNode::PhysicalQueryPlanNode(const boost::shared_ptr<PhysicalOperator>& physicalOperator,
		const std::vector<boost::shared_ptr<PhysicalQueryPlanNode> > &childNodes,
                                             bool agg, bool ddl, bool tile):
	_physicalOperator(physicalOperator),
	_childNodes(childNodes),
	_parent(), _agg(agg), _ddl(ddl), _tile(tile), _isSgMovable(true), _isSgOffsetable(true), _distribution()
{
}

bool PhysicalQueryPlanNode::isStoringSg() const
{
    if ( isSgNode() )
    {
        PhysicalOperator::Parameters params = _physicalOperator->getParameters();
        if (params.size() == 3)
        {
            return true;
        }
        if (params.size() >= 4)
        {
            bool storeResult = ((boost::shared_ptr<OperatorParamPhysicalExpression>&) params[3])->getExpression()->evaluate().getBool();
            return storeResult;
        }
    }
    return false;
}


// LogicalPlan
LogicalPlan::LogicalPlan(const boost::shared_ptr<LogicalQueryPlanNode>& root):
        _root(root)
{

}


// PhysicalPlan
PhysicalPlan::PhysicalPlan(const boost::shared_ptr<PhysicalQueryPlanNode>& root):
        _root(root)
{

}


} // namespace
