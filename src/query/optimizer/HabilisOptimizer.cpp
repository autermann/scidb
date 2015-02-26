/*
 **
 * BEGIN_COPYRIGHT
 *
 * This file is part of SciDB.
 * Copyright (C) 2008-2011 SciDB, Inc.
 *
 * SciDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation version 3 of the License, or
 * (at your option) any later version.
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
 * @file HabilisOptimizer.cpp
 *
 * @brief Our first attempt at a halfway intelligent optimizer.
 * habilis (adj.) Latin: fit, easy, adaptable, apt, handy, well-adapted, inventive,..
 *
 * @author poliocough@gmail.com
 */

#include <boost/foreach.hpp>
#include <boost/make_shared.hpp>
#include <log4cxx/logger.h>

#include <fstream>
#include <iostream>
#include <iomanip>

#include "query/optimizer/Optimizer.h"
#include "query/optimizer/HabilisOptimizer.h"
#include "system/SystemCatalog.h"
#include "network/NetworkManager.h"
#include "array/Metadata.h"
#include "array/DelegateArray.h"
#include "query/parser/ParsingContext.h"

#include <iostream>

using namespace boost;
using namespace std;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.optimizer"));

HabilisOptimizer::HabilisOptimizer(): _root(),
        _featureMask(
                     CONDENSE_SG
                   | INSERT_REPART
                   | REWRITE_STORING_SG
         )
{
    if (Config::getInstance()->getOption<bool>(CONFIG_RLE_CHUNK_FORMAT))
    {
        _featureMask |= INSERT_MATERIALIZATION;
    }

    const char* path = "/tmp/scidb_optimizer_override";
    std::ifstream inFile (path);
    if (inFile && !inFile.eof())
    {
        inFile >> _featureMask;
        LOG4CXX_DEBUG(logger, "Feature mask overridden to "<<_featureMask);
    }
    inFile.close();
}


PhysPlanPtr HabilisOptimizer::optimize(const boost::shared_ptr<Query>& query,
                                       boost::shared_ptr<LogicalPlan>& logicalPlan)
{
    assert(_root.get() == NULL);
    assert(_query.get() == NULL);

    Eraser onStack(*this);

    _query = query;
    assert(_query);

    boost::shared_ptr<LogicalQueryPlanNode> logicalRoot = logicalPlan->getRoot();
    if (!logicalRoot)
    {   return PhysPlanPtr(new PhysicalPlan(_root)); }


    bool tileMode = Config::getInstance()->getOption<bool>(CONFIG_RLE_CHUNK_FORMAT) 
        && Config::getInstance()->getOption<int>(CONFIG_TILE_SIZE) > 1;
    _root = tw_createPhysicalTree(logicalRoot, tileMode);

    if (isFeatureEnabled(INSERT_REPART))
    {
        tw_insertRepartNodes(_root);
    }

    tw_insertSgNodes(_root);

    if (isFeatureEnabled(CONDENSE_SG))
    {
        tw_collapseSgNodes(_root);
        while (tw_pushupJoinSgs(_root))
        {
            tw_collapseSgNodes(_root);
        }
    }

    tw_insertAggReducers(_root);

    if (isFeatureEnabled(INSERT_MATERIALIZATION))
    {
        tw_insertChunkMaterializers(_root);
    }

    if (isFeatureEnabled(REWRITE_STORING_SG) && query->getNodesCount()>1)
    {
        tw_rewriteStoringSG(_root);
    }


    PhysPlanPtr result(new PhysicalPlan(_root));
    // null out the root
    logicalPlan->setRoot(boost::shared_ptr<LogicalQueryPlanNode>());

    return result;
}

void HabilisOptimizer::dbg_printPlan()
{
    PhysPlanPtr plan(new PhysicalPlan(_root));
    std::ostringstream out;
    plan->toString(out);
    std::cout << out.str() << "\n";
}

void HabilisOptimizer::dbg_logPlan()
{
    PhysPlanPtr plan(new PhysicalPlan(_root));
    std::ostringstream out;
    plan->toString(out);
    LOG4CXX_DEBUG(logger, out.str().c_str())
}

void HabilisOptimizer::n_addParentNode(PhysNodePtr target, PhysNodePtr nodeToInsert)
{
    if (target->hasParent())
    {
        PhysNodePtr parent = target->getParent();
        nodeToInsert->setParent(parent);
        parent->replaceChild(target, nodeToInsert);
    }
    else
    {
        assert(_root == target);
        _root = nodeToInsert;
    }

    target->setParent(nodeToInsert);
    nodeToInsert->addChild(target);
}

void HabilisOptimizer::n_cutOutNode(PhysNodePtr nodeToRemove)
{
    vector<PhysNodePtr> children = nodeToRemove->getChildren();
    assert(children.size()<=1);

    if (nodeToRemove->hasParent())
    {
        PhysNodePtr parent = nodeToRemove->getParent();
        if (children.size() == 1)
        {
            PhysNodePtr child = children[0];
            parent->replaceChild(nodeToRemove, child);
            child->setParent(parent);
        }
        else
        {
            parent->removeChild(nodeToRemove);
        }
    }

    else
    {
        assert(_root == nodeToRemove);
        if (children.size() == 1)
        {
            PhysNodePtr child = children[0];
            _root = child;
            _root->resetParent();
        }
        else
        {
            _root.reset();
        }
    }
}

boost::shared_ptr<OperatorParam> HabilisOptimizer::n_createPhysicalParameter(const boost::shared_ptr<OperatorParam> & logicalParameter,
                                                                      const vector<ArrayDesc>& logicalInputSchemas,
                                                                      const ArrayDesc& logicalOutputSchema,
                                                                      bool tile)
{
    if (logicalParameter->getParamType() == PARAM_LOGICAL_EXPRESSION)
    {
        boost::shared_ptr<Expression> physicalExpression = boost::make_shared<Expression> ();
        boost::shared_ptr<OperatorParamLogicalExpression>& logicalExpression = (boost::shared_ptr<OperatorParamLogicalExpression>&) logicalParameter;
        try
        {
            if (logicalExpression->isConstant())
            {
               physicalExpression->compile(logicalExpression->getExpression(), _query, tile, logicalExpression->getExpectedType().typeId());
            }
            else
            {
               physicalExpression->compile(logicalExpression->getExpression(), _query, tile, logicalExpression->getExpectedType().typeId(), logicalInputSchemas,
                                            logicalOutputSchema);
            }
            if (tile && !physicalExpression->supportsTileMode()) { 
                return boost::shared_ptr<OperatorParam>();
            }
            return boost::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(logicalParameter->getParsingContext(), physicalExpression,
                                                                                  logicalExpression->isConstant()));
        } catch (Exception &e)
        {
            if (e.getLongErrorCode() == SCIDB_LE_TYPE_CONVERSION_ERROR || e.getLongErrorCode() == SCIDB_LE_TYPE_CONVERSION_ERROR2)
            {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_PARAMETER_TYPE_ERROR, logicalExpression->getParsingContext())
                    << logicalExpression->getExpectedType().name() << TypeLibrary::getType(physicalExpression->getType()).name();
            }
            else
            {
                throw;
            }
        }
    }
    else
    {
        return logicalParameter;
    }
}

PhysNodePtr HabilisOptimizer::n_createPhysicalNode(boost::shared_ptr<LogicalQueryPlanNode> logicalNode,
                                                   bool tileMode)
{
    const boost::shared_ptr<LogicalOperator>& logicalOp = logicalNode->getLogicalOperator();
    const string& logicalName = logicalOp->getLogicalName();

    OperatorLibrary* opLibrary = OperatorLibrary::getInstance();
    vector<string> physicalOperatorsNames;
    opLibrary->getPhysicalNames(logicalName, physicalOperatorsNames);
    const string &physicalName = physicalOperatorsNames[0];
    const vector<boost::shared_ptr<LogicalQueryPlanNode> >& children = logicalNode->getChildren();

    // Collection of input schemas of operator for resolving references
    vector<ArrayDesc> inputSchemas;
    tileMode &= logicalOp->getProperties().tile;
    for (size_t ch = 0; ch < children.size(); ch++)
    {
        inputSchemas.push_back(children[ch]->getLogicalOperator()->getSchema());
    }
    const ArrayDesc& outputSchema = logicalOp->getSchema();

    const LogicalOperator::Parameters& logicalParams = logicalOp->getParameters();
    size_t nParams = logicalParams.size();
    PhysicalOperator::Parameters physicalParams(nParams);
        
  Retry:
    for (size_t i = 0; i < nParams; i++)
    {
        bool paramTileMode = tileMode && logicalOp->compileParamInTileMode(i);
        boost::shared_ptr<OperatorParam> param = n_createPhysicalParameter(logicalParams[i], inputSchemas, outputSchema, paramTileMode);
                                                                           
        if (!param) {
            assert(paramTileMode);
            tileMode = false;
            goto Retry;
        }
        physicalParams[i] = param;
    }

    PhysOpPtr physicalOp = opLibrary->createPhysicalOperator(logicalName, physicalName, physicalParams, outputSchema);
    physicalOp->setQuery(_query);
    physicalOp->setTileMode(tileMode);
    return PhysNodePtr(new PhysicalQueryPlanNode(physicalOp, false, logicalNode->isDdl(), tileMode));
}

PhysNodePtr HabilisOptimizer::n_buildSgNode(const ArrayDesc & outputSchema,
                                            PartitioningSchema partSchema, bool storeArray)
{
    PhysicalOperator::Parameters sgParams;

    boost::shared_ptr<Expression> psConst = boost::make_shared<Expression> ();
    Value ps(TypeLibrary::getType(TID_INT32));

    ps.setInt32(partSchema);

    psConst->compile(false, TID_INT32, ps);
    sgParams.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(boost::make_shared<ParsingContext> (""), psConst, true)));

    if (storeArray)
    {
       boost::shared_ptr<Expression> nodeConst = boost::make_shared<Expression> ();
        Value node(TypeLibrary::getType(TID_INT64));
        node.setInt64(-1);
        nodeConst->compile(false, TID_INT64, node);
        sgParams.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(boost::make_shared<ParsingContext> (""),
                                                                                          nodeConst,
                                                                                          true)));

        sgParams.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamArrayReference(boost::make_shared<ParsingContext> (""),
                                                                                      "",
                                                                                      outputSchema.getName(),
                                                                                      true)));
    }

    PhysOpPtr sgOp = OperatorLibrary::getInstance()->createPhysicalOperator("sg", "impl_sg", sgParams, outputSchema);
    sgOp->setQuery(_query);

    PhysNodePtr sgNode(new PhysicalQueryPlanNode(sgOp, false, false, false));
    return sgNode;
}

PhysNodePtr HabilisOptimizer::tw_createPhysicalTree(boost::shared_ptr<LogicalQueryPlanNode> logicalRoot, bool tileMode)
{
   logicalRoot = logicalRewriteIfNeeded(_query, logicalRoot);

    vector<boost::shared_ptr<LogicalQueryPlanNode> > logicalChildren = logicalRoot->getChildren();
    vector<PhysNodePtr> physicalChildren(logicalChildren.size());
    bool rootTileMode = tileMode;
    for (size_t i = 0; i < logicalChildren.size(); i++)
    {
        boost::shared_ptr<LogicalQueryPlanNode> logicalChild = logicalChildren[i];
        PhysNodePtr physicalChild = tw_createPhysicalTree(logicalChild, tileMode);
        rootTileMode &= physicalChild->getPhysicalOperator()->getTileMode();
        physicalChildren[i] = physicalChild;
    }
    PhysNodePtr physicalRoot = n_createPhysicalNode(logicalRoot, rootTileMode);

    if (physicalRoot->isSgNode())
    {
        //this is a user-inserted explicit SG. So we don't mess with it.
        physicalRoot->setSgMovable(false);
        physicalRoot->setSgOffsetable(false);
    }
    for (size_t i = 0; i < physicalChildren.size(); i++)
    {
        PhysNodePtr physicalChild = physicalChildren[i];
        physicalChild->setParent(physicalRoot);
        physicalRoot->addChild(physicalChild);
    }
    boost::shared_ptr<LogicalOperator> logicalOp = logicalRoot->getLogicalOperator();
    if (logicalOp->getGlobalOperatorName().first != "" && logicalOp->getGlobalOperatorName().second != "")
    {
        PhysOpPtr globalOp =
                OperatorLibrary::getInstance()->createPhysicalOperator(logicalOp->getGlobalOperatorName().first,
                                                                       logicalOp->getGlobalOperatorName().second,
                                                                       PhysicalOperator::Parameters(),
                                                                       logicalOp->getSchema());
        globalOp->setQuery(_query);
        PhysNodePtr globalNode(new PhysicalQueryPlanNode(globalOp, true, false, false));
        physicalRoot->inferBoundaries();
        physicalRoot->setParent(globalNode);
        globalNode->addChild(physicalRoot);
        physicalRoot = globalNode;
    }

    physicalRoot->inferBoundaries();
    return physicalRoot;
}

static void s_setSgDistribution(PhysNodePtr sgNode,
                                ArrayDistribution const& dist)
{
    if (dist.isUndefined())
        throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_CANT_CREATE_SG_WITH_UNDEFINED_DISTRIBUTION);

    PhysicalOperator::Parameters _parameters = sgNode->getPhysicalOperator()->getParameters();
    PhysicalOperator::Parameters newParameters;

    boost::shared_ptr<Expression> psConst = boost::make_shared<Expression> ();
    Value ps(TypeLibrary::getType(TID_INT32));
    ps.setInt32(dist.getPartitioningSchema());
    psConst->compile(false, TID_INT32, ps);
    newParameters.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(boost::make_shared<ParsingContext> (""), psConst, true)));

    for (size_t i =1; i< _parameters.size() && i<4; i++)
    {
        newParameters.push_back(_parameters[i]);
    }

    ArrayDesc sgSchema = sgNode->getPhysicalOperator()->getSchema();

    if (_parameters.size() < 2)
    {
        boost::shared_ptr<Expression> nodeConst(new Expression());;
        Value node(TypeLibrary::getType(TID_INT64));
        node.setInt64(-1);
        nodeConst->compile(false, TID_INT64, node);
        newParameters.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(boost::make_shared<ParsingContext> (""), nodeConst, true)));

        newParameters.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamArrayReference(boost::make_shared<ParsingContext> (""),
                                                                                      "",
                                                                                      sgSchema.getName(),
                                                                                      true)));

        boost::shared_ptr<Expression> storeFlagExpr(new Expression());
        Value storeFlag(TypeLibrary::getType(TID_BOOL));
        storeFlag.setBool(false);
        storeFlagExpr->compile(false, TID_BOOL, storeFlag);
        newParameters.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(boost::make_shared<ParsingContext> (""), storeFlagExpr, true)));
    }

    DimensionVector offset,shape;
    if (dist.hasMapper())
    {
        offset = dist.getMapper()->_distOffsetVector;
        shape = dist.getMapper()->_distShapeVector;
    }

    for ( size_t i = 0;  i < offset.numDimensions(); i++)
    {
        boost::shared_ptr<Expression> vectorValueExpr(new Expression());
        Value vectorValue(TypeLibrary::getType(TID_INT64));
        vectorValue.setInt64(offset[i]);
        vectorValueExpr->compile(false, TID_INT64, vectorValue);
        newParameters.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(boost::make_shared<ParsingContext> (""), vectorValueExpr, true)));
    }

    for ( size_t i = 0; i < shape.numDimensions(); i++)
    {
        boost::shared_ptr<Expression> vectorValueExpr(new Expression());
        Value vectorValue(TypeLibrary::getType(TID_INT64));
        vectorValue.setInt64(shape[i]);
        vectorValueExpr->compile(false, TID_INT64, vectorValue);
        newParameters.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(boost::make_shared<ParsingContext> (""), vectorValueExpr, true)));
    }

    sgNode->getPhysicalOperator()->setParameters(newParameters);
}

static PhysNodePtr s_findThinPoint(PhysNodePtr root)
{
    double dataWidth = root->getDataWidth();
    PhysNodePtr candidate = root;

    while (root->isSgNode() == false &&
           root->needsSpecificDistribution() == false &&
           root->isDistributionPreserving() &&
           root->isChunkPreserving() &&
           root->getChildren().size() == 1 )
    {
        root = root->getChildren()[0];
        if (root->getDataWidth() < dataWidth)
        {
            dataWidth = root->getDataWidth();
            candidate = root;
        }
    }
    return candidate;
}

static ArrayDistribution s_propagateDistribution(PhysNodePtr node,
                                                 PhysNodePtr end = PhysNodePtr())
{
    ArrayDistribution dist;
    do
    {
        dist = node->inferDistribution();
        node = node->getParent();
    } while (node != end && node->getChildren().size() <= 1);

    return dist;
}

void HabilisOptimizer::tw_insertSgNodes(PhysNodePtr root)
{
    assert(_root.get() != NULL);

    for (size_t i = 0; i < root->getChildren().size(); i ++)
    {
        tw_insertSgNodes(root->getChildren()[i]);
    }

    if (root->isSgNode() == false)
    {
        if (root->getChildren().size() == 1)
        {
            PhysNodePtr child = root->getChildren()[0];
            ArrayDistribution cDist = child->getDistribution();
            PhysNodePtr sgCandidate = child;

            bool sgNeeded = false;
            PartitioningSchema newDist;
            bool sgMovable = true, sgOffsetable = true;

            if (child -> isChunkPreserving() == false || cDist == ArrayDistribution(psLocalNode))
            {
                sgNeeded = true;
                newDist = psRoundRobin;
                sgMovable = false;
            }

            if (root -> needsSpecificDistribution())
            {
                ArrayDistribution reqDistro = root->getDistributionRequirement().getSpecificRequirements()[0];
                if (reqDistro.isViolated())
                    throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_NOT_IMPLEMENTED) << "requiring violated distributions";

                if (reqDistro != cDist)
                {
                    sgNeeded = true;
                    newDist = reqDistro.getPartitioningSchema();
                    sgOffsetable = false;
                    sgCandidate = s_findThinPoint(child);
                }
            }

            if (sgNeeded)
            {
               PhysNodePtr sgNode = n_buildSgNode(sgCandidate->getPhysicalOperator()->getSchema(), newDist);
                n_addParentNode(sgCandidate,sgNode);
                sgNode->inferBoundaries();
                sgNode->setSgMovable(sgMovable);
                sgNode->setSgOffsetable(sgOffsetable);
                s_propagateDistribution(sgNode, root);
            }
        }
        else if (root->getChildren().size() == 2)
        {
            ArrayDistribution lhs = root->getChildren()[0]->getDistribution();
            if (root->getChildren()[0]->isChunkPreserving() == false || lhs == ArrayDistribution(psLocalNode))
            {
                PhysNodePtr sgNode = n_buildSgNode(root->getChildren()[0]->getPhysicalOperator()->getSchema(), psRoundRobin);
                n_addParentNode(root->getChildren()[0],sgNode);
                sgNode->inferBoundaries();
                sgNode->setSgMovable(false);
                lhs = s_propagateDistribution(sgNode, root);
            }

            ArrayDistribution rhs = root->getChildren()[1]->getDistribution();
            if (root->getChildren()[1]->isChunkPreserving() == false || rhs == ArrayDistribution(psLocalNode))
            {
               PhysNodePtr sgNode = n_buildSgNode(root->getChildren()[1]->getPhysicalOperator()->getSchema(), psRoundRobin);
                n_addParentNode(root->getChildren()[1],sgNode);
                sgNode->inferBoundaries();
                sgNode->setSgMovable(false);
                rhs = s_propagateDistribution(sgNode, root);
            }

            if(root->getDistributionRequirement().getReqType() == DistributionRequirement::Collocated)
            {
                if (lhs != rhs)
                {
                    bool canMoveLeftToRight = (rhs.isViolated() == false);
                    bool canMoveRightToLeft = (lhs.isViolated() == false);

                    PhysNodePtr leftCandidate = s_findThinPoint(root->getChildren()[0]);
                    PhysNodePtr rightCandidate = s_findThinPoint(root->getChildren()[1]);

                    double leftDataWidth = leftCandidate->getDataWidth();
                    double rightDataWidth = rightCandidate->getDataWidth();

                    if (leftDataWidth < rightDataWidth && canMoveLeftToRight)
                    {
                       PhysNodePtr sgNode = n_buildSgNode(leftCandidate->getPhysicalOperator()->getSchema(), rhs.getPartitioningSchema());
                        n_addParentNode(leftCandidate, sgNode);
                        sgNode->inferBoundaries();
                        s_propagateDistribution(sgNode, root);
                    }
                    else if (canMoveRightToLeft)
                    {
                       PhysNodePtr sgNode = n_buildSgNode(rightCandidate->getPhysicalOperator()->getSchema(), lhs.getPartitioningSchema());
                        n_addParentNode(rightCandidate, sgNode);
                        sgNode->inferBoundaries();
                        s_propagateDistribution(sgNode, root);
                    }
                    else
                    {
                       PhysNodePtr leftSg = n_buildSgNode(leftCandidate->getPhysicalOperator()->getSchema(), psRoundRobin);
                        n_addParentNode(leftCandidate, leftSg);
                        leftSg->inferBoundaries();
                        s_propagateDistribution(leftSg, root);

                        PhysNodePtr rightSg = n_buildSgNode(rightCandidate->getPhysicalOperator()->getSchema(), psRoundRobin);
                        n_addParentNode(rightCandidate, rightSg);
                        rightSg->inferBoundaries();
                        s_propagateDistribution(rightSg, root);
                    }
                }
            }
            else if (root->needsSpecificDistribution())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_DISTRIBUTION_SPECIFICATION_ERROR);
            }
        }
        else if (root->getChildren().size() > 2)
        {
            bool needCollocation = false;
            if(root->getDistributionRequirement().getReqType() != DistributionRequirement::Any)
            {
                if (root->getDistributionRequirement().getReqType() != DistributionRequirement::Collocated)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_DISTRIBUTION_SPECIFICATION_ERROR2);
                }
                needCollocation = true;
            }

            for(size_t i=0; i<root->getChildren().size(); i++)
            {
                bool sgNeeded = false;

                PhysNodePtr child = root->getChildren()[i];
                ArrayDistribution distro = child->getDistribution();

                if (child->isChunkPreserving()==false)
                {
                    sgNeeded = true;
                }
                else if(needCollocation && distro != ArrayDistribution(psRoundRobin))
                {
                    //We have more than two children who must be collocated. This is a hard problem.
                    //Let's make everyone roundRobin for now.
                    sgNeeded = true;
                }

                if (sgNeeded)
                {
                    PhysNodePtr sgCandidate = s_findThinPoint(child);
                    PhysNodePtr sgNode = n_buildSgNode(sgCandidate->getPhysicalOperator()->getSchema(), psRoundRobin);
                    sgNode->setSgMovable(false);
                    sgNode->setSgOffsetable(false);
                    n_addParentNode(sgCandidate, sgNode);
                    sgNode->inferBoundaries();
                    s_propagateDistribution(sgNode, root);
                }
            }
        }
    }

    root->inferDistribution();
}

static PhysNodePtr s_getChainBottom(PhysNodePtr chainRoot)
{
    PhysNodePtr chainTop = chainRoot;
    while (chainTop->getChildren().size() == 1)
    {
        chainTop = chainTop->getChildren()[0];
    }
    assert(chainTop->isSgNode() == false);
    return chainTop;
}

static PhysNodePtr s_getFirstOffsetableSg(PhysNodePtr chainRoot)
{
    if (chainRoot->isSgNode() && chainRoot->isSgOffsetable())
    {
        return chainRoot;
    }

    if (chainRoot->getChildren().size() != 1 ||
        chainRoot->isDistributionPreserving() == false ||
        chainRoot->isChunkPreserving() == false ||
        chainRoot->needsSpecificDistribution())
    {
        return PhysNodePtr();
    }

    return s_getFirstOffsetableSg(chainRoot->getChildren()[0]);
}



void HabilisOptimizer::cw_rectifyChainDistro(PhysNodePtr root,
                                             PhysNodePtr sgCandidate,
                                             const ArrayDistribution & requiredDistribution)
{
    ArrayDistribution currentDistribution = root->getDistribution();
    PhysNodePtr chainParent = root->getParent();

    if (requiredDistribution != currentDistribution)
    {
        PhysNodePtr sgNode = s_getFirstOffsetableSg(root);
        if (sgNode.get() == NULL)
        {
            sgNode = n_buildSgNode(sgCandidate->getPhysicalOperator()->getSchema(), requiredDistribution.getPartitioningSchema());
            n_addParentNode(sgCandidate,sgNode);
            sgNode->inferBoundaries();
            if (sgCandidate == root)
            {
                root = sgNode;
            }
        }
        if (requiredDistribution.isViolated())
        {
            boost::shared_ptr<DistributionMapper> requiredMapper = requiredDistribution.getMapper();
            assert(requiredMapper.get()!=NULL);
            s_setSgDistribution(sgNode, requiredDistribution);
        }

        ArrayDistribution newRdStats = s_propagateDistribution(sgNode, chainParent);
    }

    assert(root->getDistribution() == requiredDistribution);
}

void HabilisOptimizer::tw_collapseSgNodes(PhysNodePtr root)
{
    bool topChain = (root == _root);

    PhysNodePtr chainBottom = s_getChainBottom(root);
    PhysNodePtr curNode = chainBottom;
    PhysNodePtr sgCandidate = chainBottom;

    ArrayDistribution runningDistribution = curNode->getDistribution();
    ArrayDistribution chainOutputDistribution = root->getDistribution();

    do
    {
        runningDistribution = curNode->inferDistribution();

        if (curNode->isSgNode() == false &&
             (curNode->isDistributionPreserving() == false ||
              curNode->isChunkPreserving() == false ||
              curNode->getDataWidth() < sgCandidate->getDataWidth()))
        {
            sgCandidate = curNode;
        }

        if (curNode->hasParent() &&
            curNode->getParent()->getChildren().size() == 1 &&
            curNode->getParent()->needsSpecificDistribution())
        {
            ArrayDesc curSchema =  curNode->getPhysicalOperator()->getSchema();
            ArrayDistribution neededDistribution = curNode->getParent()->getDistributionRequirement().getSpecificRequirements()[0];
            if (runningDistribution != neededDistribution)
            {
                if (curNode->isSgNode() && runningDistribution.getPartitioningSchema() == neededDistribution.getPartitioningSchema())
                {
                    curNode->getPhysicalOperator()->setSchema(curSchema);
                    s_setSgDistribution(curNode, neededDistribution);
                    curNode->setSgMovable(false);
                    curNode->setSgOffsetable(false);
                    runningDistribution = curNode->inferDistribution();
                }
                else
                {
                    PhysNodePtr newSg = n_buildSgNode(curSchema, neededDistribution.getPartitioningSchema());
                    n_addParentNode(sgCandidate,newSg);
                    newSg->inferBoundaries();
                    runningDistribution = s_propagateDistribution(newSg, curNode->getParent());
                    newSg->setSgMovable(false);
                    newSg->setSgOffsetable(false);

                    if (curNode == sgCandidate)
                    {
                        curNode = newSg;
                    }
                }
            }
        }
        else if (curNode->isSgNode() && curNode->isSgMovable())
        {
            PhysNodePtr newCur = curNode->getChildren()[0];
            n_cutOutNode(curNode);
            if (curNode == sgCandidate)
            {
                sgCandidate = newCur;
            }
            curNode = newCur;
            runningDistribution = curNode->getDistribution();
        }

        root = curNode;
        curNode = curNode->getParent();
    } while (curNode.get() != NULL && curNode->getChildren().size()<=1);

    assert(root);

    if (!topChain)
    {
        PhysNodePtr parent = root->getParent();
        if (parent->getDistributionRequirement().getReqType() != DistributionRequirement::Any)
        {
            //we have a parent node that has multiple children and needs a specific distribution
            //so we must correct the distribution back to the way it was before we started messing with the chain
            cw_rectifyChainDistro(root, sgCandidate, chainOutputDistribution);
        }
    }

    for (size_t i = 0; i< chainBottom->getChildren().size(); i++)
    {
        tw_collapseSgNodes(chainBottom->getChildren()[i]);
    }
}

static PhysNodePtr s_getTopSgFromChain(PhysNodePtr chainRoot)
{
    PhysNodePtr chainTop = chainRoot;

    while (chainTop->getChildren().size() == 1)
    {
        if(chainTop->isSgNode())
        {
            return chainTop;
        }
        else if (chainTop->isDistributionPreserving() == false ||
                 chainTop->isChunkPreserving() == false)
        {
            //TODO: this case can be opened up.. but it requires subtraction of offset vectors
            return PhysNodePtr();
        }

        chainTop = chainTop->getChildren()[0];
    }
    return PhysNodePtr();
}

void HabilisOptimizer::cw_pushupSg (PhysNodePtr root, PhysNodePtr sgToRemove, PhysNodePtr sgToOffset)
{
    PhysNodePtr sgrChild = sgToRemove->getChildren()[0];
    n_cutOutNode(sgToRemove);

    ArrayDistribution newSgrDistro = sgrChild->getDistribution();

    for (PhysNodePtr n = sgrChild->getParent(); n != root; n = n->getParent())
    {
        newSgrDistro = n->inferDistribution();
    }

    assert(newSgrDistro.hasMapper());

    ArrayDistribution newDist (newSgrDistro.getPartitioningSchema(), newSgrDistro.getMapper());

    s_setSgDistribution(sgToOffset, newDist);
    ArrayDistribution newSgoDistro = sgToOffset->inferDistribution();
    for (PhysNodePtr n = sgToOffset->getParent(); n != root; n = n->getParent())
    {
        newSgoDistro = n->inferDistribution();
    }

    assert(newSgrDistro == newSgoDistro);
    root->inferDistribution();

    PhysNodePtr newSg = n_buildSgNode(root->getPhysicalOperator()->getSchema(), psRoundRobin);
    newSg->setSgMovable(true);
    newSg->setSgOffsetable(true);
    n_addParentNode(root,newSg);
    newSg->inferDistribution();
    newSg->inferBoundaries();
}

void HabilisOptimizer::cw_swapSg (PhysNodePtr root, PhysNodePtr sgToRemove, PhysNodePtr oppositeThinPoint)
{
    PhysNodePtr sgrChild = sgToRemove->getChildren()[0];
    n_cutOutNode(sgToRemove);

    ArrayDistribution newSgrDistro = sgrChild->getDistribution();

    for (PhysNodePtr n = sgrChild->getParent(); n != root; n = n->getParent())
    {
        newSgrDistro = n->inferDistribution();
    }

    assert(newSgrDistro.hasMapper());

    ArrayDistribution newDist (newSgrDistro.getPartitioningSchema(), newSgrDistro.getMapper());

    PhysNodePtr newOppositeSg = n_buildSgNode(oppositeThinPoint->getPhysicalOperator()->getSchema(), psRoundRobin);
    n_addParentNode(oppositeThinPoint, newOppositeSg);
    s_setSgDistribution(newOppositeSg, newDist);
    newOppositeSg->inferBoundaries();
    ArrayDistribution newOppositeDistro = newOppositeSg->inferDistribution();
    for (PhysNodePtr n = newOppositeSg->getParent(); n != root; n = n->getParent())
    {
        newOppositeDistro = n->inferDistribution();
    }

    assert(newSgrDistro == newOppositeDistro);
    root->inferDistribution();

    PhysNodePtr newRootSg = n_buildSgNode(root->getPhysicalOperator()->getSchema(), psRoundRobin);
    newRootSg->setSgMovable(true);
    newRootSg->setSgOffsetable(true);
    n_addParentNode(root,newRootSg);
    newRootSg->inferDistribution();

    dbg_logPlan();

    newRootSg->inferBoundaries();

    dbg_logPlan();
}

bool HabilisOptimizer::tw_pushupJoinSgs(PhysNodePtr root)
{
    //"pushup" is a transformation from root(...join(sg(A),sg(B))) into root(...sg(join(sg(A),B)))
    //Note this is advantageous if placing sg on top results in less data movement

    //True if top chain SG will be "collapsed" by subsequent collapse()
    bool parentChainWillCollapse = root==_root ||
                                   root->getDistribution().hasMapper();

    //Thinnest available data point in top chain
    double parentChainThinPoint = root->getDataWidth();

    while (root->getChildren().size() == 1)
    {
        double currentThickness = root->getChildren()[0]->getDataWidth();
        if (currentThickness < parentChainThinPoint)
        {
            parentChainThinPoint = currentThickness;
        }

        //If the closest node above the join is an SG, then we can place another
        //SG onto top chain and the two SGs will collapse.

        //Otherwise, if the closest node above join needs correct distribution,
        //new SG will have to stay on top chain and get run

        if (root->isSgNode())
        {
            parentChainWillCollapse = true;
        }
        else if (root->needsSpecificDistribution())
        {
            parentChainWillCollapse = false;
            parentChainThinPoint = currentThickness;
        }

        root = root->getChildren()[0];
    }

    bool transformPerformed = false;

    if (root->getChildren().size() == 2)
    {
        if (root->getDistributionRequirement().getReqType() == DistributionRequirement::Collocated &&
            root->getChildren()[0]->getPhysicalOperator()->getSchema().getDimensions().size() ==
            root->getChildren()[1]->getPhysicalOperator()->getSchema().getDimensions().size())
        {
            PhysNodePtr leftChainRoot = root->getChildren()[0];
            PhysNodePtr rightChainRoot = root->getChildren()[1];

            PhysNodePtr leftSg = s_getTopSgFromChain(leftChainRoot);
            PhysNodePtr rightSg = s_getTopSgFromChain(rightChainRoot);

            if (leftSg.get()!=NULL && rightSg.get()!=NULL)
            {
                double leftAttributes = leftSg->getDataWidth();
                double rightAttributes = rightSg->getDataWidth();

                //the cost of not doing anything - run left SG and right SG
                double currentCost = leftAttributes + rightAttributes;

                //the cost of removing either SG
                double moveLeftCost = rightAttributes;
                double moveRightCost = leftAttributes;

                if (parentChainWillCollapse == false)
                {
                    //we will put sg on top and it will not collapse - add to the cost
                    moveLeftCost += parentChainThinPoint;
                    moveRightCost += parentChainThinPoint;
                }

                bool canMoveLeft = leftSg->isSgMovable() &&
                                   leftSg->getChildren()[0]->getDistribution().hasMapper() &&
                                   rightSg->isSgOffsetable();

                bool canMoveRight = rightSg->isSgMovable() &&
                                    rightSg->getChildren()[0]->getDistribution().hasMapper() &&
                                    leftSg->isSgOffsetable();

                if (canMoveLeft && moveLeftCost <= moveRightCost && moveLeftCost <= currentCost)
                {
                    cw_pushupSg(root,leftSg,rightSg);
                    transformPerformed = true;
                }
                else if (canMoveRight && moveRightCost <= currentCost)
                {
                    cw_pushupSg(root,rightSg,leftSg);
                    transformPerformed = true;
                }
            }
            else if ( leftSg.get() != NULL || rightSg.get() != NULL )
            {
                PhysNodePtr sg = leftSg.get() != NULL ? leftSg : rightSg;
                PhysNodePtr oppositeChain = leftSg.get() != NULL ? rightChainRoot : leftChainRoot;
                oppositeChain = s_findThinPoint(oppositeChain);

                bool canMoveSg = sg->isSgMovable() &&
                                 sg->getChildren()[0]->getDistribution().hasMapper();

                double currentCost = sg->getDataWidth();
                double moveCost = oppositeChain->getDataWidth();

                if (parentChainWillCollapse == false)
                {
                    //we will put sg on top and it will not collapse - add to the cost
                    moveCost += parentChainThinPoint;
                }

                if ( canMoveSg && moveCost < currentCost )
                {
                    cw_swapSg(root, sg, oppositeChain);
                    transformPerformed = true;
                }
            }
        }
    }

    bool result = transformPerformed;
    for (size_t i = 0; i< root->getChildren().size(); i++)
    {
        bool transformPerformedAtChild = tw_pushupJoinSgs(root->getChildren()[i]);
        result = transformPerformedAtChild || result;
    }
    return result;
}

void HabilisOptimizer::tw_insertAggReducers(PhysNodePtr root)
{
        //Assumptions we make here. All true as of writing:
        // 1. we assume that every "agg" operator is preceded by the first-phase operator.
        // 2. we assume that the agg operator does not care about distribution of input, does not change behavior.
        // 3. we assume that the reduce_distro operator does not affect boundaries
        //If these assumptions become false in the future, we need to revisit this part.

    if (root->isAgg())
    {
        if (root->getChildren().size() <= 0 ||
                root->getChildren()[0]->getChildren().size() <= 0)
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_MALFORMED_AGGREGATE);

        PhysNodePtr inputToAggregate = root->getChildren()[0]->getChildren()[0];

        ArrayDistribution const& inputDistribution = inputToAggregate->getDistribution();
        ArrayDesc const& inputSchema = inputToAggregate->getPhysicalOperator()->getSchema();

        if (inputDistribution.getPartitioningSchema() == psReplication)
        {
            PhysicalOperator::Parameters reducerParams;
            boost::shared_ptr<Expression> psConst = boost::make_shared<Expression> ();
            Value ps(TypeLibrary::getType(TID_INT32));
            ps.setInt32(psRoundRobin);
            psConst->compile(false, TID_INT32, ps);
            reducerParams.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(boost::make_shared<ParsingContext> (""), psConst, true)));

            PhysOpPtr reducerOp = OperatorLibrary::getInstance()->createPhysicalOperator("reduce_distro",
                                                                                         "physicalReduceDistro",
                                                                                         reducerParams,
                                                                                         inputSchema);
            reducerOp->setQuery(_query);
            PhysNodePtr reducerNode(new PhysicalQueryPlanNode(reducerOp, false, false, false));
            n_addParentNode(inputToAggregate, reducerNode);
            reducerNode->inferBoundaries();
            reducerNode->inferDistribution();

            //cosmetic
            reducerNode->getParent()->inferDistribution();
        }
    }

    BOOST_FOREACH(PhysNodePtr child, root->getChildren())
    {
        tw_insertAggReducers(child);
    }
}

void HabilisOptimizer::tw_rewriteStoringSG(PhysNodePtr root)
{
    if ( root->getPhysicalOperator()->getPhysicalName() == "physicalStore" )
    {
        PhysNodePtr child = root->getChildren()[0];
        if (child->isSgNode() && !child->isStoringSg() && child->getChildren()[0]->isChunkPreserving())
        {
            PhysOpPtr storeOp = root->getPhysicalOperator();
            ArrayDesc storeSchema = storeOp->getSchema();

            ArrayDistribution distro = child->getDistribution();
            if (distro != ArrayDistribution(psRoundRobin))
                throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_NOT_IMPLEMENTED) << " storing arrays in non-roro distribution";

            PhysNodePtr newSg = n_buildSgNode(storeSchema, psRoundRobin, true);
            PhysNodePtr grandChild = child->getChildren()[0];
            n_cutOutNode(root);
            n_cutOutNode(child);
            n_addParentNode(grandChild, newSg);

            newSg->inferBoundaries();
            newSg->inferDistribution();

            root = newSg;
        }
    }

    for (size_t i =0; i<root->getChildren().size(); i++)
    {
        tw_rewriteStoringSG(root->getChildren()[i]);
    }
}

void HabilisOptimizer::tw_insertRepartNodes(PhysNodePtr root)
{
    if ( root->getChildren().size() == 1)
    {
        ArrayDesc const& inputSchema = root->getChildren()[0]->getPhysicalOperator()->getSchema();
        if (root->getPhysicalOperator()->requiresRepart(inputSchema))
        {
            ArrayDesc repartSchema = root->getPhysicalOperator()->getRepartSchema(inputSchema);

            PhysicalOperator::Parameters repartParams;
            repartParams.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamSchema(boost::make_shared<ParsingContext> (""), repartSchema)));

            PhysOpPtr repartOp = OperatorLibrary::getInstance()->createPhysicalOperator("repart", "physicalRepart", repartParams, repartSchema);
            repartOp->setQuery(_query);

            PhysNodePtr repartNode(new PhysicalQueryPlanNode(repartOp, false, false, false));
            n_addParentNode(root->getChildren()[0], repartNode);
            repartNode->inferBoundaries();
            repartNode->inferDistribution();

            root->inferBoundaries();
            root->inferDistribution();
        }
    }

    for (size_t i =0; i < root->getChildren().size(); i++)
    {
        tw_insertRepartNodes(root->getChildren()[i]);
    }
}

void HabilisOptimizer::tw_insertChunkMaterializers(PhysNodePtr root)
{
    if ( root->hasParent() && root->getChildren().size() != 0)
    {
        PhysNodePtr parent = root->getParent();
        if (root->getPhysicalOperator()->getTileMode() != parent->getPhysicalOperator()->getTileMode())
        {
            ArrayDesc const& schema = root->getPhysicalOperator()->getSchema();
            Value formatParameterValue;
            formatParameterValue.setInt64(MaterializedArray::RLEFormat);
            boost::shared_ptr<Expression> formatParameterExpr = boost::make_shared<Expression> ();
            formatParameterExpr->compile(false, TID_INT64, formatParameterValue);
            PhysicalOperator::Parameters params;
            params.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(boost::make_shared<ParsingContext> (""), formatParameterExpr, true)));

            PhysOpPtr materializeOp = OperatorLibrary::getInstance()->createPhysicalOperator("materialize", "impl_materialize", params, schema);
            materializeOp->setQuery(_query);

            PhysNodePtr materializeNode(new PhysicalQueryPlanNode(materializeOp, false, false, false));
            n_addParentNode(root, materializeNode);
            materializeNode->inferBoundaries();
            materializeNode->inferDistribution();
        }
    }

    for (size_t i =0; i < root->getChildren().size(); i++)
    {
        tw_insertChunkMaterializers(root->getChildren()[i]);
    }
}

boost::shared_ptr<Optimizer> Optimizer::createHabilis()
{
    LOG4CXX_DEBUG(logger, "Creating Habilis optimizer instance")
    return boost::shared_ptr<Optimizer> (new HabilisOptimizer());
}

} // namespace
