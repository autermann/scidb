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
 * @file HabilisOptimizer.h
 * @brief Our first attempt at a halfway intelligent optimizer.
 * @author poliocough@gmail.com
 */

// habilis (adj.) Latin: fit, easy, adaptable, apt, handy, well-adapted, inventive,.. (Google translate)
// Homo habilis: the earliest known species in the genus Homo. Perhaps the earliest primate to use stone tools (Wikipedia)

#ifndef HABILISOPTIMIZER_H_
#define HABILISOPTIMIZER_H_

#include "query/optimizer/Optimizer.h"
#include "query/QueryPlan.h"
#include "boost/shared_ptr.hpp"

#include <vector>

namespace scidb
{

class HabilisOptimizer : public Optimizer
{
public:
   friend class OptimizerTests;
   friend boost::shared_ptr<Optimizer> Optimizer::createHabilis();

   ~HabilisOptimizer()
   {}

   /**
    * Create an optimized physical plan from the given logical plan.
    * @return runnable physical plan.
    */
   PhysPlanPtr
   optimize(const boost::shared_ptr<Query>& query,
            boost::shared_ptr< LogicalPlan>& logicalPlan);

   enum FeatureMask
   {
       CONDENSE_SG     = 1,
       INSERT_REPART   = 2,
       INSERT_MATERIALIZATION = 4,
       REWRITE_STORING_SG     = 8
   };

private:
    /**
     * Create a new instance
     */
    HabilisOptimizer();

    class Eraser
    {
    public:
       Eraser(HabilisOptimizer& instance)
       : _instance(instance){}
       ~Eraser()
       {
          _instance._root.reset();
          _instance._query.reset();
       }
    private:
       Eraser();
       Eraser(const Eraser&);
       Eraser& operator=(const Eraser&);
       void* operator new(size_t size);
       HabilisOptimizer& _instance;
    };
    friend class Eraser;

    bool isFeatureEnabled (FeatureMask m) const
    {
        return _featureMask & m;
    }

    //////Data:

    /**
     * Current root of the plan. Initially empty.
     */
     PhysInstancePtr    _root;

     /**
     * Current query of the plan. Initially empty.
     */
     boost::shared_ptr<Query> _query;

    /**
     * Mask of features that are enabled
     */
    uint64_t        _featureMask;

    //////Helper functions - misc:

    /**
     * Print current WIP plan to stdout.
     */
    void
    dbg_printPlan();

    /**
     * Print current WIP plan to log.
     */
    void
    dbg_logPlan();

    //////Helper functions - instance-level manipulators:

    /**
     * Insert a instance into the plan tree. Add a instanceToInsert on top of target such that
     * target becomes child of instanceToInsert and instanceToInsert's parent becomes target's old
     * parent.
     * @param target instance used to specify the location
     * @param instanceToInsert new instance to insert.
     */
    void
    n_addParentInstance                         ( PhysInstancePtr target,  PhysInstancePtr instanceToInsert);

    /**
     * Remove a instance from the plan tree.
     * instanceToRemove's child becomes child of instanceToRemove's parent.
     * @param instanceToRemove. Must have only one child.
     */
    void
    n_cutOutInstance                            ( PhysInstancePtr instanceToRemove);

    /**
     * Build a new PhysicalParameter from a LogicalParameter.
     * Logic replicated from old optimizer.
     * @param[in] logicalParameter the parameter from the logical instance
     * @param[in] logicalInputSchemas all inputs to the logical instance
     * @param[in] logicalOutputSchema output schema inferred by the logical instance
     */
    boost::shared_ptr < OperatorParam>
    n_createPhysicalParameter (const boost::shared_ptr< OperatorParam> & logicalParameter,
                                    const std::vector< ArrayDesc>& logicalInputSchemas,
                                    const ArrayDesc& logicalOutputSchema,
                                    bool tile);

    /**
     * Build a new PhysicalQueryPlanNode from a LogicalQueryPlanNode.
     * Does not recurse.
     * @param[in] logicalInstance the instance to translate.
     */
     PhysInstancePtr
    n_createPhysicalInstance            (boost::shared_ptr < LogicalQueryPlanNode> logicalInstance, bool tileMode);


    /**
     * Build a new SGInstance from given attributes. Persist the result if storeArray is true.
     * If array is persisted, the name and id of are taken from outputSchema.
     * @param[in] outputSchema the output of the SG instance
     * @param[in] instanceId the argument to the SG operator
     * @param[in] storeArray store the result as a permanent new array (if true)
     */
     PhysInstancePtr
     n_buildSgInstance           (const ArrayDesc & outputSchema, PartitioningSchema partSchema, bool storeArray = false);


    //////Helper functions - chain walkers:

    /**
     * Remove sgToRemove from root; offset sgToOffset to match sgToRemove; put brand new, natural sg on top of root.
     * @param root a parent instance
     * @param sgToRemove child of root - an sg instance to cut out of the plan
     * @param sgToOffset parallel child of root - an sg instance to offset
     */
    void
    cw_pushupSg ( PhysInstancePtr root,  PhysInstancePtr sgToRemove,  PhysInstancePtr sgToOffset);

    /**
     * Remove sgToRemove; add new sg to oppositeThinPoint to match sgToRemove; put new sg on top of root.
     * @param root parent instance
     * @param sgToRemove child of root
     * @param oppositeThinPoint a thin point instance child of root, sibling of sgToRemove
     */
    void
    cw_swapSg (PhysInstancePtr root, PhysInstancePtr sgToRemove, PhysInstancePtr oppositeThinPoint);

    /**
     * Fix the data distribution coming out of root to match requiredStats.
     * This is accomplished by either adding an sg to the chain or finding an SG in the chain and offsetting it.
     * @param root head instance of chain (must have a join-parent or no parent)
     * @param sgCandidate the best place to put an sg
     * @param requiredStats the distribution stats the chain must have after the operation.
     */
    void
    cw_rectifyChainDistro( PhysInstancePtr root,  PhysInstancePtr sgCandidate, const  ArrayDistribution & requiredDistribution);

    //////Helper functions - tree walkers:

    /**
     * Create an entire physical tree from the logicalTree recursively.
     * Does not add all the necessary sg instances - output may not be runnable.
     * @param logicalRoot the root of the tree to translate.
     */
     PhysInstancePtr
     tw_createPhysicalTree               (boost::shared_ptr < LogicalQueryPlanNode> logicalRoot, bool tileMode);

    /**
     * Add all necessary scatter-gather instances to the tree.
     * @param root the root of the physical plan.
     */
    void
    tw_insertSgInstances                    ( PhysInstancePtr root);

    /**
     * Perform intrachain collapse of sg instances.
     * A chain is a plan segment terminated by leaf instances or instances with more than 1 child.
     * Remove all movable sg instances from chain.
     * Ensure that any instance in chain that requires correct distribution has an sg in front of it.
     * If chain output is not correctly distributed, add sg to top of chain.
     * Mark distribution stats on each instance in chain.
     * After execution, plan is correct and runnable.
     * @param root the root of the physical plan
     */
    void
    tw_collapseSgInstances          ( PhysInstancePtr root);

    /**
     * Perform interchain rotation of sg instances.
     * If root has a join with two child SGs, and one can be moved while another can be offset - evaluate costs,
     * and transform if necessary. Recurse on children.
     * After execution, plan is correct and runnable.
     * @param root root of physical plan.
     * @return true if a transformation was performed. false otherwise.
     */
    bool
    tw_pushupJoinSgs( PhysInstancePtr root);

    /**
     * For each two-phase aggregate instance,
     *    If input to first phase is psReplication
     *    add a reducer instance before first phase and reduce input to psRoundRobin
     *    infer distributiion on first phase
     * @param root root of physical plan.
     */
    void
    tw_insertAggReducers(PhysInstancePtr root);

    void
    tw_rewriteStoringSG(PhysInstancePtr root);

    void
    tw_insertRepartInstances(PhysInstancePtr root);

    void
    tw_insertChunkMaterializers(PhysInstancePtr root);
};

}

#endif /* HABILISOPTIMIZER_H_ */
