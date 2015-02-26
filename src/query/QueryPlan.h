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
 * QueryPlan.h
 *
 *  Created on: Dec 24, 2009
 *      Author: Emad, roman.simakov@gmail.com
 */

#ifndef QUERYPLAN_H_
#define QUERYPLAN_H_

#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>

#include "array/Metadata.h"
#include "query/Operator.h"
#include "query/OperatorLibrary.h"
#include "system/SystemCatalog.h"


namespace scidb
{


/**
 * Node of logical plan of query. Logical node keeps logical operator to
 * perform inferring result type and validate types.
 */
class LogicalQueryPlanNode
{
public:
    LogicalQueryPlanNode(
            boost::shared_ptr<ParsingContext> const& parsingContext,
            boost::shared_ptr<LogicalOperator> const& logicalOperator);

    LogicalQueryPlanNode(
            boost::shared_ptr<ParsingContext> const& parsingContext,
            boost::shared_ptr<LogicalOperator> const& logicalOperator,
            std::vector<boost::shared_ptr<LogicalQueryPlanNode> > const &childNodes);

    void addChild(const boost::shared_ptr<LogicalQueryPlanNode>& child)
    {
        _childNodes.push_back(child);
    }

    bool isLeaf() const
    {
        return _childNodes.size();
    }

    boost::shared_ptr<LogicalOperator> getLogicalOperator(){
        return _logicalOperator;
    }

    std::vector<boost::shared_ptr<LogicalQueryPlanNode> >& getChildren(){
        return _childNodes;
    }

    bool isDdl() const
    {
        return _logicalOperator->getProperties().ddl;
    }

    bool supportsTileMode() const
    {
        return _logicalOperator->getProperties().tile;
    }

    const ArrayDesc& inferTypes(boost::shared_ptr< Query> query);

    void inferArrayAccess(boost::shared_ptr<Query>& query);

    boost::shared_ptr<ParsingContext> getParsingContext() const
    {
        return _parsingContext;
    }

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] str buffer to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    void toString (std::ostringstream &str, int indent = 0) const
    {
        for ( int i = 0; i < indent; i++)
        {
            str<<">";
        }

        str<<"[lInstance] children "<<_childNodes.size()<<"\n";
        _logicalOperator->toString(str,indent+1);

        for (size_t i = 0; i< _childNodes.size(); i++)
        {
            _childNodes[i]->toString(str, indent+1);
        }
    }

  private:
    boost::shared_ptr<LogicalOperator> _logicalOperator;

    std::vector<boost::shared_ptr<LogicalQueryPlanNode> > _childNodes;

    boost::shared_ptr<ParsingContext> _parsingContext;
};


typedef boost::shared_ptr<PhysicalOperator> PhysOpPtr;
class PhysicalQueryPlanNode;
typedef boost::shared_ptr<PhysicalQueryPlanNode> PhysNodePtr;


/*
 *  Currently LogicalQueryPlanNode and PhysicalQueryPlanNode have similar structure.
 *  It may change in future as it needed
 */
class PhysicalQueryPlanNode : boost::noncopyable
{
  public:
    PhysicalQueryPlanNode()
    {
    }

    PhysicalQueryPlanNode(PhysOpPtr const& physicalOperator,
                          bool agg, bool ddl, bool tile);

    PhysicalQueryPlanNode(PhysOpPtr const& PhysicalOperator,
                          std::vector<PhysNodePtr> const& childNodes,
                          bool agg, bool ddl, bool tile);

    virtual ~PhysicalQueryPlanNode()
    {
    }

    void addChild(const PhysNodePtr & child)
    {
        _childNodes.push_back(child);
    }

    /**
     * Removes node pointed to by targetChild from children.
     * @param targetChild node to remove. Must be in children.
     */
    void removeChild(const PhysNodePtr & targetChild)
    {
        std::vector<PhysNodePtr> newChildren;
        for(size_t i = 0; i < _childNodes.size(); i++)
        {
            if (_childNodes[i] != targetChild)
            {
                newChildren.push_back(_childNodes[i]);
            }
        }
        assert(_childNodes.size() > newChildren.size());
        _childNodes = newChildren;
    }

    /**
     * Replaces targetChild with newChild in children.
     * @param targetChild node to remove. Must be in children.
     * @param newChild node to insert. Must be in children.
     */
    void replaceChild(const PhysNodePtr & targetChild, const PhysNodePtr & newChild)
    {
        bool removed = false;
        std::vector<PhysNodePtr> newChildren;
        for(size_t i = 0; i < _childNodes.size(); i++)
        {
            if (_childNodes[i] != targetChild)
            {
                newChildren.push_back(_childNodes[i]);
            }
            else
            {
                newChildren.push_back(newChild);
                removed = true;
            }
        }
        _childNodes = newChildren;
        assert(removed); removed = removed; // Eliminate warnings
    }

    PhysOpPtr getPhysicalOperator() {
        return _physicalOperator;
    }

    std::vector<PhysNodePtr>& getChildren() {
        return _childNodes;
    }

    bool hasParent() const
    {
        return _parent.lock().get() != NULL;
    }

    void setParent (const PhysNodePtr& parent)
    {
        _parent = parent;
    }

    void resetParent ()
    {
        _parent.reset();
    }

    const PhysNodePtr getParent()
    {
        return _parent.lock();
    }

    bool isAgg() const
    {
        return _agg;
    }

    bool isDdl() const
    {
        return _ddl;
    }

    bool supportsTileMode() const
    {
        return _tile;
    }

    //TODO: there should be a list of arbitrary markers for optimizer to scratch with.
    //Something like a std::map<std::string, boost::any>.

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] str buffer to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    void toString (std::ostringstream &str, int indent = 0) const
    {
        for ( int i = 0; i < indent; i++)
        {
            str<<">";
        }

        str<<"[pNode] "<<_physicalOperator->getPhysicalName()<<" agg "<<isAgg()<<" ddl "<<isDdl()<<" tile "<<supportsTileMode()<<" children "<<_childNodes.size()<<"\n";
        _physicalOperator->toString(str,indent+1);

        for (int i = 0; i<indent+1; i++)
        {
            str<<" ";
        }
        str<<"props sgm "<<_isSgMovable<<" sgo "<<_isSgOffsetable<<"\n";

        for (int i = 0; i<indent+1; i++)
        {
            str<<" ";
        }
        str<<"distr "<<_distribution<<"\n";

        const ArrayDesc& schema = _physicalOperator->getSchema();
        for (int i = 0; i<indent+1; i++)
        {
            str<<" ";
        }

        str<<"bound "<<_boundaries
          <<" cells "<<_boundaries.getNumCells();

        if (_boundaries.getStartCoords().size() == schema.getDimensions().size())
        {
            str  <<" chunks "<<_boundaries.getNumChunks(schema.getDimensions())
                <<" est_bytes "<<_boundaries.getSizeEstimateBytes(schema)
               <<"\n";
        }
        else
        {
            str <<" [improperly initialized]\n";
        }

        for (size_t i = 0; i< _childNodes.size(); i++)
        {
            _childNodes[i]->toString(str, indent+1);
        }
    }

    /**
     * Retrieve an ordered list of the shapes of the arrays to be input to this
     * node.
     */
    std::vector<ArrayDesc> getChildSchemas() const
    {
        std::vector<ArrayDesc> result;
        for (size_t i = 0, count = _childNodes.size(); i < count; ++i)
        {
            PhysNodePtr const& child = _childNodes[i];
            result.push_back(child->getPhysicalOperator()->getSchema());
        }
        return result;
    }

    /**
     * Determine if this node is for the PhysicalSG operator.
     * @return true if physicalOperator is PhysicalSG. False otherwise.
     */
    bool isSgNode() const
    {
        return _physicalOperator.get() != NULL &&
                _physicalOperator->getPhysicalName() == "impl_sg";
    }

    bool isStoringSg() const;

    /**
     * @return the sgMovable flag
     */
    bool isSgMovable() const
    {
        return _isSgMovable;
    }

    /**
     * Set the sgMovable flag
     * @param value value to set
     */
    void setSgMovable(bool value)
    {
        _isSgMovable = value;
    }

    /**
     * @return the sgOffsetable flag
     */
    bool isSgOffsetable() const
    {
        return _isSgOffsetable;
    }

    /**
     * Set the sgOffsetable flag
     * @param value value to set
     */
    void setSgOffsetable(bool value)
    {
        _isSgOffsetable = value;
    }

    /**
     * Delegator to physicalOperator.
     */
    bool changesDistribution() const
    {
        return _physicalOperator->changesDistribution(getChildSchemas());
    }

    /**
     * Delegator to physicalOperator.
     */
    bool outputFullChunks() const
    {
        return _physicalOperator->outputFullChunks(getChildSchemas());
    }

    /**
      * [Optimizer API] Determine if the output chunks
      * of this subtree will be completely filled.
      * Optimizer may insert SG operations for subtrees
      * that do not provide full chunks.
      * @return true if output chunking is guraranteed full, false otherwise.
      */
    bool subTreeOutputFullChunks() const
    {
        if (isSgNode()) {
            return true;
        }
        for (size_t i = 0, count = _childNodes.size(); i< count; ++i) {
            if (!_childNodes[i]->subTreeOutputFullChunks()) {
                return false;
            }
        }
        return _physicalOperator->outputFullChunks(getChildSchemas());
    }

    DistributionRequirement getDistributionRequirement() const
    {
        return _physicalOperator->getDistributionRequirement(getChildSchemas());
    }

    bool needsSpecificDistribution() const
    {
        return getDistributionRequirement().getReqType()== DistributionRequirement::SpecificAnyOrder;
    }

    /**
     * @return the number of attributes emitted by the node.
     */
    double getDataWidth()
    {
        return _boundaries.getSizeEstimateBytes(getPhysicalOperator()->getSchema());
    }

    /**
     * @return stats about distribution of node output
     */
    const ArrayDistribution& getDistribution() const
    {
        return _distribution;
    }

    /**
     * Calculate information about distribution of node output, using
     * the distribution stats of the child nodes, plus
     * the data provided from the PhysicalOperator. Sets distribution
     * stats of node to the result.
     * @param prev distribution stats of previous node's output
     * @return new distribution stats for this node's output
     */
    const ArrayDistribution& inferDistribution ()
    {
        std::vector<ArrayDistribution> childDistros;
        for (size_t i =0; i<_childNodes.size(); i++)
        {
            childDistros.push_back(_childNodes[i]->getDistribution());
        }
        _distribution = _physicalOperator->getOutputDistribution(childDistros, getChildSchemas());
        return _distribution;
    }

    //I see an STL pattern coming soon...
    const PhysicalBoundaries& getBoundaries() const
    {
        return _boundaries;
    }

    const PhysicalBoundaries& inferBoundaries()
    {
        std::vector<PhysicalBoundaries> childBoundaries;
        for (size_t i =0; i<_childNodes.size(); i++)
        {
            childBoundaries.push_back(_childNodes[i]->getBoundaries());
        }
        _boundaries = _physicalOperator->getOutputBoundaries(childBoundaries, getChildSchemas());
        return _boundaries;
    }

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & _childNodes;
        ar & _agg;
        ar & _ddl;
        ar & _tile;
        ar & _isSgMovable;
        ar & _isSgOffsetable;
        //We don't need distribution or sizing info - they are used for optimization only.

        /*
         * We not serializing whole operator object, to simplify user's life and get rid work serialization
         * user classes and inherited SciDB classes. Instead this we serializing operator name and
         * its parameters, and later construct operator by hand
         */
        if (Archive::is_loading::value)
        {
            std::string logicalName;
            std::string physicalName;
            PhysicalOperator::Parameters parameters;
            ArrayDesc schema;

            ar & logicalName;
            ar & physicalName;
            ar & parameters;
            ar & schema;

            _physicalOperator = OperatorLibrary::getInstance()->createPhysicalOperator(
                        logicalName, physicalName, parameters, schema);
            _physicalOperator->setTileMode(_tile);
        }
        else
        {
            std::string logicalName = _physicalOperator->getLogicalName();
            std::string physicalName = _physicalOperator->getPhysicalName();
            PhysicalOperator::Parameters parameters = _physicalOperator->getParameters();
            ArrayDesc schema = _physicalOperator->getSchema();

            ar & logicalName;
            ar & physicalName;
            ar & parameters;
            ar & schema;
        }
    }

private:
    PhysOpPtr _physicalOperator;

    // <RANT>
    // The boost::shared_ptr<PhysicalQueryPlanNode> should not be used internally in the implementation!!
    // boost::shared_ptrs cannot be used in cyclical graphs, so we have to use weak_ptr to parent.
    // Weak_ptr cannot be created without boost::shared_ptr so we end up having a moronic setParent method
    // instead of doing everything in addChild.
    //
    // Or we have to use a regular * and then return that to the visitor which makes things inconsistent
    // and not const-safe...
    //
    // What we should probably have is:
    // a vector<PhysicalQueryPlanNode*> internally
    // make the planNode immutable after creation
    // return const-pointers for access, return copies for creation
    // walkers create a new plan tree from subtrees.
    // destructor takes care of eveything.
    // </RANT> ap

    std::vector< PhysNodePtr > _childNodes;
    boost::weak_ptr <PhysicalQueryPlanNode> _parent;

    bool _agg;
    bool _ddl;
    bool _tile;

    bool _isSgMovable;
    bool _isSgOffsetable;

    ArrayDistribution _distribution;
    PhysicalBoundaries _boundaries;
};

/**
 * The LogicalPlan represents result of parsing query and is used for validation query.
 * It's input data for optimization and generation physical plan.
 */
class LogicalPlan
{
public:
    LogicalPlan(const boost::shared_ptr<LogicalQueryPlanNode>& root);

    boost::shared_ptr<LogicalQueryPlanNode> getRoot()
    {
        return _root;
    }

    void setRoot(const boost::shared_ptr<LogicalQueryPlanNode>& root)
    {
        _root = root;
    }

    const ArrayDesc& inferTypes(boost::shared_ptr< Query>& query)
    {
        return _root->inferTypes(query);
    }

    void inferArrayAccess(boost::shared_ptr<Query>& query)
    {
        return _root->inferArrayAccess(query);
    }

	/**
	 * Retrieve a human-readable description.
	 * Append a human-readable description of this onto str. Description takes up
	 * one or more lines. Append indent spacer characters to the beginning of
	 * each line. Call toString on interesting children. Terminate with newline.
	 * @param[out] str buffer to write to
	 * @param[in] indent number of spacer characters to start every line with.
	 */
	void toString(std::ostringstream &str, int const indent = 0) const
	{
		for ( int x = 0; x < indent; x++)
		{
			str<<">";
		}
		str << "[lPlan]:\n";
		_root->toString(str, indent+1);
	}


private:
    boost::shared_ptr<LogicalQueryPlanNode> _root;
};


/**
 * The PhysicalPlan is produced by Optimizer or in simple cases directly by query processor (DDL).
 * It has ready to execution operator nodes and will be passed to an executor.
 */
class PhysicalPlan
{
public:
    PhysicalPlan(const boost::shared_ptr<PhysicalQueryPlanNode>& root);

    boost::shared_ptr<PhysicalQueryPlanNode> getRoot() {
        return _root;
    }

    bool empty() const {
        return _root == boost::shared_ptr<PhysicalQueryPlanNode>();    // _root is NULL
    }
    
    bool isDdl() const
    {
    	assert(!empty());
    	return _root->isDdl();
    }

    bool supportsTileMode() const
    {
    	assert(!empty());
    	return _root->supportsTileMode();
    }

	void setRoot(const boost::shared_ptr<PhysicalQueryPlanNode>& root)
	{
		_root = root;
	}

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] str buffer to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
	void toString(std::ostringstream &str, int const indent = 0) const
	{
		for ( int x = 0; x < indent; x++)
		{
			str<<">";
		}
		str << "[pPlan]:";
		if (_root.get() != NULL)
		{
			str << "\n";
			_root->toString(str, indent+1);
		}
		else
		{
			str << "[NULL]\n";
		}
	}

private:
    boost::shared_ptr<PhysicalQueryPlanNode> _root;
};

typedef boost::shared_ptr<PhysicalPlan> PhysPlanPtr;


} // namespace


#endif /* QUERYPLAN_H_ */
