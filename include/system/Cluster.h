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
 * @file Cluster.h
 *
 * @brief Contains class for providing information about cluster
 *
 * @author roman.simakov@gmail.com
 */

#ifndef CLUSTER_H
#define CLUSTER_H

#include <vector>
#include <set>
#include <util/Singleton.h>
#include <array/Metadata.h>
#include <util/Notification.h>

namespace scidb
{
typedef uint64_t ViewID;

/**
 * Class that describes the cluster membership, i.e. all physical SciDB nodes.
 */
class NodeMembership
{
 public:
   NodeMembership(ViewID viewId) : _viewId(viewId){}
   NodeMembership(ViewID viewId, std::vector<NodeID>& nodes) : _viewId(viewId)
   {
      _nodes.insert(nodes.begin(), nodes.end());
   }
   virtual ~NodeMembership() {}
   const std::set<NodeID>& getNodes() const { return _nodes; }
   ViewID getViewId() const { return _viewId; }
   void addNode(NodeID nodeId) { _nodes.insert(nodeId); }
   
   bool isEqual(const NodeMembership& other) const
   {
      return ((_viewId == other._viewId) &&
              (_nodes==other._nodes));
   }
 private:
   NodeMembership(const NodeMembership&);
   NodeMembership& operator=(const NodeMembership&);
   
   ViewID _viewId;
   std::set<NodeID> _nodes;
};

/**
 * Class that describes the cluster liveness, i.e. dead/live status of all physical SciDD nodes.
 * The view ID associated with a particular liveness must correspond to a particular membership.
 * For example, over the lifetime of a given membership with a given view ID there might be many
 * livenesses corresponding to the membership (via the view ID).
 */
 
 class NodeLivenessEntry
 {
   public:
      NodeLivenessEntry() : _generationId(0), _nodeId(INVALID_NODE), _isDead(false)
      {}
      NodeLivenessEntry(NodeID nodeId, uint64_t generationId, bool isDead) :
      _generationId(generationId), _nodeId(nodeId), _isDead(isDead) {}
      virtual ~NodeLivenessEntry() {}
      uint64_t getNodeId() const { return _nodeId; }
      uint64_t getGenerationId() const { return _generationId; }
      bool isDead() const { return _isDead; }
      void setGenerationId(uint64_t id) {_generationId = id; }
      void setNodeId(NodeID id) {_nodeId = id; }
      void setIsDead(bool state) {_isDead = state; }

      bool operator<(const NodeLivenessEntry& other) const {
         assert(_nodeId != INVALID_NODE);
         assert(other._nodeId != INVALID_NODE);
         return (_nodeId < other._nodeId);
      }
      bool operator==(const NodeLivenessEntry& other) const {
         assert(_nodeId != INVALID_NODE);
         assert(other._nodeId != INVALID_NODE);
         return ((_nodeId != INVALID_NODE) &&
                 (_nodeId == other._nodeId) &&
                 (_generationId == other._generationId) &&
                 (_isDead == other._isDead));
      }
      bool operator!=(const NodeLivenessEntry& other) const {
          return !operator==(other);
      }
   private:
      NodeLivenessEntry(const NodeLivenessEntry&);
      NodeLivenessEntry& operator=(const NodeLivenessEntry&);
      uint64_t _generationId;
      NodeID _nodeId;
      bool   _isDead;
   };

}  // namespace scidb

namespace boost
{
   template<>
   bool operator< (boost::shared_ptr<const scidb::NodeLivenessEntry> const& l,
                   boost::shared_ptr<const scidb::NodeLivenessEntry> const& r);

   template<>
   bool operator== (boost::shared_ptr<const scidb::NodeLivenessEntry> const& l,
                    boost::shared_ptr<const scidb::NodeLivenessEntry> const& r);

   template<>
   bool operator!= (boost::shared_ptr<const scidb::NodeLivenessEntry> const& l,
                    boost::shared_ptr<const scidb::NodeLivenessEntry> const& r);

} // namespace boost

namespace std
{
   template<>
   struct less<boost::shared_ptr<const scidb::NodeLivenessEntry> > :
   binary_function <const boost::shared_ptr<const scidb::NodeLivenessEntry>,
                    const boost::shared_ptr<const scidb::NodeLivenessEntry>,bool>
   {
      bool operator() (const boost::shared_ptr<const scidb::NodeLivenessEntry>& l,
                       const boost::shared_ptr<const scidb::NodeLivenessEntry>& r) const ;
   };
} // namespace std

namespace scidb
{
class NodeLiveness
{
 public:
   typedef boost::shared_ptr<const NodeLivenessEntry> NodePtr;
   typedef std::set<NodePtr> DeadNodes;
   typedef std::set<NodePtr> LiveNodes;

   NodeLiveness(ViewID viewId, uint64_t version) : _viewId(viewId), _version(version) {}
   virtual ~NodeLiveness() {}
   const LiveNodes& getLiveNodes() const { return _liveNodes; }
   const DeadNodes& getDeadNodes() const { return _deadNodes; }
   ViewID   getViewId()  const { return _viewId; }
   uint64_t getVersion() const { return _version; }
   bool     isDead(const NodeID& id) const { return find(_deadNodes, id); }
   size_t   getNumDead()  const { return _deadNodes.size(); }
   size_t   getNumLive()  const { return _liveNodes.size(); }
   size_t   getNumNodes() const { return getNumDead()+getNumLive(); }

   bool insert(const NodePtr& key)
   {
      assert(key);
      if (key->isDead()) {
         if (find(_liveNodes, key)) {
            assert(false);
            return false;
         }
         return _deadNodes.insert(key).second;
      } else {
         if (find(_deadNodes, key)) {
            assert(false);
            return false;
         }
         return _liveNodes.insert(key).second;
      }
      assert(false);
      return false;
   }

   NodePtr find(const NodeID& nodeId) const
   {
      const NodePtr key(new NodeLivenessEntry(nodeId,0,false));
      NodePtr val = find(_liveNodes, key);
      if (val) {
         assert(!val->isDead());
         return val;
      }
      val = find(_deadNodes, key);
      assert(!val || val->isDead());
      return val;
   }

   bool isEqual(const NodeLiveness& other) const
   {
      return ((_viewId == other._viewId) &&
              (_deadNodes==other._deadNodes) &&
              (_liveNodes==other._liveNodes));
   }

 private:
   typedef std::set<NodePtr> NodeEntries;
   NodeLiveness(const NodeLiveness&);
   NodeLiveness& operator=(const NodeLiveness&);
   NodePtr find(const NodeEntries& nodes, const NodeID& nodeId) const
   {
      const NodePtr key(new NodeLivenessEntry(nodeId,0,false));
      return find(nodes, key);
   }
   NodePtr find(const NodeEntries& nodes, const NodePtr& key) const
   {
      NodePtr found;
      NodeEntries::const_iterator iter = nodes.find(key);
      if (iter != nodes.end()) {
         found = (*iter);
      }
      return found;
   }
   ViewID _viewId;
   uint64_t _version;
   NodeEntries _liveNodes;
   NodeEntries _deadNodes;
};
   
typedef Notification<NodeLiveness> NodeLivenessNotification;
 
class Cluster: public Singleton<Cluster>
{
public:
   /**
    * Get cluster membership
    * @return current membership
    */ 
   boost::shared_ptr<const NodeMembership> getNodeMembership();

   /**
    * Get cluster liveness
    * @return current liveness
    */ 
   boost::shared_ptr<const NodeLiveness> getNodeLiveness();

   /**
    * Get this nodes' ID
    */
   NodeID getLocalNodeId();

private:
    friend class Singleton<Cluster>;
    boost::shared_ptr<const NodeMembership> _lastMembership;
    Mutex _mutex;
};

} // namespace

#endif // CLUSTER_H
