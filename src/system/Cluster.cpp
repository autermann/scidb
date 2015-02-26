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

#include <system/Cluster.h>
#include <network/NetworkManager.h>
#include <boost/shared_ptr.hpp>

namespace scidb
{

/**
 * @todo XXX Use NetworkManager until we explicitely start managing the membership
 */
boost::shared_ptr<const NodeMembership> Cluster::getNodeMembership()
{
   ScopedMutexLock lock(_mutex);

   if (_lastMembership) {
      return _lastMembership;
   }
   std::vector<NodeID> nodes;
   NetworkManager::getInstance()->getPhysicalNodes(nodes);
   _lastMembership = boost::shared_ptr<const NodeMembership>(new NodeMembership(0, nodes));
   return _lastMembership;
}

boost::shared_ptr<const NodeLiveness> Cluster::getNodeLiveness()
{
   boost::shared_ptr<const NodeLiveness> liveness(NetworkManager::getInstance()->getNodeLiveness());
   if (liveness) {
      return liveness;
   }
   boost::shared_ptr<const NodeMembership> membership(getNodeMembership());
   boost::shared_ptr<NodeLiveness> newLiveness(new NodeLiveness(membership->getViewId(), 0));
   for (std::set<NodeID>::const_iterator i = membership->getNodes().begin();
        i != membership->getNodes().end(); ++i) {
      NodeID nodeId(*i);
      NodeLiveness::NodePtr entry(new NodeLivenessEntry(nodeId, 0, false));
      newLiveness->insert(entry);
   }
   liveness = newLiveness;
   assert(liveness->getNumLive() > 0);
   return liveness;
}

NodeID Cluster::getLocalNodeId()
{
   return NetworkManager::getInstance()->getPhysicalNodeID();
}

} // namespace scidb

namespace boost
{
   template<>
   bool operator< (boost::shared_ptr<const scidb::NodeLivenessEntry> const& l,
                   boost::shared_ptr<const scidb::NodeLivenessEntry> const& r) {
      if (!l || !r) {
         assert(false);
         return false;
      }
      return (*l.get()) < (*r.get());
   }

   template<>
   bool operator== (boost::shared_ptr<const scidb::NodeLivenessEntry> const& l,
                    boost::shared_ptr<const scidb::NodeLivenessEntry> const& r) {

      if (!l || !r) {
         assert(false);
         return false;
      }
      return (*l.get()) == (*r.get());
   }

   template<>
   bool operator!= (boost::shared_ptr<const scidb::NodeLivenessEntry> const& l,
                    boost::shared_ptr<const scidb::NodeLivenessEntry> const& r) {
       return (!operator==(l,r));
   }
} // namespace boost

namespace std
{
   bool less<boost::shared_ptr<const scidb::NodeLivenessEntry> >::operator() (
                               const boost::shared_ptr<const scidb::NodeLivenessEntry>& l,
                               const boost::shared_ptr<const scidb::NodeLivenessEntry>& r) const
   {
      return l < r;
   }
}
