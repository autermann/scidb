/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2014 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

/****************************************************************************/

#include "AST.h"                                         // For Node

/****************************************************************************/
namespace scidb { namespace parser {
/****************************************************************************/

/**
 *  Complete the construction for a node of type 't', associated with location
 *  'w' in the original source text. Its children have already been written to
 *  the end of this node earlier by the associated 'new' operator, so all that
 *  remains for us to do is to save the type and location fields.
 */
    Node::Node(type t,const location& w)
        : _type(t),
          _where(w),
          _size(_size)
{}

/**
 *  Allocate the underlying memory for a node with children 'c'.  We are using
 *  the old C programmer's trick of extending the allocation to save the array
 *  at the end of the node because a) this saves space, but, more importantly,
 *  b)  by not carrying a container, our destructor need no longer be invoked.
 *  As a result, the entire abstract syntax tree can be cheaply created in the
 *  caller's resetting arena and simply flushed in one go once the translation
 *  is complete: no need to recurse back over the tree calling destructors.
 */
void* Node::operator new(size_t n,Arena& a,cnodes c)
{
    void* p = a.malloc(n + c.size() * sizeof(Node*));    // Room for children
    Node* q = static_cast<Node*>(p);                     // Start of the node

    const_cast<size_t&>(q->_size) = c.size();            // Assign node arity
    std::copy(c.begin(),c.end(),q->getList().begin());   // Copy childen over

    return p;                                            // Return allocation
}

/**
 *  Allocate and return a shallow copy of this node; that is, a new allocation
 *  whose contents is identical to this node, and that, in particular, carries
 *  pointers to exactly the same children.
 */
Node* Node::copy(Factory& f) const
{
    return new(f.getArena(),getList()) Node(*this);      // Allocate new copy
}

/****************************************************************************/
}}
/****************************************************************************/
