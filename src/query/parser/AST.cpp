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
 * @file
 *
 * @brief Abstract syntax tree
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#include <stdarg.h>
#include <iostream>
#include <typeinfo>
#include <assert.h>
#include <boost/foreach.hpp>

#include "query/parser/AST.h"

using namespace std;
using namespace boost;

namespace scidb
{

AstNode::AstNode(AstNodeType type, boost::shared_ptr<ParsingContext> parsingContext, int8_t count, ...) :
    _type(type),
    _parsingContext(parsingContext)
{
    va_list    ptr;
    va_start (ptr, count);
    while (--count >= 0)
    {
        AstNode *n = va_arg(ptr, AstNode*);
        _child_nodes.push_back(n);
    }
    va_end(ptr);
}

AstNode::AstNode(const AstNode& origin)
{
    _type = origin._type;
    _queryString = origin._queryString;
    _pQueryString = origin._pQueryString;
    _parsingContext = origin._parsingContext;
//    _AQLType = origin._AQLType;

    BOOST_FOREACH(const AstNode *child, origin.getChilds())
    {
        if (child)
            addChild(child->clone());
        else
            addChild(NULL); //Yeah, during parsing some nodes can be null...
    }
}

AstNode::~AstNode()
{
    for(AstNodes::iterator it = _child_nodes.begin(); it != _child_nodes.end();
            it++)
    {
        if (*it)
            delete *it;
    }
}


AstNodeType AstNode::getType() const
{
    return _type;
}

void AstNode::addChild(AstNode *node)
{
    _child_nodes.push_back(node);
}

const AstNodes& AstNode::getChilds() const
{
    return _child_nodes;
}

std::vector<AstNode*>::size_type AstNode::getChildsCount() const
{
    return _child_nodes.size();
}

void AstNode::setComment(std::string const& comment)
{
    this->comment = comment;
}

std::string const& AstNode::getComment() const
{
    return comment;
}

void AstNode::setQueryString(const std::string& queryString)
{
    _queryString = queryString;

    _pQueryString = &_queryString;

    setQueryString(&_queryString);
}

void AstNode::setQueryString(std::string* queryString)
{
    _pQueryString = queryString;

    BOOST_FOREACH(AstNode *child, getChilds())
    {
        if (child)
            child->setQueryString(queryString);
    }
}

AstNode* AstNode::clone() const
{
    return new AstNode(*this);
}

AstNode* AstNode::getChild(size_t child) const
{
    return _child_nodes[child];
}

AstNodeBool* AstNode::asNodeBool() const
{
    return (AstNodeBool*) this;
}

AstNodeInt64* AstNode::asNodeInt64() const
{
    return (AstNodeInt64*) this;
}

AstNodeNull* AstNode::asNodeNull() const
{
    return (AstNodeNull*) this;
}

AstNodeReal* AstNode::asNodeReal() const
{
    return (AstNodeReal*) this;
}

AstNodeString* AstNode::asNodeString() const
{
    return (AstNodeString*) this;
}

AstNodeBool::AstNodeBool(const AstNodeBool& origin):
    AstNode(origin)
{
    _val = origin._val;
}

AstNode* AstNodeBool::clone() const
{
    return new AstNodeBool(*this);
}

AstNodeInt64::AstNodeInt64(const AstNodeInt64& origin):
    AstNode(origin)
{
    _val = origin._val;
}

AstNode* AstNodeInt64::clone() const
{
    return new AstNodeInt64(*this);
}

AstNodeNull::AstNodeNull(const AstNodeNull& origin):
    AstNode(origin)
{
}

AstNode* AstNodeNull::clone() const
{
    return new AstNodeNull(*this);
}

AstNodeReal::AstNodeReal(const AstNodeReal& origin):
    AstNode(origin)
{
    _val = origin._val;
}

AstNode* AstNodeReal::clone() const
{
    return new AstNodeReal(*this);
}


AstNodeString::AstNodeString(const AstNodeString& origin):
    AstNode(origin)
{
    _val = origin._val;
}

AstNode* AstNodeString::clone() const
{
    return new AstNodeString(*this);
}

AstNode* makeBinaryScalarOp(const std::string &opName, AstNode *left, AstNode *right,
        const boost::shared_ptr<ParsingContext> &parsingContext)
{
    return new AstNode(function, parsingContext, functionArgCount,
        new AstNodeString(operatorName, parsingContext, opName),
        new AstNode(functionArguments, parsingContext, 2, left, right),
        NULL,
        new AstNodeBool(boolNode, parsingContext, true)
        );
}

AstNode* makeUnaryScalarOp(const std::string &opName, AstNode *left,
        const boost::shared_ptr<ParsingContext> &parsingContext)
{
    return new AstNode(function, parsingContext, functionArgCount,
        new AstNodeString(operatorName, parsingContext, opName),
        new AstNode(functionArguments, parsingContext, 1, left),
        NULL,
        new AstNodeBool(boolNode, parsingContext, true)
        );

}

} // namespace scidb
