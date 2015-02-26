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
 * @file AST.h
 *
 * @brief Abstract syntax tree
 *
 * @details SciDB's AST represents query layer between user queries and coordinator
 * node as well as between coordinator and worker nodes. Abstract nodes tree can
 * be parsed later to logical or physical queries: when user sends coordinator
 * query, coordinator parsing it to AST, checking, converting to logical query,
 * passing to optimizer and executing. Physical query or it's parts during execution can
 * be sent to worker nodes this action done through serializing physical nodes to
 * physical plan string. Plan on worker node passing to parser again and AST of
 * this plan generated. After this AST converting to physical nodes backwards and
 * passing to worker's executor.
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#ifndef AST_H_
#define AST_H_

#include <vector>
#include <string>
#include <stdint.h>

#include <iostream>
#include <boost/shared_ptr.hpp>

//#include "AQLType.h"

namespace scidb
{

class ParsingContext;

/**
 * @enum AstNodeType
 * All possible nodes types excluding operators of array algebra which
 * all represented as UDFs.
 */
typedef enum
{
    unknownNode,
    identifierClause,
    attribute,
    attributesList,
    attributeName,
    attributeTypeName,
    attributeIsNullable,
    distinct,
    dimension,
    nonIntegerDimension,
    dimensionsList,
    dimensionName,
    dimensionBoundaries,
    dimensionBoundary,
    dimensionChunkInterval,
    dimensionChunkOverlap,
    stringNode,
    stringList,
    immutable,
    emptyable,
    function,
    functionName,
    functionPhysicalName,
    functionArguments,
    constant,
    null,
    aliasName,
    int64Node,
    realNode,
    boolNode,
    operatorName,
    functionAlias,
    schema,
    reference,
    objectName,
    arrayName,
    physicalProperties,
    physicalProperty,
    serializedOperatorParams,
    createArray,
    loadArray,
    sequencionalArrayAccess,
    numberedArrayAccess,
    namedExpr,
    pathExpressionList,
    selectStatement,
    selectList,
    fromList,
    joinClause,
    groupByClause,
    regridClause,
    windowClause,
    indexClause,
    exprList,
    loadStatement,
    saveStatement,
    insertStatement,
    updateStatement,
    updateList,
    updateListItem,
    deleteStatement,
    dropArrayStatement,
    caseClause,
    caseWhenClause,
    caseWhenClauseList,
    loadLibraryStatement,
    unloadLibraryStatement,
    asterisk,
    anonymousSchema,
    int64List,
    referenceQuirk,
    redimensionClause,
    windowRangesList,
    windowDimensionRange,
    olapAggregate,
    windowClauseList,
    regridDimension,
    regridDimensionsList,
    thinClause,
    thinDimension,
    thinDimensionsList,
    renameArrayStatement,
    cancelQueryStatement,
    orderByList,
    insertIntoStatement
} AstNodeType;


/**
 * @enum AstNodeArguments
 * Constants for easy accessing to some nodes childs
 */
typedef enum
{
    // CreateArray
    createArrayArgImmutable            = 0,
    createArrayArgEmpty,
    createArrayArgArrayName,
    createArrayArgSchema,
    createArrayArgCount,

    schemaArgAttributesList         = 0,
    schemaArgDimensionsList,
    schemaArgCount,

    attributeArgName                = 0,
    attributeArgTypeName,
    attributeArgIsNullable,
    attributeArgDefaultValue,
    attributeArgCompressorName,
    attributeArgReserve,
    attributeArgCount,

    dimensionArgName                = 0,
    dimensionArgBoundaries,
    dimensionArgChunkInterval,
    dimensionArgChunkOverlap,
    dimensionArgCount,

    nIdimensionArgName              = 0,
    nIdimensionArgDistinct,
    nIdimensionArgTypeName,
    nIdimensionArgBoundary,
    nIdimensionArgChunkInterval,
    nIdimensionArgChunkOverlap,
    nIdimensionArgCount,

    dimensionBoundaryArgLowBoundary    = 0,
    dimensionBoundaryArgHighBoundary,
    dimensionBoundaryArgCount,

    // LoadArray
    loadArrayArgArrayName = 0,
    loadArrayArgFilesList,
    loadArrayArgCount,

    // assignVariable
    assignVariableArgDst = 0,
    assignVariableArgSrc,
    assignVariableArgCount = 2,

    // function
    functionArgName = 0,
    functionArgParameters,
    functionArgAliasName,
    functionArgScalarOp,
    functionArgCount,

    // reference
    referenceArgArrayName = 0,
    referenceArgObjectName,
    referenceArgTimestamp,
    referenceArgSortQuirk,
    referenceArgIndex,
    referenceArgCount,

    // selectStatement
    selectClauseArgSelectList = 0,
    selectClauseArgIntoClause,
    selectClauseArgFromClause,
    selectClauseArgFilterClause,
    selectClauseArgGRWClause, //Group-by, Regrid and Window
    selectClauseArgOrderByClause,
    selectClauseArgCount,

    // namedExpr
    namedExprArgExpr = 0,
    namedExprArgName,
    namedExprArgCount,

    // joinClause
    joinClauseArgLeft = 0,
    joinClauseArgRight,
    joinClauseArgExpr,
    joinClauseArgCount,

    // groupByClause
    groupByClauseArgList = 0,
    groupByClauseArgCount,

    // regridClause
    regridClauseArgDimensionsList = 0,
    regridClauseArgCount,

    //regridDimension
    regridDimensionArgName = 0,
    regridDimensionArgStep,
    regridDimensionArgCount,

    // windowClause
    windowClauseArgName = 0,
    windowClauseArgRangesList,
    windowClauseArgVariableWindowFlag,
    windowClauseArgCount,

    // windowDimensionRange
    windowDimensionRangeArgName = 0,
    windowDimensionRangeArgPreceding,
    windowDimensionRangeArgFollowing,
    windowDimensionRangeArgCount,

    // windowDimensionCurrent
    windowDimensionCurrentArgName = 0,
    windowDimensionCurrentArgCount,

    // indexClause
    indexClauseArgName = 0,
    indexClauseArgCount,

    // loadStatement
    loadStatementArgArrayName = 0,
    loadStatementArgInstanceId,
    loadStatementArgFileName,
    loadStatementArgFormat,
    loadStatementArgErrors,
    loadStatementArgShadow,
    loadStatementArgCount,

    // saveStatement
    saveStatementArgArrayName = 0,
    saveStatementArgInstanceId,
    saveStatementArgFileName,
    saveStatementArgFormat,
    saveStatementArgCount,

    // sequencionalArrayAccess
    sequencionalArrayAccessArgReference = 0,
    sequencionalArrayAccessArgDimensionsList,
    sequencionalArrayAccessArgCount,

    // numberedArrayAccess
    numberedArrayAccessArgReference = 0,
    numberedArrayAccessArgDimensionsList,
    numberedArrayAccessArgCount,

    // insertStatement
    insertStatementArgArrayName = 0,
    insertStatementArgArrayValuesList,
    insertStatementArgCount,

    // updateStatement
    updateStatementArgArrayRef = 0,
    updateStatementArgUpdateList,
    updateStatementArgWhereClause,
    updateStatementArgCount,

    // updateListItem
    updateListItemArgName = 0,
    updateListItemArgExpr,
    updateListItemArgCount,

    // deleteStatement
    deleteStatementArgArrayName = 0,
    deleteStatementArgExpr,
    deleteStatementArgCount,

    // caseClause
    caseClauseArgArg = 0,
    caseClauseArgWhenList,
    caseClauseArgDefault,
    caseClauseArgCount,

    // caseWhenClause
    caseWhenClauseArgExpr = 0,
    caseWhenClauseArgResult,
    caseWhenClauseArgCount,
    
    // loadLibraryStatement
    loadLibraryStatementArgLibrary = 0,
    loadLibraryStatementArgCount,
    
    // unloadLibraryStatement
    unloadLibraryStatementArgLibrary = 0,
    unloadLibraryStatementArgCount,
    
    dropArrayStatementArgArray = 0,
    dropArrayStatementArgCount,
    
    //anonymousSchema
    anonymousSchemaClauseEmpty = 0,
    anonymousSchemaClauseSchema,
    anonymousSchemaArgCount,

    // olapAggregate
    olapAggregateArgFunction = 0,
    olapAggregateArgPartitionName,
    olapAggregateArgCount,

    //thinClause
    thinClauseArgArrayReference = 0,
    thinClauseArgDimensionsList,
    thinClauseArgCount,

    //thinDimension
    thinDimensionClauseArgName = 0,
    thinDimensionClauseArgStart,
    thinDimensionClauseArgStep,
    thinDimensionClauseArgCount,

    //renameArrayStatement
    renameArrayStatementArgOldName = 0,
    renameArrayStatementArgNewName,
    renameArrayStatementArgCount,

    //cancelQueryStatement
    cancelQueryStatementArgQueryId = 0,
    cancelQueryStatementArgCount,

    //insertIntoStatement
    insertIntoStatementArgDestination = 0,
    insertIntoStatementArgSource,
    insertIntoStatementArgCount
} AstNodeArguments;

typedef enum
{
    SORT_ASC,
    SORT_DESC
} SortQuirk;

class AstNode;

typedef std::pair<AstNode*, AstNode*> PairOfNodes;

typedef std::vector<AstNode*> AstNodes;

void runTypeInference(AstNode*);


class AstNodeBool;

class AstNodeInt64;

class AstNodeNull;

class AstNodeReal;

class AstNodeString;

/**
 * Base node of AST
 */
class AstNode
{
public:
    AstNode(AstNodeType type, boost::shared_ptr<ParsingContext> parsingContext, int8_t count, ...);


    /**
     * Copy constructor
     */
    AstNode(const AstNode& origin);

    /**
     * Default destructor
     */
    virtual ~AstNode();

    /**
     * Get type of node
     * @return Node's type
     */
    AstNodeType getType() const;

    /**
     * Add new child to node
     * @param node Child node
     */
    void addChild(AstNode *node);

    /**
     * Get all childs which node contain
     * @return Vector of AstNode
     */
    const AstNodes& getChilds() const;

    AstNodes::size_type getChildsCount() const;

    /**
     * Pass stage has info about tokens position, but don't has original query string.
     * This function create copy of query string in root AST and set pointers
     * to this string in each child node, so whole tree will be share same string.
     *
     * @param query Original string which was parsed into this AST.
     */
    void setQueryString(const std::string& queryString);

    const std::string& getQueryString() const
    {
        return *_pQueryString;
    }

    boost::shared_ptr<ParsingContext> getParsingContext() const
    {
        return _parsingContext;
    }

    //TODO: Move to operator []
    void setChild(size_t index, AstNode* node)
    {
        assert(index < _child_nodes.size());
        _child_nodes[index] = node;
    }

    virtual AstNode* clone() const;

//    void inferAQLTypes()
//    {
//        runTypeInference(this);
//    }

//    boost::shared_ptr<AQLType> getAQLType() const
//    { return _AQLType; }
//
//    void setAQLType(boost::shared_ptr<AQLType> t)
//    { _AQLType = t; }

    AstNode *getChild(size_t child) const;

    AstNodeBool* asNodeBool() const;

    AstNodeInt64* asNodeInt64() const;
    
    AstNodeNull* asNodeNull() const;
    
    AstNodeReal* asNodeReal() const;
    
    AstNodeString* asNodeString() const;


    void setComment(std::string const& comment);
    std::string const& getComment() const;

   
private:
    void setQueryString(std::string* queryString);

    AstNodeType _type;

    AstNodes _child_nodes;

    boost::shared_ptr<ParsingContext> _parsingContext;

    // For storing query in root of AST
    std::string _queryString;

    // For storing pointer to query in childs
    std::string* _pQueryString;

    // al-la Javadoc comment
    std::string comment;

    // AQL Type of this node
//    boost::shared_ptr<AQLType> _AQLType;
};

class AstNodeString : public AstNode
{
public:
    AstNodeString(AstNodeType type, boost::shared_ptr<ParsingContext> parsingContext, std::string val) :
        AstNode(type, parsingContext, 0),
        _val(val)
    {}

    /**
     * Copy constructor
     */
    AstNodeString(const AstNodeString& origin);

    const std::string& getVal() const
    {
        return _val;
    }

    AstNode* clone() const;

private:
    std::string _val;
};

class AstNodeInt64: public AstNode
{
public:
    AstNodeInt64(AstNodeType type, boost::shared_ptr<ParsingContext> parsingContext, int64_t val) :
        AstNode(type, parsingContext, 0),
        _val(val)
    {}

    /**
     * Copy constructor
     */
    AstNodeInt64(const AstNodeInt64& origin);

    int64_t getVal() const
    {
        return _val;
    }

    AstNode* clone() const;

private:
    int64_t _val;
};

class AstNodeReal: public AstNode
{
public:
    AstNodeReal(AstNodeType type, boost::shared_ptr<ParsingContext> parsingContext, double val) :
        AstNode(type, parsingContext, 0),
        _val(val)
    {}

    /**
     * Copy constructor
     */
    AstNodeReal(const AstNodeReal& origin);

    double getVal() const
    {
        return _val;
    }

    AstNode* clone() const;

private:
    double _val;
};

class AstNodeBool: public AstNode
{
public:
    AstNodeBool(AstNodeType type, boost::shared_ptr<ParsingContext> parsingContext, bool val) :
        AstNode(type, parsingContext, 0),
        _val(val)
    {}

    /**
     * Copy constructor
     */
    AstNodeBool(const AstNodeBool& origin);

    bool getVal() const
    {
        return _val;
    }

    AstNode* clone() const;

private:
    bool _val;
};

class AstNodeNull: public AstNode
{
public:
    AstNodeNull(AstNodeType type, boost::shared_ptr<ParsingContext> parsingContext) :
        AstNode(type, parsingContext, 0)
    {}

    /**
     * Copy constructor
     */
    AstNodeNull(const AstNodeNull& origin);

    AstNode* clone() const;
};

//Some helpers for creating tree

/**
 * Constructing function node for binary scalar operator
 * 
 * @param opName scalar operator name
 * @param left left operand node
 * @param right right operand node
 * @param parsingContext context from parser
 * @return pointer to created node. Must be deleted later.
 */
AstNode* makeBinaryScalarOp(const std::string &opName, AstNode *left, AstNode *right,
        const boost::shared_ptr<ParsingContext> &parsingContext);

/**
 * Constructing function node for unary scalar operator
 * 
 * @param opName scalar operator name
 * @param left left operand node
 * @param parsingContext context from parser
 * @return pointer to created node. Must be deleted later.
 */
AstNode* makeUnaryScalarOp(const std::string &opName, AstNode *left,
        const boost::shared_ptr<ParsingContext> &parsingContext);

} // namespace scidb

#endif /* AST_H_ */
