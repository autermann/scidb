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
 * @brief Parsing and translation AQL/AFL AST tree into AQL query tree.
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 * @author Pavel Velikhov <pavel.velikhov@gmail.com>
 */

#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>

#include "query/parser/ALTranslator.h"
#include "query/parser/AST.h"
#include "query/parser/Serialize.h"
#include "query/QueryPlan.h"
#include "query/OperatorLibrary.h"
#include "query/LogicalExpression.h"

#include "array/Metadata.h"

#include "system/Exceptions.h"
#include "system/SystemCatalog.h"

#include "network/NetworkManager.h"

#include "array/Compressor.h"

using namespace boost;

typedef map<string, string> strStrMap;

#define PLACEHOLDER_OUTPUT_FLAG (PLACEHOLDER_END_OF_VARIES << 1)

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.altranslator"));

namespace scidb
{

static shared_ptr<LogicalQueryPlanNode> passCreateArray(AstNode *ast, const shared_ptr<Query> &query);

static void passSchema(AstNode *ast, ArrayDesc &schema, const string &arrayName, bool addEmpty, bool immutable,
                       const shared_ptr<Query> &query, std::string const& comment = std::string());

static shared_ptr<LogicalQueryPlanNode> passAFLOperator(AstNode *ast, const shared_ptr<Query> &query);

static bool matchOperatorParam(AstNode *ast, const OperatorParamPlaceholders &placeholders,
        const shared_ptr<Query> &query, vector<ArrayDesc> &inputSchemas,
        vector<shared_ptr<LogicalQueryPlanNode> > &inputs, shared_ptr<OperatorParam> &param);

static void placeholdersToString(const vector<shared_ptr<OperatorParamPlaceholder> > &placeholders, string &result);

static void astParamToString(const AstNode* ast, string &result);

static bool resolveParamAttributeReference(const vector<ArrayDesc> &inputSchemas,
        shared_ptr<OperatorParamReference> &attRef, bool throwException = true);

static bool resolveParamDimensionReference(const vector<ArrayDesc> &inputSchemas,
        shared_ptr<OperatorParamReference> &dimRef, bool throwException = true);

static shared_ptr<LogicalExpression> passScalarFunction(AstNode *ast);

static shared_ptr<OperatorParamAggregateCall> passAggregateCall(AstNode *ast, const vector<ArrayDesc> &inputSchemas);

static shared_ptr<LogicalExpression> passConstant(AstNode *ast);

static shared_ptr<LogicalExpression> passReference(AstNode *ast);

static bool placeholdersVectorContainType(const vector<shared_ptr<OperatorParamPlaceholder> > &placeholders,
    OperatorParamPlaceholderType placeholderType);

static shared_ptr<LogicalQueryPlanNode> passSelectStatement(AstNode *ast, shared_ptr<Query> query);

static shared_ptr<LogicalQueryPlanNode> passJoins(AstNode *ast, shared_ptr<Query> query);

static shared_ptr<LogicalQueryPlanNode> passGeneralizedJoin(AstNode *ast, shared_ptr<Query> query);

static bool passGeneralizedJoinOnClause(vector<shared_ptr<OperatorParamReference> > &params,
                                        AstNode *ast,
                                        shared_ptr<Query> query);

static shared_ptr<LogicalQueryPlanNode> passCrossJoin(AstNode *ast, shared_ptr<Query> query);

static shared_ptr<LogicalQueryPlanNode> passJoinItem(AstNode *ast, shared_ptr<Query> query);

static shared_ptr<LogicalQueryPlanNode> passImplicitScan(AstNode *ast, shared_ptr<Query> query);

static shared_ptr<LogicalQueryPlanNode> passFilterClause(AstNode *ast, const shared_ptr<LogicalQueryPlanNode> &input,
    const shared_ptr<Query> &query);

static shared_ptr<LogicalQueryPlanNode> passIntoClause(AstNode *ast, shared_ptr<LogicalQueryPlanNode> &input,
        shared_ptr<Query> &query);

static shared_ptr<LogicalQueryPlanNode> passUpdateStatement(AstNode *ast, const shared_ptr<Query> &query);

static shared_ptr<LogicalQueryPlanNode> passLoadStatement(AstNode *ast, shared_ptr<Query> query);

static shared_ptr<LogicalQueryPlanNode> passDropArrayStatement(AstNode *ast);

static shared_ptr<LogicalQueryPlanNode> passLoadLibrary(AstNode *ast);

static shared_ptr<LogicalQueryPlanNode> passUnloadLibrary(AstNode *ast);

static bool checkAttribute(const vector<ArrayDesc> &inputSchemas, const string &aliasName,
        const string &attributeName, const shared_ptr<ParsingContext> &ctxt);

static bool checkDimension(const vector<ArrayDesc> &inputSchemas, const string &aliasName,
        const string &dimensionName, const shared_ptr<ParsingContext> &ctxt);

static void checkLogicalExpression(const vector<ArrayDesc> &inputSchemas, const ArrayDesc &outputSchema,
        const shared_ptr<LogicalExpression> &expr);

static shared_ptr<LogicalQueryPlanNode> appendOperator(
    const shared_ptr<LogicalQueryPlanNode> &node,
    const string &opName,
    const LogicalOperator::Parameters &opParams,
    const shared_ptr<ParsingContext> &opParsingContext);

/**
 * Check if AST has reference or asterisk nodes
 *
 * @param ast input AST
 * @return true if reference or asterisk found
 */
static bool astHasReferences(const AstNode *ast);

/**
 * Check if AST has function node which is in aggregate library
 *
 * @param ast input AST
 * @return true if aggregate call node found
 */
static bool astHasAggregates(const AstNode *ast);

/**
 * @brief Transform complex AST expression with aggregate into small to able evaluate in engine
 *
 * This function traverse expression with aggregate and split into 3 parts:
 * 1 - pre-aggregate evaluations, one expression for each expression inside aggregate functions
 * 2 - aggregate calls - one for each aggregate in expression
 * 3 - post-aggregate evaluation - one for all expression
 * This function not create query tree node and just transform AST from complex AQL to simple AFL.
 * Semantic check will be done on next step.
 *
 * @example
 * count(a) * 2 + foo(sum(a+b)) ->
 *      pre-eval:   a+b -> expr1
 *      aggregates: count(a) -> expr2, sum(expr1) -> expr3
 *      post-eval:  expr2 * 2 + foo(expr3)
 *
 * @param[in] ast input expression AST
 * @param[out] preAggregationEvals list of expressions which must be evaluated before aggregate
 * @param[out] aggregateFunctions list of aggregate calls
 * @param[out] internalNameCounter all evaluation results mapped to internal attribute names,
 *             this counter must tick each new name
 * @param[in] hasAggregates if true, references outside aggregate call not allowed
 * @param[in] partitionName fake nested array name produced by group-by-as/regrid-as/windows-as clause
 * @param[in] inputSchema initial input schema
 *
 * @return post-post aggregate expression AST
 *
 * @warning Result and output arguments must be destructed
 */
static AstNode* decomposeExpression(
    const AstNode *ast,
    AstNodes &preAggregationEvals,
    AstNodes &aggregateFunctions,
    unsigned int &internalNameCounter,
    bool hasAggregates,
    const string &partitionName,
    const ArrayDesc &inputSchema);

/**
 * @brief Transform expressions SELECT list clause into apply/project operators and aggregates calls
 *
 * This function transform SELECT list and GROUP BY/WINDOW/REGRID clause into query subtree with
 * APPLY/PROJECT/AGGREGATE/WINDOW/REGRID operators and append it to input query node.
 *
 * @param[in] input input query node
 * @param[in] selectList input AST node with SELECT list
 * @param[in] grwAsClause input AST node with GROUP BY, WINDOW or regrid clause
 * @param[in] query global query context
 *
 * @return Root of query tree
 */
static shared_ptr<LogicalQueryPlanNode> passSelectList(shared_ptr<LogicalQueryPlanNode> &input,
    AstNode *selectList, AstNode* grwAsClause, const shared_ptr<Query> &query);

/**
 * @brief Generate object name by user's prefix and number suffix and check if such name
 *        present in schemas. Suffix will be incremented until unique name will not be found.
 *
 * @param[in] prefix user defined prefix
 * @param[in,out] initialCounter counter from which names will generated initially
 * @param inputSchemas[in] input schemas
 * @param internal[in] if true name will be surrounded with $
 * @param namedExpressions[in] optional list with names expressions to count each named SELECT list item
 *
 * @return unique object name
 */
static string genUniqueObjectName(const string& prefix, unsigned int &initialCounter,
    const vector<ArrayDesc> &inputSchemas, bool internal, const AstNodes& namedExpressions = vector<AstNode*>());

shared_ptr<LogicalQueryPlanNode> AstToLogicalPlan(AstNode *ast, const shared_ptr<Query> &query)
{
    switch(ast->getType())
    {
        //AFL/AQL
        case function:
            return passAFLOperator(ast, query);

        case selectStatement:
            return passSelectStatement(ast, query);

        case reference:
            return passImplicitScan(ast, query);

        //DDL/DML
        case createArray:
        return passCreateArray(ast, query);

        case loadStatement:
        return passLoadStatement(ast, query);

        case updateStatement:
            return passUpdateStatement(ast, query);

        case dropArrayStatement:
            return passDropArrayStatement(ast);

        case loadLibraryStatement:
            return passLoadLibrary(ast);

        case unloadLibraryStatement:
            return passUnloadLibrary(ast);

        default:
            assert(0);
    }
}

shared_ptr<LogicalExpression> AstToLogicalExpression(AstNode *ast)
{
    switch(ast->getType())
    {
        case function:
            return passScalarFunction(ast);

        case null:
        case constant:
        case realNode:
        case int64Node:
        case stringNode:
        case boolNode:
            return passConstant(ast);

        case reference:
            return passReference(ast);

        case selectStatement:
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_SUBQUERIES_NOT_SUPPORTED, ast->getParsingContext());
        }

        case asterisk:
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_WRONG_ASTERISK_USAGE, ast->getParsingContext());
        }

        default:
            assert(0);
    }
}

static shared_ptr<LogicalQueryPlanNode> passCreateArray(AstNode *ast, const shared_ptr<Query> &query)
{
    const bool immutable = ast->getChild(createArrayArgImmutable)->asNodeBool()->getVal();

    const bool emptyArray = ast->getChild(createArrayArgEmpty)->asNodeBool()->getVal();

    const string &arrayName = ast->getChild(createArrayArgArrayName)->asNodeString()->getVal();

    ArrayDesc schema;
    passSchema(ast->getChild(createArrayArgSchema), schema, arrayName, emptyArray, immutable, query, ast->getComment());

    if (schema.getName() == "")
    {
        throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_ARRAY_NAME_REQUIRED,
            ast->getChild(createArrayArgSchema)->getParsingContext());
    }

    vector<shared_ptr<OperatorParam> > opParams;
    opParams.push_back(make_shared<OperatorParamArrayReference>(
        ast->getChild(createArrayArgArrayName)->getParsingContext(),
        "",
        arrayName,
        false));
    opParams.push_back(make_shared<OperatorParamSchema>(
        ast->getChild(createArrayArgSchema)->getParsingContext(),
        schema));

    shared_ptr<LogicalOperator> op =
            OperatorLibrary::getInstance()->createLogicalOperator("create_array");

    op->setParameters(opParams);

    return make_shared<LogicalQueryPlanNode>(ast->getParsingContext(), op);
}

static void passDimensionsList(AstNode *ast, Dimensions &dimensions, const string &arrayName, set<string> &usedNames)
{
    dimensions.reserve(ast->getChildsCount());

    BOOST_FOREACH(const AstNode* dimNode, ast->getChilds())
    {
        assert(dimNode->getType() == dimension || dimNode->getType() == nonIntegerDimension );

        const string &dim_name = dimNode->getType() == dimension ?
            dimNode->getChild(dimensionArgName)->asNodeString()->getVal() :
            dimNode->getChild(nIdimensionArgName)->asNodeString()->getVal();

        if (usedNames.find(dim_name) != usedNames.end())
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_DUPLICATE_DIMENSION_NAME,
                    dimNode->getChild(dimensionArgName)->getParsingContext()) << dim_name;
        }
        usedNames.insert(dim_name);

        if (dimNode->getType() == dimension)
        {
            const AstNode *boundaries = dimNode->getChild(dimensionArgBoundaries);
            int64_t dim_l = boundaries->getChild(dimensionBoundaryArgLowBoundary)->asNodeInt64()->getVal();
            const int64_t dim_h =  boundaries->getChild(dimensionBoundaryArgHighBoundary)->asNodeInt64()->getVal();
            const int64_t dim_i = dimNode->getChild(dimensionArgChunkInterval)->asNodeInt64()->getVal();
            const int64_t dim_o = dimNode->getChild(dimensionArgChunkOverlap)->asNodeInt64()->getVal();

            if (dim_l == MAX_COORDINATE)
                throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_DIMENSION_START_CANT_BE_UNBOUNDED,
                                           boundaries->getChild(dimensionBoundaryArgLowBoundary)->getParsingContext());
            if (dim_h < dim_l && dim_h+1 != dim_l)
                throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_HIGH_SHOULDNT_BE_LESS_LOW,
                                           boundaries->getChild(dimensionBoundaryArgHighBoundary)->getParsingContext());
            if (dim_o > dim_i)
                throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_OVERLAP_CANT_BE_LARGER_CHUNK,
                                           dimNode->getChild(dimensionArgChunkOverlap)->getParsingContext());

            dimensions.push_back(DimensionDesc(dim_name, dim_l, dim_h, dim_i, dim_o, TID_INT64, arrayName, dimNode->getComment()));
        }
        else
        {
            const string &dimTypeName =  dimNode->getChild(nIdimensionArgTypeName)->asNodeString()->getVal();
            const int64_t boundary = dimNode->getChild(nIdimensionArgBoundary)->asNodeInt64()->getVal();
            const int64_t dim_i = dimNode->getChild(nIdimensionArgChunkInterval)->asNodeInt64()->getVal();
            const int64_t dim_o = dimNode->getChild(nIdimensionArgChunkOverlap)->asNodeInt64()->getVal();

            const Type dimType(TypeLibrary::getType(dimTypeName));
            if (boundary <= 0)
                throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_DIMENSIONS_NOT_SPECIFIED,
                                           dimNode->getChild(nIdimensionArgBoundary)->getParsingContext());
            if (dim_o > dim_i)
                throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_OVERLAP_CANT_BE_LARGER_CHUNK,
                                           dimNode->getChild(dimensionArgChunkOverlap)->getParsingContext());

            const int64_t maxCoordinate = boundary >= MAX_COORDINATE ? MAX_COORDINATE : boundary - 1;
            dimensions.push_back(DimensionDesc(dim_name, 0, maxCoordinate, dim_i, dim_o, dimType.typeId(), arrayName, dimNode->getComment()));
        }
    }

}

static void passSchema(AstNode *ast, ArrayDesc &schema, const string &arrayName, bool addEmpty, bool immutable,
                       const shared_ptr<Query> &query, std::string const& comment)
{
    const vector<Compressor*>& compressors = CompressorFactory::getInstance().getCompressors();

    Attributes attributes;
    const AstNode *list = ast->getChild(schemaArgAttributesList);
    attributes.reserve(list->getChildsCount());
    set<string> usedNames;

    BOOST_FOREACH(const AstNode* attNode, list->getChilds())
    {
        assert(attNode->getType() == attribute);
        const string attName = attNode->getChild(attributeArgName)->asNodeString()->getVal();
        const string attTypeName = attNode->getChild(attributeArgTypeName)->asNodeString()->getVal();
        const bool attTypeNullable = attNode->getChild(attributeArgIsNullable)->asNodeBool()->getVal();
        const string attCompressorName = attNode->getChild(attributeArgCompressorName)->asNodeString()->getVal();

        AstNode* defaultValueNode =  attNode->getChild(attributeArgDefaultValue);
        Value defaultValue;

        if (usedNames.find(attName) != usedNames.end())
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_DUPLICATE_ATTRIBUTE_NAME,
                    attNode->getChild(attributeArgName)->getParsingContext()) << attName;
        }
        usedNames.insert(attName);

        AttributeDesc::AttributeFlags attFlags = (AttributeDesc::AttributeFlags)0;
        attFlags = (AttributeDesc::AttributeFlags)(attTypeNullable ? attFlags | AttributeDesc::IS_NULLABLE : 0);

        try
        {
            const Type   attType(TypeLibrary::getType(attTypeName));

            string serializedDefaultValueExpr = "";
            if (defaultValueNode != NULL)
            {
                if (astHasReferences(defaultValueNode))
                {
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_REFERENCE_NOT_ALLOWED_IN_DEFAULT,
                            defaultValueNode->getParsingContext());
                }

                Expression e;
                e.compile(AstToLogicalExpression(defaultValueNode), query, false, attTypeName);
                serializedDefaultValueExpr = serializePhysicalExpression(e);
                defaultValue = e.evaluate();
                if (defaultValue.isNull() && !attTypeNullable) {
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_NULL_IN_NON_NULLABLE,
                        attNode->getChild(attributeArgName)->getParsingContext()) << attName;
                }
            }
            else
            {
                defaultValue = Value(attType);
                if (attTypeNullable) {
                    defaultValue.setNull();
                } else {
                    defaultValue.setZero();
                }
            }

            const Compressor *attCompressor = NULL;
            for (std::vector<Compressor*>::const_iterator it2 = compressors.begin();
                    it2 != compressors.end(); it2++)
            {
                if (attCompressorName == (*it2)->getName())
                {
                    attCompressor = *it2;
                    break;
                }
            }

            if (!attCompressor)
            {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_COMPRESSOR_DOESNT_EXIST,
                        attNode->getChild(attributeArgCompressorName)->getParsingContext())
                        << attCompressorName;
            }

            const AttributeID attrId = attributes.size();
            AttributeDesc att;
            if (attNode->getChild(attributeArgReserve) != NULL) {
                const int16_t attReserve = attNode->getChild(attributeArgReserve)->asNodeInt64()->getVal();
                att = AttributeDesc(attrId, attName, attType.typeId(), attFlags, attCompressor->getType(),
                    std::set<std::string>(), attReserve, &defaultValue, serializedDefaultValueExpr,
                    attNode->getComment());
            } else {
                att = AttributeDesc(attrId, attName, attType.typeId(), attFlags, attCompressor->getType(),
                    std::set<std::string>(), &defaultValue, serializedDefaultValueExpr,
                    attNode->getComment());
            }
            attributes.push_back(att);
        }
        catch(SystemException& e)
        {
            if (e.getLongErrorCode() == SCIDB_LE_TYPE_NOT_REGISTERED)
            {
                throw CONV_TO_USER_QUERY_EXCEPTION(e, attNode->getChild(attributeArgTypeName)->getParsingContext());
            }
            else
            {
                throw;
            }
        }
    }

    if (addEmpty)
    {
        //FIXME: Which compressor for empty indicator attribute?
        attributes.push_back(AttributeDesc(attributes.size(), "empty_indicator",  TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0));
    }

    Dimensions dimensions;
    passDimensionsList(ast->getChild(schemaArgDimensionsList), dimensions, arrayName, usedNames);

    int flags = 0;
    if (immutable)
    {
        flags |= ArrayDesc::IMMUTABLE;
    }

    schema = ArrayDesc(0, arrayName, attributes, dimensions, flags, comment);
}

static shared_ptr<LogicalQueryPlanNode> passAFLOperator(AstNode *ast, const shared_ptr<Query> &query)
{
    const string &opName = ast->getChild(functionArgName)->asNodeString()->getVal();

    const AstNodes &astParameters = ast->getChild(functionArgParameters)->getChilds();

    const string opAlias = ast->getChild(functionArgAliasName) ?
            ast->getChild(functionArgAliasName)->asNodeString()->getVal() :
            "";

    vector<shared_ptr<LogicalQueryPlanNode> > opInputs;
    vector<ArrayDesc> inputSchemas;

    shared_ptr<LogicalOperator> op;
    try
    {
        op = OperatorLibrary::getInstance()->createLogicalOperator(opName, opAlias);
    }
    catch (Exception &e)
    {
        if (e.getLongErrorCode() == SCIDB_LE_LOGICAL_OP_DOESNT_EXIST)
        {
            throw CONV_TO_USER_QUERY_EXCEPTION(e, ast->getParsingContext());
        }
    }


    const OperatorParamPlaceholders &opPlaceholders = op->getParamPlaceholders();

    //If operator not expecting any parameters
    if (opPlaceholders.size() == 0)
    {
        //but AST has some, then throw syntax error, else just skip parameters parsing
        if (astParameters.size() > 0)
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_UNEXPECTED_OPERATOR_ARGUMENT,
                ast->getChild(functionArgParameters)->getParsingContext())
                << opName << astParameters.size();
        }
    }
    else
    {
        //If operator parameters are variable, we don't know exact needed parameters count
        //If not check AST's parameters count and placeholders count
        const bool hasVaryParams = opPlaceholders[opPlaceholders.size() - 1]->getPlaceholderType() == PLACEHOLDER_VARIES
                ? true
                : false;
        if (!hasVaryParams)
        {
            if (astParameters.size() != opPlaceholders.size())
            {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT,
                    ast->getChild(functionArgParameters)->getParsingContext())
                    << opName <<  opPlaceholders.size() << astParameters.size();
            }
        }

        OperatorParamPlaceholders supposedPlaceholders;
        //Iterate over all parameters of operator and match placeholders
        size_t astParamNo = 0;
        while(true)
        {
            //Check if we need next iteration in case fixed arguments
            if (!hasVaryParams && astParamNo >= astParameters.size())
                break;

            //First we iterating over all fixed parameters, and then over all vary parameters
            if (hasVaryParams && astParamNo >= opPlaceholders.size() - 1)
            {
                supposedPlaceholders = op->nextVaryParamPlaceholder(inputSchemas);
            }
            else
            {
                supposedPlaceholders.clear();
                supposedPlaceholders.push_back(opPlaceholders[astParamNo]);
            }

            //Now check if we need to stop parsing vary arguments
            if (astParamNo >= astParameters.size())
            {
                //Here we don't have any arguments in AST and have placeholder indicated about arguments
                //end. Stopping parsing.
                if (placeholdersVectorContainType(supposedPlaceholders, PLACEHOLDER_END_OF_VARIES))
                {
                    break;
                }
                //And here we actually expected more arguments. Throwing error.
                else
                {
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT2,
                        ast->getChild(functionArgParameters)->getParsingContext())
                        << opName;
                }
            }
            else
            {
                if (placeholdersVectorContainType(supposedPlaceholders, PLACEHOLDER_END_OF_VARIES)
                        && supposedPlaceholders.size() == 1)
                {
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT3,
                        astParameters[astParamNo]->getParsingContext())
                        << opName << astParamNo;
                }
            }

            AstNode *astParam = astParameters[astParamNo];

            try
            {
                shared_ptr<OperatorParam> opParam;
                if (matchOperatorParam(astParam, supposedPlaceholders, query, inputSchemas, opInputs, opParam))
                    op->addParameter(opParam);
            }
            catch (const UserQueryException &e)
            {
                if (e.getShortErrorCode() == SCIDB_SE_INTERNAL && e.getLongErrorCode() == SCIDB_LE_WRONG_OPERATOR_ARGUMENT)
                {
                    string placeholdersString;
                    string astParamString;
                    placeholdersToString(supposedPlaceholders, placeholdersString);
                    astParamToString(astParam, astParamString);
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_WRONG_OPERATOR_ARGUMENT,
                        astParam->getParsingContext())
                        << placeholdersString << (astParamNo + 1) << opName << astParamString;
                }
                else
                {
                    throw;
                }
            }

            ++astParamNo;
        }
    }

    shared_ptr<LogicalQueryPlanNode> result = make_shared<LogicalQueryPlanNode>(ast->getParsingContext(), op, opInputs);

    // We can't check expression before getting all operator parameters. So here we already have
    // all params and can get operator output schema. On each iteration we checking references in
    // all non-constant expressions. If ok, we trying to compile expression to check type compatibility.
    size_t paramNo = inputSchemas.size(); // Inputs parameters too, but only in AST
    BOOST_FOREACH(const shared_ptr<OperatorParam> &param, result->getLogicalOperator()->getParameters())
    {
        ++paramNo;
        if (PARAM_LOGICAL_EXPRESSION == param->getParamType())
        {
            const shared_ptr<OperatorParamLogicalExpression>& paramLE = (const shared_ptr<OperatorParamLogicalExpression>&) param;

            if (paramLE->isConstant())
                continue;

            const ArrayDesc& outputSchema = result->inferTypes(query);

            const shared_ptr<LogicalExpression>& lExpr = paramLE->getExpression();
            checkLogicalExpression(inputSchemas, outputSchema, lExpr);

            shared_ptr<Expression> pExpr = make_shared<Expression>();
            try
            {
               pExpr->compile(lExpr, query, false, paramLE->getExpectedType().typeId(), inputSchemas, outputSchema);
            }
            catch (const Exception &e)
            {
                if (e.getLongErrorCode() == SCIDB_LE_TYPE_CONVERSION_ERROR)
                {
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_PARAMETER_TYPE_ERROR,
                        param->getParsingContext())
                        << paramLE->getExpectedType().name() << pExpr->getType();
                }
                else
                {
                    throw;
                }
            }
        }
    }

    return result;
}


static shared_ptr<OperatorParamArrayReference> createArrayReferenceParam(const AstNode *arrayReferenceAST,
    bool inputSchema, const shared_ptr<Query> &query)
{
    ArrayDesc schema;
    string arrayName = arrayReferenceAST->getChild(referenceArgObjectName)->asNodeString()->getVal();
    string dimName = "";
    assert(arrayName != "");
    assert(arrayName.find('@') == string::npos);

    if (arrayReferenceAST->getChild(referenceArgArrayName))
    {
        throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_NESTED_ARRAYS_NOT_SUPPORTED,
            arrayReferenceAST->getChild(referenceArgArrayName)->getParsingContext());
    }

    if (!inputSchema)
    {
        assert(!arrayReferenceAST->getChild(referenceArgTimestamp));
        assert(!arrayReferenceAST->getChild(referenceArgIndex));
        return make_shared<OperatorParamArrayReference>(arrayReferenceAST->getParsingContext(), "",
            arrayName, inputSchema, 0, "");
    }

    SystemCatalog *systemCatalog = SystemCatalog::getInstance();
    if (!systemCatalog->containsArray(arrayName))
    {
        throw USER_QUERY_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_ARRAY_DOESNT_EXIST,
            arrayReferenceAST->getChild(referenceArgObjectName)->getParsingContext()) << arrayName;
    }

    systemCatalog->getArrayDesc(arrayName, schema);

    VersionID version = 0;
    if (!schema.isImmutable())
    {
        version = LAST_VERSION;

        if (arrayReferenceAST->getChild(referenceArgTimestamp))
        {
            if (arrayReferenceAST->getChild(referenceArgTimestamp)->getType() == asterisk)
            {
                if (arrayReferenceAST->getChild(referenceArgIndex))
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_LE_CANT_ACCESS_INDEX_FOR_ALLVERSIONS,
                        arrayReferenceAST->getChild(referenceArgIndex)->getParsingContext());

                return make_shared<OperatorParamArrayReference>(arrayReferenceAST->getParsingContext(), "",
                    arrayName, inputSchema, ALL_VERSIONS, "");
            }
            else
            {
                boost::shared_ptr<LogicalExpression> lExpr =
                    AstToLogicalExpression(arrayReferenceAST->getChild(referenceArgTimestamp));
                Expression pExpr;
                pExpr.compile(lExpr, query, false);
                const Value &value = pExpr.evaluate();

                if (pExpr.getType() == TID_INT64)
                {
                    version = value.getUint64();
                    if (version > systemCatalog->getLastVersion(schema.getId()))
                    {
                        version = 0;
                    }
                }
                else if (pExpr.getType() == TID_DATETIME)
                {
                    version = systemCatalog->lookupVersionByTimestamp(schema.getId(), value.getDateTime());
                }
                else
                {
                    assert(0);
                }
            }
        }

        if (!version)
            throw USER_QUERY_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_ARRAY_VERSION_DOESNT_EXIST,
                arrayReferenceAST->getChild(referenceArgTimestamp)->getParsingContext()) << arrayName;
        systemCatalog->getArrayDesc(arrayName, version, schema);
    }
    else
    {
        if (arrayReferenceAST->getChild(referenceArgTimestamp))
            throw USER_QUERY_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_ARRAY_VERSION_DOESNT_EXIST2,
                arrayReferenceAST->getChild(referenceArgTimestamp)->getParsingContext()) << arrayName;
    }

    if (arrayReferenceAST->getChild(referenceArgIndex))
    {
        dimName = arrayReferenceAST->getChild(referenceArgIndex)->asNodeString()->getVal();
        const Dimensions &dims = schema.getDimensions();
        size_t i, n = dims.size();
        for (i = 0; i < n && dims[i].getBaseName() != dimName; i++) ;
        if (i == n)
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_DIMENSION_NOT_EXIST,
                arrayReferenceAST->getChild(referenceArgIndex)->getParsingContext()) << dimName;
        }

        if (dims[i].getType() == TID_INT64)
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_NO_MAPPING_ARRAY,
                arrayReferenceAST->getChild(referenceArgIndex)->getParsingContext()) << dimName << arrayName;
        }

        const string& mappingArray = schema.getCoordinateIndexArrayName(i);
        if (!SystemCatalog::getInstance()->containsArray(mappingArray))
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_ARRAY_DOESNT_EXIST,
                arrayReferenceAST->getChild(referenceArgIndex)->getParsingContext()) <<
                mappingArray;
        }
    }

    assert(arrayName.find('@') == string::npos);
    return make_shared<OperatorParamArrayReference>(arrayReferenceAST->getParsingContext(), "",
        arrayName, inputSchema, version, dimName);
}


static bool matchOperatorParam(AstNode *ast, const OperatorParamPlaceholders &placeholders,
        const shared_ptr<Query> &query, vector<ArrayDesc> &inputSchemas,
        vector<shared_ptr<LogicalQueryPlanNode> > &inputs, shared_ptr<OperatorParam> &param)
{
    int matched = 0;

    const shared_ptr<ParsingContext> &paramCtxt = ast->getParsingContext();
    //Each operator parameter from AST can match several placeholders. We trying to catch best one.
    BOOST_FOREACH(const shared_ptr<OperatorParamPlaceholder>& placeholder, placeholders)
    {
        switch (placeholder->getPlaceholderType())
        {
            case PLACEHOLDER_INPUT:
            {
                shared_ptr<LogicalQueryPlanNode> input;
                //This input is implicit scan.
                if (ast->getType() == reference)
                {
                    input = passImplicitScan(ast, query);
                }
                //This input is result of other operator, so go deeper in tree and translate this operator.
                else if ( (ast->getType() == function && !ast->getChild(functionArgScalarOp)->asNodeBool()->getVal())
                        || ast->getType() == selectStatement)
                {
                    input = AstToLogicalPlan(ast, query);
                }
                else
                {
                    break;
                }

                inputSchemas.push_back(input->inferTypes(query));
                inputs.push_back(input);

                //Inputs can not be mixed in vary parameters. Return and go to next parameter.
                return false;
            }

            case PLACEHOLDER_ARRAY_NAME:
            {
                if (ast->getType() == reference)
                {
                    if (matched)
                    {
                        throw USER_QUERY_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,
                            paramCtxt);
                    }

                    if (ast->getChild(referenceArgTimestamp))
                    {
                        if (!(placeholder->getFlags() & PLACEHOLDER_ARRAY_NAME_VERSION))
                            throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_CANT_ACCESS_ARRAY_VERSION,
                                ast->getChild(referenceArgTimestamp)->getParsingContext());
                    }

                    if (ast->getChild(referenceArgIndex))
                    {
                        if (!(placeholder->getFlags() & PLACEHOLDER_ARRAY_NAME_INDEX_NAME))
                            throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_CANT_ACCESS_INDEX_ARRAY,
                                ast->getChild(referenceArgIndex)->getParsingContext());
                    }

                    if (ast->getChild(referenceArgSortQuirk))
                    {
                        throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_SORTING_QUIRK_WRONG_USAGE,
                            ast->getChild(referenceArgSortQuirk)->getParsingContext());
                    }

                    param = createArrayReferenceParam(ast, placeholder->isInputSchema(), query);

                    matched |= PLACEHOLDER_ARRAY_NAME;
                }

                break;
            }

            case PLACEHOLDER_ATTRIBUTE_NAME:
            {
                if (ast->getType() == reference && !ast->getChild(referenceArgTimestamp))
                {
                    const string aliasName = ast->getChild(referenceArgArrayName) != NULL
                            ? ast->getChild(referenceArgArrayName)->asNodeString()->getVal()
                            : "";

                    const string &attributeName = ast->getChild(referenceArgObjectName)->asNodeString()->getVal();

                    shared_ptr<OperatorParamAttributeReference> opParam = make_shared<OperatorParamAttributeReference>(
                            ast->getParsingContext(), aliasName, attributeName, placeholder->isInputSchema());

                    if (ast->getChild(referenceArgSortQuirk))
                    {
                        opParam->setSortAscent(ast->getChild(referenceArgSortQuirk)->asNodeInt64()->getVal() == SORT_ASC
                                           ? true : false);
                    }
                    else
                    {
                        opParam->setSortAscent(true);
                    }

                    //Trying resolve attribute in input schema
                    if (placeholder->isInputSchema())
                    {
                        if(!resolveParamAttributeReference(inputSchemas, (shared_ptr<OperatorParamReference>&)opParam, false))
                            break;
                    }

                    //Check if something already matched in overloaded parameter
                    if (matched)
                    {
                        //If current parameter from input schema and some previous matched was from
                        //input schema, or current parameter from output schema and some previous
                        //matched was from output schema, so we can't resolve such ambigouty
                        if ((placeholder->isInputSchema() && !(matched & PLACEHOLDER_OUTPUT_FLAG))
                             || (!placeholder->isInputSchema() && (matched & PLACEHOLDER_OUTPUT_FLAG)))
                        {
                            throw USER_QUERY_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,
                                paramCtxt);
                        }

                        //If some matched, but in different schema, prefer input schema parameter over
                        //output schema
                        if (placeholder->isInputSchema())
                        {
                            param = opParam;
                        }
                    }
                    else
                    {
                        param = opParam;
                    }

                    //Raise flags in any case, even parameter was not catched
                    matched |= PLACEHOLDER_ATTRIBUTE_NAME;
                    matched |= placeholder->isInputSchema() ? 0 : PLACEHOLDER_OUTPUT_FLAG;
                }
                break;
            }

            case PLACEHOLDER_DIMENSION_NAME:
            {
                if (ast->getType() == reference && !ast->getChild(referenceArgTimestamp))
                {
                    const string aliasName = ast->getChild(referenceArgArrayName) != NULL
                            ? ast->getChild(referenceArgArrayName)->asNodeString()->getVal()
                            : "";

                    const string &dimensionName = ast->getChild(referenceArgObjectName)->asNodeString()->getVal();

                    shared_ptr<OperatorParamReference> opParam = make_shared<OperatorParamDimensionReference>(
                            ast->getParsingContext(), aliasName, dimensionName, placeholder->isInputSchema());

                    //Trying resolve dimension in input schema
                    if (placeholder->isInputSchema())
                    {
                        if (!resolveParamDimensionReference(inputSchemas, opParam, false))
                            break;
                    }

                    //Check if something already matched in overloaded parameter
                    if (matched)
                    {
                        //If current parameter from input schema and some previous matched was from
                        //input schema, or current parameter from output schema and some previous
                        //matched was from output schema, so we can't resolve such ambigouty
                        if ((placeholder->isInputSchema() && !(matched & PLACEHOLDER_OUTPUT_FLAG))
                             || (!placeholder->isInputSchema() && (matched & PLACEHOLDER_OUTPUT_FLAG)))
                        {
                            throw USER_QUERY_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,
                                paramCtxt);
                        }

                        //If some matched, but in different schema, prefer input schema parameter over
                        //output schema
                        if (placeholder->isInputSchema())
                        {
                            param = opParam;
                        }
                    }
                    else
                    {
                        param = opParam;
                    }

                    //Raise flags in any case, even parameter was not catched
                    matched |= PLACEHOLDER_DIMENSION_NAME;
                    matched |= placeholder->isInputSchema() ? 0 : PLACEHOLDER_OUTPUT_FLAG;
                }
                break;
            }

            case PLACEHOLDER_CONSTANT:
            {
                if (ast->getType() == function || ast->getType() == stringNode
                        || ast->getType() == int64Node || ast->getType() == realNode
                        || ast->getType() == null || ast->getType() == boolNode)
                {
                    if (matched && !(matched & PLACEHOLDER_CONSTANT))
                    {
                        throw USER_QUERY_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,
                            paramCtxt);
                    }

                    shared_ptr<LogicalExpression> lExpr = AstToLogicalExpression(ast);
                    shared_ptr<Expression> pExpr = make_shared<Expression>();

                    try
                    {
                       pExpr->compile(lExpr, query, false, placeholder->getRequiredType().typeId());
                    }
                    catch (const Exception &e)
                    {
                        if (e.getLongErrorCode() == SCIDB_LE_REF_NOT_FOUND
                            || e.getLongErrorCode() == SCIDB_LE_TYPE_CONVERSION_ERROR)
                        {
                            break;
                        }
                    }

                    if (!(matched & PLACEHOLDER_CONSTANT))
                    {
                        param = make_shared<OperatorParamLogicalExpression>(ast->getParsingContext(),
                                    lExpr, placeholder->getRequiredType(), true);
                    }
                    else
                    {
                       pExpr->compile(lExpr, query, false);

                        if (pExpr->getType() == placeholder->getRequiredType().typeId())
                        {
                            param = make_shared<OperatorParamLogicalExpression>(ast->getParsingContext(),
                                        lExpr, placeholder->getRequiredType(), true);
                        }
                    }

                    matched |= PLACEHOLDER_CONSTANT;
                }
                break;
            }

            case PLACEHOLDER_EXPRESSION:
            {
                if (ast->getType() == function || ast->getType() == reference
                        || ast->getType() == stringNode || ast->getType() == int64Node
                        || ast->getType() == realNode || ast->getType() == null
                        || ast->getType() == boolNode)
                {
                    if (matched)
                    {
                        throw USER_QUERY_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,
                            paramCtxt);
                    }

                    shared_ptr<LogicalExpression> lExpr = AstToLogicalExpression(ast);

                    //We not checking expression now, because we can't get output schema. Checking
                    //will be done after getting all operator parameters
                    param = make_shared<OperatorParamLogicalExpression>(ast->getParsingContext(),
                            lExpr, placeholder->getRequiredType(), false);

                    matched |= PLACEHOLDER_EXPRESSION;
                }

                break;
            }

            case PLACEHOLDER_SCHEMA:
            {
                if (ast->getType() == anonymousSchema)
                {
                    if (matched)
                    {
                        throw USER_QUERY_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,
                            paramCtxt);
                    }

                    ArrayDesc schema;

                    const bool empty = ast->getChild(anonymousSchemaClauseEmpty)->asNodeBool()->getVal();

                    passSchema(ast->getChild(anonymousSchemaClauseSchema), schema, "", empty, false, query);

                    param = make_shared<OperatorParamSchema>(ast->getParsingContext(), schema);

                    matched |= PLACEHOLDER_SCHEMA;
                }
                else if (ast->getType() == reference)
                {
                    if (matched)
                    {
                        throw USER_QUERY_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,
                            paramCtxt);
                    }

                    if (ast->getChild(referenceArgArrayName))
                    {
                        throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_NESTED_ARRAYS_NOT_SUPPORTED,
                            ast->getParsingContext());
                    }

                    const string &arrayName = ast->getChild(referenceArgObjectName)->asNodeString()->getVal();
                    ArrayDesc schema;
                    if (!SystemCatalog::getInstance()->getArrayDesc(arrayName, schema, false))
                    {
                        throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_ARRAY_DOESNT_EXIST, paramCtxt)
                            << arrayName;
                    }

                    param = make_shared<OperatorParamSchema>(ast->getParsingContext(), schema);

                    matched |= PLACEHOLDER_SCHEMA;
                }
                break;
            }

            case PLACEHOLDER_AGGREGATE_CALL:
            {
                if (ast->getType() == function )
                {
                    if (matched)
                    {
                        throw USER_QUERY_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,
                            paramCtxt);
                    }

                    param = passAggregateCall(ast, inputSchemas);
                    matched |= PLACEHOLDER_AGGREGATE_CALL;
                }
                break;
            }

            case PLACEHOLDER_END_OF_VARIES:
                break;

            default:
                assert(0);
        }
    }

    if (!matched)
    {
        string placeholdersString;
        placeholdersToString(placeholders, placeholdersString);
        throw USER_QUERY_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_WRONG_OPERATOR_ARGUMENT2, paramCtxt)
            << placeholdersString;
    }

    return true;
}

static void placeholdersToString(const vector<shared_ptr<OperatorParamPlaceholder> > &placeholders, string &result)
{
    bool first = true;
    stringstream ss;
    BOOST_FOREACH(const shared_ptr<OperatorParamPlaceholder> &placeholder, placeholders)
    {
        if (!first)
            ss << " or ";
        first = false;
        switch (placeholder->getPlaceholderType())
        {
            case PLACEHOLDER_INPUT:
            case PLACEHOLDER_ARRAY_NAME:
                ss <<  "array name";
                if (placeholder->getPlaceholderType() == PLACEHOLDER_INPUT)
                    ss << " or array operator";
                break;

            case PLACEHOLDER_ATTRIBUTE_NAME:
                ss <<  "attribute name";
                break;

            case PLACEHOLDER_CONSTANT:
                if (placeholder->getRequiredType().typeId() == TID_VOID)
                    ss <<  "constant";
                else
                    ss << "constant with type '" << placeholder->getRequiredType().typeId() << "'";
                break;

            case PLACEHOLDER_DIMENSION_NAME:
                ss <<  "dimension name";
                break;

            case PLACEHOLDER_EXPRESSION:
                ss <<  "expression";
                break;

            case PLACEHOLDER_SCHEMA:
                ss << "schema";
                break;

            case PLACEHOLDER_AGGREGATE_CALL:
                ss << "aggregate_call";
                break;

            case PLACEHOLDER_END_OF_VARIES:
                ss <<  "end of arguments";
                break;

            default:
                assert(0);
        }
    }

    result = ss.str();
}

static void astParamToString(const AstNode* ast, string &result)
{
    stringstream ss;
    switch (ast->getType())
    {
        case function:
            result = ast->getChild(functionArgScalarOp)->asNodeBool()->getVal() ? "expression" : "operator (or function)";
            return;

        case reference:
            if (ast->getChild(referenceArgTimestamp))
                result = "array name";
            else
                result = "reference (array, attribute or dimension name)";
            return;

        case null:
            result = "constant with unknown type";
            return;

        case int64Node:
            ss << "constant with type '" << TID_INT64 << "'";
            result = ss.str();
            return;

        case realNode:
            ss << "constant with type '" << TID_DOUBLE << "'";
            result = ss.str();
            return;

        case boolNode:
            ss << "constant with type '" << TID_BOOL << "'";
            result = ss.str();
            return;

        case stringNode:
            ss << "constant with type '" << TID_STRING << "'";
            result = ss.str();
            return;

        case schema:
            result = "schema";
            return;

        case anonymousSchema:
            result = "anonymous schema";
            return;

        default:
            assert(0);
    }
}

static bool resolveParamAttributeReference(const vector<ArrayDesc> &inputSchemas, shared_ptr<OperatorParamReference> &attRef, bool throwException)
{
    const string fullName = str(format("%s%s") % (attRef->getArrayName() != "" ? attRef->getArrayName() + "." : "") % attRef->getObjectName() );
    bool found = false;

    size_t inputNo = 0;
    BOOST_FOREACH(const ArrayDesc &schema, inputSchemas)
    {
        size_t attributeNo = 0;
        BOOST_FOREACH(const AttributeDesc& attribute, schema.getAttributes())
        {
            if (attribute.getName() == attRef->getObjectName()
                    && attribute.hasAlias(attRef->getArrayName()))
            {
                if (found)
                {
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_AMBIGUOUS_ATTRIBUTE,
                        attRef->getParsingContext())
                        << fullName;
                }
                found = true;

                attRef->setInputNo(inputNo);
                attRef->setObjectNo(attributeNo);
            }
            ++attributeNo;
        }
        ++inputNo;
    }

    if (!found && throwException)
    {
        throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_ATTRIBUTE_NOT_EXIST, attRef->getParsingContext())
            << fullName;
    }

    return found;
}

static bool resolveParamDimensionReference(const vector<ArrayDesc> &inputSchemas, shared_ptr<OperatorParamReference>& dimRef, bool throwException)
{
    const string fullName = str(format("%s%s") % (dimRef->getArrayName() != "" ? dimRef->getArrayName() + "." : "") % dimRef->getObjectName() );
    bool found = false;

    size_t inputNo = 0;
    BOOST_FOREACH(const ArrayDesc &schema, inputSchemas)
    {
        size_t dimensionNo = 0;
        BOOST_FOREACH(const DimensionDesc& dimension, schema.getDimensions())
        {
            if (dimension.hasNameOrAlias(dimRef->getObjectName(), dimRef->getArrayName()))
            {
                if (found)
                {
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_AMBIGUOUS_DIMENSION, dimRef->getParsingContext())
                        << fullName;
                }
                found = true;

                dimRef->setInputNo(inputNo);
                dimRef->setObjectNo(dimensionNo);
            }
            ++dimensionNo;
        }
        ++inputNo;
    }

    if (!found && throwException)
    {
        throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_DIMENSION_NOT_EXIST, dimRef->getParsingContext())
            << fullName;
    }

    return found;
}

static shared_ptr<LogicalExpression> passScalarFunction(AstNode *ast)
{
    const string &functionName = ast->getChild(functionArgName)->asNodeString()->getVal();
    const AstNodes &args = ast->getChilds()[functionArgParameters]->getChilds();

    if (OperatorLibrary::getInstance()->hasLogicalOperator(functionName))
    {
        throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_UNEXPECTED_OPERATOR_IN_EXPRESSION,
            ast->getParsingContext());
    }

    vector<shared_ptr<LogicalExpression> > functionArgs;

    BOOST_FOREACH(AstNode *astArg, args)
    {
        functionArgs.push_back(AstToLogicalExpression(astArg));
    }

    return make_shared<Function>(ast->getParsingContext(), functionName, functionArgs);
}

static shared_ptr<OperatorParamAggregateCall> passAggregateCall(AstNode *ast, const vector<ArrayDesc> &inputSchemas)
{
    const string &aggregateName = ast->getChild(functionArgName)->asNodeString()->getVal();

    const AstNodes &args = ast->getChilds()[functionArgParameters]->getChilds();
    if (args.size() > 1 || args.size() == 0)
    {
        throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_WRONG_AGGREGATE_ARGUMENTS_COUNT,
            ast->getParsingContext());
    }

    shared_ptr<OperatorParam> opParam;
    if (args[0]->getType() == reference)
    {
        shared_ptr <AttributeReference> argument =
                        static_pointer_cast<AttributeReference>(passReference(args[0]));

        opParam = make_shared<OperatorParamAttributeReference>( args[0]->getParsingContext(),
                                                                argument->getArrayName(),
                                                                argument->getAttributeName(),
                                                                true );

        resolveParamAttributeReference(inputSchemas, (shared_ptr<OperatorParamReference>&) opParam, true);
    }
    else if (args[0]->getType() == asterisk)
    {
        opParam = make_shared<OperatorParamAsterisk>(args[0]->getParsingContext());
    }
    else
    {
        throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_WRONG_AGGREGATE_ARGUMENT, ast->getParsingContext());
    }


    const string& aliasName = ast->getChild(functionArgAliasName) != NULL ?
                              ast->getChild(functionArgAliasName)->asNodeString()->getVal() :
                              "";

    return make_shared<OperatorParamAggregateCall> (ast->getParsingContext(),
                                                    aggregateName,
                                                    opParam,
                                                    aliasName);
}

static shared_ptr<LogicalExpression> passConstant(AstNode *ast)
{
    if (typeid(*ast) == typeid(AstNodeString))
    {
        const string str = ast->asNodeString()->getVal();
        Value c(TypeLibrary::getType(TID_STRING));
        c.setData(str.c_str(), str.length() + 1);
        return make_shared<Constant>(ast->getParsingContext(), c, TID_STRING);
    }
    else if (typeid(*ast) == typeid(AstNodeInt64))
    {
        const int64_t v = ast->asNodeInt64()->getVal();
#ifdef SMART_INT_CONSTANT_TYPE
        if (v & 0xFFFFFFFF00000000LL) {
            Value c(TypeLibrary::getType(TID_INT64));
            c.setInt64(v);
            return make_shared<Constant>(ast->getParsingContext(), c, TID_INT64);
        }
        else {
            Value c(TypeLibrary::getType(TID_INT32));
            c.setInt32(v);
            return make_shared<Constant>(ast->getParsingContext(), c, TID_INT32);
        }
#else
        Value c(TypeLibrary::getType(TID_INT64));
        c.setInt64(v);
        return make_shared<Constant>(ast->getParsingContext(), c, TID_INT64);
#endif
    }
    else if (typeid(*ast) == typeid(AstNodeReal))
    {
        Value c(TypeLibrary::getType(TID_DOUBLE));
        c.setDouble(ast->asNodeReal()->getVal());
        return make_shared<Constant>(ast->getParsingContext(), c, TID_DOUBLE);
    }
    else if (typeid(*ast) == typeid(AstNodeNull))
    {
        Value c;
        c.setNull();
        return make_shared<Constant>(ast->getParsingContext(), c, TID_VOID);
    }
    else if (typeid(*ast) == typeid(AstNodeBool))
    {
        Value c(TypeLibrary::getType(TID_BOOL));
        c.setBool(ast->asNodeBool()->getVal());
        return make_shared<Constant>(ast->getParsingContext(), c, TID_BOOL);
    }
    assert(false);
}

static shared_ptr<LogicalExpression> passReference(AstNode *ast)
{
    if (ast->getChild(referenceArgTimestamp))
    {
        throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_REFERENCE_EXPECTED,
            ast->getChild(referenceArgTimestamp)->getParsingContext());
    }

    if (ast->getChild(referenceArgSortQuirk))
    {
        throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_SORTING_QUIRK_WRONG_USAGE,
            ast->getChild(referenceArgSortQuirk)->getParsingContext());
    }

    const string arrayName = ast->getChild(referenceArgArrayName) != NULL
            ? ast->getChild(referenceArgArrayName)->asNodeString()->getVal()
            : "";

    const string &attributeName = ast->getChild(referenceArgObjectName)->asNodeString()->getVal();

    return make_shared<AttributeReference>(ast->getParsingContext(), arrayName, attributeName);
}

static bool placeholdersVectorContainType(const vector<shared_ptr<OperatorParamPlaceholder> > &placeholders,
    OperatorParamPlaceholderType placeholderType)
{
    BOOST_FOREACH(const shared_ptr<OperatorParamPlaceholder> &placeholder, placeholders)
    {
        if (placeholder->getPlaceholderType() == placeholderType)
            return true;
    }
    return false;
}

static AstNode* passAggregateSelectClause(const AstNode *ast, const string &grwFakeArrayName = "")
{
    assert(selectStatement == ast->getType());

    const AstNode *aggSelectList = ast->getChild(selectClauseArgSelectList);

    const AstNode *aggFromList = ast->getChild(selectClauseArgFromClause);

    if (aggSelectList->getChildsCount() > 1)
    {
        throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_SELECT_IN_AGGREGATE_WRONG_USAGE,
            aggSelectList->getParsingContext());
    }

    BOOST_FOREACH(const AstNode *aggFromItem, aggFromList->getChilds())
    {
        switch(aggFromItem->getType())
        {
            case namedExpr:
            {
                const AstNode* expr = aggFromItem->getChild(namedExprArgExpr);

                if (expr->getType() != reference)
                {
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_SELECT_IN_AGGREGATE_WRONG_USAGE,
                        aggFromItem->getParsingContext());
                }

                if (expr->getChild(referenceArgTimestamp))
                {
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_TIMESTAMP_IN_AGGREGATE_WRONG_USAGE,
                        expr->getParsingContext());
                }

                if (grwFakeArrayName != "")
                {
                    const string &arrayName = expr->getChild(referenceArgObjectName)->asNodeString()->getVal();

                    if (grwFakeArrayName != arrayName)
                    {
                        throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_UNKNOWN_ARRAY_REFERENCE,
                            expr->getParsingContext());
                    }
                }
                else
                {
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_UNKNOWN_ARRAY_REFERENCE,
                        expr->getParsingContext());
                }

                break;
            }

            default:
            {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_SELECT_IN_AGGREGATE_WRONG_USAGE,
                    aggFromItem->getParsingContext());
            }
        }
    }

    BOOST_FOREACH(AstNode *aggSelectItem, aggSelectList->getChilds())
    {
        switch(aggSelectItem->getType())
        {
            case asterisk:
                return aggSelectItem->clone();
                break;

            case namedExpr:
            {
                AstNode* expr = aggSelectItem->getChild(namedExprArgExpr);

                if (expr->getType() != reference)
                {
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_SELECT_IN_AGGREGATE_WRONG_USAGE,
                        aggSelectItem->getParsingContext());
                }

                if (expr->getChild(referenceArgTimestamp))
                {
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_TIMESTAMP_IN_AGGREGATE_WRONG_USAGE,
                        expr->getParsingContext());
                }

                return expr->clone();
            }

            default:
            {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_SELECT_IN_AGGREGATE_WRONG_USAGE,
                    aggSelectItem->getParsingContext());
            }
        }
    }

    return NULL;
}

static shared_ptr<LogicalQueryPlanNode> passSelectStatement(AstNode *ast, shared_ptr<Query> query)
{
    shared_ptr<LogicalQueryPlanNode> result = shared_ptr<LogicalQueryPlanNode>();

    AstNode *fromClause = ast->getChild(selectClauseArgFromClause);

    AstNode *selectList = ast->getChild(selectClauseArgSelectList);

    AstNode *grwClause = ast->getChild(selectClauseArgGRWClause);

    if (fromClause)
    {
        //First of all joins,scan or nested query will be translated and used
        result = passJoins(fromClause, query);

        //Next WHERE clause
        AstNode *filterClause = ast->getChild(selectClauseArgFilterClause);
        if (filterClause)
        {
            result = passFilterClause(filterClause, result, query);
        }

        result = passSelectList(result, selectList, grwClause, query);
    }
    else
    {
        if (selectList->getChildsCount() > 1
            || asterisk == selectList->getChild(0)->getType()
            || function != selectList->getChild(0)->getChild(namedExprArgExpr)->getType()
            || !AggregateLibrary::getInstance()->hasAggregate(
                selectList->getChild(0)->getChild(namedExprArgExpr)->getChild(functionArgName)->asNodeString()->getVal()))
        {
             throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_AGGREGATE_EXPECTED,
                 selectList->getParsingContext());
        }

        const AstNode* aggregate = selectList->getChild(0)->getChild(namedExprArgExpr);
        const string &funcName = aggregate->getChild(functionArgName)->asNodeString()->getVal();
        const AstNode* funcParams = aggregate->getChild(functionArgParameters);

        if (funcParams->getChildsCount() != 1)
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_WRONG_AGGREGATE_ARGUMENTS_COUNT,
                                       funcParams->getParsingContext());
        }

        shared_ptr<LogicalQueryPlanNode> aggInput;
        switch (funcParams->getChild(0)->getType())
        {
            case reference:
                aggInput = passImplicitScan(funcParams->getChild(0), query);
                break;

            case selectStatement:
                aggInput = passSelectStatement(funcParams->getChild(0), query);
                break;

            default:
                throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_WRONG_AGGREGATE_ARGUMENT2,
                                           funcParams->getChild(0)->getParsingContext());
        }


        // First of all try to convert it as select agg(*) from A group by x as G
        // Let's check if asterisk supported
        bool asteriskSupported = true;
        try
        {
            AggregateLibrary::getInstance()->createAggregate(funcName, TypeLibrary::getType(TID_VOID));
        }
        catch (const UserException &e)
        {
            if (SCIDB_LE_AGGREGATE_DOESNT_SUPPORT_ASTERISK == e.getLongErrorCode())
            {
                asteriskSupported = false;
            }
            else
            {
                throw;
            }
        }

        const ArrayDesc &aggInputSchema = aggInput->inferTypes(query);
        shared_ptr<OperatorParamAggregateCall> aggCallParam;

        if (asteriskSupported)
        {
            AstNode *aggregateCallAst = makeUnaryScalarOp(
                        funcName,
                        new AstNode(
                            asterisk,
                            funcParams->getChild(0)->getParsingContext(),
                            0),
                        aggregate->getParsingContext()
                        );
            aggCallParam = passAggregateCall(aggregateCallAst, vector<ArrayDesc>(1, aggInputSchema));
            delete aggregateCallAst;
        }
        else
        {
            if (aggInputSchema.getAttributes().size() == 1
                || (aggInputSchema.getAttributes().size() == 2 && aggInputSchema.getEmptyBitmapAttribute()))
            {
                size_t attNo = aggInputSchema.getEmptyBitmapAttribute() && aggInputSchema.getEmptyBitmapAttribute()->getId() == 0 ? 1 : 0;

                AstNode *aggregateCallAst = makeUnaryScalarOp(
                            funcName,
                            new AstNode(
                                reference,
                                funcParams->getChild(0)->getParsingContext(),
                                referenceArgCount,
                                NULL,
                                new AstNodeString(objectName,
                                                  funcParams->getChild(0)->getParsingContext(),
                                                  aggInputSchema.getAttributes()[attNo].getName()),
                                NULL,
                                NULL,
                                NULL
                                ),
                            aggregate->getParsingContext()
                            );
                aggCallParam = passAggregateCall(
                            aggregateCallAst,
                            vector<ArrayDesc>(1, aggInputSchema));
                delete aggregateCallAst;
            }
            else
            {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_SINGLE_ATTRIBUTE_IN_INPUT_EXPECTED,
                                           funcParams->getChild(0)->getParsingContext());
            }
        }
        aggCallParam->setAlias(selectList->getChild(0)->getChild(namedExprArgName)
                               ? selectList->getChild(0)->getChild(namedExprArgName)->asNodeString()->getVal()
                               : "");
        LogicalOperator::Parameters aggParams;
        aggParams.push_back(aggCallParam);
        result = appendOperator(aggInput, "aggregate", aggParams, aggregate->getParsingContext());
    }


    AstNode *intoClause = ast->getChild(selectClauseArgIntoClause);
    if (intoClause)
    {
        result = passIntoClause(intoClause, result, query);
    }

    return result;
}

static shared_ptr<LogicalQueryPlanNode> passJoins(AstNode *ast, shared_ptr<Query> query)
{
    assert(ast->getType() == fromList);

    // Left part holding result of join constantly but initially it empty
    shared_ptr<LogicalQueryPlanNode> left = shared_ptr<LogicalQueryPlanNode>();

    // Loop for joining all inputs consequentially. Left joining part can be joined previously or
    // empty nodes. Right part will be joined to left on every iteration.
    BOOST_FOREACH(AstNode *joinItem, ast->getChilds())
    {
        shared_ptr<LogicalQueryPlanNode> right = passJoinItem(joinItem, query);

        // If we on first iteration - right part turning into left, otherwise left and right parts
        // joining and left part turning into join result.
        if (!left)
        {
            left = right;
        }
        else
        {
            shared_ptr<LogicalQueryPlanNode> node = make_shared<LogicalQueryPlanNode>(joinItem->getParsingContext(),
                    OperatorLibrary::getInstance()->createLogicalOperator("join"));
            node->addChild(left);
            node->addChild(right);
            left = node;
        }
    }

    //Check JOIN with its inferring
    try
    {
        left->inferTypes(query);
    }
    catch(const Exception &e)
    {
        throw CONV_TO_USER_QUERY_EXCEPTION(e, ast->getParsingContext());
    }

    // Ok, return join result
    return left;
}

static shared_ptr<LogicalQueryPlanNode> passGeneralizedJoin(AstNode *ast, shared_ptr<Query> query)
{
    LOG4CXX_TRACE(logger, "Translating JOIN-ON clause...");

    shared_ptr<LogicalQueryPlanNode> left = passJoinItem(ast->getChild(joinClauseArgLeft), query);
    shared_ptr<LogicalQueryPlanNode> right = passJoinItem(ast->getChild(joinClauseArgRight), query);

    vector<ArrayDesc> inputSchemas;
    inputSchemas.push_back(left->inferTypes(query));
    inputSchemas.push_back(right->inferTypes(query));

    vector<shared_ptr<OperatorParamReference> > opParams;
    // Checking JOIN-ON clause for pure DD join
    AstNode* joinOnAst = ast->getChild(joinClauseArgExpr);
    bool pureDDJoin = passGeneralizedJoinOnClause(opParams, joinOnAst, query);

    // Well it looks like DD-join but there is a probability that we have attributes or
    // duplicates in expression. Let's check it.
    for (size_t i = 0; pureDDJoin && (i < opParams.size()); i += 2)
    {
        LOG4CXX_TRACE(logger, "Probably pure DD join");

        bool isLeftDimension = resolveParamDimensionReference(inputSchemas, opParams[i], false);
        bool isLeftAttribute = resolveParamAttributeReference(inputSchemas, opParams[i], false);

        bool isRightDimension = resolveParamDimensionReference(inputSchemas, opParams[i + 1], false);
        bool isRightAttribute = resolveParamAttributeReference(inputSchemas, opParams[i + 1], false);

        const string leftFullName = str(format("%s%s") % (opParams[i]->getArrayName() != "" ?
                opParams[i]->getArrayName() + "." : "") % opParams[i]->getObjectName() );

        const string rightFullName = str(format("%s%s") % (opParams[i + 1]->getArrayName() != "" ?
                opParams[i + 1]->getArrayName() + "." : "") % opParams[i + 1]->getObjectName() );

        // Generic checks on existing and ambiguity first of all
        if (!isLeftDimension && !isLeftAttribute)
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_UNKNOWN_ATTRIBUTE_OR_DIMENSION,
                opParams[i]->getParsingContext()) << leftFullName;
        }
        else if (isLeftDimension && isLeftAttribute)
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_AMBIGUOUS_ATTRIBUTE_OR_DIMENSION,
                opParams[i]->getParsingContext()) << leftFullName;
        }

        if (!isRightDimension && !isRightAttribute)
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_UNKNOWN_ATTRIBUTE_OR_DIMENSION,
                opParams[i + 1]->getParsingContext()) << rightFullName;
        }
        else if (isRightDimension && isRightAttribute)
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_AMBIGUOUS_ATTRIBUTE_OR_DIMENSION,
                opParams[i + 1]->getParsingContext()) << rightFullName;
        }

        // No chance. There are attributes and we can not do 'SELECT * FROM A JOIN B ON A.x = A.x' with CROSS_JOIN
        if (isRightAttribute || isLeftAttribute || (opParams[i]->getInputNo() == opParams[i + 1]->getInputNo()))
        {
            LOG4CXX_TRACE(logger, "Nope. This is generalized JOIN");
            pureDDJoin = false;
            break;
        }

        //Ensure dimensions ordered by input number
        if (opParams[i]->getInputNo() == 1)
        {
            LOG4CXX_TRACE(logger, "Swapping couple of dimensions");

            shared_ptr<OperatorParamReference> newRight = opParams[i];
            opParams[i] = opParams[i+1];
            opParams[i+1] = newRight;

            isLeftAttribute = isRightAttribute;
            isRightAttribute = isLeftAttribute;

            isLeftDimension = isRightDimension;
            isRightDimension = isLeftDimension;
        }
    }

    if (pureDDJoin)
    {
        LOG4CXX_TRACE(logger, "Yep. This is really DD join. Inserting CROSS_JOIN");
        // This is DD join! We can do it fast with CROSS_JOIN
        shared_ptr<LogicalQueryPlanNode> crossJoinNode = make_shared<LogicalQueryPlanNode>(ast->getParsingContext(),
                OperatorLibrary::getInstance()->createLogicalOperator("cross_join"));

        crossJoinNode->addChild(left);
        crossJoinNode->addChild(right);
        crossJoinNode->getLogicalOperator()->setParameters(vector<shared_ptr<OperatorParam> >(opParams.begin(), opParams.end()));

        return crossJoinNode;
    }
    else
    {
        LOG4CXX_TRACE(logger, "Inserting CROSS");

        // This is generalized join. Emulating it with CROSS+FILTER
        shared_ptr<LogicalQueryPlanNode> crossNode = make_shared<LogicalQueryPlanNode>(ast->getParsingContext(),
                OperatorLibrary::getInstance()->createLogicalOperator("cross"));

        crossNode->addChild(left);
        crossNode->addChild(right);

        LOG4CXX_TRACE(logger, "Inserting FILTER");
        vector<shared_ptr<OperatorParam> > filterParams(1);
        filterParams[0] = make_shared<OperatorParamLogicalExpression>(
                            joinOnAst->getParsingContext(),
                            AstToLogicalExpression(joinOnAst),
                            TypeLibrary::getType(TID_BOOL));

        return appendOperator(crossNode, "filter", filterParams, joinOnAst->getParsingContext());
    }
}

static bool passGeneralizedJoinOnClause(vector<shared_ptr<OperatorParamReference> > &params,
                                        AstNode *ast, shared_ptr<Query> query)
{
    if (ast->getType() == function)
    {
        const string &funcName = ast->getChild(functionArgName)->asNodeString()->getVal();
        const AstNode* funcParams = ast->getChild(functionArgParameters);

        if (funcName == "and")
        {
            return passGeneralizedJoinOnClause(params, funcParams->getChild(0), query)
                   && passGeneralizedJoinOnClause(params, funcParams->getChild(1), query);
        }
        else if (funcName == "=")
        {
            BOOST_FOREACH(const AstNode *ref, funcParams->getChilds())
            {
                if (ref->getType() != reference)
                {
                    return false;
                }
            }

            const AstNode *leftDim = funcParams->getChilds()[0];
            const AstNode *rightDim = funcParams->getChilds()[1];

            const string &leftObjectName = leftDim->getChild(referenceArgObjectName)->asNodeString()->getVal();
            const string leftArrayName = leftDim->getChild(referenceArgArrayName) ?
                    leftDim->getChild(referenceArgArrayName)->asNodeString()->getVal() :
                    "";

            params.push_back(make_shared<OperatorParamDimensionReference>(
                    leftDim->getParsingContext(),
                    leftArrayName,
                    leftObjectName,
                    true));

            const string &rightObjectName = rightDim->getChild(referenceArgObjectName)->asNodeString()->getVal();
            const string rightArrayName = rightDim->getChild(referenceArgArrayName) ?
                    rightDim->getChild(referenceArgArrayName)->asNodeString()->getVal() :
                    "";

            params.push_back(make_shared<OperatorParamDimensionReference>(
                    rightDim->getParsingContext(),
                    rightArrayName,
                    rightObjectName,
                    true));

            return true;
        }
        else
        {
            return false;
        }
    }
    else
    {
        return false;
    }
}

static shared_ptr<LogicalQueryPlanNode> passCrossJoin(AstNode *ast, shared_ptr<Query> query)
{
    shared_ptr<LogicalQueryPlanNode> left = passJoinItem(ast->getChild(joinClauseArgLeft), query);
    shared_ptr<LogicalQueryPlanNode> right = passJoinItem(ast->getChild(joinClauseArgRight), query);

    shared_ptr<LogicalQueryPlanNode> node = make_shared<LogicalQueryPlanNode>(ast->getParsingContext(),
            OperatorLibrary::getInstance()->createLogicalOperator("cross"));
    node->addChild(left);
    node->addChild(right);

    return node;
}

static shared_ptr<LogicalQueryPlanNode> passJoinItem(AstNode *ast, shared_ptr<Query> query)
{
    shared_ptr<LogicalQueryPlanNode> result = shared_ptr<LogicalQueryPlanNode>();

    switch(ast->getType())
    {
        case namedExpr:
        {
            AstNode* expr = ast->getChild(namedExprArgExpr);

            if ( ( expr->getType() != function
                        || (expr->getType() == function && expr->getChild(functionArgScalarOp)->asNodeBool()->getVal()) )
                    && expr->getType() != reference
                    && expr->getType() != selectStatement)
            {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_INPUT_EXPECTED,
                    expr->getParsingContext());
            }

            result = AstToLogicalPlan(expr, query);

            const string alias = ast->getChild(namedExprArgName) ? ast->getChild(namedExprArgName)->asNodeString()->getVal() : "";
            result->getLogicalOperator()->setAliasName(alias);
            break;
        }

        case joinClause:
            if (ast->getChild(joinClauseArgExpr))
            {
                result = passGeneralizedJoin(ast, query);
            }
            else
            {
                result = passCrossJoin(ast, query);
            }
            break;

        default:
            assert(0);
            break;
    }

    return result;
}

static shared_ptr<LogicalQueryPlanNode> passImplicitScan(AstNode *ast, shared_ptr<Query> query)
{
    assert(ast->getType() == reference);
    LogicalOperator::Parameters scanParams;
    shared_ptr<OperatorParamArrayReference> ref = createArrayReferenceParam(ast, true, query);
    scanParams.push_back(ref);
    shared_ptr<LogicalOperator> scanOp = OperatorLibrary::getInstance()->createLogicalOperator(
        (ref->getVersion() == ALL_VERSIONS) ? "allversions" : "scan");
    scanOp->setParameters(scanParams);
    return make_shared<LogicalQueryPlanNode>(ast->getParsingContext(), scanOp);
}

static shared_ptr<LogicalQueryPlanNode> passFilterClause(AstNode *ast, const shared_ptr<LogicalQueryPlanNode> &input,
    const shared_ptr<Query> &query)
{
    LogicalOperator::Parameters filterParams;
    const ArrayDesc &inputSchema = input->inferTypes(query);

    shared_ptr<LogicalExpression> lExpr = AstToLogicalExpression(ast);

    checkLogicalExpression(vector<ArrayDesc>(1, inputSchema), ArrayDesc(), lExpr);

    filterParams.push_back(make_shared<OperatorParamLogicalExpression>(ast->getParsingContext(),
        lExpr, TypeLibrary::getType(TID_BOOL)));

    shared_ptr<LogicalOperator> filterOp = OperatorLibrary::getInstance()->createLogicalOperator("filter");
    filterOp->setParameters(filterParams);

    shared_ptr<LogicalQueryPlanNode> result = make_shared<LogicalQueryPlanNode>(ast->getParsingContext(), filterOp);
    result->addChild(input);
    return result;
}

static shared_ptr<LogicalQueryPlanNode> passIntoClause(AstNode *ast, shared_ptr<LogicalQueryPlanNode> &input,
        shared_ptr<Query>& query)
{
    assert(identifierClause == ast->getType());
    LOG4CXX_TRACE(logger, "Translating INTO clause...");

    ArrayDesc inputSchema = input->inferTypes(query);

    const string& targetName = ((AstNodeString*)ast)->getVal();
    const shared_ptr<ParsingContext>& parsingContext = ((AstNodeString*)ast)->getParsingContext();

    shared_ptr<LogicalQueryPlanNode> result;

    LogicalOperator::Parameters targetParams;
    targetParams.push_back(make_shared<OperatorParamArrayReference>(parsingContext, "", targetName, true));
    shared_ptr<LogicalOperator> storeOp;

    if (!SystemCatalog::getInstance()->containsArray(targetName))
    {
        LOG4CXX_TRACE(logger, str(format("Target array '%s' not existing so inserting STORE") % targetName));
        storeOp = OperatorLibrary::getInstance()->createLogicalOperator("store");
        storeOp->setParameters(targetParams);
        result = make_shared<LogicalQueryPlanNode>(ast->getParsingContext(), storeOp);
        result->addChild(input);
    }
    else
    {
        LOG4CXX_TRACE(logger, str(format("Target array '%s' existing.") % targetName));

        ArrayDesc destinationSchema;
        SystemCatalog::getInstance()->getArrayDesc(targetName, destinationSchema);

        shared_ptr<LogicalQueryPlanNode> fittedInput = input;

        /*
         * Let's check if input can fit somehow into destination array. If names differ we can insert
         * CAST. If array partitioning differ we can insert REPART. Also we can force input to be empty
         * array to fit empty destination. We can't change array size or dimensions/attributes types,
         * so if such difference found - we skipping casting/repartioning.
         */

        if (!inputSchema.getEmptyBitmapAttribute() && destinationSchema.getEmptyBitmapAttribute())
        {
            shared_ptr<LogicalQueryPlanNode> filteredInput =
                    make_shared<LogicalQueryPlanNode>(ast->getParsingContext(),
                            OperatorLibrary::getInstance()->createLogicalOperator("filter"));
            filteredInput->addChild(input);

            Value bval(TypeLibrary::getType(TID_BOOL));
            bval.setBool(true);

            vector<shared_ptr<OperatorParam> > filterParams(1);
            filterParams[0] = make_shared<OperatorParamLogicalExpression>(
                                ast->getParsingContext(),
                                make_shared<Constant>(ast->getParsingContext(), bval, TID_BOOL),
                                TypeLibrary::getType(TID_BOOL), true);
            filteredInput->getLogicalOperator()->setParameters(filterParams);

            inputSchema = filteredInput->inferTypes(query);
            fittedInput = filteredInput;
        }

        bool needCast = false;
        bool needRepart = false;

        //Give up on casting if schema objects count differ. Nothing to do here.
        if (destinationSchema.getAttributes().size() == inputSchema.getAttributes().size()
            && destinationSchema.getDimensions().size() == inputSchema.getDimensions().size())
        {
            for (size_t attrNo = 0; attrNo < inputSchema.getAttributes().size(); ++attrNo)
            {
                const AttributeDesc &inAttr = destinationSchema.getAttributes()[attrNo];
                const AttributeDesc &destAttr = inputSchema.getAttributes()[attrNo];

                //If attributes has differ names we need casting...
                if (inAttr.getName() != destAttr.getName())
                    needCast = true;

                //... but if type and flags differ we can't cast
                if (inAttr.getType() != destAttr.getType() || inAttr.getFlags() != destAttr.getFlags())
                {
                    needCast = false;
                    goto noCastAndRepart;
                }
            }

            for (size_t dimNo = 0; dimNo < inputSchema.getDimensions().size(); ++dimNo)
            {
                const DimensionDesc &inDim = destinationSchema.getDimensions()[dimNo];
                const DimensionDesc &destDim = inputSchema.getDimensions()[dimNo];

                //If dimension has differ names we need casting...
                if (inDim.getBaseName() != destDim.getBaseName())
                    needCast = true;

                //If dimension has different chunk size we need repart..
                if (inDim.getChunkOverlap() != destDim.getChunkOverlap()
                    || inDim.getChunkInterval() != destDim.getChunkInterval())
                    needRepart = true;

                //... but if length or type of dimension differ we cant cast and repart
                if (inDim.getStart() != destDim.getStart() || inDim.getType() != destDim.getType()
                    || inDim.getLength() != destDim.getLength())
                {
                    needCast = false;
                    needRepart = false;
                    goto noCastAndRepart;
                }
            }

        }
        noCastAndRepart:

        bool tryFlip = false;

        try
        {
            if (needRepart)
            {
                shared_ptr<LogicalOperator> repartOp;
                LOG4CXX_TRACE(logger, "Inserting REPART operator");
                repartOp = OperatorLibrary::getInstance()->createLogicalOperator("repart");

                LogicalOperator::Parameters repartParams (1,
                        make_shared<OperatorParamSchema>(parsingContext, destinationSchema));
                repartOp->setParameters(repartParams);

                shared_ptr<LogicalQueryPlanNode> tmpNode = make_shared<LogicalQueryPlanNode>(ast->getParsingContext(), repartOp);
                tmpNode->addChild(fittedInput);
                tmpNode->inferTypes(query);
                fittedInput = tmpNode;
            }

            if (needCast)
            {
                shared_ptr<LogicalOperator> castOp;
                LOG4CXX_TRACE(logger, "Inserting CAST operator");
                castOp = OperatorLibrary::getInstance()->createLogicalOperator("cast");

                LogicalOperator::Parameters castParams (1,
                        make_shared<OperatorParamSchema>(parsingContext, destinationSchema));
                castOp->setParameters(castParams);

                shared_ptr<LogicalQueryPlanNode>  tmpNode = make_shared<LogicalQueryPlanNode>(ast->getParsingContext(), castOp);
                tmpNode->addChild(fittedInput);
                tmpNode->inferTypes(query);
                fittedInput = tmpNode;
            }

            LOG4CXX_TRACE(logger, "Trying to insert STORE");
            storeOp = OperatorLibrary::getInstance()->createLogicalOperator("store");
            storeOp->setParameters(targetParams);
            result = make_shared<LogicalQueryPlanNode>(ast->getParsingContext(), storeOp);
            result->addChild(fittedInput);
            result->inferTypes(query);

        }
        catch (const UserException& e)
        {
            if (SCIDB_SE_INFER_SCHEMA == e.getShortErrorCode())
            {
                LOG4CXX_TRACE(logger, "Can not infer schema from REPART and/or CAST and/or STORE");
                tryFlip = true;
            }
            else
            {
                LOG4CXX_TRACE(logger, "Something going wrong");
                throw;
            }
        }

        if (!tryFlip)
                {
            LOG4CXX_TRACE(logger, "OK. We managed to fit input into destination. STORE will be used.");
            return result;
        }

        try
        {
            LOG4CXX_TRACE(logger, "Trying to insert REDIMENSION_STORE");
            storeOp = OperatorLibrary::getInstance()->createLogicalOperator("redimension_store");
            storeOp->setParameters(targetParams);
            result = make_shared<LogicalQueryPlanNode>(ast->getParsingContext(), storeOp);
            result->addChild(input);
            result->inferTypes(query);
        }
        catch (const UserException& e)
        {
            if (SCIDB_SE_INFER_SCHEMA == e.getShortErrorCode())
            {
                LOG4CXX_TRACE(logger, "Can not infer schema from REDIMENSION_STORE");
                throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_CAN_NOT_STORE,
                       ast->getParsingContext()) << targetName;
            }

            LOG4CXX_TRACE(logger, "Something going wrong");
            throw;
        }
        LOG4CXX_TRACE(logger, "OK. REDIMENSION_STORE matched.");
    }

    return result;
}

static shared_ptr<LogicalQueryPlanNode> passUpdateStatement(AstNode *ast, const shared_ptr<Query> &query)
{
    AstNode *arrayRef = ast->getChild(updateStatementArgArrayRef);
    shared_ptr<LogicalQueryPlanNode> result = passImplicitScan(arrayRef, query);

    const string &arrayName = arrayRef->getChild(referenceArgObjectName)->asNodeString()->getVal();

    ArrayDesc arrayDesc;
    SystemCatalog::getInstance()->getArrayDesc(arrayName, arrayDesc);
    const AstNode *updateList = ast->getChild(updateStatementArgUpdateList);

    strStrMap substMap;

    unsigned int counter = 0;
    BOOST_FOREACH(const AstNode *updateItem, updateList->getChilds())
    {
        const string& attName = updateItem->getChild(updateListItemArgName)->asNodeString()->getVal();

        bool found = false;
        BOOST_FOREACH(const AttributeDesc &att, arrayDesc.getAttributes())
        {
            if (att.getName() == attName)
            {
                const string newAttName = genUniqueObjectName("updated_" + attName, counter, vector<ArrayDesc>(1, arrayDesc), true);
                substMap[att.getName()] = newAttName;
                found = true;

                //placeholder
                scoped_ptr<AstNode> ph;
                AstNode* attExpr = updateItem->getChild(updateListItemArgExpr);

                vector<ArrayDesc> schemas;
                schemas.push_back(arrayDesc);

                if (expressionType(AstToLogicalExpression(attExpr), query, schemas) != TypeLibrary::getType(att.getType()).typeId())
                {
                    //Wrap expression with type converter appropriate attribute type
                    ph.reset(new AstNode(function, attExpr->getParsingContext(),
                                           functionArgCount,
                                           new AstNodeString(functionName, attExpr->getParsingContext(), TypeLibrary::getType(att.getType()).name()),
                                           new AstNode(functionArguments, attExpr->getParsingContext(), 1, attExpr->clone()),
                                           NULL,
                                           NULL));
                    attExpr = ph.get();
                }

                /* Converting WHERE predicate into iif function in aply operator parameter:
                 * apply(A, a_updated, iif(iif(is_null(whereExpr), false, whereExpr), updateExpr, a))
                 * If we have where clause, we must check if value of where predicate is null or not.
                 * Value of attribute changed only when result of predicate is TRUE. If result of
                 * predicate is NULL or FALSE, value of attribute will not be changed.
                 */
                AstNode *whereExpr = ast->getChild(updateStatementArgWhereClause);
                if (whereExpr)
                {
                    const shared_ptr<ParsingContext> ctxt = attExpr->getParsingContext();
                    ph.reset(new AstNode(function, ctxt, 2,
                                new AstNodeString(operatorName, ctxt, "iif"),
                                new AstNode(functionArguments, ctxt, 3,
                                    new AstNode(function, ctxt, 2,
                                        new AstNodeString(operatorName, ctxt, "iif"),
                                        new AstNode(functionArguments, ctxt, 3,
                                            new AstNode(function, ctxt, 2,
                                                new AstNodeString(operatorName, ctxt, "is_null"),
                                                new AstNode(functionArguments, ctxt, 1, whereExpr->clone())
                                            ),
                                            new AstNodeBool(boolNode, ctxt, false),
                                            whereExpr->clone()
                                        )
                                    ),
                                    attExpr->clone(),
                                    new AstNode(reference, ctxt, referenceArgCount,
                                        NULL,
                                        new AstNodeString(identifierClause, ctxt, attName),
                                        NULL,
                                        NULL,
                                        NULL
                                    )
                                )
                            ));
                    attExpr = ph.get();
                }

                //Wrap child nodes with apply operator created new attribute
                LogicalOperator::Parameters applyParams;
                applyParams.push_back(make_shared<OperatorParamAttributeReference>(updateItem->getParsingContext(),
                        "", newAttName, false));

                applyParams.push_back(make_shared<OperatorParamLogicalExpression>(updateItem->getParsingContext(),
                        AstToLogicalExpression(attExpr), TypeLibrary::getType(att.getType()), false));

                shared_ptr<LogicalOperator> applyOp = OperatorLibrary::getInstance()->createLogicalOperator("apply");
                applyOp->setParameters(applyParams);

                shared_ptr<LogicalQueryPlanNode> applyNode = make_shared<LogicalQueryPlanNode>(updateItem->getParsingContext(), applyOp);
                applyNode->addChild(result);
                result = applyNode;

                break;
            }
        }

        if (!found)
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_ATTRIBUTE_NOT_EXIST,
                    updateItem->getChild(updateListItemArgName)->getParsingContext())
                    << attName;
        }
    }

    //Projecting changed attributes along with unchanged to simulate real update
    vector<ArrayDesc> schemas;
    schemas.push_back(result->inferTypes(query));
    LogicalOperator::Parameters projectParams;
    BOOST_FOREACH(const AttributeDesc &att, arrayDesc.getAttributes())
    {
        shared_ptr<OperatorParamReference> newAtt;
        if (substMap[att.getName()] != "")
        {
            newAtt = make_shared<OperatorParamAttributeReference>(updateList->getParsingContext(),
                    "", substMap[att.getName()], true);
        }
        else
        {
            newAtt = make_shared<OperatorParamAttributeReference>(updateList->getParsingContext(),
                    "", att.getName(), true);
        }
        resolveParamAttributeReference(schemas, newAtt);
        projectParams.push_back(newAtt);
    }

    shared_ptr<LogicalOperator> projectOp = OperatorLibrary::getInstance()->createLogicalOperator("project");
    projectOp->setParameters(projectParams);

    shared_ptr<LogicalQueryPlanNode> projectNode = make_shared<LogicalQueryPlanNode>(updateList->getParsingContext(), projectOp);
    projectNode->addChild(result);
    result = projectNode;

    //Finally wrap input with STORE operator
    LogicalOperator::Parameters storeParams;

    storeParams.push_back(make_shared<OperatorParamArrayReference>(
            arrayRef->getChild(referenceArgObjectName)->getParsingContext(), "", arrayName, true));

    shared_ptr<LogicalOperator> storeOp = OperatorLibrary::getInstance()->createLogicalOperator("store");
    storeOp->setParameters(storeParams);

    shared_ptr<LogicalQueryPlanNode> storeNode = make_shared<LogicalQueryPlanNode>(ast->getParsingContext(), storeOp);
    storeNode->addChild(result);
    result = storeNode;

    return result;
}

static shared_ptr<LogicalQueryPlanNode> passLoadStatement(AstNode *ast, shared_ptr<Query> query)
{
    const string& arrayName = ast->getChild(loadStatementArgArrayName)->asNodeString()->getVal();
    const string& fileName = ast->getChild(loadStatementArgFileName)->asNodeString()->getVal();

    if (!SystemCatalog::getInstance()->containsArray(arrayName))
    {
        throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_ARRAY_DOESNT_EXIST,
                    ast->getChild(loadStatementArgArrayName)->getParsingContext()) << arrayName;
    }

    ArrayDesc inputArray;
    SystemCatalog::getInstance()->getArrayDesc(arrayName, inputArray);

    LogicalOperator::Parameters inputParams;
    inputParams.push_back(make_shared<OperatorParamArrayReference>(
            ast->getChild(loadStatementArgArrayName)->getParsingContext(), "", arrayName, true));

    Value sval(TypeLibrary::getType(TID_STRING));
    sval.setData(fileName.c_str(), fileName.length() + 1);
    shared_ptr<LogicalExpression> expr = make_shared<Constant>(ast->getParsingContext(), sval,  TID_STRING);
    inputParams.push_back(make_shared<OperatorParamLogicalExpression>(
            ast->getChild(loadStatementArgFileName)->getParsingContext(),
            expr, TypeLibrary::getType(TID_STRING), true));

    shared_ptr<LogicalOperator> inputOp = OperatorLibrary::getInstance()->createLogicalOperator("input");
    inputOp->setParameters(inputParams);

    if ( query->getNodesCount() == 1) { //XXXX should this getRegisteredNodeCount() ???
        LogicalOperator::Parameters storeParams;
        shared_ptr<LogicalOperator> storeOp = OperatorLibrary::getInstance()->createLogicalOperator("store");
        storeParams.push_back(make_shared<OperatorParamArrayReference>(
                ast->getChild(loadStatementArgArrayName)->getParsingContext(), "", arrayName, true));
        storeOp->setParameters(storeParams);
        shared_ptr<LogicalQueryPlanNode> storeNode = make_shared<LogicalQueryPlanNode>(ast->getParsingContext(), storeOp);
        storeNode->addChild(make_shared<LogicalQueryPlanNode>(ast->getParsingContext(), inputOp));
        return storeNode;
    } else {
        LogicalOperator::Parameters sgParams(3);
        Value ival(TypeLibrary::getType(TID_INT32));
        ival.setInt32(psRoundRobin);
        sgParams[0] = make_shared<OperatorParamLogicalExpression>(ast->getParsingContext(),
                make_shared<Constant>(ast->getParsingContext(), ival, TID_INT32),
                TypeLibrary::getType(TID_INT32), true);

        ival.setInt32(-1);
        sgParams[1] = make_shared<OperatorParamLogicalExpression>(ast->getParsingContext(),
                make_shared<Constant>(ast->getParsingContext(), ival, TID_INT32),
                TypeLibrary::getType(TID_INT32), true);

        sgParams[2] = make_shared<OperatorParamArrayReference>(ast->getParsingContext(), "", arrayName, true);

        shared_ptr<LogicalOperator> sgOp = OperatorLibrary::getInstance()->createLogicalOperator("sg");
        sgOp->setParameters(sgParams);

        shared_ptr<LogicalQueryPlanNode> sgNode = make_shared<LogicalQueryPlanNode>(ast->getParsingContext(), sgOp);

        sgNode->addChild(make_shared<LogicalQueryPlanNode>(ast->getParsingContext(), inputOp));

        return sgNode;
    }
}

static shared_ptr<LogicalQueryPlanNode> passDropArrayStatement(AstNode *ast)
{
    const string& arrayName = ast->getChild(loadStatementArgArrayName)->asNodeString()->getVal();

    if (!SystemCatalog::getInstance()->containsArray(arrayName))
    {
        throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_ARRAY_DOESNT_EXIST,
                    ast->getChild(loadStatementArgArrayName)->getParsingContext()) << arrayName;
    }

    LogicalOperator::Parameters removeParams;
    removeParams.push_back(make_shared<OperatorParamArrayReference>(
            ast->getChild(loadStatementArgArrayName)->getParsingContext(), "", arrayName, true));

    shared_ptr<LogicalOperator> removeOp = OperatorLibrary::getInstance()->createLogicalOperator("remove");
    removeOp->setParameters(removeParams);

    return make_shared<LogicalQueryPlanNode>(ast->getParsingContext(), removeOp);
}

static shared_ptr<LogicalQueryPlanNode> passLoadLibrary(AstNode *ast)
{
    const string &libraryName = ast->getChild(loadLibraryStatementArgLibrary)->asNodeString()->getVal();

    Value libName(TypeLibrary::getType(TID_STRING));
    libName.setString(libraryName.c_str());
    shared_ptr<LogicalExpression> expr = make_shared<Constant>(
            ast->getChild(loadLibraryStatementArgLibrary)->getParsingContext(), libName, TID_STRING);

    LogicalOperator::Parameters parameters(1, make_shared<OperatorParamLogicalExpression>(
            ast->getParsingContext(), expr, TypeLibrary::getType(TID_STRING), true));

    shared_ptr<LogicalOperator> op =
            OperatorLibrary::getInstance()->createLogicalOperator("load_library");
    op->setParameters(parameters);

    return make_shared<LogicalQueryPlanNode>(ast->getParsingContext(), op);
}

static shared_ptr<LogicalQueryPlanNode> passUnloadLibrary(AstNode *ast)
{
    const string &libraryName = ast->getChild(loadLibraryStatementArgLibrary)->asNodeString()->getVal();

    Value libName(TypeLibrary::getType(TID_STRING));
    libName.setString(libraryName.c_str());
    shared_ptr<LogicalExpression> expr = make_shared<Constant>(
            ast->getChild(loadLibraryStatementArgLibrary)->getParsingContext(), libName, TID_STRING);

    LogicalOperator::Parameters parameters(1, make_shared<OperatorParamLogicalExpression>(
            ast->getParsingContext(), expr, TypeLibrary::getType(TID_STRING), true));

    shared_ptr<LogicalOperator> op =
            OperatorLibrary::getInstance()->createLogicalOperator("unload_library");
    op->setParameters(parameters);

    return make_shared<LogicalQueryPlanNode>(ast->getParsingContext(), op);
}

static void checkLogicalExpression(const vector<ArrayDesc> &inputSchemas, const ArrayDesc &outputSchema,
        const shared_ptr<LogicalExpression> &expr)
{
    if (typeid(*expr) == typeid(AttributeReference))
    {
        const shared_ptr<AttributeReference> &ref = static_pointer_cast<AttributeReference>(expr);

        //We don't know exactly what type this reference, so check both attribute and dimension,
        //and if we eventually found both, so throw ambiguous exception

        const bool foundAttrIn = checkAttribute(inputSchemas, ref->getArrayName(), ref->getAttributeName(), ref->getParsingContext());
        const bool foundAttrOut = checkAttribute(vector<ArrayDesc>(1, outputSchema), ref->getArrayName(), ref->getAttributeName(), ref->getParsingContext());

        const bool foundDimIn = checkDimension(inputSchemas, ref->getArrayName(), ref->getAttributeName(), ref->getParsingContext());
        const bool foundDimOut = checkDimension(vector<ArrayDesc>(1, outputSchema), ref->getArrayName(), ref->getAttributeName(), ref->getParsingContext());

        const string fullName = str(format("%s%s") % (ref->getArrayName() != "" ? ref->getArrayName() + "." : "") % ref->getAttributeName() );

        // Checking ambiguous situation in input schema. If no ambiguity we found dimension/attribute
        // or not.
        if (foundAttrIn && foundDimIn)
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_AMBIGUOUS_ATTRIBUTE_OR_DIMENSION,
                ref->getParsingContext()) << fullName;
        }
        // If we can't find references in input schema, checking output schema.
        else if (!(foundAttrIn || foundDimIn))
        {
            // Same as for input: checking ambiguity in output schema.
            if (foundAttrOut && foundDimOut)
            {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_AMBIGUOUS_ATTRIBUTE_OR_DIMENSION,
                    ref->getParsingContext()) << fullName;
            }
            // If we can't find reference even in output schema, finally throw error
            else if (!(foundAttrOut || foundDimOut))
            {
                ArrayDesc schema;
                if (ref->getArrayName() != "" || !SystemCatalog::getInstance()->getArrayDesc(ref->getAttributeName(), schema, false) || schema.getAttributes().size() != 1 || schema.getDimensions().size() != 1 || schema.getDimensions()[0].getLength() != 1)
                {
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_UNKNOWN_ATTRIBUTE_OR_DIMENSION,
                        ref->getParsingContext()) << fullName;
                }
            }
        }
                // If no ambiguity, and found some reference, we ignoring all from output schema.
    }
    else if (typeid(*expr) == typeid(Function))
    {
        const shared_ptr<Function> &func = static_pointer_cast<Function>(expr);
        BOOST_FOREACH(const shared_ptr<LogicalExpression> &funcArg, func->getArgs())
        {
            checkLogicalExpression(inputSchemas, outputSchema, funcArg);
        }
    }
}

static bool checkAttribute(const vector<ArrayDesc> &inputSchemas, const string &aliasName, const string &attributeName,
                           const shared_ptr<ParsingContext> &ctxt)
{
    const string fullName = str(format("%s%s") % (aliasName != "" ? aliasName + "." : "") % attributeName);

    bool found = false;
    size_t schemaNo = 0;
    size_t attNo = 0;

    BOOST_FOREACH(const ArrayDesc &schema, inputSchemas)
    {
        BOOST_FOREACH(const AttributeDesc& attribute, schema.getAttributes())
        {
            if (attribute.getName() == attributeName && attribute.hasAlias(aliasName))
            {
                if (found)
                {
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_AMBIGUOUS_ATTRIBUTE, ctxt)
                        << fullName;
                }
                found = true;
            }
            ++attNo;
        }
        attNo = 0;
        ++schemaNo;
    }

    return found;
}

static bool checkDimension(const vector<ArrayDesc> &inputSchemas, const string &aliasName, const string &dimensionName,
        const shared_ptr<ParsingContext> &ctxt)
{
    const string fullName = str(format("%s%s") % (aliasName != "" ? aliasName + "." : "") % dimensionName);

    bool found = false;
    BOOST_FOREACH(const ArrayDesc &schema, inputSchemas)
    {
        BOOST_FOREACH(const DimensionDesc& dim, schema.getDimensions())
        {
            if (dim.hasNameOrAlias(dimensionName, aliasName))
            {
                if (found)
                {
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_AMBIGUOUS_DIMENSION, ctxt)
                        << fullName;
                }
                found = true;
            }
        }
    }

    return found;
}

static shared_ptr<LogicalQueryPlanNode> appendOperator(
    const shared_ptr<LogicalQueryPlanNode> &node,
    const string &opName,
    const LogicalOperator::Parameters &opParams,
    const shared_ptr<ParsingContext> &opParsingContext)
{
    shared_ptr<LogicalQueryPlanNode> newNode = make_shared<LogicalQueryPlanNode>(
            opParsingContext,
            OperatorLibrary::getInstance()->createLogicalOperator(opName));
    newNode->getLogicalOperator()->setParameters(opParams);
    newNode->addChild(node);
    return newNode;
}

static bool astHasReferences(const AstNode *ast)
{
    switch(ast->getType())
    {
        case function:
        {
            BOOST_FOREACH(AstNode *funcArg, ast->getChild(functionArgParameters)->getChilds())
            {
                if (astHasReferences(funcArg))
                    return true;
            }

            break;
        }

        case asterisk:
        case reference:
            return true;

        default:
            return false;
    }

    return false;
}

static bool astHasAggregates(const AstNode *ast)
{
    switch(ast->getType())
    {
        case function:
        {
            const string& funcName = ast->getChild(functionArgName)->asNodeString()->getVal();

            if (AggregateLibrary::getInstance()->hasAggregate(funcName))
                return true;

            BOOST_FOREACH(AstNode *funcArg, ast->getChild(functionArgParameters)->getChilds())
            {
                if (astHasAggregates(funcArg))
                    return true;
            }

            break;
        }

        default:
            return false;
    }

    return false;
}

static AstNode* decomposeExpression(
    const AstNode *ast,
    AstNodes &preAggregationEvals,
    AstNodes &aggregateFunctions,
    unsigned int &internalNameCounter,
    bool hasAggregates,
    const string &partitionName,
    const ArrayDesc &inputSchema)
{
    LOG4CXX_TRACE(logger, "Decomposing expression");
    vector<ArrayDesc> inputSchemas(1, inputSchema);

    switch (ast->getType())
    {
        case function:
        {
            LOG4CXX_TRACE(logger, "This is function");
            const string& funcName = ast->getChild(functionArgName)->asNodeString()->getVal();
            const AstNode* funcArgs = ast->getChild(functionArgParameters);
            bool isAggregate = AggregateLibrary::getInstance()->hasAggregate(funcName);

            // We found aggregate and must care of it
            if (isAggregate)
            {
                LOG4CXX_TRACE(logger, "This is aggregate call");
                // Currently framework supports only one argument aggregates so drop any other cases
                if (funcArgs->getChildsCount() != 1)
                {
                    LOG4CXX_TRACE(logger, "Passed too many arguments to aggregate call");
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_WRONG_AGGREGATE_ARGUMENTS_COUNT,
                        ast->getParsingContext());
                }

                const AstNode* aggArg = funcArgs->getChild(0);

                // Check if this sole expression has aggregate calls itself and drop if yes
                if (astHasAggregates(aggArg))
                {
                    LOG4CXX_TRACE(logger, "Nested aggregate");
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_AGGREGATE_CANT_BE_NESTED,
                        ast->getParsingContext());
                }

                // If partition name used as reference in aggregate, we must expand it into attribute
                // name.
                if (selectStatement == aggArg->getType()
                    || (reference == aggArg->getType()
                        && partitionName == aggArg->getChild(referenceArgObjectName)->asNodeString()->getVal()))
                {
                    LOG4CXX_TRACE(logger, "Aggregate's argument is partition name");
                    if (reference == aggArg->getType() && inputSchema.getAttributes().size() > 1)
                    {
                        throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_SINGLE_ATTRIBUTE_IN_INPUT_EXPECTED,
                            aggArg->getParsingContext());
                    }

                    AstNode *aggAtt = (reference == aggArg->getType()
                        ? new AstNode(reference,  ast->getChild(functionArgParameters)->getParsingContext(),
                            referenceArgCount,
                            NULL,
                            new AstNodeString(stringNode, ast->getParsingContext(), inputSchema.getAttributes()[0].getName()),
                            NULL,
                            NULL,
                            NULL)
                        : passAggregateSelectClause(aggArg, partitionName));

                    AstNode *postEvalAttName = new AstNodeString(
                        stringNode, ast->getParsingContext(),
                        genUniqueObjectName("expr", internalNameCounter, inputSchemas, true));

                    AstNode *aggregateExpression = new AstNode(function, ast->getParsingContext(), functionArgCount,
                        ast->getChild(functionArgName)->clone(),
                        new AstNode(functionArguments, ast->getChild(functionArgParameters)->getParsingContext(), 1,
                            aggAtt),
                        postEvalAttName,
                        new AstNodeBool(boolNode, ast->getParsingContext(), false));

                    aggregateFunctions.push_back(aggregateExpression);

                    return new AstNode(reference, ast->getChild(functionArgParameters)->getParsingContext(),
                        referenceArgCount, NULL, postEvalAttName->clone(), NULL, NULL, NULL);
                }
                // If function argument is reference or asterisk, we can translate to aggregate call
                // it as is but must assign alias to reference in post-eval expression
                else if (reference == aggArg->getType()
                    || asterisk == aggArg->getType())
                {
                    LOG4CXX_TRACE(logger, "Aggregate's argument is reference or asterisk");
                    AstNode *aggFunc = ast->clone();
                    AstNode *alias = new AstNodeString(
                        stringNode, ast->getParsingContext(),
                        genUniqueObjectName("expr", internalNameCounter, inputSchemas, true));
                    aggFunc->setChild(functionArgAliasName, alias);
                    aggregateFunctions.push_back(aggFunc);

                    // Finally returning reference to aggregate result in overall expression
                    return new AstNode(reference,  ast->getChild(functionArgParameters)->getParsingContext(),
                        referenceArgCount, NULL, alias->clone(), NULL, NULL, NULL);
                }
                // Handle select statement
                else if (selectStatement == aggArg->getType())
                {
                    LOG4CXX_TRACE(logger, "Aggregate's argument is SELECT");
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_UNEXPECTED_SELECT_INSIDE_AGGREGATE,
                        ast->getParsingContext());
                }
                // We found expression or constant. We can't use it in aggregate/regrid/window and
                // must create pre-evaluate
                else
                {
                    LOG4CXX_TRACE(logger, "Aggregate's argument is expression");
                    // Let's we have aggregate(expression)

                    // Prepare result attribute name for expression which must be evaluated
                    AstNode *preEvalAttName = new AstNodeString(
                        stringNode, ast->getParsingContext(),
                        genUniqueObjectName("expr", internalNameCounter, inputSchemas, true));
                    // Named expression 'expression as resname1' will be later translated into
                    // operator APPLY(input, expression, preEvalAttName)
                    AstNode *applyExpression = new AstNode(namedExpr, ast->getParsingContext(),
                        namedExprArgCount, ast->getChild(functionArgParameters)->getChild(0)->clone(), preEvalAttName);
                    // This is must be evaluated before aggregates
                    preAggregationEvals.push_back(applyExpression);

                    // Prepare result attribute name for aggregate call which must be evaluate before
                    // evaluating whole expression
                    AstNode *postEvalAttName = new AstNodeString(
                        stringNode, ast->getParsingContext(),
                        genUniqueObjectName("expr", internalNameCounter, inputSchemas, true));

                    // Aggregate call will be translated later into AGGREGATE(input, aggregate(preEvalAttName) as postEvalName)
                    AstNode *aggregateExpression = new AstNode(function, ast->getParsingContext(), functionArgCount,
                        ast->getChild(functionArgName)->clone(),
                        new AstNode(functionArguments, ast->getChild(functionArgParameters)->getParsingContext(), 1,
                            new AstNode(reference,  ast->getChild(functionArgParameters)->getParsingContext(), referenceArgCount,
                                NULL, preEvalAttName->clone(), NULL, NULL, NULL)
                            ),
                            postEvalAttName,
                            new AstNodeBool(boolNode, ast->getParsingContext(), false)
                        );

                    aggregateFunctions.push_back(aggregateExpression);

                    // Finally returning reference to aggregate result in overall expression
                    return new AstNode(reference,  ast->getChild(functionArgParameters)->getParsingContext(), referenceArgCount,
                        NULL, postEvalAttName->clone(), NULL, NULL, NULL);
                }
            }
            // This is scalar function. We must pass each argument and construct new function call
            // AST node for post-eval expression
            else
            {
                LOG4CXX_TRACE(logger, "This is scalar function");
                AstNode *newFuncCallArgs = new AstNode(functionArguments,
                    ast->getChild(functionArgParameters)->getParsingContext(), 0);
                try
                {
                    BOOST_FOREACH(AstNode* funcArg, funcArgs->getChilds())
                    {
                        LOG4CXX_TRACE(logger, "Passing function argument");
                        newFuncCallArgs->addChild(decomposeExpression(funcArg, preAggregationEvals,
                            aggregateFunctions, internalNameCounter, hasAggregates,
                            partitionName, inputSchema));
                    }
                }
                catch (...)
                {
                    delete newFuncCallArgs;
                    throw;
                }

                return new AstNode(function, ast->getParsingContext(), functionArgCount,
                    ast->getChild(functionArgName)->clone(),
                    newFuncCallArgs,
                    NULL, // no alias
                    new AstNodeBool(boolNode, ast->getParsingContext(), false));
            }

            break;
        }

        default:
        {
            LOG4CXX_TRACE(logger, "This is reference or constant");
            if (reference == ast->getType() && hasAggregates)
            {
                LOG4CXX_TRACE(logger, "We can not use references in expression with aggregate");
                throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_ITEM_MUST_BE_INSIDE_AGGREGATE,
                    ast->getParsingContext());
            }

            LOG4CXX_TRACE(logger, "Cloning node to post-evaluation expression");
            return ast->clone();

            break;
        }
    }

    assert(0);
    return NULL;
}

static shared_ptr<LogicalQueryPlanNode> passSelectList(
    shared_ptr<LogicalQueryPlanNode> &input,
    AstNode *selectList,
    AstNode *grwAsClause,
    const shared_ptr<Query> &query)
{
    LOG4CXX_TRACE(logger, "Translating SELECT list");
    const ArrayDesc& inputSchema = input->inferTypes(query);
    const vector<ArrayDesc> inputSchemas(1, inputSchema);
    LogicalOperator::Parameters projectParams;

    bool selectListHasAggregates = false;
    BOOST_FOREACH(AstNode *selItem, selectList->getChilds())
    {
        if (namedExpr == selItem->getType())
        {
            if (astHasAggregates(selItem->getChild(namedExprArgExpr)))
            {
                selectListHasAggregates = true;
                break;
            }
        }
    }

    if (grwAsClause && !selectListHasAggregates)
    {
        LOG4CXX_TRACE(logger,"GROUP BY, WINDOW, REGRID or REDIMENSION present, but SELECT list does"
                " not contain aggregates");
        throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_ITEM_MUST_BE_INSIDE_AGGREGATE,
            selectList->getChild(0)->getParsingContext());
    }

    AstNodes preAggregationEvals;
    AstNodes aggregateFunctions;
    AstNodes postAggregationEvals;

    shared_ptr<LogicalQueryPlanNode> result = input;

    // We don't care when error happen, but we must cleanup result allocated by decomposeExpression
    try
    {
        unsigned int internalNameCounter = 0;
        unsigned int externalExprCounter = 0;
        unsigned int externalAggregateCounter = 0;
        BOOST_FOREACH(AstNode *selItem, selectList->getChilds())
        {
            LOG4CXX_TRACE(logger, "Translating SELECT list item");

            switch(selItem->getType())
            {
                case namedExpr:
                {
                    LOG4CXX_TRACE(logger, "Item is named expression");

                    // If reference is attribute, we must do PROJECT
                    bool doProject = false;
                    if (reference == selItem->getChild(namedExprArgExpr)->getType()
                        && !selItem->getChild(namedExprArgName))

                    {
                        const AstNode *refNode = selItem->getChild(namedExprArgExpr);
                        const string &name = refNode->getChild(referenceArgObjectName)->asNodeString()->getVal();
                        const string &alias = refNode->getChild(referenceArgArrayName)
                            ? refNode->getChild(referenceArgObjectName)->asNodeString()->getVal()
                            : "";
                        // Strange issue with BOOST_FOREACH infinity loop. Leaving for-loop instead.
                        for(vector<AttributeDesc>::const_iterator attIt = inputSchema.getAttributes().begin();
                            attIt != inputSchema.getAttributes().end(); ++attIt)
                        {
                            LOG4CXX_TRACE(logger, "Item is named expression");
                            if (attIt->getName() == name && attIt->hasAlias(alias))
                            {
                                doProject = true;
                                break;
                            }
                        }
                    }

                    if (doProject)
                    {
                        LOG4CXX_TRACE(logger, "Item is has no name so this is projection");
                        const AstNode *refNode = selItem->getChild(namedExprArgExpr);
                        if (selectListHasAggregates)
                        {
                            LOG4CXX_TRACE(logger, "SELECT list contains aggregates so we can't do projection");
                            throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_ITEM_MUST_BE_INSIDE_AGGREGATE2,
                                refNode->getParsingContext());
                        }

                        shared_ptr<OperatorParamReference> param = make_shared<OperatorParamAttributeReference>(
                            selItem->getParsingContext(),
                            "",
                            refNode->getChild(referenceArgObjectName)->asNodeString()->getVal(),
                            true);

                        resolveParamAttributeReference(inputSchemas, param);
                        projectParams.push_back(param);
                    }
                    else
                    {
                        LOG4CXX_TRACE(logger, "This is will be expression evaluation");

                        if (astHasAggregates(selItem->getChild(namedExprArgExpr)))
                        {
                            LOG4CXX_TRACE(logger, "This is will be expression with aggregate evaluation");
                        }
                        else
                        {
                            LOG4CXX_TRACE(logger, "This is will be expression evaluation");
                            if (astHasReferences(selItem->getChild(namedExprArgExpr))
                                && selectListHasAggregates)
                            {
                                LOG4CXX_TRACE(logger, "This expression has references we can't evaluate it because we has aggregates");
                                throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_ITEM_MUST_BE_INSIDE_AGGREGATE2,
                                    selItem->getParsingContext());
                            }
                        }

                        string partitionName = "";
                        if (grwAsClause)
                        {
                            switch (grwAsClause->getType())
                            {
                                case groupByClause:
                                    partitionName = grwAsClause->getChild(groupByClauseArgAs)
                                        ? grwAsClause->getChild(groupByClauseArgAs)->asNodeString()->getVal()
                                        : "";
                                    break;
                                case windowClause:
                                    partitionName = grwAsClause->getChild(windowClauseArgAs)
                                        ? grwAsClause->getChild(windowClauseArgAs)->asNodeString()->getVal()
                                        : "";
                                    break;
                                case regridClause:
                                    partitionName = grwAsClause->getChild(regridClauseArgAs)
                                        ? grwAsClause->getChild(regridClauseArgAs)->asNodeString()->getVal()
                                        : "";
                                    break;
                                case redimensionClause:
                                    break;
                                default:
                                    assert(0);
                            }
                        }

                        AstNode *postEvalExpr = NULL;
                        postEvalExpr = decomposeExpression(selItem->getChild(namedExprArgExpr),
                            preAggregationEvals,
                            aggregateFunctions,
                            internalNameCounter,
                            selectListHasAggregates,
                            partitionName,
                            inputSchema);


                        // Prepare name for SELECT item result. If AS was used by user, we just copy it
                        // else we generate new name
                        AstNode* outputNameNode = NULL;
                        if (selItem->getChild(namedExprArgName))
                        {
                            outputNameNode = selItem->getChild(namedExprArgName)->clone();
                        }
                        else
                        {
                            // If SELECT item is single aggregate we will use function name as prefix
                            // else we will use 'expr' prefix
                            string prefix;
                            if (function == selItem->getChild(namedExprArgExpr)->getType()
                                && AggregateLibrary::getInstance()->hasAggregate(
                                    selItem->getChild(namedExprArgExpr)->getChild(functionArgName)->asNodeString()->getVal()))
                            {
                                outputNameNode = new AstNodeString(stringNode, selItem->getParsingContext(),
                                    genUniqueObjectName(
                                        selItem->getChild(namedExprArgExpr)->getChild(functionArgName)->asNodeString()->getVal(),
                                        externalAggregateCounter, inputSchemas, false, selectList->getChilds()));
                            }
                            else
                            {
                                outputNameNode = new AstNodeString(stringNode, selItem->getParsingContext(),
                                    genUniqueObjectName("expr", externalExprCounter, inputSchemas, false, selectList->getChilds()));
                            }
                        }

                        AstNode *postEvalNamedExpr = new AstNode(
                            namedExpr, selItem->getChild(namedExprArgExpr)->getParsingContext(),
                            namedExprArgCount,
                            postEvalExpr,
                            outputNameNode);

                        postAggregationEvals.push_back(postEvalNamedExpr);

                        projectParams.push_back(make_shared<OperatorParamAttributeReference>(
                            postEvalNamedExpr->getChild(namedExprArgName)->getParsingContext(),
                            "", postEvalNamedExpr->getChild(namedExprArgName)->asNodeString()->getVal(), true));
                    }

                    break;
                }

                case asterisk:
                {
                    LOG4CXX_TRACE(logger, "Item is asterisk. It will be expanded to attributes.");

                    if (selectListHasAggregates)
                    {
                        LOG4CXX_TRACE(logger, "SELECT list contains aggregates so we can't expand asterisk");
                        throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_ITEM_MUST_BE_INSIDE_AGGREGATE2,
                            selItem->getParsingContext());
                    }

                    // We can freely omit project, if asterisk is sole item in selection list
                    if (selectList->getChildsCount() == 1)
                    {
                        break;
                    }

                    // Otherwise prepare parameters for project
                    BOOST_FOREACH(const AttributeDesc &att, inputSchema.getAttributes())
                    {
                        shared_ptr<OperatorParamReference> param = make_shared<OperatorParamAttributeReference>(
                            selItem->getParsingContext(),
                            "",
                            att.getName(),
                            true);

                        resolveParamAttributeReference(inputSchemas, param);
                        projectParams.push_back(param);
                    }
                    break;
                }

                default:
                {
                    LOG4CXX_TRACE(logger, "Unknown item. Asserting.");
                    assert(0);
                }
            }
        }

        // Pass list of pre-aggregate evaluations and translate it into APPLY operators
        BOOST_FOREACH(const AstNode *namedExprNode, preAggregationEvals)
        {
            assert(namedExpr == namedExprNode->getType());
            LOG4CXX_TRACE(logger, "Translating preAggregateEval into logical operator APPLY");

            // This is internal output reference which will be used for aggregation
            shared_ptr<OperatorParamReference> refParam = make_shared<OperatorParamAttributeReference>(
                namedExprNode->getChild(namedExprArgName)->getParsingContext(),
                "", namedExprNode->getChild(namedExprArgName)->asNodeString()->getVal(), false);

            // This is expression which will be used as APPLY expression
            shared_ptr<LogicalExpression> lExpr = AstToLogicalExpression(namedExprNode->getChild(namedExprArgExpr));
            checkLogicalExpression(inputSchemas, ArrayDesc(), lExpr);
            shared_ptr<OperatorParam> exprParam = make_shared<OperatorParamLogicalExpression>(
                namedExprNode->getChild(namedExprArgExpr)->getParsingContext(),
                lExpr, TypeLibrary::getType(TID_VOID));

            LogicalOperator::Parameters applyParams;
            applyParams.push_back(refParam);
            applyParams.push_back(exprParam);
            LOG4CXX_TRACE(logger, "APPLY node appended");
            result = appendOperator(result, "apply", applyParams, namedExprNode->getParsingContext());
        }

        const vector<ArrayDesc> preEvalInputSchemas(1, result->inferTypes(query));

        // Pass list of aggregates and create single AGGREGATE/REGRID/WINDOW operator
        if (aggregateFunctions.size() > 0)
        {
            LOG4CXX_TRACE(logger, "Translating aggregate into logical aggregate call");
            LogicalOperator::Parameters aggregateParams;

            string aggregateOperator = "aggregate";
            if (grwAsClause)
            {
                switch(grwAsClause->getType())
                {
                    case windowClause:
                    case regridClause:
                    {
                        const AstNode* regridWindowSizesAst = grwAsClause->getChild(regridClauseArgSizesList);

                        if (regridWindowSizesAst->getChildsCount() != preEvalInputSchemas[0].getDimensions().size())
                        {
                            throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX,
                                SCIDB_LE_WRONG_REGRID_REDIMENSION_SIZES_COUNT,
                                regridWindowSizesAst->getParsingContext());
                        }

                        BOOST_FOREACH(AstNode* gridSize, regridWindowSizesAst->getChilds())
                        {
                            shared_ptr<LogicalExpression> sizeExpr = AstToLogicalExpression(gridSize);

                            aggregateParams.push_back(
                                    make_shared<OperatorParamLogicalExpression>(
                                        gridSize->getParsingContext(),
                                        sizeExpr, TypeLibrary::getType(TID_VOID)));
                        }

                        aggregateOperator = grwAsClause->getType() == windowClause ? "window" : "regrid";
                        break;
                    }
                    case groupByClause:
                        break;
                    case redimensionClause:
                    {
                        LOG4CXX_TRACE(logger, "Adding schema to REDIMENSION parameters");

                        //First we iterate over all aggregates and prepare attributes for schema
                        //which will be inserted to redimension. We need to extract type of attribute
                        //from previous schema, which used in aggregate to get output type
                        set<string> usedNames;
                        Attributes redimensionAttrs;
                        BOOST_FOREACH(AstNode *aggCallNode, aggregateFunctions)
                        {
                            const string &aggName = aggCallNode->getChild(functionArgName)->asNodeString()->getVal();
                            const string &aggAlias = aggCallNode->getChild(functionArgAliasName)->asNodeString()->getVal();
                            
                            Type aggParamType;
                            if (asterisk == aggCallNode->getChild(functionArgParameters)->getChild(0)->getType())
                            {
                                LOG4CXX_TRACE(logger, "Getting type of " << aggName << "(*) as " << aggAlias);
                                aggParamType = TypeLibrary::getType(TID_VOID);
                            }
                            else if (reference == aggCallNode->getChild(functionArgParameters)->getChild(0)->getType())
                            {
                                const string &aggAttrName = aggCallNode->getChild(functionArgParameters)->
                                        getChild(0)->getChild(referenceArgObjectName)->asNodeString()->getVal();

                                LOG4CXX_TRACE(logger, "Getting type of " << aggName << "(" << aggAttrName << ") as " << aggAlias);

                                BOOST_FOREACH(const AttributeDesc &attr, preEvalInputSchemas[0].getAttributes())
                                {
                                    if (attr.getName() == aggAttrName)
                                    {
                                        aggParamType = TypeLibrary::getType(attr.getType());
                                        break;
                                    }
                                }
                            }
                            else
                            {
                                assert(0);
                            }
                            
                            const TypeId &tid = AggregateLibrary::getInstance()->createAggregate(
                                    aggName, aggParamType)->getResultType().typeId();
                            LOG4CXX_TRACE(logger, "It has type " << tid);
                            redimensionAttrs.push_back(AttributeDesc(redimensionAttrs.size(),
                                                                     aggAlias,
                                                                     tid,
                                                                     AttributeDesc::IS_NULLABLE,
                                                                     0));
                            usedNames.insert(aggAlias);
                        }
                        redimensionAttrs.push_back(AttributeDesc(
                                redimensionAttrs.size(), "empty_indicator",
                                TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0));
                        
                        //Now prepare dimensions
                        Dimensions redimensionDims;
                        passDimensionsList(grwAsClause->getChild(0), redimensionDims, "", usedNames);
                        
                        //Ok. Adding schema parameter
                        ArrayDesc redimensionSchema = ArrayDesc("", redimensionAttrs, redimensionDims, 0);
                        LOG4CXX_TRACE(logger, "Schema for redimension " <<  redimensionSchema);
                        aggregateParams.push_back(
                                make_shared<OperatorParamSchema>(grwAsClause->getParsingContext(),
                                                                 redimensionSchema));

                        aggregateOperator = "redimension";
                        break;
                    }
                    default:
                        assert(0);
                }
            }

            BOOST_FOREACH(AstNode *aggCallNode, aggregateFunctions)
            {
                assert(function == aggCallNode->getType());
                LOG4CXX_TRACE(logger, "Translating aggregate into logical aggregate call");
                aggregateParams.push_back(passAggregateCall(aggCallNode, preEvalInputSchemas));
            }

            if (grwAsClause)
            {
                switch(grwAsClause->getType())
                {
                    case groupByClause:
                        BOOST_FOREACH(const AstNode *groupByItem, grwAsClause->getChild(groupByClauseArgList)->getChilds())
                        {
                            switch(groupByItem->getType())
                            {
                                case reference:
                                {
                                    if (groupByItem->getChild(referenceArgTimestamp))
                                    {
                                        throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX,
                                                                   SCIDB_LE_REFERENCE_EXPECTED,
                                                                   groupByItem->getParsingContext());
                                    }

                                    const string &alias = groupByItem->getChild(referenceArgArrayName)
                                            ? groupByItem->getChild(referenceArgArrayName)->asNodeString()->getVal()
                                            : "";
                                    shared_ptr<OperatorParamReference> refParam = make_shared<OperatorParamDimensionReference>(
                                                            groupByItem->getChild(referenceArgObjectName)->getParsingContext(),
                                                            alias,
                                                            groupByItem->getChild(referenceArgObjectName)->asNodeString()->getVal(),
                                                            true);
                                    resolveParamDimensionReference(preEvalInputSchemas, refParam);
                                    aggregateParams.push_back(refParam);
                                    break;
                                }

                                default:
                                    assert(0);
                            }
                        }
                        break;

                    case windowClause:
                    case regridClause:
                    case redimensionClause:
                        break;

                    default:
                        assert(0);
                }
            }

            LOG4CXX_TRACE(logger, "AGGREGATE/REGRID/WINDOW node appended");

            result = appendOperator(result, aggregateOperator, aggregateParams, selectList->getParsingContext());
        }

        const vector<ArrayDesc> aggInputSchemas(1, result->inferTypes(query));

        // Finally pass all post-aggregate evaluations and translate it into APPLY operators
        BOOST_FOREACH(const AstNode *namedExprNode, postAggregationEvals)
        {
            assert(namedExpr == namedExprNode->getType());
            LOG4CXX_TRACE(logger, "Translating postAggregateEval into logical operator APPLY");

            // This is user output. Final attribute name will be used from AS clause (it can be defined
            // by user in query or generated by us above)
            LogicalOperator::Parameters applyParams;
            applyParams.push_back(make_shared<OperatorParamAttributeReference>(
                namedExprNode->getChild(namedExprArgName)->getParsingContext(),
                "", namedExprNode->getChild(namedExprArgName)->asNodeString()->getVal(), false));

            // This is expression which will be used as APPLY expression
            shared_ptr<LogicalExpression> lExpr = AstToLogicalExpression(namedExprNode->getChild(namedExprArgExpr));
            checkLogicalExpression(aggInputSchemas, ArrayDesc(), lExpr);
            shared_ptr<OperatorParam> exprParam = make_shared<OperatorParamLogicalExpression>(
                namedExprNode->getChild(namedExprArgExpr)->getParsingContext(),
                lExpr, TypeLibrary::getType(TID_VOID));

            applyParams.push_back(exprParam);
            result = appendOperator(result, "apply", applyParams, namedExprNode->getParsingContext());
        }

        const vector<ArrayDesc> postEvalInputSchemas(1, result->inferTypes(query));

        if (projectParams.size() > 0)
        {
            BOOST_FOREACH(shared_ptr<OperatorParam> &param, projectParams)
            {
                shared_ptr<OperatorParamReference> &ref = (shared_ptr<OperatorParamReference>&) param;
                resolveParamAttributeReference(postEvalInputSchemas, ref);
            }

            result = appendOperator(result, "project", projectParams, selectList->getParsingContext());
        }
    }

    // Remove temporary ASTs in case exception
    catch (...)
    {
        BOOST_FOREACH(AstNode* ast, preAggregationEvals)
            delete ast;
        BOOST_FOREACH(AstNode* ast, aggregateFunctions)
            delete ast;
        BOOST_FOREACH(AstNode* ast, postAggregationEvals)
            delete ast;
        throw;
    }

    // And when it just not needed
    BOOST_FOREACH(AstNode* ast, preAggregationEvals)
        delete ast;
    BOOST_FOREACH(AstNode* ast, aggregateFunctions)
        delete ast;
    BOOST_FOREACH(AstNode* ast, postAggregationEvals)
        delete ast;

    return result;
}

static string genUniqueObjectName(const string& prefix, unsigned int &initialCounter,
    const vector<ArrayDesc> &inputSchemas, bool internal, const AstNodes& namedExpressions)
{
    string name;

    while(true)
    {
        nextName:

        if (initialCounter == 0)
        {
            name = str(format("%s%s%s")
                % (internal ? "$" : "")
                % prefix
                % (internal ? "$" : "")
                );
            ++initialCounter;
        }
        else
        {
            name = str(format("%s%s_%d%s")
                % (internal ? "$" : "")
                % prefix
                % initialCounter
                % (internal ? "$" : "")
                );
            ++initialCounter;
        }

        BOOST_FOREACH(const ArrayDesc &schema, inputSchemas)
        {
            BOOST_FOREACH(const AttributeDesc& att, schema.getAttributes())
            {
                if (att.getName() == name)
                    goto nextName;
            }

            BOOST_FOREACH(const DimensionDesc dim, schema.getDimensions())
            {
                if (dim.hasNameOrAlias(name, ""))
                    goto nextName;
            }

            BOOST_FOREACH(const AstNode* ast, namedExpressions)
            {
                if (namedExpr == ast->getType()
                    && ast->getChild(namedExprArgName)
                    && ast->getChild(namedExprArgName)->asNodeString()->getVal() == name)
                    goto nextName;
            }
        }

        break;
    }

    return name;
}


} // namespace scidb
