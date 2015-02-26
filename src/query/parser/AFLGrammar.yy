%{
#include <stdio.h>
#include <string>
#include <vector>
#include <iostream>
#include <stdint.h>
#include <limits>

#include <boost/format.hpp>

#include "array/Metadata.h"

#include "system/Config.h"
#include "system/SciDBConfigOptions.h"

#include "query/parser/AST.h"
#include "query/parser/ParsingContext.h"
#include "util/na.h"
%}

%require "2.3"
%debug
%start start
%defines
%skeleton "lalr1.cc"
%name-prefix="scidb"
%define "parser_class_name" "AFLParser"
%locations
%verbose

%parse-param { class QueryParser& glue }

%error-verbose

%union {
    int64_t int64Val;
    double realVal;
    char* stringVal;
    char* keyword;
    class AstNode* node;
    bool boolean;
}

%left '&' '?'
%left OR
%left AND
%right NOT
%left '=' NEQ '>' GTEQ '<' LSEQ
%nonassoc BETWEEN
%left '+' '-'
%left '*' '/' '%'
%right '^'
%left UNARY_MINUS

%type <node>    start statement create_array_statement identifier_clause array_attribute
    typename array_attribute_list array_dimension array_dimension_list
    dimension_boundary_end dimension_boundaries
    function function_argument_list function_argument nullable_modifier empty_modifier
    default default_value compression reserve null_value expr reduced_expr common_expr atom constant
    constant_string constant_int64 constant_real constant_NA reference function_alias schema
    constant_bool sort_quirk timestamp_clause index_clause
    beetween_expr
    case_expr case_arg case_when_clause_list case_when_clause case_default
    asterisk

%type <keyword> non_reserved_keywords

%destructor { delete $$; $$ = NULL; } <node>

%token <keyword> ARRAY AS COMPRESSION CREATE DEFAULT EMPTY FROM NOT NULL_VALUE IF THEN ELSE CASE WHEN END AGG
    PARAMS_START PARAMS_END TRUE FALSE NA IS RESERVE ASC DESC BETWEEN

%token    EOQ         0 "end of query"
%token    EOL            "end of line"
%token <stringVal>     IDENTIFIER    "identifier"
%token <int64Val>    INTEGER "integer"
%token <realVal>    REAL "real"
%token <stringVal>    STRING_LITERAL "string"
%token LEXER_ERROR;

%{
#include "query/parser/QueryParser.h"
#include "query/parser/AFLScanner.h"
#undef yylex
#define yylex glue._aflScanner->lex

// Macroses for easy getting token position
#define BC(tok) tok.begin.column
#define BL(tok) tok.begin.line
#define EC(tok) tok.end.column
#define EL(tok) tok.end.line

#define CONTEXT(tok) boost::shared_ptr<ParsingContext>(new ParsingContext(glue._parsingContext, BL(tok), BC(tok), EL(tok), EC(tok)))
%}

%%

start: statement { $$ = NULL; glue._ast = $1; }

statement:
    create_array_statement
    | function
    ;

create_array_statement:
    CREATE empty_modifier ARRAY identifier_clause schema
    {
        $$ = new AstNode(createArray, CONTEXT(@$),
            createArrayArgCount,
            $2,
            $4,
            $5);
        $$->setComment($4->getComment());
    }
    ;

empty_modifier:
    NOT EMPTY
    {
        $$ = new AstNodeBool(emptyable, CONTEXT(@$), false);
    }
    | EMPTY
    {
        $$ = new AstNodeBool(emptyable, CONTEXT(@1), true);
    }
    | dummy
    {
        $$ = new AstNodeBool(emptyable, CONTEXT(@1), Config::getInstance()->getOption<bool>(CONFIG_ARRAY_EMPTYABLE_BY_DEFAULT));
    }
    ;

identifier_clause:
    IDENTIFIER
    {
        $$ = new AstNodeString(identifierClause, CONTEXT(@1), $1);
        $$->setComment(glue._docComment);
        glue._docComment.clear();
    }
    | non_reserved_keywords
    {
        $$ = new AstNodeString(identifierClause, CONTEXT(@1), $1);
        $$->setComment(glue._docComment);
        glue._docComment.clear();
    }
    ;

array_attribute_list:
    array_attribute_list ',' array_attribute
    {
        $1->addChild($3);
        $$ = $1;
    }
    | array_attribute
    {
        $$ = new AstNode(attributesList, CONTEXT(@1), 1, $1);
    }
    ;

array_attribute:
    identifier_clause ':' typename nullable_modifier default compression reserve
    {
        $$ = new AstNode(attribute, CONTEXT(@$),
            attributeArgCount,
            $1,
            $3,
            $4,
            $5,
            $6,
            $7);
        $$->setComment(glue._docComment);
        glue._docComment.clear();
    }
    ;

nullable_modifier:
    NOT NULL_VALUE
    {
        $$ = new AstNodeBool(attributeIsNullable, CONTEXT(@$), false);
    }
    | NULL_VALUE
    {
        $$ = new AstNodeBool(attributeIsNullable, CONTEXT(@1), true);
    }
    | dummy
    {
        $$ = new AstNodeBool(attributeIsNullable, CONTEXT(@1), false);
    }
    ;

default:
    DEFAULT default_value
    {
        $$ = $2;
    }
    | dummy
    {
        $$ = NULL;
    }
    ;

default_value:
    constant
    | '-' constant
    {
        $$ = makeUnaryScalarOp("-", $2, CONTEXT(@1));
    }
    | function
    | '-' function
    {
        $$ = makeUnaryScalarOp("-", $2, CONTEXT(@1));
    }
    // Wrap complex expression to avoid conflict with '>'
    | '(' expr ')'
    {
       $$ = $2;
    }
    ;

reserve:
    RESERVE constant_int64
    {
        $$ = $2;
    }
    | dummy
    {
        $$ = NULL;
    }


compression:
    COMPRESSION constant_string
    {
        $$ = $2;
    }
    | dummy
    {
        $$ = new AstNodeString(stringNode, CONTEXT(@1), "no compression");
    }
    ;

null_value:
    NULL_VALUE
    {
        $$ = new AstNodeNull(null, CONTEXT(@1));
    }
    ;

constant_bool:
    TRUE
    {
        $$ = new AstNodeBool(boolNode, CONTEXT(@1),true);
    }
    | FALSE
    {
        $$ = new AstNodeBool(boolNode, CONTEXT(@1), false);
    }
    ;

array_dimension_list:
    array_dimension_list ',' array_dimension
    {
        $1->addChild($3);
        $$ = $1;
    }
    | array_dimension
    {
        $$ = new AstNode(dimensionsList, CONTEXT(@1), 1, $1);
    }
    ;

array_dimension:
    identifier_clause '=' dimension_boundaries ',' expr ',' expr
    {
        $$ = new AstNode(dimension, CONTEXT(@$),
            dimensionArgCount,
            $1,
            $3,
            $5,
            $7);
        $$->setComment(glue._docComment);
        glue._docComment.clear();
    }
    | identifier_clause
    {
        $$ = new AstNode(dimension, CONTEXT(@$),
            dimensionArgCount,
            $1,
            new AstNode(dimensionBoundaries, CONTEXT(@$),
                dimensionBoundaryArgCount,
                new AstNodeInt64(int64Node, CONTEXT(@$), 0),
                new AstNode(asterisk, CONTEXT(@$), 0)),
            NULL,
            new AstNodeInt64(int64Node, CONTEXT(@$), 0));

        $$->setComment(glue._docComment);
        glue._docComment.clear();
    }
    | identifier_clause '(' typename ')' '=' dimension_boundary_end ',' expr ',' expr
    {
        $$ = new AstNode(nonIntegerDimension, CONTEXT(@$),
            nIdimensionArgCount,
            $1,
            NULL,
            $3,
            $6,
            $8,
            $10);

        $$->setComment(glue._docComment);
        glue._docComment.clear();
    }
    | identifier_clause '(' typename ')'
    {
        $$ = new AstNode(nonIntegerDimension, CONTEXT(@$),
            nIdimensionArgCount,
            $1,
            NULL,
            $3,
            new AstNode(asterisk, CONTEXT(@$), 0),
            NULL,
            new AstNodeInt64(int64Node, CONTEXT(@$), 0));

        $$->setComment(glue._docComment);
        glue._docComment.clear();
    }
    ;

dimension_boundaries:
    expr ':' dimension_boundary_end
    {
        $$ = new AstNode(dimensionBoundaries, CONTEXT(@$),
            dimensionBoundaryArgCount,
            $1,
            $3);
    }
    ;

dimension_boundary_end:
    expr
    | asterisk
    ;

//FIXME: need more flexible way
typename: identifier_clause;

/**
 * This rule for recognizing scalar function as well as array operators. What is it will be
 * decided on pass stage (Pass.cpp) when getting scalar UDF and operators metadata.
 */
function:
    identifier_clause '(' ')' function_alias
    {
        $$ = new AstNode(function, CONTEXT(@$),
            functionArgCount,
            $1,
            new AstNode(functionArguments, CONTEXT(@2), 0),
            $4,
            new AstNodeBool(boolNode, CONTEXT(@1), false));
    }
    | identifier_clause '(' function_argument_list ')' function_alias
    {
        $$ = new AstNode(function, CONTEXT(@$),
            functionArgCount,
            $1,
            $3,
            $5,
            new AstNodeBool(boolNode, CONTEXT(@1), false));
    }
    | identifier_clause AS identifier_clause
    {
        $$ = new AstNode(function, CONTEXT(@$),
            functionArgCount,
            new AstNodeString(functionName, CONTEXT(@1), "scan"),
            new AstNode(functionArguments, CONTEXT(@1), 1,
                new AstNode(reference, CONTEXT(@1), referenceArgCount,
                    NULL,
                    $1,
                    NULL,
                    NULL,
                    NULL,
                    NULL
                    )
             ),
            $3,
            new AstNodeBool(boolNode, CONTEXT(@1), false));
    }
    // Little aggregate syntax sugar. Now only COUNT using asterisc to count all attributes.
    | identifier_clause '(' asterisk ')' function_alias
    {
        $$ = new AstNode(function, CONTEXT(@$),
            functionArgCount,
            $1,
            new AstNode(functionArguments, CONTEXT(@2), 1,
                $3),
            $5,
            new AstNodeBool(boolNode, CONTEXT(@1), false));
    }
    | reference AS identifier_clause
    {
        $$ = new AstNode(function, CONTEXT(@$),
            functionArgCount,
            new AstNodeString(functionName, CONTEXT(@1), "scan"),
            new AstNode(functionArguments, CONTEXT(@1), 1,
                $1
            ),
            $3,
            new AstNodeBool(boolNode, CONTEXT(@1), false));
    }
    ;

function_alias:
    AS identifier_clause
    {
        $$ = $2;
    }
    |
    {
        $$ = NULL;
    }
    ;

function_argument_list:
    function_argument_list ',' function_argument
    {
        $1->addChild($3);
        $$ = $1;
    }
    | function_argument
    {
        $$ = new AstNode(functionArguments, CONTEXT(@1), 1, $1);
    }
    ;

function_argument:
    expr
    /*
    Serialized schema from coordinator's physical plan for sending it to nodes
    and construct physical operators there. See passAPFunction function in Pass.cpp
    */
    | '[' schema ']'
    {
        $$ = $2;
    }
    | empty_modifier schema
    {
        $$ = new AstNode(anonymousSchema, CONTEXT(@$), anonymousSchemaArgCount, $1, $2);
    }
    ;

expr:
    common_expr
    | beetween_expr
    | '-' expr %prec UNARY_MINUS
    {
        $$ = makeUnaryScalarOp("-", $2, CONTEXT(@1));
    }
    | expr '+' expr
    {
        $$ = makeBinaryScalarOp("+", $1, $3, CONTEXT(@1));
    }
    | expr '-' expr
    {
        $$ = makeBinaryScalarOp("-", $1, $3, CONTEXT(@1));
    }
    | expr '*' expr
    {
        $$ = makeBinaryScalarOp("*", $1, $3, CONTEXT(@1));
    }
    | expr '^' expr
    {
        $$ = makeBinaryScalarOp("^", $1, $3, CONTEXT(@1));
    }
    | expr '=' expr
    {
        $$ = makeBinaryScalarOp("=", $1, $3, CONTEXT(@1));
    }
    | expr '/' expr
    {
        $$ = makeBinaryScalarOp("/", $1, $3, CONTEXT(@1));
    }
    | expr '%' expr
    {
        $$ = makeBinaryScalarOp("%", $1, $3, CONTEXT(@1));
    }
    | expr '<' expr
    {
        $$ = makeBinaryScalarOp("<", $1, $3, CONTEXT(@1));
    }
    | expr LSEQ expr
    {
        $$ = makeBinaryScalarOp("<=", $1, $3, CONTEXT(@1));
    }
    | expr NEQ expr
    {
        $$ = makeBinaryScalarOp("<>", $1, $3, CONTEXT(@1));
    }
    | expr GTEQ expr
    {
        $$ = makeBinaryScalarOp(">=", $1, $3, CONTEXT(@1));
    }
    | expr '>' expr
    {
        $$ = makeBinaryScalarOp(">", $1, $3, CONTEXT(@1));
    }
    | NOT expr
    {
        $$ = makeUnaryScalarOp("not", $2, CONTEXT(@2));
    }
    | expr AND expr
    {
        $$ = makeBinaryScalarOp("and", $1, $3, CONTEXT(@1));
    }
    | expr OR expr
    {
        $$ = makeBinaryScalarOp("or", $1, $3, CONTEXT(@1));
    }
    | expr IS NULL_VALUE
    {
        $$ = makeUnaryScalarOp("is_null", $1, CONTEXT(@1));
    }
    | expr IS NOT NULL_VALUE
    {
        $$ = makeUnaryScalarOp("not",
                 makeUnaryScalarOp("is_null", $1, CONTEXT(@1)),
                 CONTEXT(@1));
    }
    ;

//Expression rule without boolean ops for using where it causing problems, for example
//reduce/reduce conflict in BETWEEN
//
// NOTE: If you changing this rules, don't forget update expr!
//
reduced_expr:
    common_expr
    | '-' reduced_expr %prec UNARY_MINUS
    {
        $$ = makeUnaryScalarOp("-", $2, CONTEXT(@1));
    }
    | reduced_expr '+' reduced_expr
    {
        $$ = makeBinaryScalarOp("+", $1, $3, CONTEXT(@1));
    }
    | reduced_expr '-' reduced_expr
    {
        $$ = makeBinaryScalarOp("-", $1, $3, CONTEXT(@1));
    }
    | reduced_expr '*' reduced_expr
    {
        $$ = makeBinaryScalarOp("*", $1, $3, CONTEXT(@1));
    }
    | reduced_expr '^' reduced_expr
    {
        $$ = makeBinaryScalarOp("^", $1, $3, CONTEXT(@1));
    }
    | reduced_expr '=' reduced_expr
    {
        $$ = makeBinaryScalarOp("=", $1, $3, CONTEXT(@1));
    }
    | reduced_expr '/' reduced_expr
    {
        $$ = makeBinaryScalarOp("/", $1, $3, CONTEXT(@1));
    }
    | reduced_expr '%' reduced_expr
    {
        $$ = makeBinaryScalarOp("%", $1, $3, CONTEXT(@1));
    }
    | reduced_expr '<' reduced_expr
    {
        $$ = makeBinaryScalarOp("<", $1, $3, CONTEXT(@1));
    }
    | reduced_expr LSEQ reduced_expr
    {
        $$ = makeBinaryScalarOp("<=", $1, $3, CONTEXT(@1));
    }
    | reduced_expr NEQ reduced_expr
    {
        $$ = makeBinaryScalarOp("<>", $1, $3, CONTEXT(@1));
    }
    | reduced_expr GTEQ reduced_expr
    {
        $$ = makeBinaryScalarOp(">=", $1, $3, CONTEXT(@1));
    }
    | reduced_expr '>' reduced_expr
    {
        $$ = makeBinaryScalarOp(">", $1, $3, CONTEXT(@1));
    }
    | reduced_expr IS NULL_VALUE
    {
        $$ = makeUnaryScalarOp("is_null", $1, CONTEXT(@1));
    }
    | reduced_expr IS NOT NULL_VALUE
    {
        $$ = makeUnaryScalarOp("not",
                 makeUnaryScalarOp("is_null", $1, CONTEXT(@1)),
                 CONTEXT(@1));
    }
    ;

// Common part for expr and reduced_expr.
common_expr:
    atom
    ;

atom:
    constant
    | function
    | reference
    | case_expr
    | '(' expr ')'
    {
        $$ = $2;
    }
    ;

constant:
    constant_int64
    | constant_real
    | constant_NA
    | constant_string
    | null_value
    | constant_bool
    ;

constant_string:
    STRING_LITERAL
    {
        $$ = new AstNodeString(stringNode, CONTEXT(@1), $1);
    }
    ;

constant_int64:
    INTEGER
    {
        $$ = new AstNodeInt64(int64Node, CONTEXT(@1), $1);
    }
    ;

constant_real:
    REAL
    {
        $$ = new AstNodeReal(realNode, CONTEXT(@1), $1);
    }
    ;

constant_NA:
    NA
    {
        $$ = new AstNodeReal(realNode, CONTEXT(@1), NA::NAInfo<double>::value());
    }
    ;

//General rule for implicit scans, arrays, attributes and dimensions. We don't know exactly what it is
//and don't know what operator required in this position, so decision will be made on pass stage.
reference:
    identifier_clause timestamp_clause index_clause sort_quirk
    {
        $$ = new AstNode(reference, CONTEXT(@$), referenceArgCount,
            NULL,
            $1,
            $2,
            $4,
            $3
        );
    }
    | identifier_clause '.' identifier_clause sort_quirk
    {
        $$ = new AstNode(reference, CONTEXT(@$), referenceArgCount,
            $1,
            $3,
            NULL,
            $4,
            NULL
        );
    }
    ;

timestamp_clause:
    '@' expr
    {
        $$ = $2;
    }
    | '@' asterisk
    {
        $$ = $2;
    }
    |
    {
        $$ = NULL;
    }
    ;

index_clause:
    ':' identifier_clause
    {
        $$ = $2;
    }
    |
    {
        $$ = NULL;
    }
    ;

schema:
    '<' array_attribute_list '>' '[' array_dimension_list ']'
    {
        $$ = new AstNode(schema, CONTEXT(@$), schemaArgCount,
            $2,
            $5
        );
    }

case_expr:
    CASE case_arg case_when_clause_list case_default END
    {
        //Here we rewriting IIFs list to expression which emulate CASE
        //Three main rewrites:
        //1. If we have case_arg then we should add compare operator to IIF's first function_argument
        //2. Each next IIF going to previous IIF's third argument
        //3. If we have case_default then we should add it to last IIF's third argument
        AstNode* iifNode = NULL;
        AstNode* currentIif = NULL;
        AstNode* lastIif = NULL;
        bool first = true;
        for(size_t i = 0, n = $3->getChildsCount(); i < n; ++i)
        {
            currentIif = $3->getChild(i);
            $3->setChild(i, NULL);

            if (!iifNode)
            {
                iifNode = currentIif;
            }

            if ($2)
            {
                //Updating first parameter of IIF
                currentIif->getChild(functionArgParameters)->setChild(0,
                    makeBinaryScalarOp(
                        "=", first ? $2 : $2->clone(), //We should avoid using same nodes in different branches, so clone it
                        currentIif->getChild(functionArgParameters)->getChild(0),
                        currentIif->getChild(functionArgParameters)->getParsingContext()));
            }
            else
            {
                if (function != currentIif->getChild(functionArgParameters)->getChild(0)->getType())
                {
                    glue.error(
                        currentIif->getChild(functionArgParameters)->getChild(0)->getParsingContext(),
                        "Function or scalar operator expected");
                    delete currentIif;
                    delete $2;
                    delete $3;
                    delete $4;
                    YYABORT;
                }
            }

            if(lastIif)
            {
                //Filling third parameter of IIF
                lastIif->getChild(functionArgParameters)->setChild(2, currentIif);
            }
            lastIif = currentIif;
            first = false;
        }

        //Filling third parameter of IIF for ELSE clause
        lastIif->getChild(functionArgParameters)->setChild(2,
            $4 ? $4 : new AstNodeNull(null, CONTEXT(@4)));

        delete $3;
        $$ = iifNode;
    }
    ;

case_arg:
    expr
    |
    {
        $$ = NULL;
    }

case_when_clause_list:
    case_when_clause
    {
        $$ = new AstNode(caseWhenClauseList, CONTEXT(@1), 1, $1);
    }
    | case_when_clause_list case_when_clause
    {
        $1->addChild($2);
        $$ = $1;
    }
    ;

case_when_clause:
    WHEN expr THEN expr
    {
        $$ = new AstNode(function, CONTEXT(@$),
                functionArgCount,
                new AstNodeString(identifierClause, CONTEXT(@$), "iif"),
                new AstNode(functionArguments, CONTEXT(@$), 3,
                    $2, //will be updated in case_expr
                    $4,
                    NULL //will be filled in case_expr
                ),
                NULL,
                new AstNodeBool(boolNode, CONTEXT(@$), false)
            );
    }
    ;

case_default:
    ELSE expr
    {
        $$ = $2;
    }
    |
    {
        $$ = NULL;
    }
    ;

beetween_expr:
    expr BETWEEN reduced_expr AND reduced_expr %prec BETWEEN
    {
        $$ = makeBinaryScalarOp("and",
                makeBinaryScalarOp(">=", $1, $3, CONTEXT(@$)),
                makeBinaryScalarOp("<=", $1->clone(), $5, CONTEXT(@$)),
                CONTEXT(@$)
                );
    }
    ;

// Dummy rule for getting approximately position of optional token (e.g. NOT NULL,
// UPDATABLE, NOT EMPTY)
dummy:
    {
    }
    ;

sort_quirk:
    ASC
    {
        $$ = new AstNodeInt64(int64Node, CONTEXT(@1), SORT_ASC);
    }
    | DESC
    {
        $$ = new AstNodeInt64(int64Node, CONTEXT(@1), SORT_DESC);
    }
    |
    {
        $$ = NULL;
    }
    ;

asterisk:
    '*'
    {
        $$ = new AstNode(asterisk, CONTEXT(@1), 0);
    }
    ;

non_reserved_keywords:
      ARRAY
    | AS
    | ASC
    | BETWEEN
    | COMPRESSION
    | CREATE
    | DESC
    | DEFAULT
    | END
    | IS
    | RESERVE
    ;

%%

void scidb::AFLParser::error(const AFLParser::location_type& loc,
    const std::string& msg)
{
    glue.error(loc, msg);
}
