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
    std::string* stringVal;
    //std::string* keyword;
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
%left UNARY

%type <node>    start statement create_array_statement immutable_modifier identifier_clause array_attribute
    distinct typename array_attribute_list array_dimension array_dimension_list
    dimension_boundary dimension_boundaries
    function function_argument_list function_argument nullable_modifier empty_modifier
    default default_value compression reserve null_value expression atom constant
    constant_string constant_int64 constant_real constant_NA reference function_alias schema
    constant_bool sort_quirk timestamp_clause index_clause

%type <boolean> negative_index

%destructor { delete $$; $$ = NULL; } IDENTIFIER STRING_LITERAL <node>

%token ARRAY AS COMPRESSION CREATE DEFAULT EMPTY FROM NOT NULL_VALUE IMMUTABLE IF THEN ELSE CASE WHEN END AGG
    PARAMS_START PARAMS_END TRUE FALSE NA IS RESERVE ASC DESC ALL DISTINCT

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
    CREATE immutable_modifier empty_modifier ARRAY identifier_clause schema 
    {
        $$ = new AstNode(createArray, CONTEXT(@1),
            createArrayArgCount,
            $2,
            $3,
            $5,
            $6);
        $$->setComment($5->getComment());
    }
    ;

immutable_modifier:
    IMMUTABLE
    {
        $$ = new AstNodeBool(immutable, CONTEXT(@1), true);
    }
    | dummy
    {
        $$ = new AstNodeBool(immutable, CONTEXT(@1), false);
    }
    ;

empty_modifier:
    NOT EMPTY
    {
        $$ = new AstNodeBool(emptyable, CONTEXT(@1), false);
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
        $$ = new AstNodeString(identifierClause, CONTEXT(@1), *$1);
        $$->setComment(glue._docComment);
        glue._docComment.clear();
        delete $1;
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
    IDENTIFIER ':' typename nullable_modifier default compression reserve
    {
        $$ = new AstNode(attribute, CONTEXT(@1),
            attributeArgCount, 
            new AstNodeString(attributeName, CONTEXT(@1), *$1),
            $3,
            $4,
            $5,
            $6,
            $7);
        $$->setComment(glue._docComment);
        glue._docComment.clear();
        delete $1;
    }
    ;

nullable_modifier:
    NOT NULL_VALUE
    {
        $$ = new AstNodeBool(attributeIsNullable, CONTEXT(@1), false);
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
    | '(' expression ')'
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
    identifier_clause '=' dimension_boundaries ',' INTEGER ',' INTEGER
    {
        if ($5 <= 0 || $5 > std::numeric_limits<uint32_t>::max())
        {
            glue.error(@2, boost::str(boost::format("Chunk size must be between 1 and %d") % std::numeric_limits<uint32_t>::max()));
            delete $1;
            delete $3;
            YYABORT;
        }

        if ($7 < 0 || $7 > std::numeric_limits<uint32_t>::max())
        {
            glue.error(@2, boost::str(boost::format("Overlap length must be between 0 and %d") % std::numeric_limits<uint32_t>::max()));
            delete $1;
            delete $3;
            YYABORT;
        }
    
        $$ = new AstNode(dimension, CONTEXT(@1),
            dimensionArgCount, 
            $1,
            $3,
            new AstNodeInt64(dimensionChunkInterval, CONTEXT(@5), $5),
            new AstNodeInt64(dimensionChunkOverlap, CONTEXT(@7), $7));
        $$->setComment(glue._docComment);
        glue._docComment.clear();
    }
    | identifier_clause
    {
        $$ = new AstNode(dimension, CONTEXT(@1),
            dimensionArgCount, 
            $1,
            new AstNode(dimensionBoundaries, CONTEXT(@1),
                dimensionBoundaryArgCount,
                new AstNodeInt64(dimensionBoundary, CONTEXT(@1), 0),
                new AstNodeInt64(dimensionBoundary, CONTEXT(@1), MAX_COORDINATE)),
            NULL,
            new AstNodeInt64(dimensionChunkOverlap, CONTEXT(@1), 0));

        $$->setComment(glue._docComment);
        glue._docComment.clear();
    }    
    | identifier_clause '(' distinct typename ')' '=' dimension_boundary ',' INTEGER ',' INTEGER
    {
        if ($9 <= 0 || $9 > std::numeric_limits<uint32_t>::max())
        {
            glue.error(@2, boost::str(boost::format("Chunk size must be between 1 and %d") % std::numeric_limits<uint32_t>::max()));
            delete $1;
            delete $3;
            delete $4;
            delete $7;
            YYABORT;
        }

        if ($11 < 0 || $11 > std::numeric_limits<uint32_t>::max())
        {
            glue.error(@2, boost::str(boost::format("Overlap length must be between 0 and %d") % std::numeric_limits<uint32_t>::max()));
            delete $1;
            delete $3;
            delete $4;
            delete $7;
            YYABORT;
        }

        $$ = new AstNode(nonIntegerDimension, CONTEXT(@1),
            nIdimensionArgCount,
            $1,
            $3,
            $4,
            $7,
            new AstNodeInt64(dimensionChunkInterval, CONTEXT(@8), $9),
            new AstNodeInt64(dimensionChunkOverlap, CONTEXT(@10), $11));
    }
    | identifier_clause '(' distinct typename ')'
    {
        $$ = new AstNode(nonIntegerDimension, CONTEXT(@1),
            nIdimensionArgCount,
            $1,
            $3,
            $4,
            new AstNodeInt64(dimensionBoundary, CONTEXT(@1), MAX_COORDINATE),
            NULL,
            new AstNodeInt64(dimensionChunkOverlap, CONTEXT(@1), 0));

        $$->setComment(glue._docComment);
        glue._docComment.clear();
    }
    ;

dimension_boundaries:
    dimension_boundary ':' dimension_boundary
    {
        $$ = new AstNode(dimensionBoundaries, CONTEXT(@1),
            dimensionBoundaryArgCount,
            $1,
            $3);        
    }
    ;

dimension_boundary:
    negative_index INTEGER
    {
        if ($2 <= MIN_COORDINATE || $2 >= MAX_COORDINATE)
        {
            glue.error(@2, "Dimension boundaries must be between -4611686018427387903 and 4611686018427387903");
            YYABORT;
        }
        $$ = new AstNodeInt64(dimensionBoundary, CONTEXT(@1), $1 ? -$2 : $2);
    }
    | '*'
    {
        $$ = new AstNodeInt64(dimensionBoundary, CONTEXT(@1), MAX_COORDINATE);
    }
    ;

negative_index:
    '-'
    {
        $$ = true;
    }
    | '+'
    {
        $$ = true;
    }
    |
    {
        $$ = false;
    }
    ;

distinct:
    ALL 
    {
        $$ = new AstNodeBool(distinct, CONTEXT(@1), false);
    }
    | 
    DISTINCT 
    {
        $$ = new AstNodeBool(distinct, CONTEXT(@1), true);
    }
    | 
    {
        $$ = NULL;
    }    
    ;

//FIXME: need more flexible way
typename:
    IDENTIFIER
    {
        $$ = new AstNodeString(attributeTypeName, CONTEXT(@1), *$1);
        delete $1;
    }
    ;
    
/**
 * This rule for recognizing scalar function as well as array operators. What is it will be 
 * decided on pass stage (Pass.cpp) when getting scalar UDF and operators metadata. 
 */
function:
    IDENTIFIER '(' ')' function_alias
    {
        $$ = new AstNode(function, CONTEXT(@1),
            functionArgCount,
            new AstNodeString(functionName, CONTEXT(@1), *$1),
            new AstNode(functionArguments, CONTEXT(@2), 0),
            $4,
            new AstNodeBool(boolNode, CONTEXT(@1), false));
        delete $1;
    }
    | IDENTIFIER '(' function_argument_list ')' function_alias
    {
        $$ = new AstNode(function, CONTEXT(@1),
            functionArgCount,
            new AstNodeString(functionName, CONTEXT(@1), *$1),
            $3,
            $5,
            new AstNodeBool(boolNode, CONTEXT(@1), false));
        delete $1;
    }
    | IDENTIFIER AS IDENTIFIER
    {
        $$ = new AstNode(function, CONTEXT(@1),
            functionArgCount,
            new AstNodeString(functionName, CONTEXT(@1), "scan"),
            new AstNode(functionArguments, CONTEXT(@1), 1,            
                new AstNode(reference, CONTEXT(@1), referenceArgCount,
                    NULL,
                    new AstNodeString(objectName, CONTEXT(@1), *$1),
                    NULL,
                    NULL,
                    NULL,
                    NULL
                    )
             ),
            new AstNodeString(functionAlias, CONTEXT(@3), *$3),
            new AstNodeBool(boolNode, CONTEXT(@1), false));
        delete $1;
        delete $3;
    }
    // Little aggregate syntax sugar. Now only COUNT using asterisc to count all attributes.
    | IDENTIFIER '(' '*' ')' function_alias
    {
        $$ = new AstNode(function, CONTEXT(@1),
            functionArgCount,
            new AstNodeString(functionName, CONTEXT(@1), *$1),
            new AstNode(functionArguments, CONTEXT(@2), 1,
                new AstNode(asterisk, CONTEXT(@1), 0)),
            $5,
            new AstNodeBool(boolNode, CONTEXT(@1), false));
        delete $1;
    }
    | reference AS identifier_clause
    {
        $$ = new AstNode(function, CONTEXT(@1),
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
    AS IDENTIFIER
    {
        $$ = new AstNodeString(functionAlias, CONTEXT(@2), *$2),
        delete $2;
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
    expression
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
        $$ = new AstNode(anonymousSchema, CONTEXT(@1), anonymousSchemaArgCount, $1, $2);
    }
    ;
    
expression:
    atom
    | '-' expression %prec UNARY
    {
        $$ = makeUnaryScalarOp("-", $2, CONTEXT(@1));
    }
    | expression '+' expression
    {
        $$ = makeBinaryScalarOp("+", $1, $3, CONTEXT(@1));
    }
    | expression '-' expression
    {
        $$ = makeBinaryScalarOp("-", $1, $3, CONTEXT(@1));
    }
    | expression '*' expression
    {
        $$ = makeBinaryScalarOp("*", $1, $3, CONTEXT(@1));
    }
    | expression '^' expression
    {
        $$ = makeBinaryScalarOp("^", $1, $3, CONTEXT(@1));
    }
    | expression '=' expression
    {
        $$ = makeBinaryScalarOp("=", $1, $3, CONTEXT(@1));
    }
    | expression '/' expression
    {
        $$ = makeBinaryScalarOp("/", $1, $3, CONTEXT(@1));
    }
    | expression '%' expression
    {
        $$ = makeBinaryScalarOp("%", $1, $3, CONTEXT(@1));
    }
    | expression '<' expression
    {
        $$ = makeBinaryScalarOp("<", $1, $3, CONTEXT(@1));
    }
    | expression LSEQ expression
    {
        $$ = makeBinaryScalarOp("<=", $1, $3, CONTEXT(@1));
    }
    | expression NEQ expression
    {
        $$ = makeBinaryScalarOp("<>", $1, $3, CONTEXT(@1));
    }
    | expression GTEQ expression
    {
        $$ = makeBinaryScalarOp(">=", $1, $3, CONTEXT(@1));
    }
    | expression '>' expression
    {
        $$ = makeBinaryScalarOp(">", $1, $3, CONTEXT(@1));
    }
    | NOT expression
    {
        $$ = makeUnaryScalarOp("not", $2, CONTEXT(@2));
    }
    | expression AND expression
    {
        $$ = makeBinaryScalarOp("and", $1, $3, CONTEXT(@1));
    }
    | expression OR expression
    {
        $$ = makeBinaryScalarOp("or", $1, $3, CONTEXT(@1));
    }
    | expression IS NULL_VALUE
    {
        $$ = makeUnaryScalarOp("is_null", $1, CONTEXT(@1));
    }
    | expression IS NOT NULL_VALUE
    {
        $$ = makeUnaryScalarOp("not",
                 makeUnaryScalarOp("is_null", $1, CONTEXT(@1)),
                 CONTEXT(@1));
    }
    ;
    


atom:
    constant
    | function
    | reference
    | '(' expression ')'
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
        $$ = new AstNodeString(stringNode, CONTEXT(@1), *$1);
        delete $1;
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
    IDENTIFIER timestamp_clause index_clause sort_quirk
    {
        $$ = new AstNode(reference, CONTEXT(@1), referenceArgCount,
            NULL,
            new AstNodeString(objectName, CONTEXT(@1), *$1),
            $2,
            $4,
            $3
        );
        delete $1;
    }
    | IDENTIFIER '.' IDENTIFIER sort_quirk
    {
        $$ = new AstNode(reference, CONTEXT(@1), referenceArgCount,
            new AstNodeString(arrayName, CONTEXT(@1), *$1),
            new AstNodeString(objectName, CONTEXT(@3), *$3),
            NULL,
            $4,
            NULL
        );
        delete $1;
        delete $3;
    }
    ;

timestamp_clause:
    '@' expression
    {
        $$ = $2;
    }
    | '@' '*'
    {
        $$ = new AstNode(asterisk, CONTEXT(@1), 0);
    }
    |
    {
        $$ = NULL;
    }
    ;

index_clause:
    ':' IDENTIFIER
    {
        $$ = new AstNodeString(stringNode, CONTEXT(@2), *$2);
        delete $2;
    }
    |
    {
        $$ = NULL;
    }
    ;

schema:
    '<' array_attribute_list '>' '[' array_dimension_list ']'
    {
        $$ = new AstNode(schema, CONTEXT(@1), schemaArgCount,
            $2,
            $5
        );
    }

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
 
%%

void scidb::AFLParser::error(const AFLParser::location_type& loc,
    const std::string& msg)
{
    glue.error(loc, msg);
}
