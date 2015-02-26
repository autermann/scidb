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
%}

%require "2.3"
%debug
%start start
%defines
%skeleton "lalr1.cc"
%name-prefix="scidb"
%define "parser_class_name" "AQLParser"
%locations
%verbose

%parse-param { class QueryParser& glue }

%error-verbose

%union {
    int64_t int64Val;
    double realVal;    
    std::string* stringVal;
    std::string* keyword;
    //std::string* keyword;
    class AstNode* node;
    bool boolean;
}

%left '@' //for array referencing by timestamp (overall expression after '@' reduced in favor version timestamp, to prevent it parentheses must be used)
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

%type <node> start statement create_array_statement immutable_modifier array_attribute
    distinct typename array_attribute_list array_dimension array_dimension_list
    dimension_boundary dimension_boundaries nullable_modifier empty_modifier
    default default_value compression reserve schema 
    select_statement select_list into_clause path_expression
    identifier_clause expr filter_clause atom constant constant_string constant_int64
    constant_real null_value function function_argument_list path_expression_list grw_as_clause as_clause
    from_list reference_input joined_input case_expr case_arg case_when_clause_list case_when_clause case_default
    beetween_expr reduced_expr named_expr load_statement timestamp_clause array_access
    expr_list update_statement update_list
    update_list_item where_clause common_expr load_library_statement unload_library_statement constant_bool
    drop_array_statement function_argument int64_list sort_quirk select_list_item index_clause
    load_save_instance_id save_statement save_format

%type <boolean> negative_index

%type <keyword> non_reserved_keywords

%destructor { delete $$; $$ = NULL; } IDENTIFIER STRING_LITERAL <node>

%token <keyword> ARRAY AS COMPRESSION CREATE DEFAULT EMPTY FROM NOT NULL_VALUE IMMUTABLE IF THEN ELSE CASE WHEN END
    SELECT WHERE GROUP BY JOIN ON REGRID LOAD INTO INDEX
    VALUES UPDATE SET LIBRARY UNLOAD TRUE FALSE DROP IS RESERVE CROSS WINDOW ASC DESC REDIMENSION ALL DISTINCT
    CURRENT INSTANCE INSTANCES SAVE BETWEEN

%token EOQ         0              "end of query"
%token EOL                        "end of line"
%token <stringVal> IDENTIFIER     "identifier"
%token <int64Val>  INTEGER        "integer"
%token <realVal>   REAL           "real"
%token <stringVal> STRING_LITERAL "string"

%{
#include "query/parser/QueryParser.h"
#include "query/parser/AQLScanner.h"
#undef yylex
#define yylex glue._aqlScanner->lex

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
    | select_statement
    | load_statement
    | save_statement
    | update_statement
    | drop_array_statement
    | load_library_statement
    | unload_library_statement
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
        $$ = new AstNode(attribute, CONTEXT(@1),
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
            glue.error2(@2, boost::str(boost::format("Chunk size must be between 1 and %d") % std::numeric_limits<uint32_t>::max()));
        }

        if ($7 < 0 || $7 > std::numeric_limits<uint32_t>::max())
        {
            glue.error2(@2, boost::str(boost::format("Overlap length must be between 0 and %d") % std::numeric_limits<uint32_t>::max()));
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
            glue.error2(@2, boost::str(boost::format("Chunk size must be between 1 and %d") % std::numeric_limits<uint32_t>::max()));
        }

        if ($11 < 0 || $11 > std::numeric_limits<uint32_t>::max())
        {
            glue.error2(@2, boost::str(boost::format("Overlap length must be between 0 and %d") % std::numeric_limits<uint32_t>::max()));
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
            glue.error2(@2, "Dimension boundaries must be between -4611686018427387903 and 4611686018427387903");
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
    identifier_clause
    {
        $$ = $1;
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
    ;

// Dummy rule for getting approximately position of optional token (e.g. NOT NULL, 
// UPDATABLE, NOT EMPTY)
dummy:
    {
    }
    ;

select_statement:
    SELECT select_list into_clause FROM from_list filter_clause grw_as_clause
    {
        $$ = new AstNode(selectStatement, CONTEXT(@1), selectClauseArgCount,
            $2,
            $3,
            $5,
            $6,
            $7);
    }
    | SELECT select_list into_clause
    {
        $$ = new AstNode(selectStatement, CONTEXT(@1), selectClauseArgCount,
            $2,
            $3,
            NULL,
            NULL,
            NULL);
    }
    ;

select_list:
    select_list ',' select_list_item
    {
        $1->addChild($3);
        $$ = $1;
    }
    | select_list_item
    {
        $$ = new AstNode(selectList, CONTEXT(@1), 1, $1);
    }
    ;
    
select_list_item:
    named_expr
    | '*'
    {
        $$ = new AstNode(asterisk, CONTEXT(@1), 0);
    }
    ;

into_clause:
    INTO identifier_clause
    {
        $$ = $2;
    }
    |
    {
        $$ = NULL;
    }

from_list:
    reference_input
    {    
        $$ = new AstNode(fromList, CONTEXT(@1), 1, $1);
    }
    | from_list ',' reference_input
    {
        $1->addChild($3);
        $$ = $1;    
    }
    ;

filter_clause:
    WHERE expr
    {
        $$ = $2;        
    }
    |
    {
        $$ = NULL;
    }
    ;

grw_as_clause:
    GROUP BY path_expression_list as_clause
    {
        $$ = new AstNode(groupByClause, CONTEXT(@1), groupByClauseArgCount, $3, $4);
    }
    | REGRID int64_list as_clause
    {
        $$ = new AstNode(regridClause, CONTEXT(@1), regridClauseArgCount, $2, $3);
    }
    | WINDOW int64_list as_clause
    {
        $$ = new AstNode(windowClause, CONTEXT(@1), windowClauseArgCount, $2, $3);
    }
    | REDIMENSION BY '[' array_dimension_list ']'
    {
        $$ = new AstNode(redimensionClause, CONTEXT(@1), 1, $4);
    }
    |
    {
        $$ = NULL;
    }
    ;

as_clause:
    AS identifier_clause
    {
        $$ = $2;
    }
    |
    {
        $$ = NULL;
    }
    ;

int64_list:
    int64_list ',' constant_int64
    {
        $1->addChild($3);
        $$ = $1;
    }
    | constant_int64
    {
        $$ = new AstNode(int64List, CONTEXT(@1), 1, $1);
    }
    ;

named_expr:
    expr
    {
        $$ = new AstNode(namedExpr, CONTEXT(@1), namedExprArgCount,
            $1,
            NULL);
    }
    | expr AS identifier_clause
    {
        $$ = new AstNode(namedExpr, CONTEXT(@1), namedExprArgCount,
            $1,
            $3);
    }
    | expr identifier_clause
    {
        $$ = new AstNode(namedExpr, CONTEXT(@1), namedExprArgCount,
            $1,
            $2);
    }
    ;

path_expression:
    identifier_clause timestamp_clause index_clause sort_quirk
    {
        $$ = new AstNode(reference, CONTEXT(@1), referenceArgCount,
            NULL,
            $1,
            $2,
            $4,
            $3
        );
    }
    | identifier_clause '.' identifier_clause sort_quirk
    {
        $$ = new AstNode(reference, CONTEXT(@1), referenceArgCount,
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
    ':' identifier_clause
    {
        $$ = $2;
    }
    |
    {
        $$ = NULL;
    }
    ;

path_expression_list:
    path_expression_list ',' path_expression
    {
        $1->addChild($3);
        $$ = $1;
    }
    | path_expression
    {
        $$ = new AstNode(pathExpressionList, CONTEXT(@1), 1, $1);
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
    | non_reserved_keywords
    {
        $$ = new AstNodeString(identifierClause, CONTEXT(@1), *$1);
        $$->setComment(glue._docComment);
        glue._docComment.clear();
        delete $1;
    }
    ;

reference_input:
    named_expr
    | joined_input
    ;

joined_input:
    reference_input JOIN reference_input ON expr
    {
        $$ = new AstNode(joinClause, CONTEXT(@1),
            joinClauseArgCount,
            $1,
            $3,
            $5);
    }
    | reference_input CROSS JOIN reference_input
    {
        $$ = new AstNode(joinClause, CONTEXT(@1),
            joinClauseArgCount,
            $1,
            $4,
            NULL);
    }
    ;

//Full expression rule.
//
// NOTE: If you changing this rules, don't forget update reduced_expr!
//
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

// Using common_expr instead expr or reduced_expr for eliminating shift-reduce conflict. Also using
// expr here is quite useless so we do not lose anything. 
array_access:
    common_expr '[' expr_list ']'
    {
        $$ = new AstNode(sequencionalArrayAccess, CONTEXT(@1), sequencionalArrayAccessArgCount, $1, $3);
    }
    | common_expr '{' expr_list '}'
    {
        $$ = new AstNode(numberedArrayAccess, CONTEXT(@1), numberedArrayAccessArgCount, $1, $3);
    }
    ;

expr_list:
    expr_list ',' expr
    {
        $1->addChild($3);
        $$ = $1;
    }
    | expr
    {
        $$ = new AstNode(exprList, CONTEXT(@1), 1, $1);
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
    | array_access    
    //FIXME: Additional parentheses looks stupid in aggregates, but this is only way to remove shift/reduce
    //FIXME: conflicts. But may be handle select clause inside functions other way? 
    | '(' select_statement ')' %prec UNARY_MINUS
    {
        $$ = $2;
    }
    ;

atom:
    path_expression %prec '@'
    | constant
    | function
    | case_expr
    | '(' expr ')'
    {
        $$ = $2;
    }
    ;    

constant:
    constant_int64
    | constant_real
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

function:
    identifier_clause '(' ')'
    {
        $$ = new AstNode(function, CONTEXT(@1),
            functionArgCount,
            $1,
            new AstNode(functionArguments, CONTEXT(@2), 0),
            NULL,
            new AstNodeBool(boolNode, CONTEXT(@1), false));
    }
    | identifier_clause '(' function_argument_list ')'
    {
        $$ = new AstNode(function, CONTEXT(@1),
            functionArgCount,
            $1,
            $3,
            NULL,
            new AstNodeBool(boolNode, CONTEXT(@1), false));
    }
    | identifier_clause '(' '*' ')'
    {
        $$ = new AstNode(function, CONTEXT(@1),
            functionArgCount,
            $1,
            new AstNode(functionArguments, CONTEXT(@2), 1,
                new AstNode(asterisk, CONTEXT(@1), 0)),
            NULL,
            new AstNodeBool(boolNode, CONTEXT(@1), false));
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
    | select_statement
    | empty_modifier schema
    {
        $$ = new AstNode(anonymousSchema, CONTEXT(@1), anonymousSchemaArgCount, $1, $2);
    }
    ;

case_expr:
    CASE case_arg case_when_clause_list case_default END
    {
        $$ = new AstNode(caseClause, CONTEXT(@1), caseClauseArgCount, $2, $3, $4);
    }
    ;

case_arg:
    expr
    {
        $$ = $1;
    }
    |
    {
        $$ = NULL;
    }
    ;

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
        $$ = new AstNode(caseWhenClause, CONTEXT(@1), caseWhenClauseArgCount, $2, $4);
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
                makeBinaryScalarOp(">=", $1, $3, CONTEXT(@1)),
                makeBinaryScalarOp("<=", $1->clone(), $5, CONTEXT(@1)),
                CONTEXT(@1)
                );
    }
    ;

load_statement:
    LOAD identifier_clause FROM load_save_instance_id constant_string
    {
        $$ = new AstNode(loadStatement, CONTEXT(@1), loadStatementArgCount,
            $2,
            $4,
            $5);
    }
    ;

save_statement:
    SAVE path_expression INTO load_save_instance_id constant_string save_format
    {
        $$ = new AstNode(saveStatement, CONTEXT(@1), saveStatementArgCount,
            $2,
            $4,
            $5,
            $6);
    }
    ;

save_format:
    AS constant_string
    {
        $$ = $2;
    }
    |
    {
        $$ = NULL;
    }
    ;

load_save_instance_id:
    constant_int64
    {
        $$ = $1;
    }
    | INSTANCE constant_int64
    {
        $$ = $2;
    }
    | CURRENT INSTANCE
    {
        $$ = new AstNodeInt64(int64Node, CONTEXT(@1), -2);
    }
    | ALL INSTANCES
    {
        $$ = new AstNodeInt64(int64Node, CONTEXT(@1), -1);
    }
    | dummy
    {
        $$ = new AstNodeInt64(int64Node, CONTEXT(@1), -2);   
    }
    ;

update_statement:
    UPDATE path_expression SET update_list where_clause  
    {
        $$ = new AstNode(updateStatement, CONTEXT(@1), updateStatementArgCount,
            $2,
            $4,
            $5);
    }
    ;
    
update_list:
    update_list ',' update_list_item
    {
        $1->addChild($3);
        $$ = $1;    
    }
    | update_list_item
    {
        $$ = new AstNode(updateList, CONTEXT(@1), 1, $1);
    }
    ;
    
update_list_item:
    identifier_clause '=' expr
    {
        $$ = new AstNode(updateListItem, CONTEXT(@1), updateListItemArgCount,
            $1,
            $3
            );
    }
    ;

drop_array_statement:
    DROP ARRAY identifier_clause
    {
        $$ = new AstNode(dropArrayStatement, CONTEXT(@1), dropArrayStatementArgCount,
            $3
            );        
    }
    ;

where_clause:
    WHERE expr
    {
        $$ = $2;
    }
    | 
    {
        $$ = NULL;
    }
    ;

load_library_statement:
    LOAD LIBRARY constant_string
    {
        $$ = new AstNode(loadLibraryStatement, CONTEXT(@1), loadLibraryStatementArgCount,
            $3
            );
    }
    ;

unload_library_statement:
    UNLOAD LIBRARY constant_string
    {
        $$ = new AstNode(unloadLibraryStatement, CONTEXT(@1), unloadLibraryStatementArgCount,
            $3
            );
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

non_reserved_keywords:
    ALL
    | ARRAY
    | AS
    | ASC
    | BETWEEN
    | BY
    | COMPRESSION
    | CREATE
    | CURRENT
    | DESC
    | DEFAULT
    | DISTINCT
    | DROP
    | END
    | IMMUTABLE
    | INSTANCE
    | INSTANCES
    | IS
    | LIBRARY
    | LOAD
    | RESERVE
    | SAVE
    | VALUES
    ;

%%

void scidb::AQLParser::error(const AQLParser::location_type& loc,
    const std::string& msg)
{
    glue.error(loc, msg);
}
