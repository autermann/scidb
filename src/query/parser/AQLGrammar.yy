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
    char* stringVal;
    char* keyword;
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
    constant_real null_value function function_argument_list path_expression_list grw_as_clause
    from_list reference_input joined_input case_expr case_arg case_when_clause_list case_when_clause case_default
    beetween_expr reduced_expr named_expr load_statement timestamp_clause array_access
    expr_list update_statement update_list
    update_list_item where_clause common_expr load_library_statement unload_library_statement constant_bool
    drop_array_statement function_argument sort_quirk select_list_item index_clause
    load_save_instance_id save_statement save_format as_format
    array_literal array_literal_schema array_literal_input array_literal_alias anonymous_schema
    format_error_shadow error_shadow shadow
    window_clause window_dimensions_ranges_list window_dimension_range window_range_value
    olap_aggregate window_clause_list fixed_window_clause variable_window_clause regrid_clause regrid_dimensions_list regrid_dimension
    thin_clause thin_dimension thin_dimensions_list named_array_source array_source
    group_by_clause redimension_clause fixed_window_sole_clause variable_window_sole_clause
    rename_array_statement cancel_query_statement order_by_clause order_by_list
    insert_into_statement insert_into_source

%type <boolean> negative_index

%type <keyword> non_reserved_keywords

%destructor { if ($$) delete $$; $$ = NULL; } <node>

%token <keyword> ARRAY AS COMPRESSION CREATE DEFAULT EMPTY FROM NOT NULL_VALUE IMMUTABLE IF THEN ELSE CASE WHEN END
    SELECT WHERE GROUP BY JOIN ON REGRID LOAD INTO INDEX
    VALUES UPDATE SET LIBRARY UNLOAD TRUE FALSE DROP IS RESERVE CROSS WINDOW ASC DESC REDIMENSION ALL DISTINCT
    CURRENT INSTANCE INSTANCES SAVE BETWEEN ERRORS SHADOW
    PARTITION PRECEDING FOLLOWING UNBOUND STEP OVER
    START THIN
    RENAME TO CANCEL QUERY
    VARIABLE FIXED
    ORDER
    INSERT

%token EOQ         0              "end of query"
%token EOL                        "end of line"
%token <stringVal> IDENTIFIER     "identifier"
%token <int64Val>  INTEGER        "integer"
%token <realVal>   REAL           "real"
%token <stringVal> STRING_LITERAL "string"
%token LEXER_ERROR

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
    | rename_array_statement
    | cancel_query_statement
    | insert_into_statement
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
    SELECT select_list into_clause FROM from_list filter_clause grw_as_clause order_by_clause
    {
        $$ = new AstNode(selectStatement, CONTEXT(@1), selectClauseArgCount,
            $2,
            $3,
            $5,
            $6,
            $7,
            $8);
    }
    | SELECT select_list into_clause
    {
        $$ = new AstNode(selectStatement, CONTEXT(@1), selectClauseArgCount,
            $2,
            $3,
            NULL,
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
    group_by_clause
    | window_clause_list
    | redimension_clause
    | regrid_clause
    |
    {
        $$ = NULL;
    }
    ;

group_by_clause:
    GROUP BY path_expression_list
    {
        $$ = new AstNode(groupByClause, CONTEXT(@1), groupByClauseArgCount, $3);
    }
    ;

redimension_clause:
    REDIMENSION BY '[' array_dimension_list ']'
    {
        $$ = new AstNode(redimensionClause, CONTEXT(@1), 1, $4);
    }
    ;

regrid_clause:
    REGRID AS '(' PARTITION BY regrid_dimensions_list ')'
    {
        $$ = new AstNode(regridClause, CONTEXT(@1), regridClauseArgCount, $6);
    }
    ;

regrid_dimensions_list:
    regrid_dimensions_list ',' regrid_dimension
    {
        $1->addChild($3);
        $$ = $1;
    }
    | regrid_dimension
    {
        $$ = new AstNode(regridDimensionsList, CONTEXT(@1), 1, $1);
    }
    ;

regrid_dimension:
    path_expression constant_int64
    {
        $$ = new AstNode(regridDimension, CONTEXT(@$), regridDimensionArgCount, $1, $2);
    }
    | path_expression CURRENT
    {
        $$ = new AstNode(regridDimension, CONTEXT(@$), regridDimensionArgCount, $1,
            new AstNodeInt64(int64Node, CONTEXT(@1), 1));
    }
    ;

window_clause_list:
    window_clause_list ',' window_clause
    {
        $1->addChild($3);
        $$ = $1;
    }
    | window_clause
    {
        $$ = new AstNode(windowClauseList, CONTEXT(@1), 1, $1);
    }
    | fixed_window_sole_clause
    {
        $$ = new AstNode(windowClauseList, CONTEXT(@1), 1, $1);
    }
    | variable_window_sole_clause
    {
        $$ = new AstNode(windowClauseList, CONTEXT(@1), 1, $1);
    }
    ;
    

window_clause:
    fixed_window_clause
    | variable_window_clause
    ;

variable_window_clause:
    VARIABLE WINDOW identifier_clause AS '(' PARTITION BY window_dimension_range ')'
    {
        $$ = new AstNode(windowClause, CONTEXT(@$), windowClauseArgCount,
            $3,
            new AstNode(windowRangesList, CONTEXT(@8), 1, $8),
            new AstNodeBool(boolNode, CONTEXT(@1), true));
    }
    ;

fixed_window_clause:
    fixed_window_noise WINDOW identifier_clause AS '(' PARTITION BY window_dimensions_ranges_list ')'
    {
        $$ = new AstNode(windowClause, CONTEXT(@$), windowClauseArgCount,
            $3,
            $8,
            new AstNodeBool(boolNode, CONTEXT(@1), false));
    }

variable_window_sole_clause:
    VARIABLE WINDOW AS '(' PARTITION BY window_dimension_range ')'
    {
        $$ = new AstNode(windowClause, CONTEXT(@1), windowClauseArgCount,
            new AstNodeString(identifierClause, CONTEXT(@1), ""),
            new AstNode(windowRangesList, CONTEXT(@7), 1, $7),
            new AstNodeBool(boolNode, CONTEXT(@1), true));
    }

fixed_window_sole_clause:
    fixed_window_noise WINDOW AS '(' PARTITION BY window_dimensions_ranges_list ')'
    {
        $$ = new AstNode(windowClause, CONTEXT(@$), windowClauseArgCount,
            new AstNodeString(identifierClause, CONTEXT(@1), ""),
            $7,
            new AstNodeBool(boolNode, CONTEXT(@1), false));
    }
    ;

fixed_window_noise:
    FIXED
    |
    ;

window_dimensions_ranges_list:
    window_dimensions_ranges_list ',' window_dimension_range
    {
        $1->addChild($3);
        $$ = $1;
    }
    | window_dimension_range
    {
        $$ = new AstNode(windowRangesList, CONTEXT(@1), 1, $1);
    }
    ;

window_dimension_range:
    path_expression window_range_value PRECEDING AND window_range_value FOLLOWING
    {
        $$ = new AstNode(windowDimensionRange, CONTEXT(@1), windowDimensionRangeArgCount, $1, $2, $5);
    }
    | path_expression CURRENT
    {
        $$ = new AstNode(windowDimensionRange, CONTEXT(@1), windowDimensionRangeArgCount, $1,
            new AstNodeInt64(int64Node, CONTEXT(@1), 0),
            new AstNodeInt64(int64Node, CONTEXT(@1), 0));
    }
    ;

window_range_value:
    constant_int64
    {
        $$ = $1;
    }
    | UNBOUND
    {
        $$ = new AstNodeInt64(int64Node, CONTEXT(@1), -1);
    }
    ;

order_by_clause:
    ORDER BY order_by_list
    {
        $$ = $3;
    }
    |
    {
        $$ = NULL;
    }
    ;

order_by_list:
    order_by_list ',' path_expression
    {
        $1->addChild($3);
        $$ = $1;
    }
    | path_expression
    {
        $$ = new AstNode(orderByList, CONTEXT(@1), 1, $1);
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

reference_input:
    named_array_source
    | joined_input
    | array_literal
    | thin_clause
    ;


named_array_source:
    array_source
    {
        $$ = new AstNode(namedExpr, CONTEXT(@1), namedExprArgCount,
            $1,
            NULL);
    }
    | array_source AS identifier_clause
    {
        $$ = new AstNode(namedExpr, CONTEXT(@1), namedExprArgCount,
            $1,
            $3);
    }
    | array_source identifier_clause
    {
        $$ = new AstNode(namedExpr, CONTEXT(@1), namedExprArgCount,
            $1,
            $2);
    }
    ;

array_source:
    array_access    
    | path_expression %prec '@'
    | function
    | '(' select_statement ')' %prec UNARY_MINUS
    {
        $$ = $2;
    }

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

//This is alias for BUILD(<schema>, '<data>', true)
array_literal:
    ARRAY '(' array_literal_schema ',' array_literal_input ')' array_literal_alias
    {
        $$ = new AstNode(namedExpr, CONTEXT(@1), namedExprArgCount,
            new AstNode(function, CONTEXT(@1),
                functionArgCount,
                new AstNodeString(identifierClause, CONTEXT(@1), "build"),
                new AstNode(functionArguments, CONTEXT(@3), 3,
                    $3,
                    $5,
                    new AstNodeBool(boolNode, CONTEXT(@1), true)),
                NULL,
                new AstNodeBool(boolNode, CONTEXT(@1), false)
            ),
            $7);
    }
    ;

array_literal_schema:
    path_expression
    | anonymous_schema
    ;

array_literal_input:
    constant_string
    ;

array_literal_alias:
    identifier_clause
    | AS identifier_clause
    {
        $$ = $2;
    }
    |
    {
        $$ = NULL;
    }
    ;

thin_clause:
    THIN array_source BY '(' thin_dimensions_list ')'
    {
        $$ = new AstNode(thinClause, CONTEXT(@1), thinClauseArgCount, $2, $5);
    }
    ;
    
thin_dimensions_list:
    thin_dimensions_list ',' thin_dimension
    {
        $1->addChild($3);
        $$ = $1;
    }
    | thin_dimension
    {
        $$ = new AstNode(thinDimensionsList, CONTEXT(@1), 1, $1);
    }
    ;

thin_dimension:
    path_expression START constant_int64 STEP constant_int64
    {
        $$ = new AstNode(thinDimension, CONTEXT(@1), thinDimensionClauseArgCount,
            $1, $3, $5);
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
    ;

atom:
    path_expression %prec '@'
    | constant
    | function
    | olap_aggregate
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

olap_aggregate:
    function OVER identifier_clause
    {
        $$ = new AstNode(olapAggregate, CONTEXT(@1), olapAggregateArgCount,
            $1,
            $3);
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
    | anonymous_schema
    | '(' select_statement ')'
    {
        $$ = $2;
    }
    ;

anonymous_schema:
    empty_modifier schema
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
    LOAD identifier_clause FROM load_save_instance_id constant_string format_error_shadow
    {
        AstNode* format = NULL;
        AstNode* error = NULL;
        AstNode* shadow = NULL;
        if ($6)
        {
            format = $6->getChild(0)->clone();
            if ($6->getChild(1))
            {
                error = $6->getChild(1)->getChild(0)->clone();
                if ($6->getChild(1)->getChild(1))
                {
                    shadow = $6->getChild(1)->getChild(1)->getChild(0)->clone();
                }
            }
        }

        $$ = new AstNode(loadStatement, CONTEXT(@1), loadStatementArgCount,
            $2,
            $4,
            $5,
            format,
            error,
            shadow);

        delete $6;
    }
    ;

format_error_shadow:
    as_format error_shadow
    {
        $$ = new AstNode(unknownNode, CONTEXT(@1), 2, $1, $2);
    }
    |
    {
        $$ = NULL;
    }
    ;

error_shadow:
    ERRORS constant_int64 shadow
    {
        $$ = new AstNode(unknownNode, CONTEXT(@2), 2, $2, $3);
    }
    |
    {
        $$ = NULL;
    }
    ;

shadow:
    SHADOW ARRAY identifier_clause
    {
        $$ = new AstNode(unknownNode, CONTEXT(@3), 1, $3);
    }
    |
    {
        $$ = NULL;
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

as_format:
    AS constant_string
    {
        $$ = $2;
    }
    ;

save_format:
    as_format
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

rename_array_statement:
    RENAME ARRAY identifier_clause TO identifier_clause
    {
        $$ = new AstNode(renameArrayStatement, CONTEXT(@$), renameArrayStatementArgCount,
            $3,
            $5);
    }
    ;

cancel_query_statement:
    CANCEL QUERY constant_int64
    {
        $$ = new AstNode(cancelQueryStatement, CONTEXT(@$), cancelQueryStatementArgCount,
            $3);
    }
    ;

insert_into_statement:
    INSERT INTO identifier_clause insert_into_source
    {
        $$ = new AstNode(insertIntoStatement, CONTEXT(@$), insertIntoStatementArgCount, $3, $4);
    }
    ;

insert_into_source:
    select_statement
    | constant_string
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
    | ERRORS
    | SHADOW
    | STEP
    | PARTITION
    | PRECEDING
    | FOLLOWING
    | UNBOUND
    | OVER
    | START
    | THIN
    | TO
    | QUERY
    ;

%%

void scidb::AQLParser::error(const AQLParser::location_type& loc,
    const std::string& msg)
{
    glue.error(loc, msg);
}
