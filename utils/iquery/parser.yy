%{
#include <stdio.h>
#include <string>
#include <vector>
#include <iostream>
#include <stdint.h>
#include <limits>

#include <boost/format.hpp>

#include "iquery/commands.h"
%}

%require "2.3"
%debug
%start start
%defines
%skeleton "lalr1.cc"
%name-prefix="yy"
%define "parser_class_name" "Parser"
%locations
%verbose

%parse-param { class IqueryParser& glue }

%error-verbose

%union {
	int64_t int64Val;
	double realVal;	
    std::string* stringVal;
	class IqueryCmd* node;
	bool boolean;
}

%type <node> start commands set setlang setfetch settimer setverbose help quit knownerror afl_help

%type <int64Val> langtype

%destructor { delete $$; } IDENTIFIER STRING_LITERAL

%token SET LANG AFL AQL NO FETCH VERBOSE TIMER HELP QUIT

%token	EOQ     	0 "end of query"
%token	EOL			"end of line"
%token <stringVal> 	IDENTIFIER	"identifier"
%token <int64Val>	INTEGER "integer"
%token <realVal>	REAL "real"
%token <stringVal>	STRING_LITERAL "string"

%{
#include "iquery/iquery_parser.h"
#include "iquery/scanner.h"
#undef yylex
#define yylex glue._scanner->lex
%}

%%

start:
    commands
    {
        //This is well-parsed iquery command, mark it
        glue._iqueryCommand = true;
        glue._cmd = $1;
    }
    ;

commands:
    set
    | setlang
    | setfetch
    | settimer
    | setverbose
    | help
    | quit
    | afl_help
    | knownerror
    ;
    
knownerror:
    SET error
    {
        // This is syntax error, but it clearly iquery command, so we mark it and later not sending
        // to server as AQL/AFL query showing iquery's error message instead.
        glue._iqueryCommand = true;
        $$ = NULL;
        YYABORT;
    }
    ;

set:
    SET
    {
        $$ = new SimpleIqueryCmd(SimpleIqueryCmd::SET, 1);
    }
    ;

setlang:
    SET LANG langtype
    {
        $$ = new SimpleIqueryCmd(SimpleIqueryCmd::LANG, $3);
    }
    ;

langtype:
    AQL
    {
        $$ = 0;
    }
    | AFL
    {
        $$ = 1;
    }
    ;

setfetch:
    SET FETCH
    {
        $$ = new SimpleIqueryCmd(SimpleIqueryCmd::FETCH, 1);
    }
    | SET NO FETCH
    {
        $$ = new SimpleIqueryCmd(SimpleIqueryCmd::FETCH, 0);
    }
    ;

settimer:
    SET TIMER
    {
        $$ = new SimpleIqueryCmd(SimpleIqueryCmd::TIMER, 1);
    }
    | SET NO TIMER
    {
        $$ = new SimpleIqueryCmd(SimpleIqueryCmd::TIMER, 0);
    }
    ;

setverbose:
    SET VERBOSE
    {
        $$ = new SimpleIqueryCmd(SimpleIqueryCmd::VERBOSE, 1);
    }
    | SET NO VERBOSE
    {
        $$ = new SimpleIqueryCmd(SimpleIqueryCmd::VERBOSE, 0);
    }
    ;

help:
    HELP
    {
        $$ = new SimpleIqueryCmd(SimpleIqueryCmd::HELP, 1);
    }
    ;

quit:
    QUIT
    {
        $$ = new SimpleIqueryCmd(SimpleIqueryCmd::QUIT, 1);
    }
    ;

afl_help:
    HELP '(' error
    {
        //Stop parsing when such combination found in begin to avoid overlap with AFL operator HELP
        glue._iqueryCommand = false;
        $$ = NULL;
        YYABORT;
    }
    ;

%%

void yy::Parser::error(const yy::Parser::location_type& loc,
	const std::string& msg)
{
	glue.error(loc, msg);
}
