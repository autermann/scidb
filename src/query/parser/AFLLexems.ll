/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2013 SciDB, Inc.
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

%{
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/format.hpp>
#include "query/parser/AFLScanner.h"
#include "query/parser/ALKeywords.h"
#include "query/parser/ParsingContext.h"
#include "query/parser/QueryParser.h"

typedef scidb::AFLParser::token token;
%}

%option c++
%option prefix="zz"
%option batch
%option debug
%option yywrap nounput
%option stack

%{
#define YY_USER_ACTION     yylloc->columns(yyleng);
#define YY_USER_INIT \
    yylloc->begin.column = yylloc->begin.line = 1; \
    yylloc->end.column   = yylloc->end.line   = 1;
#define yyterminate()      return token::EOQ
#define YY_DECL                             \
    short scidb::AFLScanner::lex(           \
        AFLParser::semantic_type* yylval,   \
        AFLParser::location_type* yylloc)
%}

Space                   [ \t\r\f]
NewLine                 [\n]
NonNewLine              [^\n]
OneLineComment          ("--"{NonNewLine}*)
Whitespace              ({Space}+|{OneLineComment})
IdentifierFirstChar     [A-Za-z_\$]
IdentifierOtherChars    [A-Za-z0-9_\$]
Identifier              {IdentifierFirstChar}{IdentifierOtherChars}*
QuotedIdentifier        \"{IdentifierFirstChar}{IdentifierOtherChars}*\"
Digit                   [0-9]
Integer                 {Digit}+
Decimal                 (({Digit}*\.{Digit}+)|({Digit}+\.{Digit}*))
Real                    ({Integer}|{Decimal})[Ee][-+]?{Digit}+
Other                   .

%%

%{
    yylloc->step();
%}

"<="        {return token::LSEQ;}
"<>"        {return token::NEQ;}
"!="        {return token::NEQ;}
">="        {return token::GTEQ;}

{OneLineComment} {
    if (strncmp(yytext, "---", 3) == 0)
    {
        _glue.setComment(yytext+3);
    }
}

{Integer} {
    try
    {
        yylval->int64Val = boost::lexical_cast<int64_t>(yytext);
    }
    catch(boost::bad_lexical_cast &e)
    {
        _glue.error(*yylloc, boost::str(boost::format("Can not interpret '%s' as int64 value. Value too big.") % yytext));
        return token::LEXER_ERROR;
    }
    return token::INTEGER;
}

"inf" |
"nan" |
"NaN" |
{Real} {
    try
    {
        yylval->realVal = boost::lexical_cast<double>(yytext);
    }
    catch(boost::bad_lexical_cast &e)
    {
        _glue.error(*yylloc, boost::str(boost::format("Can not interpret '%s' as real value. Value too big.") % yytext));
        return token::LEXER_ERROR;
    }

    return token::REAL;
}

{Decimal} {
    try
    {
        yylval->realVal = boost::lexical_cast<double>(yytext);
    }
    catch(boost::bad_lexical_cast &e)
    {
        _glue.error(*yylloc, boost::str(boost::format("Can not interpret '%s' as decimal value. Value too big.") % yytext));
        return token::LEXER_ERROR;
    }

    return token::REAL;
}

L?'(\\.|[^'])*' {
    std::string  s(yytext, 1, yyleng - 2);
    boost::replace_all(s,"\\'","'"); //FIXME: Ugly unescaping.

    yylval->stringVal = copyLexeme(s.size(),&s[0]);
    return token::STRING_LITERAL;
}

{Identifier} {
    if (const ALKeyword* kw = getAFLKeyword(yytext))
    {
        yylval->keyword = kw->name; // NB: keyword names are statically allocated
        return kw->token;
    }
    else
    {
        yylval->stringVal = copyLexeme(yyleng,yytext);
        return token::IDENTIFIER;
    }
}

{QuotedIdentifier} {
    yylval->stringVal = copyLexeme(yyleng-2,yytext+1);
    return token::IDENTIFIER;
}

{Whitespace} {
    yylloc->step();
}

{NewLine} {
    yylloc->lines(yyleng);
    yylloc->end.column = yylloc->begin.column = 1;
    yylloc->step();
}

{Other}     {return yytext[0];}

%%

scidb::AFLScanner::AFLScanner(QueryParser& glue,std::istream* in,std::ostream* out)
    : AFLBaseFlexLexer(in, out),
      _glue(glue),
      _arena("AFLScanner")
{}

void scidb::AFLScanner::set_debug(bool b)
{
    yy_flex_debug = b;
}

char* scidb::AFLScanner::copyLexeme(size_t n,const char* p)
{
    assert(p!=0 && yyleng>=0 && n<=size_t(yyleng));

    void* q = _arena.calloc(n + 1);
    memcpy(q,p,n);
    return static_cast<char*>(q);
}

int AFLBaseFlexLexer::yylex()
{
    std::cerr << "in QueryFlexLexer::yylex() !" << std::endl;
    return 0;
}

int AFLBaseFlexLexer::yywrap()
{
    return 1;
}
