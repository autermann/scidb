%{
#include <string>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/make_shared.hpp>
#include <boost/format.hpp>

#include "system/Exceptions.h"

#include "query/parser/AFLScanner.h"
#include "query/parser/AFLKeywords.h"
#include "query/parser/ParsingContext.h"
#include "query/parser/QueryParser.h"

typedef scidb::AFLParser::token token;
typedef scidb::AFLParser::token_type token_type;
#define yyterminate() return token::EOQ

#define    YY_DECL                                    \
    scidb::AFLParser::token_type                \
    scidb::AFLScanner::lex(                        \
        scidb::AFLParser::semantic_type* yylval,\
        scidb::AFLParser::location_type* yylloc    \
    )

%}

%option c++
%option prefix="zz"
%option batch
%option debug
%option yywrap nounput
%option stack

%{
#define YY_USER_ACTION  yylloc->columns(yyleng);

#define YY_USER_INIT \
        yylloc->begin.column = yylloc->begin.line = 1; \
        yylloc->end.column = yylloc->end.line = 1;
%}

Space [ \t\r\f]

NewLine [\n]

NonNewLine [^\n]

OneLineComment ("--"{NonNewLine}*)

Whitespace ({Space}+|{OneLineComment})


IdentifierFirstChar [A-Za-z_\$]
IdentifierOtherChars [A-Za-z0-9_\$]
Identifier {IdentifierFirstChar}{IdentifierOtherChars}*

Digit [0-9]
Integer    {Digit}+
Decimal            (({Digit}*\.{Digit}+)|({Digit}+\.{Digit}*))
Real            ({Integer}|{Decimal})[Ee][-+]?{Digit}+

Other .

%%

%{
    yylloc->step();
%}

"<=" {
    return token::LSEQ;
}

"<>" {
    return token::NEQ;
}

"!=" {
    return token::NEQ;
}

">=" {
    return token::GTEQ;
}

(?i:not) {
    return token::NOT;
}

(?i:and) {
    return token::AND;
}

(?i:or) {
    return token::OR;
}


{OneLineComment} {
    if (strncmp(yytext, "---", 3) == 0)  {
        _glue.setComment(yytext+3);
    }
}

{Identifier} {
    const AFLKeyword *kw = FindAFLKeyword(yytext);
    if (kw)
    {
        //yylval->keyword = new std::string(kw->name);
        //std::cout  << "KEYWORD: " << *yylval->keyword  << std::endl; 
        return kw->tok;
    }
    yylval->stringVal = new std::string(yytext, yyleng);
    //std::cout  << "IDENTIFIER: " << *yylval->stringVal << std::endl; 
    return token::IDENTIFIER;
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

L?\'(\\.|[^\\\'])*\'    {
    //FIXME: Ugly unescaping.
    std::string *str = new std::string(yytext, 1, yyleng - 2);
    boost::replace_all(*str, "\\'", "'");
    yylval->stringVal = str;  
    return token::STRING_LITERAL;
}

{Whitespace} {
    yylloc->step();
}

{NewLine} {
    yylloc->lines(yyleng);
        yylloc->end.column = yylloc->begin.column = 1;
    yylloc->step();
}

{Other} {
    return static_cast<token_type>(*yytext);
}

%%

namespace scidb {

AFLScanner::AFLScanner(QueryParser &glue, std::istream* in,
    std::ostream* out)
    : AFLBaseFlexLexer(in, out), _glue(glue)
{
}

AFLScanner::~AFLScanner()
{
}

void AFLScanner::set_debug(bool b)
{
    yy_flex_debug = b;
}

}

#ifdef yylex
#undef yylex
#endif

int AFLBaseFlexLexer::yylex()
{
    std::cerr << "in QueryFlexLexer::yylex() !" << std::endl;
    return 0;
}

int AFLBaseFlexLexer::yywrap()
{
    return 1;
}
