%{
#include <string>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/make_shared.hpp>
#include <boost/format.hpp>

#include "system/Exceptions.h"

#include "query/parser/AQLScanner.h"
#include "query/parser/AQLKeywords.h"
#include "query/parser/ParsingContext.h"
#include "query/parser/QueryParser.h"

typedef scidb::AQLParser::token token;
typedef scidb::AQLParser::token_type token_type;
#define yyterminate() return token::EOQ

#define    YY_DECL                                    \
    scidb::AQLParser::token_type                \
    scidb::AQLScanner::lex(                        \
        scidb::AQLParser::semantic_type* yylval,\
        scidb::AQLParser::location_type* yylloc    \
    )
%}

%option c++
%option prefix="Query"
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
QuotedIdentifier \"{IdentifierFirstChar}{IdentifierOtherChars}*\"

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
    const AQLKeyword *kw = FindAQLKeyword(yytext);
    if (kw)
    {
        yylval->keyword = new std::string(yytext, yyleng);
        return kw->tok;
    }
    yylval->stringVal = new std::string(yytext, yyleng);
    return token::IDENTIFIER;
}

{QuotedIdentifier} {
    yylval->stringVal = new std::string(yytext, 1, yyleng - 2);  
    return token::IDENTIFIER;
}

{Integer} {
    try
    {
        yylval->int64Val = boost::lexical_cast<int64_t>(yytext);
    }
    catch(boost::bad_lexical_cast &e)
    {
        _glue.error2(*yylloc, boost::str(boost::format("Can not interpret '%s' as int64 value. Value too big.") % yytext));
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
        _glue.error2(*yylloc, boost::str(boost::format("Can not interpret '%s' as real value. Value too big.") % yytext));
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
        _glue.error2(*yylloc, boost::str(boost::format("Can not interpret '%s' as decimal value. Value too big.") % yytext));
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

AQLScanner::AQLScanner(QueryParser& glue, std::istream* in,
    std::ostream* out)
    : AQLBaseFlexLexer(in, out), _glue(glue)
{
}

AQLScanner::~AQLScanner()
{
}

void AQLScanner::set_debug(bool b)
{
    yy_flex_debug = b;
}

}

#ifdef yylex
#undef yylex
#endif

int AQLBaseFlexLexer::yylex()
{
    std::cerr << "in QueryFlexLexer::yylex() !" << std::endl;
    return 0;
}

int AQLBaseFlexLexer::yywrap()
{
    return 1;
}
