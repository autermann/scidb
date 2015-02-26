%{
#include <string>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/make_shared.hpp>
#include <boost/format.hpp>

#include "system/Exceptions.h"

#include "iquery/scanner.h"
#include "iquery/keywords.h"
#include "iquery/iquery_parser.h"

typedef yy::Parser::token token;
typedef yy::Parser::token_type token_type;
#define yyterminate() return token::EOQ

#define	YY_DECL							    \
    yy::Parser::token_type				    \
    yy::Scanner::lex(						\
        yy::Parser::semantic_type* yylval,  \
        yy::Parser::location_type* yylloc	\
	)
%}

%option c++
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
Integer	{Digit}+
Decimal			(({Digit}*\.{Digit}+)|({Digit}+\.{Digit}*))
Real			({Integer}|{Decimal})[Ee][-+]?{Digit}+

Other .

%%

%{
	yylloc->step();
%}

(?i:auto) {
    return token::AUTO;
}
(?i:opaque) {
    return token::OPAQUE;
}

(?i:csv) {
    return token::CSV;
}

(?i:csv\+) {
    return token::CSV_PLUS;
}

(?i:lcsv\+) {
    return token::LCSV_PLUS;
}

(?i:dense) {
    return token::DENSE;
}

(?i:sparse) {
    return token::SPARSE;
}

(?i:lsparse) {
    return token::LSPARSE;
}

(?i:text) {
    return token::TEXT;
}

(?i:dcsv) {
    return token::DCSV;
}

{OneLineComment} {
}

{Identifier} {
	const Keyword *kw = FindKeyword(yytext);
	if (kw)
	{
		return kw->tok;
	}
	yylval->stringVal = new std::string(yytext, yyleng);
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

L?\'(\\.|[^\\\'])*\'	{
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

namespace yy {

Scanner::Scanner(IqueryParser& glue, std::istream* in,
	std::ostream* out)
	: BaseFlexLexer(in, out), _glue(glue)
{
}

Scanner::~Scanner()
{
}

void Scanner::set_debug(bool b)
{
    yy_flex_debug = b;
}

}

#ifdef yylex
#undef yylex
#endif

int BaseFlexLexer::yylex()
{
	std::cerr << "in QueryFlexLexer::yylex() !" << std::endl;
	return 0;
}

int BaseFlexLexer::yywrap()
{
	return 1;
}
