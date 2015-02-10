
/* description: Parses end executes mathematical expressions. */

/* lexical grammar */
%lex
DecimalDigit [0-9]
DecimalDigits [0-9]+
NonZeroDigit [1-9]
DecimalIntegerLiteral [0]|({NonZeroDigit}{DecimalDigits}*)
DecimalLiteral ({DecimalIntegerLiteral}\.{DecimalDigits}*)|(\.{DecimalDigits})|({DecimalIntegerLiteral})

StringLiteral \"([^\"\\]*(\\.[^\"\\]*)*)\"|\'([^\'\\]*(\\.[^\'\\]*)*)\'

%x REGEXP
%options flex
%%
/* KET: The following comment is actually needed for parsing purposes, removing it would combine the first two lines */
\s+                   			   /* skip whitespace */
{StringLiteral}                    return "STRING_LITERAL";
{DecimalLiteral}                   return "NUMERIC_LITERAL";
"true"                             return "TRUE";
"'true'"						   return "TRUE";
"false"                            return "FALSE";
"'false'"						   return "FALSE";
"("                                return "(";
")"                                return ")";
"["                                return "[";
"]"                                return "]";
"."                                return ".";
"=="                               return "==";
"!="                               return "!=";
"<="                               return "<=";
"<"                                return "<";
">="                               return ">=";
">"                                return ">";
"&&"                               return "&&";
"||"                               return "||";
<<EOF>>                            return "EOF";
.                                  return "ERROR";

%%

/lex

/* operator associations and precedence */

%left '==' '!=' '>' '<' '>=' '<='
%left '&&' '||'

%start expressions

%% /* language grammar */

expressions
    : e EOF
        { typeof console !== 'undefined' ? console.log($1) : print($1);
          return $1; }
    ;

e
    : e '&&' e
        { $$ = ($1 && $3); }
    | e '||' e
        {$$ = ($1 || $3); }
	| e '==' e
		{
			$$ = ($1 == $3);
		}
	| e '!=' e
		{
			$$ = (parseInt($1) != parseInt($3));
		}
	| e '>' e
		{
			$$ = (parseInt($1) > parseInt($3));
		}
	| e '<' e
		{
			$$ = (parseInt($1) < parseInt($3));
		}
	| e '>=' e
		{
			$$ = (parseInt($1) >= parseInt($3));
		}
	| e '<=' e
		{
			$$ = (parseInt($1) <= parseInt($3));
		}
    | '(' e ')'
        {$$ = $2;}
	| TRUE
		{$$ = true;}
	| FALSE
		{$$ = false;}
	| StringLiteral
	| DecimalLiteral
	;

DecimalLiteral
	: "NUMERIC_LITERAL"
		{{
			$$ = stripQuotes($1);
		}}
	;
	
StringLiteral
	: "STRING_LITERAL"
		{{
			var unquoted = stripQuotes($1);
			if(unquoted.toLowerCase() == 'true') {
				$$ = true;
			}
			else if(unquoted.toLowerCase() == 'false') {
				$$ = false;
			}
			else {
				$$ = unquoted;
			}
		}}
	;

%%
	
/* Begin Custom Functions */
function stripQuotes(string){
	if(typeof string === 'string') {
		var toReturn = string.replace(/^\'|\'$/g, "");
		return toReturn;
	}
	else {
		return string;
	}
}
	