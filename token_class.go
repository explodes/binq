package binq

import (
	"github.com/pkg/errors"
	"strings"
	"unicode"
)

const (
	trueString  = "TRUE"
	falseString = "FALSE"
)

var isUnsupportedToken [tokenMax]bool

func init() {
	isUnsupportedToken[TokenScalarU16] = true
	isUnsupportedToken[TokenScalarU8] = true
	isUnsupportedToken[TokenScalarBool] = true
	isUnsupportedToken[TokenTypeBool] = true
	isUnsupportedToken[TokenStringLiteral] = true
	isUnsupportedToken[TokenSignedIntegerLiteral] = true
	isUnsupportedToken[TokenFloatLiteral] = true
}

var functionOrKeywordTokens = []struct {
	value        string
	funcToken    Token
	keywordToken Token
}{
	// Functions only
	{"KEY", TokenKey, TokenUnknown},
	{"VALUE", TokenValue, TokenUnknown},
	{"JUMP", TokenJump, TokenUnknown},
	// Function or ident/operator
	{"U64", TokenScalarU64, TokenTypeU64LE},
	{"U32", TokenScalarU32, TokenTypeU32LE},
	{"U16", TokenScalarU32, TokenTypeU16LE},
	{"U8", TokenScalarU8, TokenTypeU8},
	{"BOOL", TokenScalarBool, TokenTypeBool},
	// Keyword only
	{"U64LE", TokenUnknown, TokenTypeU64LE},
	{"U64BE", TokenUnknown, TokenTypeU64BE},
	{"U32LE", TokenUnknown, TokenTypeU32LE},
	{"U32BE", TokenUnknown, TokenTypeU32BE},
	{"U16LE", TokenUnknown, TokenTypeU16LE},
	{"U16BE", TokenUnknown, TokenTypeU16BE},
	// Operator "keywords" only
	{"AND", TokenUnknown, TokenAnd},
	{"OR", TokenUnknown, TokenOr},
	{"!=", TokenUnknown, TokenNeq},
	{"=", TokenUnknown, TokenEq},
	{"<", TokenUnknown, TokenLess},
	{"<=", TokenUnknown, TokenLessEq},
	{">", TokenUnknown, TokenGreater},
	{">=", TokenUnknown, TokenGreaterEq},
}

func unexpectedToken(value string) (Token, error) {
	return TokenUnknown, errors.Errorf(`unexpected token "%s"`, value)
}

func unexpectedFunction(value string) (Token, error) {
	return TokenUnknown, errors.Errorf(`unexpected function call "%s"`, value)
}

func classifyToken(value string, nextToken Token) (Token, error) {
	// "Control" tokens are already classified (comma, parenthesis, comments)
	// so we can tell if the token is part of a function call or not by using the next token.

	// For each token, determine if it a function or keyword.
	for _, ident := range functionOrKeywordTokens {
		if !strings.EqualFold(value, ident.value) {
			continue
		}
		// If this key matches, we'll either be followed by a left-paren or not.
		if nextToken == TokenLeftParen {
			// If we have a left-paren, then this value needs to have an associated function token.
			if ident.funcToken == TokenUnknown {
				return unexpectedFunction(value)
			}
			return ident.funcToken, nil
		} else {
			// If we do not have a left-paren, then this token needs to have an associated operator
			// or identity token.
			if ident.keywordToken == TokenUnknown {
				return unexpectedToken(value)
			}
			return ident.keywordToken, nil
		}
	}

	// If the next token is a left-paren, this value is attempting to make an unknown function call.
	if nextToken == TokenLeftParen {
		return unexpectedFunction(value)
	}

	// Maybe classify this token as a bool literal.
	boolToken, err := classifyBoolLiteral(value)
	if err != nil {
		return TokenUnknown, err
	}
	if boolToken != TokenUnknown {
		return boolToken, nil
	}

	// Maybe classify this token as a string type.
	stringToken, err := classifyStringToken(value)
	if err != nil {
		return TokenUnknown, err
	}
	if stringToken != TokenUnknown {
		return stringToken, nil
	}

	// Maybe classify this token as a numeric type.
	numericToken, err := classifyNumericToken(value)
	if err != nil {
		return TokenUnknown, err
	}
	if numericToken != TokenUnknown {
		return numericToken, nil
	}

	return unexpectedToken(value)
}

func classifyBoolLiteral(value string) (Token, error) {
	if strings.EqualFold(value, trueString) || strings.EqualFold(value, falseString) {
		return TokenBoolLiteral, nil
	}
	return TokenUnknown, nil
}

func classifyStringToken(value string) (Token, error) {
	if len(value) < 2 || value[0] != '"' {
		return TokenUnknown, nil
	}
	if value[len(value)-1] != '"' {
		return TokenUnknown, errors.New("unterminated string literal")
	}
	return TokenStringLiteral, nil
}

func invalidNumericLiteral(value string) (Token, error) {
	return TokenUnknown, errors.Errorf(`invalid numeric literal "%s"`, value)
}

func classifyNumericToken(value string) (Token, error) {
	index := 0
	signed := value[0] == '-'
	if signed {
		index++
	}
	hasDecimal := false
	hasExponent := false
	hasDigit := false

	for _, r := range value[index:] {
		switch {
		case r == 'e' || r == 'E':
			if hasExponent {
				return invalidNumericLiteral(value)
			}
			hasExponent = true
		case r == '.':
			if hasDecimal || hasExponent {
				return invalidNumericLiteral(value)
			}
			hasDecimal = true
		case unicode.IsDigit(r):
			hasDigit = true
		default:
			return invalidNumericLiteral(value)
		}
	}

	switch {
	case !hasDigit:
		return TokenUnknown, nil
	case hasDecimal || hasExponent:
		return TokenFloatLiteral, nil
	case signed:
		return TokenSignedIntegerLiteral, nil
	default:
		return TokenUnsignedIntegerLiteral, nil
	}
}
