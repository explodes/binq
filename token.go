package binq

import "github.com/pkg/errors"

const (
	TokenUnknown Token = iota

	/* Special characters */
	TokenComment     // # This is a comment. All text hereafter is not parsed.
	TokenComma       // ,
	TokenLeftParen   // (
	TokenRightParen  // )
	TokenSpace       // <whitespace>

	/* Value functions */
	TokenKey    // KEY(offset OR jump, type)
	TokenValue  // VALUE(offset OR jump, type)
	TokenJump   // JUMP(offset OR jump, type)

	/* Scalar functions */
	TokenScalarU64   // U64(0)
	TokenScalarU32   // U32(0)
	TokenScalarU16   // U16(0)
	TokenScalarU8    // U8(0)
	TokenScalarBool  // BOOL([true|false]) OR true OR false

	/* Type identifiers */
	TokenTypeU64LE  // U64, U64LE
	TokenTypeU64BE  // U64BE
	TokenTypeU32LE  // U32, U32LE
	TokenTypeU32BE  // U32BE
	TokenTypeU16LE  // U16, U16LE
	TokenTypeU16BE  // U16BE
	TokenTypeU8     // U8
	TokenTypeBool   // BOOL

	/* Literal values */
	TokenUnsignedIntegerLiteral  // 1000
	TokenSignedIntegerLiteral    // -1000
	TokenFloatLiteral            // -100e-4
	TokenStringLiteral           // "abc123"
	TokenBoolLiteral             // false | true

	/* Operators */
	TokenAnd        // AND
	TokenOr         // OR
	TokenLess       // <
	TokenLessEq     // <=
	TokenGreater    // >
	TokenGreaterEq  // >=
	TokenEq         // =
	TokenNeq        // !=

	tokenMax
)

type Token uint16

func (t Token) String() string {
	switch t {
	case TokenComment:
		return "COMMENT"
	case TokenComma:
		return "COMMA"
	case TokenLeftParen:
		return "LEFT_PAREN"
	case TokenRightParen:
		return "RIGHT_PAREN"
	case TokenSpace:
		return "SPACE"
	case TokenKey:
		return "KEY"
	case TokenValue:
		return "VALUE"
	case TokenJump:
		return "JUMP"
	case TokenScalarU64:
		return "SCALAR_U64"
	case TokenScalarU32:
		return "SCALAR_U32"
	case TokenScalarU16:
		return "SCALAR_U16"
	case TokenScalarU8:
		return "SCALAR_U8"
	case TokenScalarBool:
		return "SCALAR_BOOL"
	case TokenTypeU64LE:
		return "TYPE_U64LE"
	case TokenTypeU64BE:
		return "TYPE_U64BE"
	case TokenTypeU32LE:
		return "TYPE_U64LE"
	case TokenTypeU32BE:
		return "TYPE_U32BE"
	case TokenTypeU16LE:
		return "TYPE_U16LE"
	case TokenTypeU16BE:
		return "TYPE_U16BE"
	case TokenTypeU8:
		return "U8"
	case TokenTypeBool:
		return "BOOL"
	case TokenUnsignedIntegerLiteral:
		return "UNSIGNED_INTEGER"
	case TokenSignedIntegerLiteral:
		return "SIGNED_INTEGER"
	case TokenFloatLiteral:
		return "FLOAT"
	case TokenStringLiteral:
		return "STRING"
	case TokenBoolLiteral:
		return "BOOL_LITERAL"
	case TokenAnd:
		return "AND"
	case TokenOr:
		return "OR"
	case TokenLess:
		return "LESS"
	case TokenLessEq:
		return "LESS_EQ"
	case TokenGreater:
		return "GREATER"
	case TokenGreaterEq:
		return "GREATER_EQ"
	case TokenEq:
		return "EQUAL"
	case TokenNeq:
		return "NOT_EQUAL"
	case TokenUnknown:
		return "UNKNOWN"
	default:
		return "<unknown>"
	}
}

func (t Token) IsFunction() bool {
	switch t {
	case TokenKey, TokenValue, TokenJump,
		TokenScalarU64, TokenScalarU32, TokenScalarU16, TokenScalarU8, TokenScalarBool:
		return true
	default:
		return false
	}
}

func (t Token) NumArgs() int {
	if !t.IsFunction() {
		panic("asking for number of args on non function token")
	}
	switch t {
	case TokenKey, TokenValue, TokenJump:
		return 2
	case TokenScalarU64, TokenScalarU32, TokenScalarU16, TokenScalarU8, TokenScalarBool:
		return 1
	default:
		panic("unhandled function token")
	}
}

func (t Token) IsTypeIdentifier() bool {
	switch t {
	case TokenTypeU64LE, TokenTypeU64BE,
		TokenTypeU32LE, TokenTypeU32BE,
		TokenTypeU16LE, TokenTypeU16BE,
		TokenTypeU8,
		TokenTypeBool:
		return true
	default:
		return false
	}
}

func (t Token) IsOperator() bool {
	switch t {
	case TokenLess, TokenLessEq,
		TokenGreater, TokenGreaterEq,
		TokenEq, TokenNeq, TokenAnd, TokenOr:
		return true
	default:
		return false
	}
}

func (t Token) IsBinaryOperator() bool {
	switch t {
	case TokenLess, TokenLessEq,
		TokenGreater, TokenGreaterEq,
		TokenEq, TokenNeq, TokenAnd, TokenOr:
		return true
	default:
		return false
	}
}

func (t Token) IsLiteral() bool {
	switch t {
	case TokenUnsignedIntegerLiteral, TokenSignedIntegerLiteral, TokenStringLiteral, TokenBoolLiteral:
		return true
	default:
		return false
	}
}

func (t Token) IsIgnored() bool {
	switch t {
	case TokenComment, TokenSpace, TokenComma:
		return true
	default:
		return false
	}
}

func (t Token) IsParenthesis() bool {
	switch t {
	case TokenLeftParen, TokenRightParen:
		return true
	default:
		return false
	}
}

func (t Token) Precedence() int {
	switch t {
	case TokenAnd,
		TokenOr,
		TokenLess,
		TokenLessEq,
		TokenGreater,
		TokenGreaterEq,
		TokenEq,
		TokenNeq:
		return 10
	default:
		panic(errors.Errorf("unhandled precedence for %s", t.String()))
	}
}

func (t Token) IsLeftAssociative() bool {
	switch t {
	case TokenAnd,
		TokenOr,
		TokenLess,
		TokenLessEq,
		TokenGreater,
		TokenGreaterEq,
		TokenEq,
		TokenNeq:
		return true
	default:
		panic(errors.Errorf("unhandled associativity for %s", t.String()))
	}
}
