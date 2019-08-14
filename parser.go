package binq

import (
	"fmt"
	"github.com/pkg/errors"
	"io"
	"strconv"
	"strings"
	"unicode"
)

const (
	Comment          = '#'
	CommentString    = string(Comment)
	Comma            = ','
	CommaString      = string(Comma)
	LeftParen        = '('
	LeftParenString  = string(LeftParen)
	RightParen       = ')'
	RightParenString = string(RightParen)
)

type ParserValue struct {
	// token is the type of token this value represents.
	token Token
	// value is the string value of tokens.
	value string
	// pos is the position of the value.
	pos int
	// line is the line number of the value.
	line int
	// linePos is the position within the value's line this value starts at.
	linePos int
}

func (v *ParserValue) setUnknownValue(r []rune) *ParserValue {
	v.token = TokenUnknown
	v.value = string(r)
	return v
}

func (v *ParserValue) setTokenRunes(token Token, r []rune) *ParserValue {
	v.token = token
	v.value = string(r)
	return v
}

func (v *ParserValue) setTokenString(token Token, s string) *ParserValue {
	v.token = token
	v.value = s
	return v
}

func (v *ParserValue) Line() int     { return v.line }
func (v *ParserValue) LinePos() int  { return v.linePos }
func (v *ParserValue) Pos() int      { return v.pos }
func (v *ParserValue) Token() Token  { return v.token }
func (v *ParserValue) Value() string { return v.value }

var _ error = positionalError{}

type positionalError struct {
	err error
	// line is the line number of the error.
	line int
	// linePos is the position within the error's line this error starts at.
	linePos int
}

func newPositionalError(v *ParserValue, err error) error {
	return positionalError{
		err:     err,
		line:    v.line,
		linePos: v.linePos,
	}
}

func (e positionalError) Error() string {
	return fmt.Sprintf("error at line %d position %d: %v", e.line, e.linePos, e.err)
}

type Parser struct {
	// s is the runes of the string being read.
	s []rune
	// pos is the position of the next character to read.
	pos int
	// line is the current line number being read.
	line int
	// linePos is a stack of current positions in the line being read.
	linePos []int
}

func NewParser(s string) *Parser {
	linePos := make([]int, 1, 16)
	linePos[0] = 0
	return &Parser{
		s:       []rune(s),
		pos:     0,
		line:    0,
		linePos: linePos,
	}
}

func (p *Parser) classifyUnknownTokens(values []*ParserValue) error {
	for index, value := range values {
		if value.token != TokenUnknown {
			continue
		}
		var nextToken Token
		if index+1 < len(values) {
			nextToken = values[index+1].token
		} else {
			nextToken = TokenUnknown
		}
		// Classify the token.
		// If there is no error, token is NOT TokenUnknown, congratulations.
		token, err := classifyToken(value.value, nextToken)
		if err != nil {
			return newPositionalError(value, err)
		}
		value.token = token
	}
	return nil
}

func (p *Parser) ReadValues() (values []*ParserValue, err error) {
	values, err = p.ReadUnsupportedValues()
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		// Some tokens are parsable but not supported yet.
		if isUnsupportedToken[value.token] {
			return nil, newPositionalError(value, errors.Errorf(`token %s "%s" is currently not supported`, value.token, value.value))
		}
	}
	return values, err
}

func (p *Parser) ReadUnsupportedValues() (values []*ParserValue, err error) {
	values, err = p.consumeValues()
	if err != nil {
		return nil, err
	}
	if err := p.classifyUnknownTokens(values); err != nil {
		return nil, err
	}
	return values, err
}

func (p *Parser) consumeValues() (values []*ParserValue, err error) {
	values = make([]*ParserValue, 0, 16)
	for {
		value, err := p.consumeValue()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func (p *Parser) consumeValue() (value *ParserValue, err error) {
	value = &ParserValue{
		pos:     p.pos,
		line:    p.line,
		linePos: p.linePos[len(p.linePos)-1],
	}

	// If we're at the end of the line, return EOF
	if p.pos == len(p.s) {
		return value, io.EOF
	}

	runes := make([]rune, 0, 16)
	for {
		r, err := p.advance()
		if err == io.EOF && len(runes) > 0 {
			// We've hit the end but we have runes.
			// Ignore the error and return our token/
			return value.setUnknownValue(runes), nil
		} else if err != nil {
			return value.setUnknownValue(runes), err
		}

		// If we encounter whitespace, we have ended our current token.
		if unicode.IsSpace(r) {
			if len(runes) > 0 {
				p.unread()
				return value.setUnknownValue(runes), nil
			} else {
				p.unread()
				whitespace, err := p.consumeWhitespace()
				if err != nil {
					return value, err
				}
				return value.setTokenRunes(TokenSpace, whitespace), nil
			}
		}

		// If we encounter a special character, either we've ended
		// our current token or begin a special sequence.
		isSpecial := r == LeftParen || r == RightParen || r == Comment || r == Comma
		if isSpecial {
			if len(runes) > 0 {
				p.unread()
				return value.setUnknownValue(runes), nil
			} else if r == LeftParen {
				return value.setTokenString(TokenLeftParen, LeftParenString), nil
			} else if r == RightParen {
				return value.setTokenString(TokenRightParen, RightParenString), nil
			} else if r == Comma {
				return value.setTokenString(TokenComma, CommaString), nil
			} else if r == Comment {
				p.unread()
				comment, err := p.consumeLine()
				if err != nil {
					return value, err
				}
				return value.setTokenRunes(TokenComment, comment), nil
			}
		}

		runes = append(runes, r)
	}
}

func (p *Parser) consumeWhitespace() ([]rune, error) {
	runes := make([]rune, 0, 16)
	for {
		r, err := p.advance()
		if err == io.EOF && len(runes) > 0 {
			return runes, nil
		}
		if err != nil {
			return nil, err
		}
		if !unicode.IsSpace(r) {
			p.unread()
			return runes, nil
		}
		runes = append(runes, r)
	}
}

func (p *Parser) consumeLine() ([]rune, error) {
	runes := make([]rune, 0, 16)
	for {
		r, err := p.advance()
		if err == io.EOF && len(runes) > 0 {
			return runes, nil
		}
		if err != nil {
			return nil, err
		}
		if r == '\n' {
			p.unread()
			return runes, nil
		}
		runes = append(runes, r)
	}
}

func (p *Parser) advance() (rune, error) {
	if p.pos == len(p.s) {
		return 0, io.EOF
	}
	r := p.s[p.pos]
	if r == '\n' {
		p.line++
		p.linePos = append(p.linePos, 0)
	} else {
		p.linePos[len(p.linePos)-1]++
	}
	p.pos++
	return r, nil
}

func (p *Parser) unread() {
	p.pos--
	if p.s[p.pos] == '\n' {
		p.line--
		p.linePos = p.linePos[:len(p.linePos)-1]
	} else {
		p.linePos[len(p.linePos)-1]--
	}
}

func (p *Parser) ToPostfix(values []*ParserValue) ([]*ParserValue, error) {
	var output, operators []*ParserValue
	for _, value := range values {
		token := value.token
		switch {
		case token.IsIgnored():
			continue
		case token.IsLiteral() || token.IsTypeIdentifier():
			output = append(output, value)
		case token.IsFunction():
			operators = append(operators, value)
		case token.IsOperator():
			for len(operators) > 0 {
				top := operators[len(operators)-1]
				topToken := top.token
				if topToken != TokenLeftParen && (
					(topToken.IsFunction()) ||
						(topToken.IsOperator() && topToken.Precedence() > token.Precedence()) ||
						(topToken.IsOperator() && topToken.Precedence() == token.Precedence() && topToken.IsLeftAssociative())) {
					topOut := output[len(output)-1]
					output = output[:len(output)-1]
					operators = append(operators, topOut)
				} else {
					break
				}
			}
			operators = append(operators, value)
		case token == TokenLeftParen:
			operators = append(operators, value)
		case token == TokenRightParen:
			for {
				if len(operators) == 0 {
					return nil, newPositionalError(value, errors.New("unmatched parenthesis"))
				}
				topOp := operators[len(operators)-1]
				if topOp.token == TokenLeftParen {
					break
				}
				operators = operators[:len(operators)-1]
				output = append(output, topOp)
			}
			if len(operators) > 0 && operators[len(operators)-1].token == TokenLeftParen {
				operators = operators[:len(operators)-1]
			}
		default:
			return nil, newPositionalError(value, errors.Errorf(`unhandled token "%s"`, value.value))
		}
	}
	for len(operators) > 0 {
		topOp := operators[len(operators)-1]
		if topOp.token.IsParenthesis() {
			return nil, newPositionalError(topOp, errors.New("unmatched parenthesis"))
		}
		operators = operators[:len(operators)-1]
		output = append(output, topOp)
	}

	return output, nil
}

func (p *Parser) ReadPredicate() (predicate *Predicate, err error) {
	values, err := p.ReadUnsupportedValues()
	if err != nil {
		return nil, err
	}
	values, err = p.ToPostfix(values)
	if err != nil {
		return nil, err
	}

	var arg1, arg2 interface{}
	var stack []interface{}
	for _, value := range values {
		token := value.token
		switch {
		case token.IsLiteral():
			stack = append(stack, value)
		case token.IsFunction():
			switch token.NumArgs() {
			case 1:
				arg1, stack, err = p.pop1(stack)
				if err != nil {
					return nil, err
				}
				f, err := p.valueToSingleArgFunc(value, arg1)
				if err != nil {
					return nil, err
				}
				stack = append(stack, f)
			case 2:
				arg1, arg2, stack, err = p.pop2(stack)
				if err != nil {
					return nil, err
				}
			default:
				panic("unhandled function args")
			}
		}
	}

	return nil, nil
}

func (p *Parser) pop1(s []interface{}) (interface{}, []interface{}, error) {
	if len(s) < 1 {
		return nil, nil, errors.New("not enough arguments")
	}
	a := s[len(s)-1]
	return a, s[:len(s)-1], nil
}

func (p *Parser) pop2(s []interface{}) (interface{}, interface{}, []interface{}, error) {
	if len(s) < 2 {
		return nil, nil, nil, errors.New("not enough arguments")
	}
	a := s[len(s)-1]
	b := s[len(s)-2]
	return a, b, s[:len(s)-2], nil
}

func (p *Parser) unexpectedArg(value *ParserValue, got interface{}, want string) error {
	return newPositionalError(value, errors.Errorf("unexpected argument got %T want %s", got, want))
}

func (p *Parser) unexpectedArgToken(value *ParserValue, got *ParserValue, want Token) error {
	return newPositionalError(value, errors.Errorf("unexpected argument got %s want %s", got.token, want))
}

func (p *Parser) valueToExpression(value *ParserValue) (*Expression, error) {
	return nil, errors.New("not implemented")
}

func (p *Parser) valueToBinaryOperation(value *ParserValue) (*BinaryOperation, error) {
	return nil, errors.New("not implemented")
}

func (p *Parser) valueToValue(value *ParserValue) (*Value, error) {
	return nil, errors.New("not implemented")
}

func (p *Parser) valueToSingleArgFunc(value *ParserValue, arg1 interface{}) (interface{}, error) {
	switch value.token {
	case TokenScalarU64:
		argValue, ok := arg1.(*ParserValue)
		if !ok {
			return nil, p.unexpectedArg(value, arg1, "*ParserValue")
		}
		if argValue.token != TokenUnsignedIntegerLiteral {
			return nil, p.unexpectedArgToken(value, argValue, TokenUnsignedIntegerLiteral)
		}
		u64, err := strconv.ParseUint(argValue.value, 10, 64)
		if err != nil {
			return nil, err
		}
		return &Scalar{
			Value: &Scalar_U64{
				U64: u64,
			},
		}, nil
	case TokenScalarU32:
		argValue, ok := arg1.(*ParserValue)
		if !ok {
			return nil, p.unexpectedArg(value, arg1, "*ParserValue")
		}
		if argValue.token != TokenUnsignedIntegerLiteral {
			return nil, p.unexpectedArgToken(value, argValue, TokenUnsignedIntegerLiteral)
		}
		u32, err := strconv.ParseUint(argValue.value, 10, 64)
		if err != nil {
			return nil, err
		}
		return &Scalar{
			Value: &Scalar_U32{
				U32: uint32(u32),
			},
		}, nil
	case TokenScalarBool:
		argValue, ok := arg1.(*ParserValue)
		if !ok {
			return nil, p.unexpectedArg(value, arg1, "*ParserValue")
		}
		if argValue.token != TokenBoolLiteral {
			return nil, p.unexpectedArgToken(value, argValue, TokenBoolLiteral)
		}
		isTrue := strings.EqualFold(argValue.value, trueString)
		return &Scalar{
			Value: &Scalar_Bool{
				Bool: isTrue,
			},
		}, nil
	case TokenScalarU16:
		fallthrough
	case TokenScalarU8:
		fallthrough
	default:
		return nil, errors.Errorf("unsupported function %s", value.token.String())
	}
}

func (p *Parser) valueToScalar(value *ParserValue, literal interface{}) (*Scalar, error) {
	return nil, errors.New("not implemented")
}
