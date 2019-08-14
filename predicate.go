package binq

import "errors"

var (
	booleanOps = map[BinaryOpCode]struct{}{
		BinaryOpCode_BINARY_OP_CODE_EQ:         {},
		BinaryOpCode_BINARY_OP_CODE_NEQ:        {},
		BinaryOpCode_BINARY_OP_CODE_LESS:       {},
		BinaryOpCode_BINARY_OP_CODE_LESS_EQ:    {},
		BinaryOpCode_BINARY_OP_CODE_GREATER:    {},
		BinaryOpCode_BINARY_OP_CODE_GREATER_EQ: {},
	}
)

func PredicateToMatcher(pred *Predicate) (Matcher, error) {
	switch t := pred.GetPredicate().(type) {
	case *Predicate_Expression:
		matcher, err := expressionToMatcher(t.Expression)
		if err != nil {
			return nil, wrap(err, "unable to convert expression to matcher")
		}
		return matcher, nil
	case *Predicate_Any:
		matchers, err := expressionsToMatchers(t.Any.Expressions)
		if err != nil {
			return nil, wrap(err, "unable to convert expressions to matchers")
		}
		return Any(matchers...), nil
	case *Predicate_All:
		matchers, err := expressionsToMatchers(t.All.Expressions)
		if err != nil {
			return nil, wrap(err, "unable to convert expressions to matchers")
		}
		return All(matchers...), nil
	default:
		return nil, unhandledType("predicate type", t)
	}
}
func expressionsToMatchers(exs []*Expression) ([]Matcher, error) {
	matchers := make([]Matcher, len(exs))
	for index, ex := range exs {
		matcher, err := expressionToMatcher(ex)
		if err != nil {
			return nil, wrap(err, "unable to sub-expression to matcher")
		}
		matchers[index] = matcher
	}
	return matchers, nil
}

func expressionToMatcher(ex *Expression) (Matcher, error) {
	evaluator, returnType, err := expressionToEvaluator(ex)
	if err != nil {
		return nil, wrap(err, "invalid expression")
	}
	if returnType != ReturnType_RETURN_TYPE_BOOL {
		return nil, errors.New("expression is not a boolean expression")
	}
	matcher := MatcherFunc(func(b []byte) (bool, error) {
		value, _, err := evaluator.Evaluate(b)
		if err != nil {
			return false, wrap(err, "error evaluating expression")
		}
		return value.(bool), nil
	})
	return matcher, nil
}

func expressionToEvaluator(ex *Expression) (Evaluator, ReturnType, error) {
	switch t := ex.GetExpression().(type) {
	case *Expression_Scalar:
		evaluator, returnType, err := scalarToEvaluator(t.Scalar)
		if err != nil {
			return nil, ReturnType_RETURN_TYPE_UNKNOWN, wrap(err, "unable to convert scalar to evaluator")
		}
		return evaluator, returnType, nil
	case *Expression_Value:
		evaluator, returnType, err := valueToEvaluator(t.Value)
		if err != nil {
			return nil, ReturnType_RETURN_TYPE_UNKNOWN, wrap(err, "unable to convert value to evaluator")
		}
		return evaluator, returnType, nil
	case *Expression_BinaryOperation:
		evaluator, returnType, err := binaryOperationEvaluator(t.BinaryOperation)
		if err != nil {
			return nil, ReturnType_RETURN_TYPE_UNKNOWN, wrap(err, "unable to convert value to evaluator")
		}
		return evaluator, returnType, nil
	default:
		return nil, ReturnType_RETURN_TYPE_UNKNOWN, unhandledType("expression type", t)
	}
}

func binaryOperationEvaluator(op *BinaryOperation) (EvaluatorFunc, ReturnType, error) {
	leftEvaluator, leftType, err := expressionToEvaluator(op.Left)
	if err != nil {
		// nowrap: recursive call
		return nil, ReturnType_RETURN_TYPE_UNKNOWN, err
	}
	rightEvaluator, rightType, err := expressionToEvaluator(op.Right)
	if err != nil {
		// nowrap: recursive call
		return nil, ReturnType_RETURN_TYPE_UNKNOWN, err
	}
	upscaleLeft, upscaleRight, upscaledType, err := getUpscaler(leftType, rightType)
	if err != nil {
		return nil, ReturnType_RETURN_TYPE_UNKNOWN, wrap(err, "invalid expression")
	}
	opCode := op.BinaryOpCode
	returnType := getReturnType(upscaledType, opCode)
	evaluator := EvaluatorFunc(func(b []byte) (interface{}, ReturnType, error) {
		leftValue, _, err := leftEvaluator.Evaluate(b)
		if err != nil {
			return nil, ReturnType_RETURN_TYPE_UNKNOWN, wrap(err, "unable to evaluate left hand expression")
		}
		rightValue, _, err := rightEvaluator.Evaluate(b)
		if err != nil {
			return nil, ReturnType_RETURN_TYPE_UNKNOWN, wrap(err, "unable to evaluate right hand expression")
		}
		leftValue = upscaleLeft(leftValue)
		rightValue = upscaleRight(rightValue)
		value, err := performBinaryOperation(upscaledType, leftValue, rightValue, opCode)
		if err != nil {
			return nil, ReturnType_RETURN_TYPE_UNKNOWN, wrap(err, "unable to evaluate binary expression")
		}
		return value, returnType, nil
	})
	return evaluator, returnType, nil
}

func getReturnType(returnType ReturnType, code BinaryOpCode) ReturnType {
	if _, isBinaryOp := booleanOps[code]; isBinaryOp {
		return ReturnType_RETURN_TYPE_BOOL
	}
	return returnType
}

type scalarEvaluatorImpl struct {
	val        interface{}
	returnType ReturnType
}

func (s scalarEvaluatorImpl) evaluate([]byte) (interface{}, ReturnType, error) {
	return s.val, s.returnType, nil
}

func scalarToEvaluator(s *Scalar) (EvaluatorFunc, ReturnType, error) {
	var eval scalarEvaluatorImpl
	switch t := s.Value.(type) {
	case *Scalar_Bool:
		eval = scalarEvaluatorImpl{val: t.Bool, returnType: ReturnType_RETURN_TYPE_BOOL}
	case *Scalar_U32:
		eval = scalarEvaluatorImpl{val: t.U32, returnType: ReturnType_RETURN_TYPE_U32}
	case *Scalar_U64:
		eval = scalarEvaluatorImpl{val: t.U64, returnType: ReturnType_RETURN_TYPE_U64}
	default:
		return nil, ReturnType_RETURN_TYPE_UNKNOWN, unhandledType("scalar type", t)
	}
	return eval.evaluate, eval.returnType, nil
}

type getterFunc func([]byte) (interface{}, error)

type valueEvaluatorImpl struct {
	getter     getterFunc
	returnType ReturnType
}

func valueToEvaluator(v *Value) (EvaluatorFunc, ReturnType, error) {
	jumper, err := jumpToJumper(v.Jump)
	if err != nil {
		return nil, ReturnType_RETURN_TYPE_UNKNOWN, wrap(err, "invalid value jump")
	}
	var eval valueEvaluatorImpl
	switch v.Type {
	case ValueType_VALUE_TYPE_U64LE:
		eval = valueEvaluatorImpl{getter: GetU64le, returnType: ReturnType_RETURN_TYPE_U64}
	case ValueType_VALUE_TYPE_U64BE:
		eval = valueEvaluatorImpl{getter: GetU64be, returnType: ReturnType_RETURN_TYPE_U64}
	case ValueType_VALUE_TYPE_U32LE:
		eval = valueEvaluatorImpl{getter: GetU32le, returnType: ReturnType_RETURN_TYPE_U32}
	case ValueType_VALUE_TYPE_U32BE:
		eval = valueEvaluatorImpl{getter: GetU32be, returnType: ReturnType_RETURN_TYPE_U32}
	case ValueType_VALUE_TYPE_U16LE:
		eval = valueEvaluatorImpl{getter: GetU16le, returnType: ReturnType_RETURN_TYPE_U16}
	case ValueType_VALUE_TYPE_U16BE:
		eval = valueEvaluatorImpl{getter: GetU16be, returnType: ReturnType_RETURN_TYPE_U16}
	case ValueType_VALUE_TYPE_U8:
		eval = valueEvaluatorImpl{getter: GetU8, returnType: ReturnType_RETURN_TYPE_U8}
	default:
		return nil, ReturnType_RETURN_TYPE_UNKNOWN, unhandledEnum("value type", v.Type)
	}
	evaluator := valueEvaluatorImplWithJump(jumper, eval)
	return evaluator, eval.returnType, nil
}

// valueEvaluatorImplWithJump creates an EvaluatorFunc for data at position that is jumped to.
func valueEvaluatorImplWithJump(jumper Jumper, value valueEvaluatorImpl) EvaluatorFunc {
	return func(bytes []byte) (interface{}, ReturnType, error) {
		jumped, err := jumper.Jump(bytes)
		if err != nil {
			return nil, ReturnType_RETURN_TYPE_UNKNOWN, wrap(err, "unable to jump")
		}
		gotValue, err := value.getter(jumped)
		if err != nil {
			return nil, ReturnType_RETURN_TYPE_UNKNOWN, wrap(err, "unable to run matcher")
		}
		return gotValue, value.returnType, nil
	}
}

func jumpToJumper(j *Jump) (Jumper, error) {
	var jumper Jumper
	switch t := j.Jump.(type) {
	case *Jump_Offset:
		jumper = JumpOffset(t.Offset)
	case *Jump_U64Le:
		jumper = JumpToU64le(t.U64Le)
	case *Jump_U64Be:
		jumper = JumpToU64be(t.U64Be)
	case *Jump_U32Le:
		jumper = JumpToU32le(t.U32Le)
	case *Jump_U32Be:
		jumper = JumpToU32be(t.U32Be)
	case *Jump_U16Le:
		jumper = JumpToU16le(t.U16Le)
	case *Jump_U16Be:
		jumper = JumpToU16be(t.U16Be)
	case *Jump_U8:
		jumper = JumpToU8(t.U8)
	default:
		return nil, unhandledType("jump type", t)
	}
	return jumper, nil
}
