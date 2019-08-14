package binq

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPerformBinaryOperationUnknownType(t *testing.T) {
	result, err := performBinaryOperation(ReturnType_RETURN_TYPE_UNKNOWN, 0, 0, BinaryOpCode_BINARY_OP_CODE_EQ)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestUintUnknownOps(t *testing.T) {
	t.Parallel()
	for _, valueType := range uintTypes {
		valueType := valueType
		t.Run(valueType.String(), func(t *testing.T) {
			t.Parallel()
			value := makeReturnTypeValue(t, valueType)
			result, err := performBinaryOperation(valueType, value, value, BinaryOpCode_BINARY_OP_CODE_UNKNOWN)
			assert.Error(t, err)
			assert.Nil(t, result)
		})
	}
}

func TestUintBooleanOps(t *testing.T) {
	t.Parallel()
	for _, valueType := range uintTypes {
		valueType := valueType
		t.Run(valueType.String(), func(t *testing.T) {
			t.Parallel()
			for op := range booleanOps {
				op := op
				t.Run(op.String(), func(t *testing.T) {
					t.Parallel()
					value := makeReturnTypeValue(t, valueType)
					result, err := performBinaryOperation(valueType, value, value, op)
					assert.NoError(t, err)
					assert.IsType(t, false, result)
				})
			}
		})
	}
}
