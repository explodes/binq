package binq

func performBinaryOperation(valueType ReturnType, valueA, valueB interface{}, op BinaryOpCode) (interface{}, error) {
	switch valueType {
	case ReturnType_RETURN_TYPE_U64:
		return performOpU64(valueA.(uint64), valueB.(uint64), op)
	case ReturnType_RETURN_TYPE_U32:
		return performOpU32(valueA.(uint32), valueB.(uint32), op)
	case ReturnType_RETURN_TYPE_U16:
		return performOpU16(valueA.(uint16), valueB.(uint16), op)
	case ReturnType_RETURN_TYPE_U8:
		return performOpU8(valueA.(uint8), valueB.(uint8), op)
	case ReturnType_RETURN_TYPE_BOOL:
		return performOpBool(valueA.(bool), valueB.(bool), op)
	default:
		return nil, unhandledEnum("binary op value type", valueType)
	}
}

func performOpU64(a, b uint64, op BinaryOpCode) (interface{}, error) {
	switch op {
	case BinaryOpCode_BINARY_OP_CODE_EQ:
		return a == b, nil
	case BinaryOpCode_BINARY_OP_CODE_NEQ:
		return a != b, nil
	case BinaryOpCode_BINARY_OP_CODE_LESS:
		return a < b, nil
	case BinaryOpCode_BINARY_OP_CODE_LESS_EQ:
		return a <= b, nil
	case BinaryOpCode_BINARY_OP_CODE_GREATER:
		return a > b, nil
	case BinaryOpCode_BINARY_OP_CODE_GREATER_EQ:
		return a >= b, nil
	default:
		return nil, unhandledEnum("u64 op code", op)
	}
}

func performOpU32(a, b uint32, op BinaryOpCode) (interface{}, error) {
	switch op {
	case BinaryOpCode_BINARY_OP_CODE_EQ:
		return a == b, nil
	case BinaryOpCode_BINARY_OP_CODE_NEQ:
		return a != b, nil
	case BinaryOpCode_BINARY_OP_CODE_LESS:
		return a < b, nil
	case BinaryOpCode_BINARY_OP_CODE_LESS_EQ:
		return a <= b, nil
	case BinaryOpCode_BINARY_OP_CODE_GREATER:
		return a > b, nil
	case BinaryOpCode_BINARY_OP_CODE_GREATER_EQ:
		return a >= b, nil
	default:
		return nil, unhandledEnum("u32 op code", op)
	}
}

func performOpU16(a, b uint16, op BinaryOpCode) (interface{}, error) {
	switch op {
	case BinaryOpCode_BINARY_OP_CODE_EQ:
		return a == b, nil
	case BinaryOpCode_BINARY_OP_CODE_NEQ:
		return a != b, nil
	case BinaryOpCode_BINARY_OP_CODE_LESS:
		return a < b, nil
	case BinaryOpCode_BINARY_OP_CODE_LESS_EQ:
		return a <= b, nil
	case BinaryOpCode_BINARY_OP_CODE_GREATER:
		return a > b, nil
	case BinaryOpCode_BINARY_OP_CODE_GREATER_EQ:
		return a >= b, nil
	default:
		return nil, unhandledEnum("u16 op code", op)
	}
}

func performOpU8(a, b uint8, op BinaryOpCode) (interface{}, error) {
	switch op {
	case BinaryOpCode_BINARY_OP_CODE_EQ:
		return a == b, nil
	case BinaryOpCode_BINARY_OP_CODE_NEQ:
		return a != b, nil
	case BinaryOpCode_BINARY_OP_CODE_LESS:
		return a < b, nil
	case BinaryOpCode_BINARY_OP_CODE_LESS_EQ:
		return a <= b, nil
	case BinaryOpCode_BINARY_OP_CODE_GREATER:
		return a > b, nil
	case BinaryOpCode_BINARY_OP_CODE_GREATER_EQ:
		return a >= b, nil
	default:
		return nil, unhandledEnum("u16 op code", op)
	}
}

func performOpBool(a, b bool, op BinaryOpCode) (interface{}, error) {
	switch op {
	case BinaryOpCode_BINARY_OP_CODE_EQ:
		return a == b, nil
	case BinaryOpCode_BINARY_OP_CODE_NEQ:
		return a != b, nil
	case BinaryOpCode_BINARY_OP_CODE_LESS:
		return boolInt(a) < boolInt(b), nil
	case BinaryOpCode_BINARY_OP_CODE_LESS_EQ:
		return boolInt(a) <= boolInt(b), nil
	case BinaryOpCode_BINARY_OP_CODE_GREATER:
		return boolInt(a) > boolInt(b), nil
	case BinaryOpCode_BINARY_OP_CODE_GREATER_EQ:
		return boolInt(a) >= boolInt(b), nil
	default:
		return nil, unhandledEnum("bool op code", op)
	}
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
