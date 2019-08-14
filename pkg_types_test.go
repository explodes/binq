package binq

var uintValueTypes = []ValueType{
	ValueType_VALUE_TYPE_U8,
	ValueType_VALUE_TYPE_U16LE,
	ValueType_VALUE_TYPE_U16BE,
	ValueType_VALUE_TYPE_U32LE,
	ValueType_VALUE_TYPE_U32BE,
	ValueType_VALUE_TYPE_U64LE,
	ValueType_VALUE_TYPE_U64BE,
}

func makeValueTypeValue(t TestType, valueType ValueType) interface{} {
	t.Helper()
	switch valueType {
	case ValueType_VALUE_TYPE_UNKNOWN:
		t.Fatal("cannot create unknown type")
		return nil
	case ValueType_VALUE_TYPE_U8:
		return u8(0)
	case ValueType_VALUE_TYPE_U16LE:
		return u16le(0)
	case ValueType_VALUE_TYPE_U16BE:
		return u16be(0)
	case ValueType_VALUE_TYPE_U32LE:
		return u32le(0)
	case ValueType_VALUE_TYPE_U32BE:
		return u32be(0)
	case ValueType_VALUE_TYPE_U64LE:
		return u64le(0)
	case ValueType_VALUE_TYPE_U64BE:
		return u64be(0)
	default:
		t.Fatal(unhandledEnum("value type", valueType))
		return nil
	}
}

func makeReturnTypeValue(t TestType, returnType ReturnType) interface{} {
	t.Helper()
	switch returnType {
	case ReturnType_RETURN_TYPE_UNKNOWN:
		t.Fatal("cannot create unknown type")
		return nil
	case ReturnType_RETURN_TYPE_BOOL:
		return false
	case ReturnType_RETURN_TYPE_U8:
		return uint8(0)
	case ReturnType_RETURN_TYPE_U16:
		return uint16(0)
	case ReturnType_RETURN_TYPE_U32:
		return uint32(0)
	case ReturnType_RETURN_TYPE_U64:
		return uint64(0)
	default:
		t.Fatal(unhandledEnum("return type", returnType))
		return nil
	}
}
