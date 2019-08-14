package binq



func makeValueExpression(valueType ValueType, offset uint64) *Expression {
	return &Expression{
		Expression: &Expression_Value{
			Value: &Value{
				Type: valueType,
				Jump: &Jump{
					Jump: &Jump_Offset{
						Offset: offset,
					},
				},
			},
		},
	}
}

func makeScalarExpression(t TestType, value interface{}) *Expression {
	scalar := &Scalar{}
	switch vt := value.(type) {
	case bool:
		scalar.Value = &Scalar_Bool{Bool: vt}
	case uint32:
		scalar.Value = &Scalar_U32{U32: vt}
	case u32le:
		scalar.Value = &Scalar_U32{U32: uint32(vt)}
	case u32be:
		scalar.Value = &Scalar_U32{U32: uint32(vt)}
	case uint64:
		scalar.Value = &Scalar_U64{U64: vt}
	case u64le:
		scalar.Value = &Scalar_U64{U64: uint64(vt)}
	case u64be:
		scalar.Value = &Scalar_U64{U64: uint64(vt)}
	default:
		t.Fatal(unhandledType("scalar value", vt))
		return nil
	}
	return &Expression{
		Expression: &Expression_Scalar{
			Scalar: scalar,
		},
	}
}
