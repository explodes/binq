package binq

import "github.com/pkg/errors"

type upscaleFunc func(val interface{}) interface{}

var (
	upscaleFunctionMap = make(map[int64]upscaleFunc)
)

func init() {
	// Upscale bool types
	registerUpscale(ReturnType_RETURN_TYPE_BOOL, ReturnType_RETURN_TYPE_U8, func(val interface{}) interface{} {
		if val.(bool) {
			return uint8(1)
		} else {
			return uint8(0)
		}
	})
	registerUpscale(ReturnType_RETURN_TYPE_BOOL, ReturnType_RETURN_TYPE_U16, func(val interface{}) interface{} {
		if val.(bool) {
			return uint16(1)
		} else {
			return uint16(0)
		}
	})
	registerUpscale(ReturnType_RETURN_TYPE_BOOL, ReturnType_RETURN_TYPE_U32, func(val interface{}) interface{} {
		if val.(bool) {
			return uint32(1)
		} else {
			return uint32(0)
		}
	})
	registerUpscale(ReturnType_RETURN_TYPE_BOOL, ReturnType_RETURN_TYPE_U64, func(val interface{}) interface{} {
		if val.(bool) {
			return uint64(1)
		} else {
			return uint64(0)
		}
	})

	// Upscale u8 types
	registerUpscale(ReturnType_RETURN_TYPE_U8, ReturnType_RETURN_TYPE_U16, func(val interface{}) interface{} {
		return uint16(val.(uint8))
	})
	registerUpscale(ReturnType_RETURN_TYPE_U8, ReturnType_RETURN_TYPE_U32, func(val interface{}) interface{} {
		return uint32(val.(uint8))
	})
	registerUpscale(ReturnType_RETURN_TYPE_U8, ReturnType_RETURN_TYPE_U64, func(val interface{}) interface{} {
		return uint64(val.(uint8))
	})

	// Upscale u16 types
	registerUpscale(ReturnType_RETURN_TYPE_U16, ReturnType_RETURN_TYPE_U32, func(val interface{}) interface{} {
		return uint32(val.(uint16))
	})
	registerUpscale(ReturnType_RETURN_TYPE_U16, ReturnType_RETURN_TYPE_U64, func(val interface{}) interface{} {
		return uint64(val.(uint16))
	})

	// Upscale u32 types
	registerUpscale(ReturnType_RETURN_TYPE_U32, ReturnType_RETURN_TYPE_U64, func(val interface{}) interface{} {
		return uint64(val.(uint32))
	})
}

func identityUpscale(val interface{}) interface{} {
	return val
}

func upscaleKey(typeA, typeB ReturnType) int64 {
	return (int64(typeA) << 32) | int64(typeB)
}

func registerUpscale(typeA, typeB ReturnType, upscaleFunc upscaleFunc) {
	key := upscaleKey(typeA, typeB)
	upscaleFunctionMap[key] = upscaleFunc
}

func getUpscaler(typeA, typeB ReturnType) (upscaledA, upscaledB upscaleFunc, valueTypes ReturnType, err error) {
	// Same types, no conversion required.
	if typeA == typeB {
		return identityUpscale, identityUpscale, typeA, nil
	}
	// Upscale A to B?
	leftToRightKey := upscaleKey(typeA, typeB)
	if upscaler, ok := upscaleFunctionMap[leftToRightKey]; ok {
		return upscaler, identityUpscale, typeB, nil
	}
	// Upscale B to A?
	rightToLeftKey := upscaleKey(typeB, typeA)
	if upscaler, ok := upscaleFunctionMap[rightToLeftKey]; ok {
		return identityUpscale, upscaler, typeA, nil
	}
	// Cannot upscale.
	return nil, nil, ReturnType_RETURN_TYPE_UNKNOWN, errors.Errorf("cannot upscale %s to %s", typeA, typeB)
}
