package binq

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// TestUpscaleUintTypes asserts that uint types can be upscaled between each other.
func TestUpscaleUintTypes(t *testing.T) {
	t.Parallel()
	for _, aType := range uintTypes {
		aType := aType
		t.Run(aType.String(), func(t *testing.T) {
			t.Parallel()
			for _, bType := range uintTypes {
				bType := bType
				t.Run(bType.String(), func(t *testing.T) {
					t.Parallel()
					aFunc, bFunc, resultType, err := getUpscaler(aType, bType)
					assert.NoError(t, err)
					assert.NotEqual(t, ReturnType_RETURN_TYPE_UNKNOWN, resultType)
					assert.NotNil(t, aFunc)
					assert.NotNil(t, bFunc)

					aValue := makeReturnTypeValue(t, aType)
					bValue := makeReturnTypeValue(t, bType)
					aUpscaled := aFunc(aValue)
					bUpscaled := bFunc(bValue)
					assert.IsType(t, makeReturnTypeValue(t, resultType), aUpscaled)
					assert.IsType(t, makeReturnTypeValue(t, resultType), bUpscaled)
				})
			}
		})
	}
}
