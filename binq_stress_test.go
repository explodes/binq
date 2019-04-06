package binq

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strconv"
	"strings"
	"testing"
)

func TestFile_Put_StressTest(t *testing.T) {
	const (
		maxIterations = 1e3
		maxValueSize  = 1e7
	)
	for iterations := 10; iterations <= maxIterations; iterations *= 10 {
		t.Run(fmt.Sprintf("iterations%d", iterations), func(t *testing.T) {
			for valueSize := 10; valueSize < maxValueSize; valueSize *= 10 {
				t.Run(fmt.Sprintf("valueSize%d", valueSize), func(t *testing.T) {
					stressTest(t, iterations, valueSize)
				})
			}
		})
	}
}

func TestFile_Put_StressTest_Small(t *testing.T) {
	stressTest(t, 500, 500)
}

func stressTest(t *testing.T, iterations, valueSize int) {
	var (
		value = []byte(strings.Repeat("X", valueSize))
	)
	temp := NewTempFile(t)
	defer temp.Delete()

	bq := mustOpenBinq(t, temp.Name())
	defer mustClose(t, bq)

	var key []byte
	for i := 0; i <= iterations; i++ {
		key = []byte(strconv.Itoa(i))
		err := bq.Put(key, value)
		assert.NoError(t, err)
	}

	got, err := bq.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, got)
}
