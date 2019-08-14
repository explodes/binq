package binq

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTokenString(t *testing.T) {
	t.Parallel()
	for i := TokenUnknown; i < tokenMax; i++ {
		s := i.String()
		assert.NotEqual(t, "<unknown>", s)
	}
}
