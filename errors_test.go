package binq

import (
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMultiError_NoError(t *testing.T) {
	err := multiError("test")
	assert.NoError(t, err)
}

func TestMultiError_NilErrors(t *testing.T) {
	err := multiError("test", nil, nil)
	assert.NoError(t, err)
}

func TestMultiError_SingleError(t *testing.T) {
	err := multiError("test", nil, errors.New("fail"), nil)
	assert.EqualError(t, err, "test: fail")
}

func TestMultiError_MultipleErrors(t *testing.T) {
	err := multiError("test", nil, errors.New("fail1"), nil, errors.New("fail2"))
	assert.EqualError(t, err, "test (multiple errors): fail1, fail2")
}
