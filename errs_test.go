package binq

import (
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWrap(t *testing.T) {
	t.Parallel()

	assert.NoError(t, wrap(nil, "msg"))
	assert.EqualError(t, wrap(errors.New("err"), "msg"), "msg: err")
}
func TestWrap2(t *testing.T) {
	t.Parallel()

	err1 := errors.New("err1")
	err2 := errors.New("err2")

	assert.NoError(t, wrap2(nil, nil, "msg"))
	assert.EqualError(t, wrap2(err1, nil, "msg"), "msg: err1")
	assert.EqualError(t, wrap2(err2, nil, "msg"), "msg: err2")
	assert.EqualError(t, wrap2(err1, err2, "msg"), "msg (multiple errors): err1, err2")
}
func TestWrap3(t *testing.T) {
	t.Parallel()

	err1 := errors.New("err1")
	err2 := errors.New("err2")
	err3 := errors.New("err3")

	assert.NoError(t, wrap3(nil, nil, nil, "msg"))
	assert.EqualError(t, wrap3(err1, nil, nil, "msg"), "msg: err1")
	assert.EqualError(t, wrap3(nil, err2, nil, "msg"), "msg: err2")
	assert.EqualError(t, wrap3(nil, nil, err3, "msg"), "msg: err3")
	assert.EqualError(t, wrap3(err1, err2, nil, "msg"), "msg (multiple errors): err1, err2")
	assert.EqualError(t, wrap3(nil, err2, err3, "msg"), "msg (multiple errors): err2, err3")
	assert.EqualError(t, wrap3(err1, err2, err3, "msg"), "msg (multiple errors): err1, err2, err3")
}
