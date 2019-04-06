package binq

import (
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"testing"
)

func TestOpen_BadMagic(t *testing.T) {
	temp := NewTempFile(t)
	defer temp.Delete()

	// Write a bad magic number to the file we attempt to open as a binq file.
	func() {
		f, err := os.OpenFile(temp.Name(), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0777)
		if err != nil {
			t.Fatal(err)
		}
		defer mustClose(t, f)
		_, err = f.Write([]byte{0xff, 0xff, 0xff, 0xff})
		if err != nil {
			t.Fatal(err)
		}
	}()

	bq, err := Open(temp.Name())
	assert.EqualError(t, err, "invalid file: mismatched magic number")
	assert.Nil(t, bq)
}

func TestFile_Get_EmptyFile(t *testing.T) {
	temp := NewTempFile(t)
	defer temp.Delete()

	bq := mustOpenBinq(t, temp.Name())
	defer mustClose(t, bq)

	got, err := bq.Get([]byte{})
	assert.Error(t, err)
	assert.Nil(t, got)
}

func TestFile_Put_KeyTooLarge(t *testing.T) {
	var (
		key = []byte(strings.Repeat("X", MaxKeySize+1))
	)

	temp := NewTempFile(t)
	defer temp.Delete()

	bq := mustOpenBinq(t, temp.Name())
	defer mustClose(t, bq)

	err := bq.Put(key, []byte{})
	assert.Error(t, err)
}

func TestFile_PutGet_SingleValue(t *testing.T) {
	var (
		key   = []byte("hello")
		value = []byte("world")
	)
	temp := NewTempFile(t)
	defer temp.Delete()

	bq := mustOpenBinq(t, temp.Name())
	defer mustClose(t, bq)

	err := bq.Put(key, value)
	assert.NoError(t, err)

	got, err := bq.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, got)
}

func TestFile_PutGet_MultiValue(t *testing.T) {
	var (
		key1   = []byte("hello1")
		value1 = []byte("world1")
		key2   = []byte("hello2")
		value2 = []byte("world2")
	)
	temp := NewTempFile(t)
	defer temp.Delete()

	bq := mustOpenBinq(t, temp.Name())
	defer mustClose(t, bq)

	err := bq.Put(key1, value1)
	assert.NoError(t, err)
	err = bq.Put(key2, value2)
	assert.NoError(t, err)

	got, err := bq.Get(key1)
	assert.NoError(t, err)
	assert.Equal(t, value1, got)

	got, err = bq.Get(key2)
	assert.NoError(t, err)
	assert.Equal(t, value2, got)
}

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
