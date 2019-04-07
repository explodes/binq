package binq

import (
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

	got, err := bq.Get(testContext(), []byte{})
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

	err := bq.Put(testContext(), key, []byte{})
	assert.Error(t, err)
}

func TestFile_Put_Overwrite_InPlace(t *testing.T) {
	var (
		key          = []byte("key")
		smallValue   = []byte("some small value")
		smallerValue = []byte("smaller value")
	)

	temp := NewTempFile(t)
	defer temp.Delete()

	bq := mustOpenBinq(t, temp.Name())
	defer mustClose(t, bq)

	must(t, bq.Put(testContext(), key, smallValue))

	err := bq.Put(testContext(), key, smallerValue)
	assert.NoError(t, err)

	got, err := bq.Get(testContext(), key)
	assert.NoError(t, err)
	assert.Equal(t, smallerValue, got)
}

func TestFile_Put_Overwrite_OutOfPlace(t *testing.T) {
	var (
		key         = []byte("key")
		smallValue  = []byte("some small value")
		largerValue = []byte("this value is much larger than the first value")
	)

	temp := NewTempFile(t)
	defer temp.Delete()

	bq := mustOpenBinq(t, temp.Name())
	defer mustClose(t, bq)

	must(t, bq.Put(testContext(), key, smallValue))

	err := bq.Put(testContext(), key, largerValue)
	assert.NoError(t, err)

	got, err := bq.Get(testContext(), key)
	assert.NoError(t, err)
	assert.Equal(t, largerValue, got)
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

	err := bq.Put(testContext(), key, value)
	assert.NoError(t, err)

	got, err := bq.Get(testContext(), key)
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

	err := bq.Put(testContext(), key1, value1)
	assert.NoError(t, err)
	err = bq.Put(testContext(), key2, value2)
	assert.NoError(t, err)

	got, err := bq.Get(testContext(), key1)
	assert.NoError(t, err)
	assert.Equal(t, value1, got)

	got, err = bq.Get(testContext(), key2)
	assert.NoError(t, err)
	assert.Equal(t, value2, got)
}

func TestFile_PutGet_NotFound(t *testing.T) {
	var (
		key1   = []byte("hello1")
		value1 = []byte("world1")
		key2   = []byte("hello2")
		value2 = []byte("world2")
		key3   = []byte("hello3")
	)
	temp := NewTempFile(t)
	defer temp.Delete()

	bq := mustOpenBinq(t, temp.Name())
	defer mustClose(t, bq)

	must(t, bq.Put(testContext(), key1, value1))
	must(t, bq.Put(testContext(), key2, value2))

	got, err := bq.Get(testContext(), key3)
	assert.Equal(t, ErrNotFound, err)
	assert.Nil(t, got)
}

func TestFile_PutGet_EarlyExit(t *testing.T) {
	var (
		key2   = []byte("hello2")
		value2 = []byte("world2")
		key3   = []byte("hello3")
		key4   = []byte("hello4")
		value4 = []byte("world4")
	)
	temp := NewTempFile(t)
	defer temp.Delete()

	bq := mustOpenBinq(t, temp.Name())
	defer mustClose(t, bq)

	must(t, bq.Put(testContext(), key2, value2))
	must(t, bq.Put(testContext(), key4, value4))

	got, err := bq.Get(testContext(), key3)
	assert.Equal(t, ErrNotFound, err)
	assert.Nil(t, got)
}

func TestFile_Scan(t *testing.T) {

	temp := NewTempFile(t)
	defer temp.Delete()

	bq := mustOpenBinq(t, temp.Name())
	defer mustClose(t, bq)

	must(t, bq.Put(testContext(), []byte("a"), []byte("a")))
	must(t, bq.Put(testContext(), []byte("d"), []byte("d")))
	must(t, bq.Put(testContext(), []byte("b"), []byte("b")))
	must(t, bq.Put(testContext(), []byte("c"), []byte("c")))
	must(t, bq.Put(testContext(), []byte("e"), []byte("e")))

	var keys, values []string
	err := bq.Scan(testContext(), func(key, value []byte) (stop bool) {
		keys = append(keys, string(key))
		values = append(values, string(value))
		return false
	})
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c", "d", "e"}, keys)
	assert.Equal(t, []string{"a", "b", "c", "d", "e"}, values)
}
