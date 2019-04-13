package db2

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/pkg/errors"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"
	"unsafe"
)

const (
	userReadWrite = 0600
)

type testType interface {
	Helper()
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
}

type TempFile struct {
	t    testType
	name string
}

func NewTempFile(t testType) *TempFile {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	randBuf := make([]byte, 8)
	_, err := rng.Read(randBuf)
	if err != nil {
		t.Fatal(err)
	}
	fileName := fmt.Sprintf("db2_test_%d_%s", time.Now().UnixNano(), hex.EncodeToString(randBuf))
	filePath := path.Join(os.TempDir(), fileName)
	return &TempFile{
		t:    t,
		name: filePath,
	}
}

func (t *TempFile) FullPath() string {
	return t.name
}

func (t *TempFile) Delete() {
	t.t.Helper()
	err := os.Remove(t.name)
	if err != nil && !os.IsNotExist(err) {
		t.t.Error(err)
	}
}

var _ DataSizer = dataSizer{}

type dataSizer struct {
	size uint16
}

func (d dataSizer) DataSize() uint16 {
	return d.size
}

func must(t testType, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func encodeBytes(t testType, size uintptr, objs ...interface{}) []byte {
	b := makeBytes(t, objs...)
	if uintptr(len(b)) < size {
		rem := size - uintptr(len(b))
		add := make([]byte, rem)
		b = append(b, add...)
	}
	return b
}

func makeBytes(t testType, objs ...interface{}) []byte {
	t.Helper()

	var out []byte
	for _, obj := range objs {
		var b []byte
		switch val := obj.(type) {
		case []byte:
			b = val
		case string:
			b = []byte(val)
		case uint64:
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, val)
			b = buf
		case uint32:
			buf := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf, val)
			b = buf
		case uint16:
			buf := make([]byte, 2)
			binary.LittleEndian.PutUint16(buf, val)
			b = buf
		case uint8:
			b = []byte{byte(val)}
		default:
			t.Fatal(errors.Errorf("cannot serialize bytes of %T", val))
		}
		out = append(out, b...)
	}
	return out
}

func testWithTable(t *testing.T, tableDataSize uint16, f func(t *testing.T, table *Table)) {
	t.Helper()

	file := NewTempFile(t)
	defer file.Delete()

	pager, err := OpenPager(file.FullPath(), os.O_RDWR|os.O_CREATE, userReadWrite)
	must(t, err)
	defer func() {
		must(t, pager.Close())
	}()

	table, err := Open(pager, tableDataSize)
	must(t, err)

	f(t, table)
}

const (
	cstackRowSize     = 4 + 32 + 255
	sentinelPadSize   = cstackRowSize - unsafe.Sizeof(sentinelHeader{})
	sentinelValueSize = unsafe.Sizeof(sentinelValue{})
)

type sentinelHeader struct {
	expectedKey   KeyType
	sentinelStart uint64
	sentinelEnd   uint64
}

type sentinelValue struct {
	sentinelHeader
	pad [sentinelPadSize]byte
}

func newSentinelValue(t testType, key KeyType) *sentinelValue {
	t.Helper()

	sentinelNumber := uint64(^(2 * key))

	var pad [sentinelPadSize]byte
	for i := 0; i < int(sentinelPadSize); i++ {
		pad[i] = byte(i)
	}
	return &sentinelValue{
		sentinelHeader: sentinelHeader{
			expectedKey:   key,
			sentinelStart: sentinelNumber,
			sentinelEnd:   sentinelNumber,
		},
		pad: pad,
	}
}

func parseSentinelValue(t testType, b []byte) *sentinelValue {
	t.Helper()
	x := (*sentinelValue)(unsafe.Pointer(&b[0]))
	return x
}

func (v sentinelValue) wellFormed(t testType, expectedKey KeyType) bool {
	t.Helper()
	if v.expectedKey != expectedKey {
		t.Errorf("unexpected key, got %d want %d", v.expectedKey, expectedKey)
		return false
	}
	sentinelNumber := uint64(^(2 * expectedKey))
	if sentinelNumber != v.sentinelStart {
		t.Errorf("unexpected sentinelStart, got %d want %d", v.sentinelStart, sentinelNumber)
		return false
	}
	for i := 0; i < int(sentinelPadSize); i++ {
		if v.pad[i] != byte(i) {
			t.Errorf("unexpected pad byte at %d, got %d want %d", i, v.pad[i], byte(i))
			return false
		}
	}
	if sentinelNumber != v.sentinelEnd {
		t.Errorf("unexpected sentinelEnd, got %d want %d", v.sentinelEnd, sentinelNumber)
		return false
	}
	return true
}

func (v sentinelValue) toBytes(t testType) []byte {
	t.Helper()
	arr := *(*[sentinelValueSize]byte)(unsafe.Pointer(&v))
	return arr[:]
}

func (v *sentinelValue) toInsertStatement(t testType, table *Table) *insertStatement {
	t.Helper()

	return &insertStatement{
		table: table,
		key:   v.expectedKey,
		value: v.toBytes(t),
	}
}

func cursorCount(t testType, c *Cursor) int {
	t.Helper()
	count := 0
	for ; !c.End(); c.Next() {
		_, _, err := c.Value()
		if err != nil {
			t.Fatalf("count not perform count: %v", err)
		}
		count++
	}
	return count
}

type cursorValue struct {
	key   KeyType
	value []byte
}

func cursorConsume(t testType, c *Cursor) []cursorValue {
	t.Helper()
	var out []cursorValue
	for ; !c.End(); c.Next() {
		key, value, err := c.Value()
		if err != nil {
			t.Fatalf("count not perform consume: %v", err)
		}
		out = append(out, cursorValue{key: key, value: value})
	}
	return out
}
