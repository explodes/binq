package db3

import (
	"github.com/pkg/errors"
	"unsafe"
)

const (
	sentinelsPerLeaf  = maxValues
	sentinelPadSize   = leafNodeMaxCellData/sentinelsPerLeaf - unsafe.Sizeof(KeyType(0)) - unsafe.Sizeof(sentinelHeader{}) -10
	sentinelValueSize = unsafe.Sizeof(sentinelValue{})
)

func init() {
	table := &Table{dataSize: uint16(sentinelValueSize)}
	leaf := &leafNode{}
	leaf.init()
	if leaf.getMaxNumCells(table) != sentinelsPerLeaf {
		panic(errors.Errorf("unexpected number of sentinels per leaf: want %d got %d", sentinelsPerLeaf, leaf.getMaxNumCells(table)))
	}
}

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
