package binqtree

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"unsafe"
)

func TestBtreeSizes(t *testing.T) {
	assert.True(t, unsafe.Sizeof(branchNode{}) <= PageSize, "A branch node should be able to fit in a single page.")
	assert.True(t, unsafe.Sizeof(leafNode{}) <= PageSize, "A leaf node should be able to fit in a single page.")
}

func TestKeyFromBytes(t *testing.T) {
	bin := []byte{0xff, 0xee, 0xdd, 0xcc}
	key := keyFromBytes(bin)
	assert.Equal(t, KeyType(0xccddeeff), key)
}

func TestKey_encodeToBytes(t *testing.T) {
	bin := make([]byte, unsafe.Sizeof(KeyType(0)))
	key := KeyType(0xccddeeff)
	encodeKeyToBytes(key, bin)
	assert.Equal(t, []byte{0xff, 0xee, 0xdd, 0xcc}, bin)
}

func TestLeafNode_putGetCell(t *testing.T) {
	const (
		dataSize = 11
	)
	sizer := dataSizer{dataSize}
	leaf := leafNode{}

	leaf.putCell(sizer, 2, 8, []byte("hello world"))
	leaf.putCell(sizer, 3, 9, []byte("dlrow olleh"))

	k1, v1 := leaf.getCell(sizer, 2)
	assert.Equal(t, KeyType(8), k1)
	assert.Equal(t, []byte("hello world"), v1)
	assert.Equal(t, k1, leaf.getCellKey(sizer, 2))
	assert.Equal(t, v1, leaf.getCellValue(sizer, 2))

	k2, v2 := leaf.getCell(sizer, 3)
	assert.Equal(t, KeyType(9), k2)
	assert.Equal(t, []byte("dlrow olleh"), v2)
	assert.Equal(t, k2, leaf.getCellKey(sizer, 3))
	assert.Equal(t, v2, leaf.getCellValue(sizer, 3))
}

func TestLeafNode_numCells(t *testing.T) {
	const (
		dataSize = 11
	)
	sizer := dataSizer{dataSize}
	leaf := leafNode{}

	expectedNumCells := (PageSize - unsafe.Sizeof(leafNodeHeader{})) / (unsafe.Sizeof(KeyType(0)) + dataSize)
	assert.Equal(t, cellptr(expectedNumCells), leaf.getMaxNumCells(sizer))
}

func TestLeafNode_insert_withCellSpaceRemaining(t *testing.T) {
	const (
		dataSize = 11
	)
	var (
		key1 = KeyType(9)
		val1 = []byte("hello world")
		key2 = KeyType(10)
		val2 = []byte("hey world!!")
	)
	testWithTable(t, dataSize, func(t *testing.T, table *Table) {
		cursor := &Cursor{table: table, cellNum: 0}
		leaf := leafNode{}

		// Insert the 2nd value at position 0
		must(t, leaf.insert(cursor, key2, val2))
		// Insert the 1st value at position 0
		must(t, leaf.insert(cursor, key1, val1))

		assert.Equal(t, cellptr(2), leaf.numCells)
		expected := makeBytes(t, key1, val1, key2, val2)
		assert.Equal(t, expected, leaf.cellData[:len(expected)])
	})
}
