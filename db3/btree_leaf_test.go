package db3

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"unsafe"
)

func TestLeafNode_putGetCell(t *testing.T) {
	const (
		dataSize = 11
	)
	sizer := dataSizer{dataSize}
	leaf := &leafNode{}
	leaf.init()

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
	leaf := &leafNode{}
	leaf.init()

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
	testWithLimitedTable(t, dataSize, func(t *testing.T, table *Table) {
		cursor := &Cursor{table: table, cellNum: 0}
		leaf := &leafNode{}
		leaf.init()

		// Insert the 2nd value at position 0
		must(t, leaf.insert(cursor, key2, val2))
		// Insert the 1st value at position 0
		must(t, leaf.insert(cursor, key1, val1))

		assert.Equal(t, cellptr(2), leaf.numCells)
		expected := makeBytes(t, key1, val1, key2, val2)
		assert.Equal(t, expected, leaf.cellData[:len(expected)])
	})
}

func TestLeafNodeInsert_withSpace(t *testing.T) {
	testWithLimitedTable(t, uint16(unsafe.Sizeof(uint64(0))), func(t *testing.T, table *Table) {
		leaf := &leafNode{}
		leaf.init()

		cursor := &Cursor{table: table, cellNum: 0}
		must(t, leaf.insert(cursor, 6, makeUint64Value(0x66)))
		if !verifyCellData(t, table, leaf,
			celldata{6, 0x66}) {
			return
		}

		cursor = &Cursor{table: table, cellNum: 1}
		must(t, leaf.insert(cursor, 8, makeUint64Value(0x88)))
		if !verifyCellData(t, table, leaf,
			celldata{6, 0x66},
			celldata{8, 0x88}) {
			return
		}

		cursor = &Cursor{table: table, cellNum: 1}
		must(t, leaf.insert(cursor, 7, makeUint64Value(0x77)))
		if !verifyCellData(t, table, leaf,
			celldata{6, 0x66},
			celldata{7, 0x77},
			celldata{8, 0x88}) {
			return
		}
	})
}

func makeUint64Value(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}

func getUint64Value(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

type celldata struct {
	key   KeyType
	value uint64
}

func verifyCellData(t *testing.T, table *Table, leaf *leafNode, data ...celldata) bool {
	result := true
	result = assert.Equal(t, cellptr(len(data)), leaf.numCells)
	for index, dat := range data {
		cell := cellptr(index)
		result = assert.Equal(t, dat.key, leaf.getCellKey(table, cell)) && result
		result = assert.Equal(t, dat.value, getUint64Value(leaf.getCellValue(table, cell))) && result
	}
	return result
}

func TestLeafNodeInsert_withoutSpace_insertLeftNode(t *testing.T) {
	const size = leafNodeMaxCellData/3 - keySize

	testWithLimitedTable(t, uint16(size), func(t *testing.T, table *Table) {
		mustPage := func(pg PagePointer) *Page {
			page, err := table.pager.GetPage(pg)
			must(t, err)
			return page
		}
		mustLeaf := func(pg PagePointer) *leafNode {
			page := mustPage(pg)
			leaf := pageToLeafNode(page)
			if !leaf.isLeaf {
				t.Fatal("unexpected branch")
			}
			return leaf
		}
		leaf := mustLeaf(table.rootPageNum)

		if max := leaf.getMaxNumCells(table); max != 3 {
			t.Fatalf("unexpected max cells: %d", max)
		}

		cursor := &Cursor{table: table, cellNum: 0}
		must(t, leaf.insert(cursor, 3, makeUint64Value(0x33)))
		cursor = &Cursor{table: table, cellNum: 1}
		must(t, leaf.insert(cursor, 5, makeUint64Value(0x55)))
		cursor = &Cursor{table: table, cellNum: 2}
		must(t, leaf.insert(cursor, 7, makeUint64Value(0x77)))
		if !verifyCellData(t, table, leaf,
			celldata{3, 0x33},
			celldata{5, 0x55},
			celldata{7, 0x77}) {
			fmt.Println(leaf.String(table))
			return
		}

		cursor = &Cursor{table: table, cellNum: 0}
		must(t, leaf.insert(cursor, 1, makeUint64Value(0x11)))
		leaf = mustLeaf(2)
		if !verifyCellData(t, table, leaf,
			celldata{1, 0x11},
			celldata{3, 0x33}) {
			fmt.Println(leaf.String(table))
			return
		}
		leaf = mustLeaf(1)
		if !verifyCellData(t, table, leaf,
			celldata{5, 0x55},
			celldata{7, 0x77}) {
			fmt.Println(leaf.String(table))
			return
		}
	})
}

func TestLeafNodeInsert_withoutSpace_insertRightNode(t *testing.T) {
	const size = leafNodeMaxCellData/3 - keySize

	testWithLimitedTable(t, uint16(size), func(t *testing.T, table *Table) {
		mustPage := func(pg PagePointer) *Page {
			page, err := table.pager.GetPage(pg)
			must(t, err)
			return page
		}
		mustLeaf := func(pg PagePointer) *leafNode {
			page := mustPage(pg)
			leaf := pageToLeafNode(page)
			if !leaf.isLeaf {
				t.Fatal("unexpected branch")
			}
			return leaf
		}
		leaf := mustLeaf(table.rootPageNum)

		if max := leaf.getMaxNumCells(table); max != 3 {
			t.Fatalf("unexpected max cells: %d", max)
		}

		cursor := &Cursor{table: table, cellNum: 0}
		must(t, leaf.insert(cursor, 3, makeUint64Value(0x33)))
		cursor = &Cursor{table: table, cellNum: 1}
		must(t, leaf.insert(cursor, 5, makeUint64Value(0x55)))
		cursor = &Cursor{table: table, cellNum: 2}
		must(t, leaf.insert(cursor, 7, makeUint64Value(0x77)))
		if !verifyCellData(t, table, leaf,
			celldata{3, 0x33},
			celldata{5, 0x55},
			celldata{7, 0x77}) {
			return
		}

		cursor = &Cursor{table: table, cellNum: 3}
		must(t, leaf.insert(cursor, 9, makeUint64Value(0x99)))
		leaf = mustLeaf(2)
		if !verifyCellData(t, table, leaf,
			celldata{3, 0x33},
			celldata{5, 0x55}) {
			return
		}

		leaf = mustLeaf(1)
		if !verifyCellData(t, table, leaf,
			celldata{7, 0x77},
			celldata{9, 0x99}) {
			return
		}

	})
}
