package db3

import (
	"fmt"
	"os"
	"testing"
)

const (
	maxKeys     = 3
	maxChildren = maxKeys + 1 // 3 keys + 1 right child
	maxValues   = 3
)

func init() {
	if uintptr(maxKeys) > branchNodeMaxCells {
		panic("too many keys")
	}
}

func testWithLimitedTable(t *testing.T, rowSize uint16, f func(t *testing.T, table *Table)) {
	t.Helper()
	if err := _setMaxKeysPerBranchOverride(maxKeys); err != nil {
		t.Fatal(err)
	}

	file := NewTempFile(t)
	defer file.Delete()

	pager, err := OpenPager(file.FullPath(), os.O_RDWR|os.O_CREATE, userReadWrite)
	must(t, err)
	defer func() {
		must(t, pager.Close())
	}()

	table, err := Open(pager, rowSize)
	must(t, err)

	f(t, table)
}

func dumpTable(table *Table) {
	fmt.Println("PAGES")
	table.printPages()
	fmt.Println("TABLE")
	table.printTree()
}
