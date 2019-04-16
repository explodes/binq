package binqtree

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTableOpen(t *testing.T) {
	testWithTable(t, 16, func(t *testing.T, table *Table) {
		assert.NotNil(t, table)
	})
}

func TestTableStart(t *testing.T) {
	testWithTable(t, 16, func(t *testing.T, table *Table) {
		cursor, err := table.Start()
		assert.NoError(t, err)
		assert.NotNil(t, cursor)
	})
}
