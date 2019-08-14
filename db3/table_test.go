package db3

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTableOpen(t *testing.T) {
	testWithLimitedTable(t, 16,  func(t *testing.T, table *Table) {
		assert.NotNil(t, table)
	})
}

func TestTableStart(t *testing.T) {
	testWithLimitedTable(t, 16,  func(t *testing.T, table *Table) {
		cursor, err := table.Start()
		assert.NoError(t, err)
		assert.NotNil(t, cursor)
	})
}
