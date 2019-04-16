package binqtree

import (
	"github.com/pkg/errors"
)

// Statement is a database operation that does not return results.
type Statement interface {
	// Execute executes this statement.
	Execute() error
}

// Query is a database operation that returns a Cursor.
type Query interface {
	// Query executes this query.
	Query() (*Cursor, error)
}

var _ Statement = (*insertStatement)(nil)

// insertStatement is a statement that inserts data into a specific table.
type insertStatement struct {
	// table is the table to insert into.
	table *Table
	// key is the key of the data to insert.
	key KeyType
	// value is the data to insert.
	value []byte
}

// Execute executes this insert statement.
func (s *insertStatement) Execute() error {
	// Validate the input data.
	if len(s.value) != int(s.table.dataSize) {
		return errors.Errorf("invalid insert data length %d, want %d", len(s.value), s.table.dataSize)
	}

	// Find the location to insert a record.
	cursor, err := s.table.Find(s.key)
	if err != nil {
		return wrap(err, "unable to get cursor")
	}

	// Get the page pointed to by the cursor.
	insertPage, err := s.table.pager.GetPage(cursor.pageNum)
	if err != nil {
		return wrap(err, "unable to get page")
	}
	leaf := pageToLeafNode(insertPage)

	// Check for a duplicate key.
	if cursor.cellNum < leaf.numCells {
		// The position of our cursor on an identical key?
		keyAtCursor := leaf.getCellKey(s.table, cursor.cellNum)
		if keyAtCursor == s.key {
			// Duplicate key found.
			return errors.Errorf("cannot insert duplicate key %v", keyAtCursor)
		}
	}

	// Insert the data.
	if err := leaf.insert(cursor, s.key, s.value); err != nil {
		return wrap(err, "unable to insert record")
	}

	return nil
}

var _ Query = (*selectStatement)(nil)

// selectStatement is a Query that gets a Cursor for the whole table.
type selectStatement struct {
	table *Table
}

// selectEntireTable returns an unfiltered select statement
// for the table.
func selectEntireTable(table *Table) Query {
	return &selectStatement{
		table: table,
	}
}

// Query executes this select statement.
func (s *selectStatement) Query() (*Cursor, error) {
	cursor, err := s.table.Start()
	if err != nil {
		return nil, wrap(err, "unable to get cursor")
	}
	return cursor, nil
}
