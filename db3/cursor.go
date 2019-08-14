package db3

import "github.com/pkg/errors"

// Cursor is an object used to navigate rows in a Table.
type Cursor struct {
	// table is the to navigate.
	table *Table
	// pageNum is the current page pointed to.
	pageNum PagePointer
	// cellNum is the current cell pointed to.
	cellNum cellptr
	// endOfTable indicates a position one past the last
	// cell in the table.
	endOfTable bool
	// advanceError stores any error encountered when
	// advancing the cursor.
	advanceError error
}

// Value gets the value pointed to by this cursor.
func (c *Cursor) Value() (key KeyType, value []byte, err error) {
	// Return any previous error we've encountered.
	if c.advanceError != nil {
		return zeroKey, nil, c.advanceError
	}

	// Get the current page.
	page, err := c.table.pager.GetPage(c.pageNum)
	if err != nil {
		// Save this error.
		c.advanceError = errors.Wrap(err, "unable to get page")
		return zeroKey, nil, c.advanceError
	}

	// We always point to a leaf node.
	leaf := pageToLeafNode(page)

	//  Get the cell data.
	key, value = leaf.getCell(c.table, c.cellNum)

	return key, value, nil
}

// Next advances the cursor to the next position.
func (c *Cursor) Next() {
	// Return if we've previously encountered any error.
	// Return if we've reached the end of the table.
	if c.advanceError != nil || c.endOfTable {
		return
	}

	// Get the current page.
	page, err := c.table.pager.GetPage(c.pageNum)
	if err != nil {
		// Save this error.
		c.advanceError = errors.Wrap(err, "unable to get page")
		return
	}

	// We always point to a leaf node.
	leaf := pageToLeafNode(page)

	// Advance.
	if c.cellNum+1 < leaf.numCells {
		// Advance our cell pointer.
		c.cellNum++
	} else if leaf.nextLeaf != 0 {
		// Move to the next page.
		c.pageNum = leaf.nextLeaf
		c.cellNum = 0
	} else {
		// This was the rightmost leaf.
		c.endOfTable = true
	}
}

// End indicates if this cursor can no longer advance.
func (c *Cursor) End() bool {
	return c.endOfTable || c.advanceError != nil
}
