package db3

var _ DataSizer = (*Table)(nil)

// Table is a B+Tree manager backed by a file.
type Table struct {
	// pager is responsible for managing reading and
	// writing changes to and from disk.
	pager *Pager
	// dataSize is the size of data within cells in the B+Tree
	dataSize uint16
	// rootPageNum is the page index where the root node
	// is stored in the pager.
	rootPageNum PagePointer
}

// Open opens a database table file with the given pager.
// dataSize is the amount of bytes used in B+Tree cells for rows of data.
func Open(pager *Pager, dataSize uint16) (*Table, error) {
	const (
		rootPageNum = 0
	)
	if pager.NumPages() == 0 {
		// This is a new database file.
		// Initialize page 0 as a leaf node.
		page, err := pager.GetPage(rootPageNum)
		if err != nil {
			return nil, wrap(err, "unable to get root page")
		}
		leaf := pageToLeafNode(page)
		leaf.init()
		leaf.isRoot = true
		if err := pager.sync1(rootPageNum); err != nil {
			return nil, wrap(err, "unable to save new database")
		}
	}
	table := &Table{
		pager:       pager,
		dataSize:    dataSize,
		rootPageNum: rootPageNum,
	}
	return table, nil
}

// DataSize satisfies the DataSizer interface for B+Tree paging.
func (t *Table) DataSize() uint16 {
	return t.dataSize
}

// Start returns a cursor pointing to the first record in the database.
func (t *Table) Start() (*Cursor, error) {
	// Seek out the target page.
	cursor, err := t.Find(0)
	if err != nil {
		return nil, wrap(err, "unable to find start of table")
	}

	// Get the leaf page so we can determine if the cursor is done before
	// it even begins.
	page, err := t.pager.GetPage(cursor.pageNum)
	if err != nil {
		return nil, wrap(err, "unable to get root page")
	}

	// Mark our cursor as endOfTable if there are no cells in the first page.
	leaf := pageToLeafNode(page)
	cursor.endOfTable = leaf.numCells == 0

	return cursor, nil
}

// Find returns the position of the given key. If the key
// is not present, return the location where it should be
// inserted in order. The cursor is guaranteed to be pointing
// at a leaf node.
func (t *Table) Find(key KeyType) (*Cursor, error) {
	root, err := t.pager.GetPage(t.rootPageNum)
	if err != nil {
		return nil, wrap(err, "unable to get page")
	}
	return t.findInPage(root, t.rootPageNum, key)
}

// findInPage recursively searches a page for the given key.
func (t *Table) findInPage(page *Page, pageNum PagePointer, key KeyType) (*Cursor, error) {
	node := pageToNodeHeader(page)
	if node.isLeaf {
		// Recursive call, do not wrap error.
		return t.findInLeafNode(pageToLeafNode(page), pageNum, key)
	} else {
		// Recursive call, do not wrap error.
		return t.findInBranchNode(pageToBranchNode(page), pageNum, key)
	}
}

// findInLeafNode searches a leaf node for a given key.
func (t *Table) findInLeafNode(leaf *leafNode, pageNum PagePointer, key KeyType) (*Cursor, error) {
	cursor := &Cursor{
		table:   t,
		pageNum: pageNum,
	}
	cursor.cellNum = leaf.findKeyIndex(t, key)
	return cursor, nil
}

// findInBranchNode recursively search a branch node for a given key.
func (t *Table) findInBranchNode(branch *branchNode, pageNum PagePointer, key KeyType) (*Cursor, error) {
	// Find the child that could contain the key.
	childIndex := branch.findKeyIndex(key)
	childNum := branch.getChildPage(childIndex)
	child, err := t.pager.GetPage(childNum)
	if err != nil {
		return nil, wrap(err, "unable to get page")
	}
	return t.findInPage(child, childNum, key)
}
