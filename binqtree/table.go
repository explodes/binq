package binqtree

import (
	"fmt"
	"github.com/pkg/errors"
)

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
	// Binary search
	// TODO: make this a function of leafNode
	minIndex := uint16(0)
	onePastMaxIndex := leaf.numCells
	for onePastMaxIndex != minIndex {
		index := (minIndex + onePastMaxIndex) / 2
		keyAtIndex := leaf.getCellKey(t, index)
		if key == keyAtIndex {
			cursor.cellNum = index
			return cursor, nil
		}
		if key < keyAtIndex {
			onePastMaxIndex = index
		} else {
			minIndex = index + 1
		}
	}

	cursor.cellNum = minIndex
	return cursor, nil
}

// findInBranchNode recursively search a branch node for a given key.
func (t *Table) findInBranchNode(branch *branchNode, pageNum PagePointer, key KeyType) (*Cursor, error) {
	// Find the child that could contain the key.
	childIndex := t.findBranchNodeChild(branch, key)
	childNum := branch.getChildPage(childIndex)
	child, err := t.pager.GetPage(childNum)
	if err != nil {
		return nil, wrap(err, "unable to get page")
	}
	return t.findInPage(child, childNum, key)
}

// findBranchNodeChild returns the index of the child which should contain the given key.
// TODO: make this a function of branchNode, not Table.
func (t *Table) findBranchNodeChild(branch *branchNode, key KeyType) cellptr {
	// Binary search for the child that could contain the key.
	minIndex := cellptr(0)
	maxIndex := branch.numCells // there is one more child than key
	for minIndex != maxIndex {
		index := (minIndex + maxIndex) / 2
		keyToRight := branch.cells[index].key
		if keyToRight >= key {
			maxIndex = index
		} else {
			minIndex = index + 1
		}
	}
	return minIndex
}

// createNewRoot creates a new root node an re-parents the nodes accordingly.
func (t *Table) createNewRoot(rightChild *leafNode, rightChildPageNum PagePointer) error {
	pager := t.pager

	// Handle splitting the root.
	// Old root copied to new page becomes left child.
	// Re-init root page to contain the new root node.
	// New root node points to two children.

	// Get our root branch node.
	root, err := pager.GetPage(t.rootPageNum)
	if err != nil {
		return wrap(err, "unable to get page")
	}
	// This node may/may not be a branch currently.
	// We initialize it as a branch later.
	// But first we need to copy its state into the leftChild.

	// Get our left child page.
	leftChildPageNum, err := pager.GetUnusedPageNum()
	if err != nil {
		return wrap(err, "unable to get free page")
	}
	leftChild, err := pager.GetPage(leftChildPageNum)
	if err != nil {
		return wrap(err, "unable to get page")
	}
	leftLeaf := pageToLeafNode(leftChild)

	// Update the left child.
	// Left child has data copied from old root
	copy(leftChild[:], root[:])
	leftLeaf.isRoot = false
	leftLeaf.parentPointer = t.rootPageNum

	// Root node is a new branch node with one key and two children.
	// root may be a leaf at this line, but we init immediately, so it is ok.
	rootBranch := pageToBranchNode(root)
	rootBranch.init()
	rootBranch.isRoot = true
	rootBranch.numCells = 1
	// Direct access to child pointers is fine because the
	// child to modify is actually contained on this branch (numCells==1).
	rootBranch.cells[0].child = leftChildPageNum
	rootBranch.cells[0].key = leftLeaf.getMaxKey(t)
	rootBranch.rightChild = rightChildPageNum

	// Update the right child.
	rightChild.parentPointer = t.rootPageNum

	// Sync our changes.
	if err := pager.sync3(t.rootPageNum, leftChildPageNum, rightChildPageNum); err != nil {
		return wrap(err, "unable to sync new root")
	}
	return nil
}

// updateBranchNodeKey sets the existing key to a new key.
func (t *Table) updateBranchNodeKey(branch *branchNode, pageNum PagePointer, oldKey, newKey KeyType) error {
	oldChildIndex := t.findBranchNodeChild(branch, oldKey)
	_assert(int(oldChildIndex) != len(branch.cells), "attempting to set max key of right child: oldChildIndex=%d oldKey=%d newKey=%d", oldChildIndex, oldKey, newKey)
	branch.cells[oldChildIndex].key = newKey
	if err := t.pager.sync1(pageNum); err != nil {
		return wrap(err, "unable to sync updated node key")
	}
	return nil
}

// Add a new child/key pair to the parent that corresponds to child
func (t *Table) branchNodeInsert(parentBranch *branchNode, parentPageNum PagePointer, childLeaf *leafNode, childPageNum PagePointer) error {
	childMaxKey := childLeaf.getMaxKey(t)

	index := t.findBranchNodeChild(parentBranch, childMaxKey)

	originalNumKeys := parentBranch.numCells
	parentBranch.numCells++

	if originalNumKeys >= cellptr(branchNodeMaxCells) {
		// TODO: implement splitting branch node
		return errors.New("need to implement splitting branch node")
	}

	rightChildPageNum := parentBranch.rightChild
	rightChildPage, err := t.pager.GetPage(rightChildPageNum)
	if err != nil {
		return wrap(err, "unable to get page")
	}
	rightLeaf := pageToLeafNode(rightChildPage)

	rightChildPageMaxKey := rightLeaf.getMaxKey(t)
	if childMaxKey > rightChildPageMaxKey {
		// Replace right child.
		// Access this branch node's cell's child directly is ok because we have increased our
		// number of keys. originalNumKeys points to the last cell as of this line.
		parentBranch.cells[originalNumKeys].child = rightChildPageNum
		parentBranch.cells[originalNumKeys].key = rightChildPageMaxKey
		parentBranch.rightChild = childPageNum
	} else {
		// Make room for the new cell.
		for i := originalNumKeys; i > index; i-- {
			src := parentBranch.cells[i-1]
			parentBranch.cells[i].key = src.key
			parentBranch.cells[i].child = src.child
		}
		// Access this branch node's cell's child directly is ok because we have increased our
		// number of keys. index points to the a cell within the node as of this line.
		parentBranch.cells[index].child = childPageNum
		parentBranch.cells[index].key = childMaxKey
	}

	if err := t.pager.sync1(parentPageNum); err != nil {
		return wrap(err, "unable to sync updated parent")
	}

	return nil
}

func (t *Table) printTree() {
	t.printTreeHelper(t.rootPageNum, 0)
}

func (t *Table) printTreeHelper(pageNum PagePointer, indentationLevel int) {
	page, err := t.pager.GetPage(pageNum)
	if err != nil {
		fmt.Printf("unable to get page: %v\n", err)
	}
	node := pageToNodeHeader(page)
	if node.isLeaf {
		leaf := pageToLeafNode(page)
		t.printIndent(indentationLevel)
		fmt.Printf("- leaf (size %d) keys:", leaf.numCells)
		for i := cellptr(0); i < leaf.numCells; i++ {
			fmt.Printf("%d,", leaf.getCellKey(t, i))
		}
		fmt.Println()
	} else {
		branch := pageToBranchNode(page)
		t.printIndent(indentationLevel)
		fmt.Printf("- branch (size %d)\n", branch.numCells)
		for i := cellptr(0); i < branch.numCells; i++ {
			child := branch.cells[i].child
			t.printTreeHelper(child, indentationLevel+1)
			t.printIndent(indentationLevel + 1)
			fmt.Printf("- key %d\n", branch.cells[i].key)
		}
		t.printTreeHelper(branch.rightChild, indentationLevel+1)
	}
}

func (t *Table) printIndent(indentationLevel int) {
	for i := 0; i < indentationLevel; i++ {
		fmt.Print(" ")
	}
}
