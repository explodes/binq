package db2

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"unsafe"
)

const (
	// branchNodeMaxCells is the maximum amount of branchNodeCells that
	// can fit in a branchNode while also fitting in a single page.
	branchNodeMaxCells = (PageSize - unsafe.Sizeof(branchNodeHeader{})) / unsafe.Sizeof(branchNodeCell{})

	// leafNodeMaxCellData is the amount of data in a leafNode
	// reserved for key-value pairs.
	leafNodeMaxCellData = PageSize - unsafe.Sizeof(leafNodeHeader{})

	// keySize is the size of a single key in memory.
	keySize = unsafe.Sizeof(KeyType(0))
)

// cellptr is the type used for indexing individual cells.
// For branchNode children, this value can be 1 greater than the max index
// to indicate that the rightChild is the value at the index.
type cellptr = uint16

// DataSizer is the interface that supplies the
// size of data stored in leaf node cells.
type DataSizer interface {
	// DataSize returns the size of data stored in leaf node cells.
	DataSize() uint16
}

// KeyType is the primary key type of records in the tree.
type KeyType = uint32

const (
	// zeroKey is the zero-value of keys.
	zeroKey KeyType = 0
)

// keyFromBytes parses bytes as a KeyType.
func keyFromBytes(b []byte) KeyType {
	return KeyType(binary.LittleEndian.Uint32(b))
}

// encodeToBytes writes this KeyType into a byte slice.
func encodeKeyToBytes(key KeyType, b []byte) {
	binary.LittleEndian.PutUint32(b, uint32(key))
}

// getPageMaxKey returns the highest key contain in a Page's cells.
func getPageMaxKey(sizer DataSizer, page *Page) KeyType {
	n := pageToNodeHeader(page)
	if n.isLeaf {
		return pageToLeafNode(page).getMaxKey(sizer)
	} else {
		return pageToBranchNode(page).getMaxKey()
	}
}

// nodeHeader is the header common to leaf and branch nodes.
type nodeHeader struct {
	// isLeaf indicates if this node is a leaf or not.
	isLeaf bool
	// isRoot indicates if this node is the root node or not.
	isRoot bool
	// parentPointer points to the page that is this node's parent.
	parentPointer PagePointer
}

// pageToNodeHeader converts a page to a nodeHeader.
func pageToNodeHeader(page *Page) *nodeHeader {
	return (*nodeHeader)(unsafe.Pointer(&page[0]))
}

func (n *nodeHeader) String() string {
	return fmt.Sprintf("{isLeaf:%v,isRoot:%v,parentPointer:%d}", n.isLeaf, n.isRoot, n.parentPointer)
}

// branchNodeHeader is the header for all branch nodes.
type branchNodeHeader struct {
	nodeHeader
	// numCells indicates the number of cells in this node.
	// cells for branch nodes are key-child pairs.
	numCells cellptr
	// rightChild points to the next child of this node.
	// branch nodes contain N=branchNodeMaxCells [child,key]
	// pairs and an additional child.
	rightChild PagePointer
}

func (n *branchNodeHeader) String() string {
	return fmt.Sprintf("{nodeHeader:%s,numCells:%d,rightChild:%d}", n.nodeHeader.String(), n.numCells, n.rightChild)
}

// branchNodeCell is a cell within a branch node.
type branchNodeCell struct {
	// child is the page pointed to by this cell.
	child PagePointer
	// key is the the key for this B+Tree cell.
	key KeyType
}

func (n *branchNodeCell) String() string {
	return fmt.Sprintf("{key:%d,child:%d}", n.key, n.child)
}

// branchNode is a Page that acts like an branch node in the B+Tree.
type branchNode struct {
	branchNodeHeader
	cells [branchNodeMaxCells]branchNodeCell
}

var branchConvertWhitelist map[string]struct{}

func init() {
	if makeAssertions {
		branchConvertWhitelist = map[string]struct{}{
			"splitAndInsert": {},
			"createNewRoot":  {},
		}
	}
}

// pageToBranchNode converts a page to a branchNode.
func pageToBranchNode(page *Page) *branchNode {
	branch := (*branchNode)(unsafe.Pointer(&page[0]))
	if makeAssertions && branch.isLeaf {
		caller := callerName()
		if _, whitelisted := branchConvertWhitelist[caller]; !whitelisted {
			_assert(false, "%s: branch: wrong node type leaf\n", caller)
		}
	}
	return branch
}

func (n *branchNode) String() string {
	buf := &bytes.Buffer{}
	_, _ = fmt.Fprintf(buf, "{branchNodeHeader:%s,cells:[", n.branchNodeHeader.String())
	for index := cellptr(0); index < n.numCells; index++ {
		_, _ = fmt.Fprint(buf, n.cells[index].String())
	}
	buf.WriteString("]}")
	return buf.String()
}

func (n *branchNode) init() {
	n.isLeaf = false
	n.isRoot = false
	n.numCells = 0
}

// getChildPage returns the page pointed to for a given child.
// For branchNode children, childIndex can be 1 greater than the max index
// to indicate that the rightChild is the value at the index.
func (n *branchNode) getChildPage(childNum cellptr) PagePointer {
	if childNum > n.numCells {
		panic(errors.Errorf("tried to access childNum %d > numCells %d", childNum, n.numCells))
	}
	if n.numCells == childNum {
		return n.rightChild
	}
	return n.cells[childNum].child
}

// getMaxKey returns the highest key contained in this branch node's cells.
func (n *branchNode) getMaxKey() KeyType {
	_assert(!(*nodeHeader)(unsafe.Pointer(n)).isLeaf, "not a branch!!")
	return n.cells[n.numCells-1].key
}

// leafNodeHeader is the header for all leaf nodes.
type leafNodeHeader struct {
	nodeHeader
	// numCells indicates the number of cells in this node.
	// cells for branch nodes are key-child pairs.
	numCells cellptr
	// nextLeaf points to the next sibling page.
	// 0 represents no sibling.
	nextLeaf PagePointer
}

func (n *leafNodeHeader) String() string {
	return fmt.Sprintf("{nodeHeader:%s,numCells:%d,nextLeaf:%d}", n.nodeHeader.String(), n.numCells, n.nextLeaf)
}

// leafNode is a Page that acts like a leaf node in the B+Tree.
type leafNode struct {
	leafNodeHeader
	// cellData holds arbitrary data in cells.
	// The format of a cell is {KeyType(key), [dataSize]byte}.
	cellData [leafNodeMaxCellData]byte
}

var leafConvertWhitelist map[string]struct{}

func init() {
	if makeAssertions {
		leafConvertWhitelist = map[string]struct{}{
			"splitAndInsert": {},
			"Open":           {},
			"createNewRoot":  {},
		}
	}
}

// pageToLeafNode converts a page to a leafNode.
func pageToLeafNode(page *Page) *leafNode {
	leaf := (*leafNode)(unsafe.Pointer(&page[0]))
	if makeAssertions && !leaf.isLeaf {
		caller := callerName()
		if _, whitelisted := leafConvertWhitelist[caller]; !whitelisted {
			_assert(false, "%s leaf: wrong node type branch\n", caller)
		}
	}
	return leaf
}

func (n *leafNode) String(sizer DataSizer) string {
	buf := &bytes.Buffer{}
	_, _ = fmt.Fprintf(buf, "{leafNodeHeader:%s,cells:[", n.leafNodeHeader.String())
	for index := cellptr(0); index < n.numCells; index++ {
		key := n.getCellKey(sizer, index)
		_, _ = fmt.Fprintf(buf, "{%d:%d}", index, key)
	}
	buf.WriteString("]}")
	return buf.String()
}

// init initializes the default values for this leafNode.
func (n *leafNode) init() {
	n.isLeaf = true
	n.isRoot = false
	n.numCells = 0
	// 0 represents no sibling.
	n.nextLeaf = 0
}

// getCellBin returns the {keyType(key), [dataSize]byte} bytes for a given index.
func (n *leafNode) getCellBin(sizer DataSizer, index cellptr) (cell []byte) {
	cellSize := n.getCellSize(sizer)
	offset := uintptr(index) * cellSize
	return n.cellData[offset : offset+cellSize]
}

// getCellSize returns the size of cells in this node, including key.
func (n *leafNode) getCellSize(sizer DataSizer) uintptr {
	return keySize + uintptr(sizer.DataSize())
}

// getMaxNumCells is the maximum number of cells that can be held in this node.
func (n *leafNode) getMaxNumCells(sizer DataSizer) cellptr {
	cellSize := n.getCellSize(sizer)
	return cellptr(leafNodeMaxCellData / cellSize)
}

// getCell returns the key-value pair stored in a cell at the given index.
func (n *leafNode) getCell(sizer DataSizer, index cellptr) (key KeyType, value []byte) {
	bin := n.getCellBin(sizer, index)
	key = keyFromBytes(bin)
	value = bin[keySize:]
	return key, value
}

// getCellKey returns the key of the key-value pair stored in a cell at the given index.
func (n *leafNode) getCellKey(sizer DataSizer, index cellptr) KeyType {
	bin := n.getCellBin(sizer, index)
	return keyFromBytes(bin)
}

// getCellValue returns the value of the key-value pair stored in a cell at the given index.
func (n *leafNode) getCellValue(sizer DataSizer, index cellptr) []byte {
	bin := n.getCellBin(sizer, index)
	return bin[keySize:]
}

// putCell sets the key-value pair stored in a particular cell.
func (n *leafNode) putCell(sizer DataSizer, index cellptr, key KeyType, value []byte) {
	bin := n.getCellBin(sizer, index)
	encodeKeyToBytes(key, bin)
	copy(bin[keySize:], value)
}

// getNodeMaxKey gets the highest key in this node.
func (n *leafNode) getMaxKey(sizer DataSizer) KeyType {
	_assert((*nodeHeader)(unsafe.Pointer(n)).isLeaf, "not a leaf")
	return n.getCellKey(sizer, n.numCells-1)
}

// getSplitCounts gets the amount of nodes that remain in the old node after a split.
func (n *leafNode) getSplitCounts(sizer DataSizer) (oldSplitCount, newSplitCount cellptr) {
	maxCells := n.getMaxNumCells(sizer)
	oldSplitCount = (maxCells + 1) / 2
	newSplitCount = (maxCells + 1) - oldSplitCount
	return oldSplitCount, newSplitCount
}

// insert inserts a key-value pair at the cursor position into the B+Tree.
func (n *leafNode) insert(cursor *Cursor, key KeyType, value []byte) error {
	// If the node is full, we have to split it.
	if n.numCells >= n.getMaxNumCells(cursor.table) {
		// Node is full.
		if err := n.splitAndInsert(cursor, key, value); err != nil {
			return wrap(err, "unable to split leaf node")
		}
		return nil
	}

	// Otherwise just put the value straight into this node.

	// If the target cell is within our current cells
	if cursor.cellNum < n.numCells {
		// Make room for the new value by shifting everything at and after
		// the target cellNum forward one cell.
		cellSize := n.getCellSize(cursor.table)
		offset := uintptr(cursor.cellNum) * cellSize
		copy(n.cellData[offset+cellSize:], n.cellData[offset:])
	}

	// Write the cell into the new space.
	n.numCells++
	n.putCell(cursor.table, cursor.cellNum, key, value)

	// Sync changes made to this page.
	if err := cursor.table.pager.sync1(cursor.pageNum); err != nil {
		return wrap(err, "unable to sync leaf node")
	}

	return nil
}

// splitAndInserts splits this node and inserts the value in one of the two new nodes.
func (n *leafNode) splitAndInsert(cursor *Cursor, key KeyType, value []byte) error {
	oldLeaf := n
	pager := cursor.table.pager
	table := cursor.table
	var sizer DataSizer = table

	// Get this value before we modify the max key.
	// We'll need this for updating the parenting.
	oldMax := oldLeaf.getMaxKey(sizer)

	// Create the new leaf page.
	newPageNum, err := pager.GetUnusedPageNum()
	if err != nil {
		return wrap(err, "unable to get free page")
	}
	newPage, err := pager.GetPage(newPageNum)
	if err != nil {
		return wrap(err, "unable to get page")
	}
	newLeaf := pageToLeafNode(newPage)
	newLeaf.init()

	// Make the new leaf a child of the same parent as the old leaf.
	newLeaf.parentPointer = oldLeaf.parentPointer

	// Insert the new leaf between the old leaf and the
	// old leaf's next leaf.
	newLeaf.nextLeaf = oldLeaf.nextLeaf
	oldLeaf.nextLeaf = newPageNum

	// All existing keys plus the new key should be divided evenly between the
	// old node and the new nodes.
	oldSplitCount, newSplitCount := n.getSplitCounts(sizer)
	leafMaxCells := n.getMaxNumCells(sizer)

	// TODO: Simplify. This loop is only moving values from old to new and putting the new value in.
	// Starting from the right, move each key to the correct position.
	oldPos := oldSplitCount - 1
	newPos := newSplitCount - 1
	for i := cellptr(leafMaxCells); ; i-- {
		var writingToNew bool
		var dest *leafNode
		var indexWithinNode cellptr
		if i >= oldSplitCount {
			// Copy to the new leaf.
			writingToNew = true
			dest = newLeaf
			indexWithinNode = newPos
			newPos--
		} else {
			// Copy to the old leaf.
			writingToNew = false
			dest = oldLeaf
			indexWithinNode = oldPos
			oldPos--
		}

		if i == cursor.cellNum {
			dest.putCell(sizer, indexWithinNode, key, value)
		} else {
			destCell := dest.getCellBin(sizer, indexWithinNode)
			var oldCell []byte
			var srcIndex cellptr
			if i > cursor.cellNum {
				srcIndex = i - 1
			} else {
				srcIndex = i
			}
			// Don't copy old cells into itself.
			if writingToNew || (srcIndex != indexWithinNode) {
				oldCell = oldLeaf.getCellBin(sizer, srcIndex)
				copy(destCell, oldCell)
			}
		}

		// unsigned int type, break after we've processed the first position.
		if i == 0 {
			break
		}
	}

	// Update our cell counts.
	oldLeaf.numCells = oldSplitCount
	newLeaf.numCells = newSplitCount

	// Sync our changes to the old and new nodes.
	if err := pager.sync2(cursor.pageNum, newPageNum); err != nil {
		return wrap(err, "unable to sync split")
	}

	// Update the node's parent.
	if oldLeaf.isRoot {
		// We'll need to create a new root for these nodes.
		if err := table.createNewRoot(newLeaf, newPageNum); err != nil {
			return wrap(err, "unable to create new root")
		}
		return nil
	} else {
		// Set the old max key where it is to be the new max key.
		newMax := oldLeaf.getMaxKey(sizer)
		parentPageNum := oldLeaf.parentPointer
		parentPage, err := pager.GetPage(parentPageNum)
		if err != nil {
			return wrap(err, "unable to get page")
		}
		parentBranch := pageToBranchNode(parentPage)
		if parentBranch.numCells+1 >= cellptr(branchNodeMaxCells) {
			// TODO: implement splitting branch node
			return errors.New("need to implement splitting branch node")
		}
		if err := table.updateBranchNodeKey(parentBranch, parentPageNum, oldMax, newMax); err != nil {
			return wrap(err, "unable to update node key")
		}
		if err := table.branchNodeInsert(parentBranch, parentPageNum, newLeaf, newPageNum); err != nil {
			return wrap(err, "unable insert branch node")
		}
		return nil
	}
}
