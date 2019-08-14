package db3

import (
	"bytes"
	"fmt"
	"unsafe"
)

const (
	// leafNodeMaxCellData is the amount of data in a leafNode
	// reserved for key-value pairs.
	leafNodeMaxCellData = PageSize - unsafe.Sizeof(leafNodeHeader{})
)

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
			"insert":         {},
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
	if makeAssertions {
		_assert(n.isLeaf, "not a leaf")
	}

	cellSize := n.getCellSize(sizer)
	offset := uintptr(index) * cellSize
	return n.cellData[offset : offset+cellSize]
}

// getCellSize returns the size of cells in this node, including key.
func (n *leafNode) getCellSize(sizer DataSizer) uintptr {
	if makeAssertions {
		_assert(n.isLeaf, "not a leaf")
	}

	return keySize + uintptr(sizer.DataSize())
}

// getMaxNumCells is the maximum number of cells that can be held in this node.
func (n *leafNode) getMaxNumCells(sizer DataSizer) cellptr {
	if makeAssertions {
		_assert(n.isLeaf, "not a leaf")
	}

	cellSize := n.getCellSize(sizer)
	return cellptr(leafNodeMaxCellData / cellSize)
}

// getCell returns the key-value pair stored in a cell at the given index.
func (n *leafNode) getCell(sizer DataSizer, index cellptr) (key KeyType, value []byte) {
	if makeAssertions {
		_assert(n.isLeaf, "not a leaf")
	}

	bin := n.getCellBin(sizer, index)
	key = keyFromBytes(bin)
	value = bin[keySize:]
	return key, value
}

// getCellKey returns the key of the key-value pair stored in a cell at the given index.
func (n *leafNode) getCellKey(sizer DataSizer, index cellptr) KeyType {
	if makeAssertions {
		_assert(n.isLeaf, "not a leaf")
	}

	bin := n.getCellBin(sizer, index)
	return keyFromBytes(bin)
}

// getCellValue returns the value of the key-value pair stored in a cell at the given index.
func (n *leafNode) getCellValue(sizer DataSizer, index cellptr) []byte {
	if makeAssertions {
		_assert(n.isLeaf, "not a leaf")
	}

	bin := n.getCellBin(sizer, index)
	return bin[keySize:]
}

// putCell sets the key-value pair stored in a particular cell.
func (n *leafNode) putCell(sizer DataSizer, index cellptr, key KeyType, value []byte) {
	if makeAssertions {
		_assert(n.isLeaf, "not a leaf")
	}

	bin := n.getCellBin(sizer, index)
	encodeKeyToBytes(key, bin)
	copy(bin[keySize:], value)
}

// getNodeMaxKey gets the highest key in this node.
func (n *leafNode) getMaxKey(sizer DataSizer) KeyType {
	if makeAssertions {
		_assert(n.isLeaf, "not a leaf")
	}

	return n.getCellKey(sizer, n.numCells-1)
}

// getSplitCounts gets the amount of cells to put in the old and new nodes after a split.
func (n *leafNode) getSplitCounts(sizer DataSizer) (oldSplitCount, newSplitCount cellptr) {
	if makeAssertions {
		_assert(n.isLeaf, "not a leaf")
	}

	maxCells := n.getMaxNumCells(sizer)
	oldSplitCount = (maxCells + 1) / 2
	newSplitCount = (maxCells + 1) - oldSplitCount
	return oldSplitCount, newSplitCount
}

// findInLeafNode binary-searches a leaf node for a given key or where a key should be inserted.
func (n *leafNode) findKeyIndex(sizer DataSizer, key KeyType) cellptr {
	if makeAssertions {
		_assert(n.isLeaf, "not a leaf")
	}

	// Binary search
	minIndex := cellptr(0)
	onePastMaxIndex := n.numCells
	for onePastMaxIndex != minIndex {
		index := (minIndex + onePastMaxIndex) / 2
		keyAtIndex := n.getCellKey(sizer, index)
		if key == keyAtIndex {
			return index
		}
		if key < keyAtIndex {
			onePastMaxIndex = index
		} else {
			minIndex = index + 1
		}
	}

	return minIndex
}

// makeRoomForInsert slides cells to the right to make room for an insertion.
func (n *leafNode) makeRoomForInsert(sizer DataSizer, pos cellptr) {
	if makeAssertions {
		_assert(n.isLeaf, "not a leaf")
		_assert(n.numCells < n.getMaxNumCells(sizer), "leaf node too big to add room")
	}

	cellSize := cellptr(n.getCellSize(sizer))
	tailBytes := cellSize * (n.numCells - pos)

	srcStart := pos * cellSize
	srcEnd := srcStart + tailBytes

	dstStart := srcStart + cellSize
	dstEnd := dstStart + tailBytes
	if lastByte := n.getMaxNumCells(sizer) * cellSize; dstEnd > lastByte {
		dstEnd = lastByte
	}

	copy(n.cellData[dstStart:dstEnd], n.cellData[srcStart:srcEnd])
}

// insertDirect inserts a key-value into the cursor position
// for a node that has space to insert the value directly.
func (n *leafNode) insertDirect(sizer DataSizer, pos cellptr, key KeyType, value []byte) {
	if makeAssertions {
		_assert(n.isLeaf, "not a leaf")
		_assert(n.numCells < n.getMaxNumCells(sizer), "leaf node too big for direct insert")
	}

	n.makeRoomForInsert(sizer, pos)
	n.putCell(sizer, pos, key, value)
	n.numCells++
}

func (n *leafNode) insert(cursor *Cursor, key KeyType, value []byte) error {
	if makeAssertions {
		_assert(n.isLeaf, "not a leaf")
	}

	table := cursor.table
	pager := table.pager
	var sizer DataSizer = table

	leftLeafPageNum := cursor.pageNum
	leftLeaf := n
	leafMaxCells := leftLeaf.getMaxNumCells(sizer)
	// If the leaf node still has space, we can insert the key-value directly into the leaf.
	if leftLeaf.numCells < leafMaxCells {
		leftLeaf.insertDirect(sizer, cursor.cellNum, key, value)
		if err := cursor.table.pager.sync1(leftLeafPageNum); err != nil {
			return wrap(err, "unable to sync page")
		}
		return nil
	}

	/* We need to split the leaf. */

	leftLeafOldMaxKey := leftLeaf.getMaxKey(sizer)

	// Create a new leaf to split into.
	rightLeafPageNum, err := pager.GetUnusedPageNum()
	if err != nil {
		return wrap(err, "unable to get free page")
	}
	rightLeafPage, err := pager.GetPage(rightLeafPageNum)
	if err != nil {
		return wrap(err, "unable to get page")
	}
	rightLeaf := pageToLeafNode(rightLeafPage)
	rightLeaf.init()
	rightLeaf.parentPointer = leftLeaf.parentPointer

	// Point the old node to the new node to the next node for a
	// continuous linked list of leaf nodes.
	rightLeaf.nextLeaf = leftLeaf.nextLeaf
	leftLeaf.nextLeaf = rightLeafPageNum

	leftLeafSplitSize, rightLeafSplitSize := leftLeaf.getSplitCounts(sizer)

	// Does the value go into the left or right node?
	newEntryGoesLeft := leftLeaf.getCellKey(sizer, leftLeafSplitSize-1) > key

	// Copy the larger keys to the new node.
	cellSize := cellptr(leftLeaf.getCellSize(sizer))
	splitCellStart := cellSize * leftLeafSplitSize
	if newEntryGoesLeft {
		splitCellStart -= cellSize
		// New key-value will be added to the left leaf.
		leftLeaf.numCells = leftLeafSplitSize - 1
		rightLeaf.numCells = rightLeafSplitSize
	} else {
		leftLeaf.numCells = leftLeafSplitSize
		// New key-value will be added to the right leaf.
		rightLeaf.numCells = rightLeafSplitSize - 1
	}
	copy(rightLeaf.cellData[:], leftLeaf.cellData[splitCellStart:])

	// Insert the new key-value into the correct leaf.
	if newEntryGoesLeft {
		index := leftLeaf.findKeyIndex(sizer, key)
		leftLeaf.insertDirect(sizer, index, key, value)
	} else {
		index := rightLeaf.findKeyIndex(sizer, key)
		rightLeaf.insertDirect(sizer, index, key, value)
	}

	if err := cursor.table.pager.sync2(leftLeafPageNum, rightLeafPageNum); err != nil {
		return wrap(err, "unable to sync pages")
	}

	/* Modify the parent */

	if key == 24 {
		sink()
	}

	// In the simple case, we're already at the root. We just need to parent
	// the left and right node to a new root.
	if leftLeaf.isRoot {

		// If the old leaf is a root node, we need a new root.
		// We want to preserve the table's root node position.
		// Lets keep the root at the same page as the leaf, so
		// we copy our left leaf onto a new page.

		// We'll need our leftLeaf as a page for conversion.
		leftLeafPage := (*Page)(unsafe.Pointer(leftLeaf))

		// Create the new left leaf to copy into.
		newLeftLeafPageNum, err := pager.GetUnusedPageNum()
		if err != nil {
			return wrap(err, "unable to get free page")
		}
		newLeftLeafPage, err := pager.GetPage(newLeftLeafPageNum)
		if err != nil {
			return wrap(err, "unable to get page")
		}
		newLeftLeaf := pageToLeafNode(newLeftLeafPage)

		// Copy the leaf to the new leaf.
		copy(newLeftLeafPage[:], leftLeafPage[:])
		newLeftLeaf.isRoot = false
		newLeftLeaf.parentPointer = leftLeafPageNum

		// Convert the leftLeaf to a root.
		root := pageToBranchNode(leftLeafPage)
		root.init()
		root.isRoot = true
		root.numCells = 1
		root.cells[0].key = newLeftLeaf.getMaxKey(sizer)
		root.cells[0].child = newLeftLeafPageNum
		root.rightChild = rightLeafPageNum
		// At this point we have the following configuration:
		//          branch pg0: [child 1, key max(1), child 2]
		//                        /                   \
		// leaf pg2: [0-50% key-values]  ->  leaf pg1: [51-100% key-values]

		// Sync the changes.
		rootPageNum := leftLeafPageNum
		if err := cursor.table.pager.sync2(rootPageNum, newLeftLeafPageNum); err != nil {
			return wrap(err, "unable to sync pages")
		}
		return nil
	} else {
		// If our destination is not the root, we need to update the parents,
		// possibly all the way up to the root where we may yet split the root again.
		// Get the parent of these two nodes and update it appropriately.
		parentPageNum := leftLeaf.parentPointer
		parentPage, err := pager.GetPage(parentPageNum)
		if err != nil {
			return wrap(err, "unable to get page")
		}
		parentBranch := pageToBranchNode(parentPage)
		leftLeafNewMaxKey := leftLeaf.getMaxKey(sizer)

		fmt.Println("after leaf split")
		table.printTree()

		if err := parentBranch.insertAfterSplit(table, sizer, pager, parentPageNum, leftLeafOldMaxKey, leftLeafNewMaxKey, rightLeafPageNum); err != nil {
			return wrap(err, "unable to update parent branch")
		}
		fmt.Println("after leaf split insert")
		table.printTree()
		sink()
	}
	return nil
}

func (n *leafNode) String(sizer DataSizer) string {
	if makeAssertions {
		_assert(n.isLeaf, "not a leaf")
	}

	buf := &bytes.Buffer{}
	_, _ = fmt.Fprintf(buf, "{leafNodeHeader:%s,cells:[", n.leafNodeHeader.String())
	for index := cellptr(0); index < n.numCells; index++ {
		key := n.getCellKey(sizer, index)
		_, _ = fmt.Fprintf(buf, "{#%d:%d}", index, key)
	}
	buf.WriteString("]}")
	return buf.String()
}

func (n *leafNode) StringVerbose(sizer DataSizer, valueString func([]byte) string) string {
	if makeAssertions {
		_assert(n.isLeaf, "not a leaf")
	}

	buf := &bytes.Buffer{}
	_, _ = fmt.Fprintf(buf, "{leafNodeHeader:%s,cells:[", n.leafNodeHeader.String())
	for index := cellptr(0); index < n.numCells; index++ {
		key, value := n.getCell(sizer, index)
		_, _ = fmt.Fprintf(buf, "{#%d:%d=%s}", index, key, valueString(value))
	}
	buf.WriteString("]}")
	return buf.String()
}
