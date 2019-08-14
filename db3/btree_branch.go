package db3

import (
	"bytes"
	"fmt"
	"sort"
	"unsafe"
)

const (
	// branchNodeMaxCells is the maximum amount of branchNodeCells that
	// can fit in a branchNode while also fitting in a single page.
	branchNodeMaxCells = (PageSize - unsafe.Sizeof(branchNodeHeader{})) / unsafe.Sizeof(branchNodeCell{})
)

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
			"insert":         {},
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
	if makeAssertions {
		_assert(!n.isLeaf, "not a branch")
	}

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
	if makeAssertions {
		_assert(!n.isLeaf, "not a branch")
		_assert(childNum <= n.numCells, "tried to access childNum %d > numCells %d", childNum, n.numCells)
	}

	if n.numCells == childNum {
		return n.rightChild
	}
	return n.cells[childNum].child
}

// getMaxKey returns the highest key contained in this branch node's cells.
func (n *branchNode) getMaxKey() KeyType {
	if makeAssertions {
		_assert(!n.isLeaf, "not a branch")
	}

	return n.cells[n.numCells-1].key
}

// getMaxNumCells is the maximum number of cells that can be held in this node.
func (n *branchNode) getMaxNumCells() cellptr {
	const (
		defaultBranchNodeMaxCells = cellptr(branchNodeMaxCells)
	)
	if makeAssertions {
		_assert(!n.isLeaf, "not a branch")
	}
	if debug {
		return _maxKeysPerBranchOverride()
	}
	return defaultBranchNodeMaxCells
}

// getSplitCounts gets the amount of cells to put in the old and new nodes after a split.
func (n *branchNode) getSplitCounts() (oldSplitCount, newSplitCount cellptr) {
	if makeAssertions {
		_assert(!n.isLeaf, "not a branch")
	}

	maxCells := n.getMaxNumCells()
	oldSplitCount = (maxCells + 1) / 2
	newSplitCount = maxCells - oldSplitCount
	return oldSplitCount, newSplitCount
}

// findBranchNodeChild returns the index of the child which should contain the given key.
func (n *branchNode) findKeyIndex(key KeyType) cellptr {
	if makeAssertions {
		_assert(!n.isLeaf, "not a branch")
	}

	// Binary search for the child that could contain the key.
	minIndex := cellptr(0)
	maxIndex := n.numCells // there is one more child than key
	for minIndex != maxIndex {
		index := (minIndex + maxIndex) / 2
		keyToRight := n.cells[index].key
		if keyToRight >= key {
			maxIndex = index
		} else {
			minIndex = index + 1
		}
	}
	return minIndex
}

// makeRoomForInsert slides cells to the right to make room for an insertion.
// Does not sync.
func (n *branchNode) makeRoomForInsert(pos cellptr) {
	if makeAssertions {
		_assert(!n.isLeaf, "not a branch")
		_assert(n.numCells < n.getMaxNumCells(), "branch node too big to add room")
	}

	maxCells := n.getMaxNumCells()
	tailCells := n.numCells - pos

	srcStart := pos
	srcEnd := srcStart + tailCells

	dstStart := srcStart + 1
	dstEnd := dstStart + tailCells
	if dstEnd > maxCells {
		dstEnd = maxCells
	}

	copy(n.cells[dstStart:dstEnd], n.cells[srcStart:srcEnd])
}

// insertDirect inserts a key-value into the cursor position
// for a node that has space to insert the value directly.
// Does not sync.
func (n *branchNode) insertDirect(sizer DataSizer, pager *Pager, pos cellptr, key KeyType, child PagePointer) error {
	if makeAssertions {
		_assert(!n.isLeaf, "not a branch")
		_assert(n.numCells < n.getMaxNumCells(), "branch node too big for direct insert")
	}

	//if pos == n.numCells {
	rightChildMaxKey, err := n.getRightChildMaxKey(sizer, pager)
	if err != nil {
		return wrap(err, "unable to get max key in node")
	}
	if key > rightChildMaxKey {
		// Replace the right child.
		rightChildPageNum := n.rightChild
		originalNumCells := n.numCells
		n.cells[originalNumCells].child = rightChildPageNum
		n.cells[originalNumCells].key = rightChildMaxKey
		n.rightChild = child
		n.numCells++
		return nil
	}
	//}
	n.makeRoomForInsert(pos)
	n.cells[pos].key = key
	n.cells[pos].child = child
	n.numCells++
	return nil
}

func (n *branchNode) getRightChildMaxKey(sizer DataSizer, pager *Pager) (KeyType, error) {
	if makeAssertions {
		_assert(!n.isLeaf, "not a branch")
	}

	rightPage, err := pager.GetPage(n.rightChild)
	if err != nil {
		return 0, wrap(err, "unable to get page")
	}
	rightNode := pageToNodeHeader(rightPage)
	max := rightNode.getMaxKey(sizer)
	return max, nil
}

func (n *branchNode) updateMaximum(table *Table, pager *Pager, pageNum PagePointer, oldMax, newMax KeyType) error {
	if makeAssertions {
		_assert(!n.isLeaf, "not a branch")
		//_assert(oldMax > newMax, "key not decreased after a split")
	}

	fmt.Println("before Tree")
	table.printTree()
	children := collectChildPages(n)
	fmt.Println("before", children)
	keys := collectKeys(table, pager, n)
	fmt.Println("before", keys)

	// After a split, the key stored for the left leaf needs to be updated.
	pageIndex := n.findKeyIndex(oldMax)
	if pageIndex < n.numCells {
		// If the old key does not belong to the right child, we update
		// that key to point to the new, lower value key.
		n.cells[pageIndex].key = newMax
		if err := pager.sync1(pageNum); err != nil {
			return wrap(err, "unable to sync page")
		}
	}

	fmt.Println("after Tree")
	table.printTree()
	children = collectChildPages(n)
	fmt.Println("after", children)
	keys = collectKeys(table, pager, n)
	fmt.Println("after", keys)

	if pageIndex == n.numCells {
		// We may need to update the parent.
		if n.isRoot {
			// We only update the parent if there is one.
			return nil
		}
		// Propagate the update to the parent.
		parentPageNum := n.parentPointer
		parentPage, err := pager.GetPage(parentPageNum)
		if err != nil {
			return wrap(err, "unable to get page")
		}
		parentBranch := pageToBranchNode(parentPage)
		fmt.Println("update parent")
		if err := parentBranch.updateMaximum(table, pager, parentPageNum, oldMax, newMax); err != nil {
			// nowrap: recursive call
			return err
		}
	}
	return nil
}

// insertAfterSplit inserts a new child into a branch node after a split, updating previous max keys.
func (n *branchNode) insertAfterSplit(table *Table, sizer DataSizer, pager *Pager, pageNum PagePointer, oldMax, newMax, childPageNum PagePointer) error {
	if makeAssertions {
		_assert(!n.isLeaf, "not a branch")
		_assert(oldMax > newMax, "key not decreased after a split")
	}

	if err := n.updateMaximum(table, pager, pageNum, oldMax, newMax); err != nil {
		return wrap(err, "unable to update maximum")
	}
	if err := n.insert(table, sizer, pager, pageNum, childPageNum); err != nil {
		// nowrap: indirectly recursive call
		return err
	}
	return nil
}

// insert inserts a new child into a branch node and splits parents recursively if necessary.
func (n *branchNode) insert(table *Table, sizer DataSizer, pager *Pager, pageNum PagePointer, childPageNum PagePointer) error {
	if makeAssertions {
		_assert(!n.isLeaf, "not a branch")
	}

	childPage, err := pager.GetPage(childPageNum)
	if err != nil {
		return wrap(err, "unable to get page")
	}
	childNode := pageToNodeHeader(childPage)
	childMaxKey := childNode.getMaxKey(sizer)

	leftBranchPageNum := pageNum
	leftBranch := n
	branchMaxCells := leftBranch.getMaxNumCells()

	// If this branch has room for a new key, simply add the new key.
	if leftBranch.numCells < branchMaxCells {
		originalNumCells := n.numCells
		rightChildPageNum := n.rightChild
		rightChildMaxKey, err := n.getRightChildMaxKey(sizer, pager)
		if err != nil {
			return wrap(err, "unable to get max key in node")
		}
		if childMaxKey > rightChildMaxKey {
			// Replace the right child.
			leftBranch.cells[originalNumCells].child = rightChildPageNum
			leftBranch.cells[originalNumCells].key = rightChildMaxKey
			leftBranch.rightChild = childPageNum
			leftBranch.numCells++

			parentPageNum := leftBranch.parentPointer
			parentPage, err := pager.GetPage(parentPageNum)
			if err != nil {
				return wrap(err, "unable to get page")
			}
			parentBranch := pageToBranchNode(parentPage)
			//leftBranchNewMaxKey, err := leftBranch.getRightChildMaxKey(sizer, pager)
			//if err != nil {
			//	return wrap(err, "unable to get child maximum")
			//}
			if childMaxKey == 36 {
				sink()
			}
			if err := parentBranch.updateMaximum(table, pager, parentPageNum, rightChildMaxKey, childMaxKey); err != nil {
				// nowrap: indirectly recursive call
				return err
			}
			fmt.Println("After right child update max")
			table.printTree()

			if err := pager.sync1(pageNum); err != nil {
				return wrap(err, "unable to sync page")
			}
			return nil
		} else {
			// Insert the key directly into our cells.
			index := leftBranch.findKeyIndex(childMaxKey)
			if err := leftBranch.insertDirect(sizer, pager, index, childMaxKey, childPageNum); err != nil {
				return wrap(err, "unable to insert key")
			}
			if err := pager.sync1(pageNum); err != nil {
				return wrap(err, "unable to sync page")
			}
			return nil
		}
	}

	/*We have to split the branch. */

	// Create a new branch to split into.
	rightBranchPageNum, err := pager.GetUnusedPageNum()
	if err != nil {
		return wrap(err, "unable to get free page")
	}
	rightBranchPage, err := pager.GetPage(rightBranchPageNum)
	if err != nil {
		return wrap(err, "unable to get page")
	}
	rightBranch := pageToBranchNode(rightBranchPage)
	rightBranch.init()
	rightBranch.parentPointer = leftBranch.parentPointer

	leftBranchOldMaxKey, err := leftBranch.getRightChildMaxKey(sizer, pager)
	if err != nil {
		return wrap(err, "unable to get child maximum")
	}

	// TODO: remove debug code
	startKeys := collectKeys(sizer, pager, leftBranch)
	startChildren := collectChildPages(leftBranch)
	sink(startKeys, startChildren)

	// Create our complete list of cells.
	newCells := make([]branchNodeCell, leftBranch.numCells+2)
	copy(newCells, leftBranch.cells[:leftBranch.numCells])
	// Insert the right child.
	newCells[len(newCells)-2].key = leftBranchOldMaxKey
	newCells[len(newCells)-2].child = leftBranch.rightChild
	// Insert the new cell.
	idx := sort.Search(len(newCells)-1, func(i int) bool {
		return leftBranch.cells[i].key >= childMaxKey
	})
	copy(newCells[idx+1:], newCells[idx:])
	newCells[idx].key = childMaxKey
	newCells[idx].child = childPageNum

	leftBranchSplitSize, rightBranchSplitSize := leftBranch.getSplitCounts()

	copy(leftBranch.cells[:], newCells[:leftBranchSplitSize])
	leftBranch.rightChild = newCells[leftBranchSplitSize].child
	leftBranch.numCells = leftBranchSplitSize

	copy(rightBranch.cells[:], newCells[leftBranchSplitSize+1:])
	rightBranch.rightChild = newCells[len(newCells)-1].child
	rightBranch.numCells = rightBranchSplitSize

	// TODO: remove debug code
	leftPages := collectChildPages(leftBranch)
	rightPages := collectChildPages(rightBranch)
	sink(leftPages, rightPages)
	endKeys := collectKeys(sizer, pager, leftBranch, rightBranch)
	endChildren := collectChildPages(leftBranch, rightBranch)
	sink(endKeys, endChildren)
	if !samePages(childPageNum, endChildren, startChildren) || !sameKeys(childMaxKey, endKeys, startKeys) {
		fmt.Println("unequal stuff after split.")
	}

	// Sync our changes.
	if err := pager.sync2(leftBranchPageNum, rightBranchPageNum); err != nil {
		return wrap(err, "unable to sync pages")
	}
	// Reparent the children.
	// Our leftBranch children already point to the correct parent page,
	// but the rightBranch children do not.
	if err := rightBranch.reparentChildren(pager, rightBranchPageNum); err != nil {
		return wrap(err, "unable to reparent children")
	}

	/* Modify the parent */

	// In the simple case, we're already at the root. We just need to parent
	// the left and right node to a new root.
	if leftBranch.isRoot {
		// If the old branch is a root node, we need a new root.
		// We want to preserve the table's root node position.
		// Lets keep the root at the same page as the branch, so
		// we copy our left branch onto a new page.

		// We'll need our leftBranch as a page for conversion.
		leftBranchPage := (*Page)(unsafe.Pointer(leftBranch))

		// Create the new left branch to copy into.
		newLeftBranchPageNum, err := pager.GetUnusedPageNum()
		if err != nil {
			return wrap(err, "unable to get free page")
		}
		newLeftBranchPage, err := pager.GetPage(newLeftBranchPageNum)
		if err != nil {
			return wrap(err, "unable to get page")
		}
		newLeftBranch := pageToBranchNode(newLeftBranchPage)

		// Copy the branch to the new branch.
		copy(newLeftBranchPage[:], leftBranchPage[:])
		newLeftBranch.isRoot = false
		newLeftBranch.parentPointer = leftBranchPageNum

		newLeftBranchMaxKey, err := newLeftBranch.getRightChildMaxKey(sizer, pager)
		if err != nil {
			return wrap(err, "unable to get max key")
		}

		// Convert the leftBranch to a root.
		root := pageToBranchNode(leftBranchPage)
		root.init()
		root.isRoot = true
		root.numCells = 1
		root.cells[0].key = newLeftBranchMaxKey
		root.cells[0].child = newLeftBranchPageNum
		root.rightChild = rightBranchPageNum
		// At this point we have the following configuration:
		//          branch 0: [child 1, key max(1), child 2]
		//                        /                   \
		// branch 1: [0-50% key-children]      branch 2: [51-100% key-children]

		// Sync the changes.
		rootPageNum := leftBranchPageNum
		if err := pager.sync2(rootPageNum, newLeftBranchPageNum); err != nil {
			return wrap(err, "unable to sync pages")
		}
		// Reparent the children.
		// Our rightBranch children already point to the correct parent page,
		// but the leftBranch children do not.
		if err := newLeftBranch.reparentChildren(pager, newLeftBranchPageNum); err != nil {
			return wrap(err, "unable to reparent children")
		}

		return nil
	} else {
		// Otherwise, we need to recursively insert the key into the parent.
		parentPageNum := leftBranch.parentPointer
		parentPage, err := pager.GetPage(parentPageNum)
		if err != nil {
			return wrap(err, "unable to get page")
		}
		parentBranch := pageToBranchNode(parentPage)
		leftBranchNewMaxKey, err := leftBranch.getRightChildMaxKey(sizer, pager)
		if err != nil {
			return wrap(err, "unable to get child maximum")
		}
		if childMaxKey == 19 {
			sink()
		}
		if err := parentBranch.insertAfterSplit(table, sizer, pager, parentPageNum, leftBranchOldMaxKey, leftBranchNewMaxKey, rightBranchPageNum); err != nil {
			// nowrap: indirectly recursive call
			return err
		}
		return nil
	}
}

// reparentChildren updates all child nodes to point to the pageNum of this node.
func (n *branchNode) reparentChildren(pager *Pager, pageNum PagePointer) error {
	maxCells := n.getMaxNumCells()
	for i := cellptr(0); i < maxCells && i < n.numCells; i++ {
		childPageNum := n.cells[i].child
		if err := n.reparentChild(pager, pageNum, childPageNum); err != nil {
			return wrap(err, "unable to reparent child")
		}
	}
	if err := n.reparentChild(pager, pageNum, n.rightChild); err != nil {
		return wrap(err, "unable to reparent child")
	}
	return nil
}

// reparentChildren updates a child node to point to the pageNum of this node.
func (n *branchNode) reparentChild(pager *Pager, pageNum, childPageNum PagePointer) error {
	childPage, err := pager.GetPage(childPageNum)
	if err != nil {
		return wrap(err, "unable to get page")
	}
	childNode := pageToNodeHeader(childPage)
	childNode.parentPointer = pageNum
	if err := pager.sync1(childPageNum); err != nil {
		return wrap(err, "unable to sync child")
	}
	return nil
}

func collectChildPages(branches ...*branchNode) []PagePointer {
	var out []PagePointer
	for _, branch := range branches {
		for i := cellptr(0); i < branch.numCells; i++ {
			out = append(out, branch.cells[i].child)
		}
		out = append(out, branch.rightChild)
	}
	return out
}

func collectKeys(sizer DataSizer, pager *Pager, branches ...*branchNode) []KeyType {
	var out []KeyType
	for _, branch := range branches {
		for i := cellptr(0); i < branch.numCells; i++ {
			out = append(out, branch.cells[i].key)
		}
		x, err := branch.getRightChildMaxKey(sizer, pager)
		if err != nil {
			panic(err)
		}
		out = append(out, x)
	}
	return out
}

func samePages(skip PagePointer, end, start []PagePointer) bool {
	// TODO: remove debug code
	if len(end)-1 != len(start) {
		return false
	}
	endi, starti := 0, 0
	for range end {
		if end[endi] == skip {
			endi++
			continue
		}
		if end[endi] != start[starti] {
			return false
		}
		endi++
		starti++
	}
	return true
}

func sameKeys(skip KeyType, end, start []KeyType) bool {
	// TODO: remove debug code
	if len(end)-1 != len(start) {
		return false
	}
	endi, starti := 0, 0
	for range end {
		if end[endi] == skip {
			endi++
			continue
		}
		if end[endi] != start[starti] {
			return false
		}
		endi++
		starti++
	}
	return true
}

func sink(...interface{}) {
	// TODO: remove debug code
}
