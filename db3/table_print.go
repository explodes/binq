package db3

import "fmt"

// printTree prints out the basic structure of the tree contained in this table.
func (t *Table) printTree() {
	t.printTreeHelper(make(map[PagePointer]struct{}), t.rootPageNum, 0)
}

// printTreeHelper is the recursive utility for printing the tree contained in this table.
func (t *Table) printTreeHelper(visited map[PagePointer]struct{}, pageNum PagePointer, indentationLevel int) {
	if _, inVisited := visited[pageNum]; inVisited {
		t.printIndent(indentationLevel)
		fmt.Printf("- CYCLE (page %d)\n", pageNum)
		return
	} else {
		visited[pageNum] = struct{}{}
	}
	page, err := t.pager.GetPage(pageNum)
	if err != nil {
		fmt.Printf("unable to get page: %v\n", err)
	}
	node := pageToNodeHeader(page)
	if node.isLeaf {
		leaf := pageToLeafNode(page)
		t.printIndent(indentationLevel)
		fmt.Printf("- leaf#%d (size %d) keys:", pageNum, leaf.numCells)
		for i := cellptr(0); i < leaf.numCells; i++ {
			fmt.Printf("%d,", leaf.getCellKey(t, i))
		}
		fmt.Println()
	} else {
		branch := pageToBranchNode(page)
		t.printIndent(indentationLevel)
		fmt.Printf("- branch#%d (size %d)\n", pageNum, branch.numCells)
		for i := cellptr(0); i < branch.numCells; i++ {
			child := branch.cells[i].child
			t.printTreeHelper(visited, child, indentationLevel+1)
			t.printIndent(indentationLevel + 1)
			fmt.Printf("- key %d\n", branch.cells[i].key)
		}
		t.printTreeHelper(visited, branch.rightChild, indentationLevel+1)
	}
}

// printIndent prints an indentation with a given size.
func (t *Table) printIndent(indentationLevel int) {
	for i := 0; i < indentationLevel; i++ {
		fmt.Print("  ")
	}
}

// printPages dumps all the pages in this table.
func (t *Table) printPages() {
	for pageNum, p := range t.pager.pages {
		if pageToNodeHeader(p).isLeaf {
			fmt.Println(pageNum, pageToLeafNode(p).String(t))
		} else {
			fmt.Println(pageNum, pageToBranchNode(p))
		}
	}
}
