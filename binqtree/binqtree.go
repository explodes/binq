// BTree is an implementation of a B-Tree designed to work with mfile.File.

package binqtree

import "bytes"

// BTree is an implementation of a B-Tree designed to work with mfile.File.
// Some important reminders about b-trees:
// - "t" is the minimum degree of the tree.
// - Root nodes may contain a minimum of 1 key.
// - Non-root nodes must contain at least t-1 keys.
// - All nodes may contain at most 2t-1 keys.
// - The number of children in a node is equal to the number of keys in it plus 1.
type BTree struct {
	// root is the top level node in this tree.
	root *bTreeNode
	// minDegree is the defining attribute of this tree. It is used for balancing.
	// A B-Tree is defined by the term minimum degree ‘t’.
	// The value of t depends upon disk block size.
	minDegree int
}

func New(minDegree int) *BTree {
	return &BTree{
		root:      nil,
		minDegree: minDegree,
	}
}

// Traverse traverses this tree until the stop condition is met.
func (b *BTree) Traverse(handler func([]byte) bool) {
	if b.root != nil {
		b.root.traverse(handler)
	}
}

// Search searches this tree for a key.
func (b *BTree) Search(key []byte) *bTreeEntry {
	if b.root == nil {
		return nil
	}
	return b.root.search(key)
}

// Insert adds a new bTreeEntry to this tree.
func (b *BTree) Insert(key []byte) {
	entry := newBTreeEntry(key)

	if b.root == nil {
		// If tree is empty, create the root node.
		b.root = newBTreeNode(b.minDegree, true)
		b.root.keys = append(b.root.keys, entry)
		return
	}

	if len(b.root.keys) == maxKeys(b.minDegree) {
		// If the root is full then the tree grows in height.

		// Allocate the non-leaf node.
		node := newBTreeNode(b.minDegree, false)

		// Make the old root a child of the new root.
		node.children = append(node.children, b.root)

		// Split the old root and move one key to the new root.
		node.splitChild(b.minDegree, 0, b.root)

		// The new root has two children now.
		// Decide which of the two children is going to have the new key.
		var i int
		if node.keys[0].compare(entry.key) < 0 {
			i = 1
		} else {
			i = 0
		}
		node.children[i].insertNonFull(b.minDegree, entry)

		// Change the root.
		b.root = node

		return
	}

	// If the root is not full, insert the value into the root.
	b.root.insertNonFull(b.minDegree, entry)
}

type bTreeEntry struct {
	key []byte
}

func newBTreeEntry(key []byte) *bTreeEntry {
	return &bTreeEntry{
		key: key,
	}
}

func (b *bTreeEntry) compare(other []byte) int {
	return bytes.Compare(b.key, other)
}

type bTreeNode struct {
	// keys are the keys stored in this node.
	// Every node except root must contain at least t-1 keys. Root may contain minimum 1 key.
	// All nodes (including root) may contain at most 2t-1 keys.
	// All keys of a node are sorted in increasing order.
	// The child between two keys k1 and k2 contains all keys in the range from k1 and k2.
	keys []*bTreeEntry
	// children is the set of child nodes in this tree.
	// Number of children of a node is equal to the number of keys in it plus 1.
	children []*bTreeNode
	isLeaf   bool
}

func newBTreeNode(minDegree int, isLeaf bool) *bTreeNode {
	return &bTreeNode{
		isLeaf:   isLeaf,
		keys:     make([]*bTreeEntry, 0, maxKeys(minDegree)),
		children: make([]*bTreeNode, 0, maxChildren(minDegree)),
	}
}

// traverse visits all nodes in a subtree rooted with this node until it is stopped.
func (b *bTreeNode) traverse(handler func(key []byte) (stop bool)) (stop bool) {
	// There are n keys and n+1 children.
	// Traverse through n keys and first n children.
	for index, key := range b.keys {
		stop := false
		if !b.isLeaf {
			stop = b.children[index].traverse(handler)
		}
		if stop || handler(key.key) {
			return true
		}
	}
	// Traverse the subtree rooted with last child.
	if !b.isLeaf && len(b.children) > 0 {
		if stop := b.children[len(b.children)-1].traverse(handler); stop {
			return true
		}
	}
	return false
}

// search find a key in the subtree rooted at this node.
func (b *bTreeNode) search(key []byte) *bTreeEntry {
	// Find the first key greater than or equal to the input key.
	i := 0
	N := len(b.keys)
	cmp := 0
	for i < N {
		cmp = b.keys[i].compare(key)
		if cmp >= 0 {
			break
		}
		i++
	}

	// If the found key is equal to the input key, return this node.
	if cmp == 0 {
		return b.keys[i]
	}

	// If the key is not found here and this is a leaf node we did not find the key.
	if b.isLeaf {
		return nil
	}

	// Search the appropriate child.
	return b.children[i].search(key)
}

// insertNonFull is a utility function to insert a new key in the subtree rooted with
// this node. The assumption is, the node must be non-full when this function is called.
func (b *bTreeNode) insertNonFull(minDegree int, entry *bTreeEntry) {

	// Initialize an index as the index of the last key.
	i := len(b.keys) - 1

	if b.isLeaf {
		// The following loops does two things:
		// a) Finds the location of the new key to be inserted.
		// b) Moves all greater keys to one place ahead.
		b.keys = append(b.keys, nil)
		for i >= 0 && b.keys[i].compare(entry.key) > 0 {
			b.keys[i+1] = b.keys[i]
			i--
		}

		// Insert the new key at the found location.
		b.keys[i+1] = entry

		return
	}

	// Find the child which is going to have the new key.
	for i >= 0 && b.keys[i].compare(entry.key) > 0 {
		i--
	}

	if len(b.children[i+1].keys) == maxKeys(minDegree) {
		// If the found child is full then split it.
		b.splitChild(minDegree, i+1, b.children[i+1])

		// After the split, the middle key of the child goes up and the child is split in two.
		// See which of the two is going to have the new key.
		if b.keys[i+1].compare(entry.key) < 0 {
			i++
		}
	}

	b.children[i+1].insertNonFull(minDegree, entry)
}

// splitChild utility function to split the child of this node. index is the index of child in
// children. The child must be full when this function is called
func (b *bTreeNode) splitChild(minDegree, childIndex int, child *bTreeNode) {
	// Create a new node which is going to store (t-1) keys of the child.
	node := newBTreeNode(minDegree, child.isLeaf)

	// Copy the last (t-1) keys of the child to the new node.
	for j := 0; j < minDegree-1; j++ {
		node.keys = append(node.keys, child.keys[j+minDegree])
	}

	if !child.isLeaf {
		// Move the last t children of the child to the new node.
		for j := 0; j < minDegree; j++ {
			node.children = append(node.children, child.children[j+minDegree])
		}
		child.children = child.children[:minDegree]
	}

	// Reduce the number of keys and children in child.
	child.keys = child.keys[:minDegree]

	// Since this node is going to have a new child, create space for the new child.
	b.children = append(b.children, nil)
	for j := len(b.keys); j >= childIndex+1; j-- {
		b.children[j+1] = b.children[j]
	}

	// Link the new child to this node.
	b.children[childIndex+1] = node

	// Move the middle key of child to this node.
	b.keys = insertKey(b.keys, child.keys[minDegree-1], childIndex)
	child.keys = deleteKey(child.keys, minDegree-1)
}

func maxKeys(minDegree int) int {
	return 2*minDegree - 1
}

func maxChildren(minDegree int) int {
	return 2 * minDegree
}
