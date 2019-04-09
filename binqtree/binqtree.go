// BTree is an implementation of a B-Tree designed to work with mfile.File.

package binqtree

import (
	"bytes"
	"github.com/pkg/errors"
)

const (
	MinMinDegree = 3
)

type KeyType []byte

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

func New(minDegree int) (*BTree, error) {
	if minDegree < MinMinDegree {
		return nil, errors.New("minDegree is too small")
	}
	b := &BTree{
		root:      nil,
		minDegree: minDegree,
	}
	return b, nil
}

// Traverse traverses this tree until the stop condition is met.
func (b *BTree) Traverse(handler func(KeyType) bool) {
	if b.root != nil {
		b.root.traverse(handler)
	}
}

// Search searches this tree for a key.
func (b *BTree) Search(key KeyType) *bTreeEntry {
	if b.root == nil {
		return nil
	}
	return b.root.search(key)
}

// Insert adds a new bTreeEntry to this tree.
func (b *BTree) Insert(key KeyType) {
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

// Remove removes an element with the key.
// Returns true if the key was found.
func (b *BTree) Remove(key KeyType) bool {
	if b.root == nil {
		return false
	}

	removed := b.root.remove(b.minDegree, key)

	// If the root node has no keys left, make its first child the new root if it has a child.
	if len(b.root.keys) == 0 {
		if b.root.isLeaf {
			b.root = nil
		} else {
			b.root = b.root.children[0]
		}
	}

	return removed
}

type bTreeEntry struct {
	key KeyType
}

func newBTreeEntry(key KeyType) *bTreeEntry {
	return &bTreeEntry{
		key: key,
	}
}

func (b *bTreeEntry) compare(other KeyType) int {
	return bytes.Compare(b.key, other)
}

func (b *bTreeEntry) equals(other KeyType) bool {
	return bytes.Equal(b.key, other)
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
func (b *bTreeNode) traverse(handler func(key KeyType) (stop bool)) (stop bool) {
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
func (b *bTreeNode) search(key KeyType) *bTreeEntry {
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
	if i < N && cmp == 0 {
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

// A function that returns the index of the first key that is greater
// or equal to k
func (b *bTreeNode) findKey(key KeyType) int {
	index := 0
	for index < len(b.keys) && b.keys[index].compare(key) < 0 {
		index++
	}
	return index
}

// remove removes the key k in subtree rooted with this node.
// Returns true if the key was removed.
func (b *bTreeNode) remove(minDegree int, key KeyType) bool {
	index := b.findKey(key)

	if index < len(b.keys) && b.keys[index].equals(key) {
		// The key to be removed is present in this node.
		if b.isLeaf {
			return b.removeFromLeaf(index)
		} else {
			return b.removeFromNonLeaf(minDegree, index)
		}
	}

	if b.isLeaf {
		// The key is not present in the tree.
		return false
	}

	// The key to be removed is present in the sub-tree rooted with this node
	// The flag indicates whether the key is present in the sub-tree rooted
	// with the last child of this node.
	isEnd := index == len(b.keys) // FIXME possible off-by-one error

	// If the child where the key is supposed to exist has less than t keys,
	// we fill that child.
	if len(b.children[index].keys) < minDegree {
		b.fill(minDegree, index)
	}

	// If the last child has been merged, it must have merged with the previous
	// child and so we recurse on the (index-1)th child. Else, we recurse on the
	// (index)th child which now has at least t keys.
	if isEnd && index > len(b.keys) { // FIXME possible off-by-one error
		return b.children[index-1].remove(minDegree, key)
	} else {
		return b.children[index].remove(minDegree, key)
	}
}

// removeFromLeaf removes the key present at the index in this node which is a leaf node.
// Returns true if the key was removed.
func (b *bTreeNode) removeFromLeaf(index int) bool {
	b.keys = deleteKey(b.keys, index)
	return true
}

// removeFromLeaf removes the key present at the index in this node which is a non-leaf node.
// Returns true if the key was removed.
func (b *bTreeNode) removeFromNonLeaf(minDegree, index int) bool {
	key := b.keys[index].key

	// If the child that precedes key (children[index]) has at least t keys,
	// find the predecessor of key in the subtree rooted at children[index].
	// Replace key by predecessor.
	// Recursively delete predecessor in children[index].
	if len(b.children[index].keys) >= minDegree {
		predecessor := b.getPredecessor(index)
		b.keys[index] = predecessor
		return b.children[index].remove(minDegree, predecessor.key)
	}

	// If the child children[index] has less that t keys, examine children[index+1].
	// If children[index+1] has at least t keys, find the successor of key in
	// the subtree rooted at children[index+1].
	// Replace key by successor.
	// Recursively delete successor in children[index+1]
	if len(b.children[index+1].keys) >= minDegree {
		successor := b.getSuccessor(index)
		b.keys[index] = successor
		return b.children[index+1].remove(minDegree, successor.key)
	}

	// If both children[index] and children[index+1] have less than t keys, merge
	// key and all of children[index+1] into children[index].
	// Now children[index] contains 2t-1 keys.
	// Free children[index+1] and recursively delete key from children[index].
	b.merge(minDegree, index)
	return b.children[index].remove(minDegree, key)
}

// getPredecessor gets the predecessor of the key- where the key
// is present at the index in the node.
func (b *bTreeNode) getPredecessor(index int) *bTreeEntry {
	current := b.children[index]
	// Keep moving to the right most node until we reach a leaf.
	for !current.isLeaf {
		current = current.children[len(current.keys)]
	}
	// Return the last key of the leaf.
	return current.keys[len(current.keys)-1]
}

// getSuccessor gets the successor of the key- where the key
// is present at the index in the node.
func (b *bTreeNode) getSuccessor(index int) *bTreeEntry {
	current := b.children[index+1]
	// Keep moving to the left most node starting from children[index+1] until we reach a leaf.
	for !current.isLeaf {
		current = current.children[0]
	}
	// Return the first key of the leaf.
	return current.keys[0]
}

// fill fills up the child node present at the index in the C[] array
// if that child has less than t-1 keys.
func (b *bTreeNode) fill(minDegree, index int) {
	// If the previous child has more that t-1 keys, borrow a key from that child.
	if index != 0 && len(b.children[index-1].keys) >= minDegree {
		b.borrowFromPrevious(index)
		return
	}

	// If the next child has more than t-1 keys, borrow a key from that child.
	if index != len(b.keys) && len(b.children[index+1].keys) >= minDegree {
		b.borrowFromNext(index)
		return
	}

	// Merge children[index] with its sibling.
	// If children[index] is the last child, merge it with its previous sibling,
	// otherwise merge it with its next sibling.
	if index != len(b.keys) { // FIXME possible off by one
		b.merge(minDegree, index)
	} else {
		b.merge(minDegree, index-1)
	}
}

// borrowFromPrevious borrows a key from the children[index-1] node and
// place it in the child[index] node.
func (b *bTreeNode) borrowFromPrevious(index int) {
	child := b.children[index]
	sibling := b.children[index-1]

	// The last key from children[index-1] goes up to the parent and key[index-1]
	// from the parent is inserted as the first key in children[index].
	// Thus, the sibling loses a key a the child gains one key.

	// Move all child keys forward.
	child.keys = insertKey(child.keys, nil, 0)

	if !child.isLeaf {
		// Move all child children forward.
		child.children = insertChild(child.children, nil, 0)
	}

	// Set child's first key to keys[index-1] from the current node.
	// Don't remove the slot keys[index-1] as we will fill it later.
	child.keys[0] = b.keys[index-1]

	if !child.isLeaf {
		// Move sibling's last child to the front of the child's children.
		child.children[0] = sibling.children[len(sibling.keys)]
		sibling.children = popChild(sibling.children)
	}

	// Move the key from the sibling to the parent.
	b.keys[index-1] = sibling.keys[len(sibling.keys)-1]
	sibling.keys = popKey(sibling.keys)
}

// borrowFromPrevious borrows a key from the children[index+1] node and
// place it in the child[index] node.
func (b *bTreeNode) borrowFromNext(index int) {
	child := b.children[index]
	sibling := b.children[index+1]

	// keys[index] is appended to child keys.
	// Don't remove the slot keys[index] as we will fill it later.
	child.keys = append(child.keys, b.keys[index])

	if !child.isLeaf {
		// Sibling's first child is inserted as the last child into the child's children.
		child.children = append(child.children, sibling.children[0])
	}

	// The first key from sibling is moved into keys[index].
	b.keys[index] = sibling.keys[0]
	sibling.keys = deleteKey(sibling.keys, 0)

	if !sibling.isLeaf {
		// Move sibling children one step behind.
		sibling.children = deleteChild(sibling.children, 0)
	}
}

// merge merges the child at the index of the node with the child at index+1.
func (b *bTreeNode) merge(minDegree, index int) {
	child := b.children[index]
	sibling := b.children[index+1]
	numSiblingKeys := len(sibling.keys)

	// Move key[index] into the child's keys.
	child.keys = append(child.keys, b.keys[index])

	// Append keys from child[i+t:] to the end of sibling's keys.
	for i := 0; i < numSiblingKeys; i++ {
		child.keys = append(child.keys, sibling.keys[i])
	}

	if !child.isLeaf {
		// Move children from sibling to child.
		for i := 0; i <= numSiblingKeys; i++ {
			child.children = append(child.children, sibling.children[i])
		}
	}

	// Move down our keys and children.
	b.keys = deleteKey(b.keys, index)
	b.children = deleteChild(b.children, index+1)
}

func maxKeys(minDegree int) int {
	return 2*minDegree - 1
}

func maxChildren(minDegree int) int {
	return 2 * minDegree
}
