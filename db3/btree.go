package db3

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

const (
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

func (n *nodeHeader) getMaxKey(sizer DataSizer) KeyType {
	if n.isLeaf {
		return (*leafNode)(unsafe.Pointer(n)).getMaxKey(sizer)
	} else {
		return (*branchNode)(unsafe.Pointer(n)).getMaxKey()
	}
}

func (n *nodeHeader) String() string {
	return fmt.Sprintf("{isLeaf:%v,isRoot:%v,parentPointer:%d}", n.isLeaf, n.isRoot, n.parentPointer)
}
