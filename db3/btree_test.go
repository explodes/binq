package db3

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"unsafe"
)

func TestBtreeSizes(t *testing.T) {
	assert.True(t, unsafe.Sizeof(branchNode{}) <= PageSize, "A branch node should be able to fit in a single page.")
	assert.True(t, unsafe.Sizeof(leafNode{}) <= PageSize, "A leaf node should be able to fit in a single page.")
}

func TestKeyFromBytes(t *testing.T) {
	bin := []byte{0xff, 0xee, 0xdd, 0xcc}
	key := keyFromBytes(bin)
	assert.Equal(t, KeyType(0xccddeeff), key)
}

func TestKey_encodeToBytes(t *testing.T) {
	bin := make([]byte, unsafe.Sizeof(KeyType(0)))
	key := KeyType(0xccddeeff)
	encodeKeyToBytes(key, bin)
	assert.Equal(t, []byte{0xff, 0xee, 0xdd, 0xcc}, bin)
}
