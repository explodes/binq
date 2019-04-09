package binqtree_test

import (
	"encoding/binary"
	"explodes/github.com/binq/binqtree"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func TestBTree(t *testing.T) {
	for numKeys := 0; numKeys <= 1e4; numKeys *= 10 {
		t.Run(fmt.Sprintf("numKeys%d", numKeys), func(t *testing.T) {
			for minDegree := 10; minDegree <= 1e5; minDegree *= 10 {
				t.Run(fmt.Sprintf("minDegree%d", minDegree), func(t *testing.T) {
					testBTree(t, minDegree, numKeys)
				})
			}
		})
		if numKeys == 0 {
			numKeys = 1
		}
	}
}

func testBTree(t *testing.T, minDegree, numKeys int) {
	tree, err := binqtree.New(minDegree)
	if err != nil {
		t.Fatal(err)
	}

	orderedKeys := make([]binqtree.KeyType, numKeys)
	keys := make([]binqtree.KeyType, numKeys)
	for i := 0; i < len(keys); i++ {
		key := makeKey(i)
		keys[i] = key
		orderedKeys[i] = key
	}
	rng := rand.New(rand.NewSource(42))
	rng.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	for _, key := range keys {
		tree.Insert(key)
	}

	result := getAllKeys(tree, numKeys)

	if len(result) != numKeys {
		t.Fatal("invalid number of keys")
	}
	for i := 0; i < numKeys; i++ {
		if !assert.Equal(t, orderedKeys[i], result[i], "unexpected result %d at %d", unpackKey(result[i]), i) {
			return
		}
	}

	testSearchInDefaultRange(t, tree, numKeys, -1)
	testSearchInDefaultRange(t, tree, numKeys, 0)
	testSearchInDefaultRange(t, tree, numKeys, 6)
	testSearchInDefaultRange(t, tree, numKeys, 15)
	testSearchInDefaultRange(t, tree, numKeys, numKeys/2)
	testSearchInDefaultRange(t, tree, numKeys, numKeys-1)
	testSearchInDefaultRange(t, tree, numKeys, numKeys)
	testSearchInDefaultRange(t, tree, numKeys, numKeys+1)
	testSearchInDefaultRange(t, tree, numKeys, numKeys*2)

	for index, key := range keys {
		rawKey := unpackKey(key)
		removed := tree.Remove(key)
		if !assert.True(t, removed, "key #%d %v was not removed", index, rawKey) {
			return
		}
		currentNumberOfKeys := numKeys - index - 1
		testSearch(t, tree, rawKey, false)
		if !assert.Equal(t, currentNumberOfKeys, len(getAllKeys(tree, numKeys))) {
			return
		}
	}
}

func getAllKeys(tree *binqtree.BTree, numKeys int) []binqtree.KeyType {
	result := make([]binqtree.KeyType, 0, numKeys)
	tree.Traverse(func(key binqtree.KeyType) bool {
		result = append(result, key)
		return false
	})
	return result
}

func inDefaultRange(numKeys, rawKey int) bool {
	return rawKey >= 0 && rawKey < numKeys
}

func testSearchInDefaultRange(t *testing.T, tree *binqtree.BTree, numKeys, rawKey int) {
	t.Helper()
	testSearch(t, tree, rawKey, inDefaultRange(numKeys, rawKey))
}

func testSearch(t *testing.T, tree *binqtree.BTree, rawKey int, shouldFind bool) {
	t.Helper()
	x := tree.Search(makeKey(rawKey))
	actuallyFound := x != nil
	assert.Equal(t, shouldFind, actuallyFound)
}

func makeKey(i int) binqtree.KeyType {
	return makeBytesKey(i)
}

func unpackKey(b binqtree.KeyType) int {
	return unpackBytesKey(b)
}

func makeBytesKey(i int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	return b
}

func unpackBytesKey(b []byte) int {
	return int(binary.BigEndian.Uint64(b))
}
