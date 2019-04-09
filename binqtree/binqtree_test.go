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
	const (
		micro  = 3
		mini   = 20
		small  = 200
		large  = 2000
		huge   = 20000
		insane = 200000
	)
	minDegreeSizes := []int{
		micro,
		mini,
		small,
		large,
		huge,
		insane,
	}
	numKeySizes := []int{
		mini,
		small,
		large,
		huge,
		//insane,
	}
	for _, minDegree := range minDegreeSizes {
		t.Run(fmt.Sprintf("minDegree%d", minDegree), func(t *testing.T) {
			for _, numKeys := range numKeySizes {
				t.Run(fmt.Sprintf("numKeys%d", numKeys), func(t *testing.T) {
					testBTree(t, minDegree, numKeys)
				})
			}
		})
	}
}

func testBTree(t *testing.T, minDegree, numKeys int) {
	tree := binqtree.New(minDegree)

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

	testSearch(t, false, tree, makeKey(-1))
	testSearch(t, true, tree, makeKey(0))
	testSearch(t, 6 < numKeys, tree, makeKey(6))
	testSearch(t, 15 < numKeys, tree, makeKey(15))
	testSearch(t, true, tree, makeKey(numKeys/2))
	testSearch(t, true, tree, makeKey(numKeys-1))
	testSearch(t, false, tree, makeKey(numKeys))
	testSearch(t, false, tree, makeKey(numKeys+1))
	testSearch(t, false, tree, makeKey(numKeys*2))

	for index, key := range keys {
		realKey := unpackKey(key)
		removed := tree.Remove(key)
		if !assert.True(t, removed, "key #%d %v was not removed", index, realKey) {
			return
		}
		testSearch(t, false, tree, key)
		if !assert.Equal(t, len(keys)-index-1, len(getAllKeys(tree, numKeys))) {
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

func testSearch(t *testing.T, found bool, tree *binqtree.BTree, key binqtree.KeyType) {
	x := tree.Search(key)
	actuallyFound := x != nil
	assert.Equal(t, found, actuallyFound)
}

func makeKey(i int) binqtree.KeyType {
	return makeBytesKey(i)
}

func unpackKey(b binqtree.KeyType) int {
	return unpackBytesKey(b)
}

//func makeKeyType(i int) binqtree.KeyType {
//	return binqtree.KeyType(i)
//}
//
//func unpackKeyType(b binqtree.KeyType) int {
//	return int(b)
//}

func makeBytesKey(i int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	return b
}

func unpackBytesKey(b []byte) int {
	return int(binary.BigEndian.Uint64(b))
}
