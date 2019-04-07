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
		mini   = 20
		small  = 200
		large  = 2000
		huge   = 20000
		insane = 200000
	)
	sizes := []int{
		mini,
		small,
		large,
		huge,
		//insane,
	}
	for _, minDegree := range sizes {
		t.Run(fmt.Sprintf("minDegree%d", minDegree), func(t *testing.T) {
			for _, numKeys := range sizes {
				t.Run(fmt.Sprintf("numKeys%d", numKeys), func(t *testing.T) {
					testBTree(t, minDegree, numKeys)
				})
			}
		})
	}
}

func testBTree(t *testing.T, minDegree, numKeys int) {
	tree := binqtree.New(minDegree)

	orderedKeys := make([][]byte, numKeys)
	keys := make([][]byte, numKeys)
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

	result := make([][]byte, 0, numKeys)
	tree.Traverse(func(key []byte) bool {
		result = append(result, key)
		return false
	})

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
	testSearch(t, false, tree, makeKey(numKeys))
	testSearch(t, false, tree, makeKey(numKeys+1))
	testSearch(t, false, tree, makeKey(numKeys*2))
}

func testSearch(t *testing.T, found bool, tree *binqtree.BTree, key []byte) {
	x := tree.Search(key)
	actuallyFound := x != nil
	assert.Equal(t, found, actuallyFound)
}

func makeKey(i int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	return b
}

func unpackKey(b []byte) int {
	return int(binary.BigEndian.Uint64(b))
}
