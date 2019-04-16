package binqtree

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

const (
	// validateIntermediateOrderForNumKeys indicates a limit as
	// to validate that data is inserted in order.
	validateIntermediateOrderForNumKeys = 10
)

func TestSelectStatement_Execute_empty(t *testing.T) {
	testWithTable(t, uint16(sentinelValueSize), func(t *testing.T, table *Table) {
		cursor, err := selectEntireTable(table).Query()
		must(t, err)

		count := cursorCount(t, cursor)
		assert.Equal(t, 0, count)
	})
}

func TestInsertSelectStatement(t *testing.T) {
	cases := []struct {
		name    string
		numKeys int
	}{
		{"empty", 0},
		{"single", 1},
		{"small", 10},
		{"4-leaf", 30},
		{"known-limitation", 1400},
		{"medium", 1e2},
		{"large", 1e3},
		{"huge", 1e4},
		{"epic", 1e5},
		{"million", 1e6},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("linear_%s_%d", tc.name, tc.numKeys), func(t *testing.T) {
			for i := 0; i < 10; i++ {
				testInsertedValuesAreOrdered(t, tc.numKeys)
			}
		})
		t.Run(fmt.Sprintf("shuffled_%s_%d", tc.name, tc.numKeys), func(t *testing.T) {
			for i := 0; i < 10; i++ {
				testShuffledInsertedValuesAreOrdered(t, tc.numKeys)
			}
		})
	}
}

func testInsertedValuesAreOrdered(t *testing.T, numKeys int) {
	testWithTable(t, uint16(sentinelValueSize), func(t *testing.T, table *Table) {
		const (
			startKey = KeyType(1)
		)
		for i := 0; i < numKeys; i++ {
			key := startKey + KeyType(i)
			sentinel := newSentinelValue(t, key)
			insert := sentinel.toInsertStatement(t, table)
			err := insert.Execute()
			if err != nil {
				t.Fatalf("error at insert #%d (key %d): %v", i, key, err)
			}
			if i < validateIntermediateOrderForNumKeys {
				if !assertOrdered(t, table, startKey, i+1) {
					break
				}
			}
		}
		assertOrdered(t, table, startKey, numKeys)
	})
}

func testShuffledInsertedValuesAreOrdered(t *testing.T, numKeys int) {
	testWithTable(t, uint16(sentinelValueSize), func(t *testing.T, table *Table) {
		const (
			startKey = KeyType(1)
		)
		keys := make([]KeyType, numKeys)

		for i := 0; i < numKeys; i++ {
			key := startKey + KeyType(i)
			keys[i] = key
		}
		rand.Shuffle(numKeys, func(i, j int) {
			keys[i], keys[j] = keys[j], keys[i]
		})
		for i := 0; i < numKeys; i++ {
			key := keys[i]
			sentinel := newSentinelValue(t, key)
			insert := sentinel.toInsertStatement(t, table)
			err := insert.Execute()
			if err != nil {
				t.Fatalf("error at insert #%d (key %d): %v", i, key, err)
			}
		}
		assertOrdered(t, table, startKey, numKeys)
	})
}

func assertOrdered(t testType, table *Table, startKey KeyType, numKeys int) bool {
	cursor, err := selectEntireTable(table).Query()
	must(t, err)
	values := cursorConsume(t, cursor)
	// Avoid assert.Len for huge lists, the output is ridiculous.
	if !assert.Equal(t, numKeys, len(values)) {
		printPages(table)
		return false
	}
	for i := 0; i < numKeys; i++ {
		selected := parseSentinelValue(t, values[i].value)
		key := startKey + KeyType(i)
		if !assert.True(t, selected.wellFormed(t, key)) {
			printPages(table)
			return false
		}
	}
	return true
}

func printPages(table *Table) {
	for pageNum, p := range table.pager.pages {
		if pageToNodeHeader(p).isLeaf {
			fmt.Println(pageNum, pageToLeafNode(p).String(table))
		} else {
			fmt.Println(pageNum, pageToBranchNode(p))
		}
	}
}
