package db3

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
	testWithLimitedTable(t, uint16(sentinelValueSize), func(t *testing.T, table *Table) {
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
		//{"empty", 0},
		//{"depth1", maxValues},
		//{"depth1+1", 1 + maxValues},
		//{"depth2", maxChildren * maxValues},
		//{"depth2+1", 1 + maxChildren*maxValues},
		{"depth3", maxChildren * maxChildren * maxValues},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("linear_%s_%d", tc.name, tc.numKeys), func(t *testing.T) {
			//testInsertedValuesAreOrdered(t, tc.numKeys)
		})
		t.Run(fmt.Sprintf("shuffled_%s_%d", tc.name, tc.numKeys), func(t *testing.T) {
			testShuffledInsertedValuesAreOrdered(t, tc.numKeys)
		})
	}
}

func testInsertedValuesAreOrdered(t *testing.T, numKeys int) {
	testWithLimitedTable(t, uint16(sentinelValueSize), func(t *testing.T, table *Table) {
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
					//dumpTable(table)
					break
				}
			}
		}
		if !assertOrdered(t, table, startKey, numKeys) {
			//dumpTable(table)
		}
	})
}

func testShuffledInsertedValuesAreOrdered(t *testing.T, numKeys int) {
	testWithLimitedTable(t, uint16(sentinelValueSize), func(t *testing.T, table *Table) {
		const (
			startKey = KeyType(1)
		)
		keys := make([]KeyType, numKeys)

		for i := 0; i < numKeys; i++ {
			key := startKey + KeyType(i)
			keys[i] = key
		}
		rand.New(rand.NewSource(42)).Shuffle(numKeys, func(i, j int) {
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
			dumpTable(table)
		}
		if !assertOrdered(t, table, startKey, numKeys) {
			//dumpTable(table)
		}
	})
}

func assertOrdered(t testType, table *Table, startKey KeyType, numKeys int) bool {
	cursor, err := selectEntireTable(table).Query()
	must(t, err)
	values := cursorConsume(t, cursor, numKeys)
	// Avoid assert.Len for huge lists, the output is ridiculous.
	if !assert.Equal(t, numKeys, len(values)) {
		fmt.Printf("unexpected total num keys, got %d want %d\n", len(values), numKeys)
		//return false
	}
	for i := 0; i < numKeys; i++ {
		selected := parseSentinelValue(t, values[i].value)
		key := startKey + KeyType(i)
		if !assert.True(t, selected.wellFormed(t, key)) {
			fmt.Printf("unexpected row at %d\n", i)
			return false
		}
	}
	return true
}
