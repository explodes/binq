package db3

type cursorValue struct {
	key   KeyType
	value []byte
}

func cursorConsume(t testType, c *Cursor, max int) []cursorValue {
	t.Helper()
	var out []cursorValue
	count := 0
	for ; !c.End(); c.Next() {
		key, value, err := c.Value()
		if err != nil {
			t.Fatalf("count not perform consume: %v", err)
		}
		out = append(out, cursorValue{key: key, value: value})
		count++
		if count == max {
			//break
		}
	}
	if count == max && !c.End() {
		t.Fatalf("more nodes available beyond max of %d", max)
	}
	return out
}

func cursorCount(t testType, c *Cursor) int {
	t.Helper()
	count := 0
	for ; !c.End(); c.Next() {
		_, _, err := c.Value()
		if err != nil {
			t.Fatalf("count not perform count: %v", err)
		}
		count++
	}
	return count
}
