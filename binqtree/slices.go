package binqtree

func insertKey(s []*bTreeEntry, x *bTreeEntry, i int) []*bTreeEntry {
	s = append(s, nil)
	copy(s[i+1:], s[i:])
	s[i] = x
	return s
}

func deleteKey(a []*bTreeEntry, i int) []*bTreeEntry {
	copy(a[i:], a[i+1:])
	a[len(a)-1] = nil // or the zero value of T
	a = a[:len(a)-1]
	return a
}