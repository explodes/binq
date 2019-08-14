package binq

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestJumpOffset(t *testing.T) {
	t.Parallel()
	matches100 := MatcherFunc(func(b []byte) (bool, error) {
		v, err := GetU64le(b)
		return v.(uint64) == 100, err
	})
	cases := []struct {
		name          string
		bytes         []byte
		offset        uint64
		expectedMatch bool
		expectedErr   bool
	}{
		{"too-small", []byte{}, 100, false, true},
		{"jump-initial-equal", makeBytes(t, u64le(100)), 0, true, false},
		{"jump-initial-unequal", makeBytes(t, u64le(999)), 0, false, false},
		{"jump-offset-equal", makeBytes(t, u64le(999), u64le(100)), 8, true, false},
		{"jump-offset-unequal", makeBytes(t, u64le(999), u64le(999)), 8, false, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			matches, err := WithJump(JumpOffset(tc.offset), matches100)(tc.bytes)
			assert.Equal(t, tc.expectedErr, err != nil, "(un)expected error")
			assert.Equal(t, tc.expectedMatch, matches, "(un)expected match")
		})
	}
}

func TestJumpToU64le(t *testing.T) {
	t.Parallel()
	matches100 := MatcherFunc(func(b []byte) (bool, error) {
		v, err := GetU64le(b)
		return v.(uint64) == 100, err
	})
	cases := []struct {
		name          string
		bytes         []byte
		expectedMatch bool
		expectedErr   bool
	}{
		{"too-small", []byte{}, false, true},
		{"jump-too-small", makeBytes(t, u64le(8)), false, true},
		{"jump-first-equal", makeBytes(t, u64le(8), u64le(100)), true, false},
		{"jump-first-unequal", makeBytes(t, u64le(8), u64le(999)), false, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			matches, err := WithJump(JumpToU64le(0), matches100)(tc.bytes)
			assert.Equal(t, tc.expectedErr, err != nil, "(un)expected error")
			assert.Equal(t, tc.expectedMatch, matches, "(un)expected match")
		})
	}
}

func TestJumpToU64be(t *testing.T) {
	t.Parallel()
	matches100 := MatcherFunc(func(b []byte) (bool, error) {
		v, err := GetU64be(b)
		return v.(uint64) == 100, err
	})
	cases := []struct {
		name          string
		bytes         []byte
		expectedMatch bool
		expectedErr   bool
	}{
		{"too-small", []byte{}, false, true},
		{"jump-too-small", makeBytes(t, u64be(8)), false, true},
		{"jump-first-equal", makeBytes(t, u64be(8), u64be(100)), true, false},
		{"jump-first-unequal", makeBytes(t, u64be(8), u64be(999)), false, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			matches, err := WithJump(JumpToU64be(0), matches100)(tc.bytes)
			assert.Equal(t, tc.expectedErr, err != nil, "(un)expected error")
			assert.Equal(t, tc.expectedMatch, matches, "(un)expected match")
		})
	}
}

func TestJumpToU32le(t *testing.T) {
	t.Parallel()
	matches100 := MatcherFunc(func(b []byte) (bool, error) {
		v, err := GetU32le(b)
		return v.(uint32) == 100, err
	})
	cases := []struct {
		name          string
		bytes         []byte
		expectedMatch bool
		expectedErr   bool
	}{
		{"too-small", []byte{}, false, true},
		{"jump-too-small", makeBytes(t, u32le(4)), false, true},
		{"jump-first-equal", makeBytes(t, u32le(4), u32le(100)), true, false},
		{"jump-first-unequal", makeBytes(t, u32le(4), u32le(999)), false, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			matches, err := WithJump(JumpToU32le(0), matches100)(tc.bytes)
			assert.Equal(t, tc.expectedErr, err != nil, "(un)expected error")
			assert.Equal(t, tc.expectedMatch, matches, "(un)expected match")
		})
	}
}

func TestJumpToU32be(t *testing.T) {
	t.Parallel()
	matches100 := MatcherFunc(func(b []byte) (bool, error) {
		v, err := GetU32be(b)
		return v.(uint32) == 100, err
	})
	cases := []struct {
		name          string
		bytes         []byte
		expectedMatch bool
		expectedErr   bool
	}{
		{"too-small", []byte{}, false, true},
		{"jump-too-small", makeBytes(t, u32be(4)), false, true},
		{"jump-first-equal", makeBytes(t, u32be(4), u32be(100)), true, false},
		{"jump-first-unequal", makeBytes(t, u32be(4), u32be(999)), false, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			matches, err := WithJump(JumpToU32be(0), matches100)(tc.bytes)
			assert.Equal(t, tc.expectedErr, err != nil, "(un)expected error")
			assert.Equal(t, tc.expectedMatch, matches, "(un)expected match")
		})
	}
}

func TestJumpToU16le(t *testing.T) {
	t.Parallel()
	matches100 := MatcherFunc(func(b []byte) (bool, error) {
		v, err := GetU16le(b)
		return v.(uint16) == 100, err
	})
	cases := []struct {
		name          string
		bytes         []byte
		expectedMatch bool
		expectedErr   bool
	}{
		{"too-small", []byte{}, false, true},
		{"jump-too-small", makeBytes(t, u16le(2)), false, true},
		{"jump-first-equal", makeBytes(t, u16le(2), u16le(100)), true, false},
		{"jump-first-unequal", makeBytes(t, u16le(2), u16le(999)), false, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			matches, err := WithJump(JumpToU16le(0), matches100)(tc.bytes)
			assert.Equal(t, tc.expectedErr, err != nil, "(un)expected error")
			assert.Equal(t, tc.expectedMatch, matches, "(un)expected match")
		})
	}
}

func TestJumpToU16be(t *testing.T) {
	t.Parallel()
	matches100 := MatcherFunc(func(b []byte) (bool, error) {
		v, err := GetU16be(b)
		return v.(uint16) == 100, err
	})
	cases := []struct {
		name          string
		bytes         []byte
		expectedMatch bool
		expectedErr   bool
	}{
		{"too-small", []byte{}, false, true},
		{"jump-too-small", makeBytes(t, u16be(2)), false, true},
		{"jump-first-equal", makeBytes(t, u16be(2), u16be(100)), true, false},
		{"jump-first-unequal", makeBytes(t, u16be(2), u16be(999)), false, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			matches, err := WithJump(JumpToU16be(0), matches100)(tc.bytes)
			assert.Equal(t, tc.expectedErr, err != nil, "(un)expected error")
			assert.Equal(t, tc.expectedMatch, matches, "(un)expected match")
		})
	}
}

func TestJumpToU8(t *testing.T) {
	t.Parallel()
	matches100 := MatcherFunc(func(b []byte) (bool, error) {
		v, err := GetU8(b)
		return v.(uint8) == 100, err
	})
	cases := []struct {
		name          string
		bytes         []byte
		expectedMatch bool
		expectedErr   bool
	}{
		{"too-small", []byte{}, false, true},
		{"jump-too-small", makeBytes(t, u8(1)), false, true},
		{"jump-first-equal", makeBytes(t, u8(1), u8(100)), true, false},
		{"jump-first-unequal", makeBytes(t, u8(1), u8(99)), false, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			matches, err := WithJump(JumpToU8(0), matches100)(tc.bytes)
			assert.Equal(t, tc.expectedErr, err != nil, "(un)expected error")
			assert.Equal(t, tc.expectedMatch, matches, "(un)expected match")
		})
	}
}
