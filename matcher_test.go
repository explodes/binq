package binq

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAll(t *testing.T) {
	t.Parallel()
	var testData []byte

	cases := []struct {
		name          string
		matchers      []Matcher
		expectedMatch bool
		expectedErr   bool
	}{
		{"empty", []Matcher{}, true, false},
		{"single-match", []Matcher{matchesAny()}, true, false},
		{"single-no-match", []Matcher{matchesNone()}, false, false},
		{"single-err", []Matcher{matchesErr()}, false, true},
		{"multi-match", []Matcher{matchesAny(), matchesAny()}, true, false},
		{"multi-no-match", []Matcher{matchesAny(), matchesNone()}, false, false},
		{"multi-err", []Matcher{matchesAny(), matchesErr()}, false, true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			matches, err := All(tc.matchers...)(testData)
			assert.Equal(t, tc.expectedErr, err != nil, "(un)expected error")
			assert.Equal(t, tc.expectedMatch, matches, "(un)expected match")
		})
	}
}

func TestAny(t *testing.T) {
	t.Parallel()
	var testData []byte

	cases := []struct {
		name          string
		matchers      []Matcher
		expectedMatch bool
		expectedErr   bool
	}{
		{"empty", []Matcher{}, false, false},
		{"single-match", []Matcher{matchesAny()}, true, false},
		{"single-no-match", []Matcher{matchesNone()}, false, false},
		{"single-err", []Matcher{matchesErr()}, false, true},
		{"multi-match", []Matcher{matchesAny(), matchesAny()}, true, false},
		{"multi-one-match", []Matcher{matchesAny(), matchesNone()}, true, false},
		{"multi-no-match", []Matcher{matchesNone(), matchesNone()}, false, false},
		{"multi-match-err", []Matcher{matchesAny(), matchesErr()}, true, false},
		{"multi-no-match-err", []Matcher{matchesNone(), matchesErr()}, false, true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			matches, err := Any(tc.matchers...)(testData)
			assert.Equal(t, tc.expectedErr, err != nil, "(un)expected error")
			assert.Equal(t, tc.expectedMatch, matches, "(un)expected match")
		})
	}
}

func TestLen(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name          string
		bytesLength   int
		testLength    int
		expectedMatch bool
		expectedErr   bool
	}{
		{"empty-smaller", 0, 1, false, false},
		{"empty-exact", 0, 0, true, false},
		{"empty-larger", 0, -1, true, false},
		{"not-empty-smaller", 9, 10, false, false},
		{"not-empty-exact", 10, 10, true, false},
		{"not-empty-larger", 11, 10, true, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testData := make([]byte, tc.bytesLength)
			matches, err := Len(tc.testLength)(testData)
			assert.Equal(t, tc.expectedErr, err != nil, "(un)expected error")
			assert.Equal(t, tc.expectedMatch, matches, "(un)expected match")
		})
	}
}
