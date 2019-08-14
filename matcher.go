package binq

import (
	"github.com/pkg/errors"
)

var (
	// ErrBytesTooSmall indicates that an unexpectedly smaller number of bytes was encountered.
	ErrBytesTooSmall = errors.New("data too small")

	MatchAnything = MatcherFunc(func([]byte) (bool, error) { return true, nil })
	MatchNothing  = MatcherFunc(func([]byte) (bool, error) { return false, nil })
)

// Matcher is the interface for matching patterns on binary data.
type Matcher interface {
	// Match returns true if the byte data matches.
	Match([]byte) (bool, error)
}

var _ Matcher = (MatcherFunc)(nil)

// MatcherFunc is a Matcher composed of a single function.
type MatcherFunc func([]byte) (bool, error)

// Match satisfies the Matcher interface.
func (f MatcherFunc) Match(b []byte) (bool, error) {
	return f(b)
}

// All creates a Matcher that matches if all Matcher predicates are satisfied.
func All(funcs ...Matcher) MatcherFunc {
	return func(bytes []byte) (bool, error) {
		for _, f := range funcs {
			result, err := f.Match(bytes)
			if err != nil {
				return false, wrap(err, "unable to run matcher")
			}
			if !result {
				return false, nil
			}
		}
		return true, nil
	}
}

// Any creates a Matcher that matches if at least one Matcher predicate is satisfied.
func Any(funcs ...Matcher) MatcherFunc {
	return func(bytes []byte) (bool, error) {
		for _, f := range funcs {
			result, err := f.Match(bytes)
			if err != nil {
				return false, wrap(err, "unable to run matcher")
			}
			if result {
				return true, nil
			}
		}
		return false, nil
	}
}

// Len creates a Matcher that matches if the length of the data is at least a particular size.
func Len(minLength int) MatcherFunc {
	return func(bytes []byte) (bool, error) {
		return len(bytes) >= minLength, nil
	}
}

type Evaluator interface {
	Evaluate([]byte) (interface{}, ReturnType, error)
}

var _ Evaluator = (EvaluatorFunc)(nil)

type EvaluatorFunc func([]byte) (interface{}, ReturnType, error)

func (e EvaluatorFunc) Evaluate(b []byte) (interface{}, ReturnType, error) {
	return e(b)
}
