package db2

import (
	"fmt"
	"github.com/pkg/errors"
)

// wrap wraps an error with an additional message.
// Returns nil if there was no error.
func wrap(err error, msg string) error {
	return errors.Wrap(err, msg)
}

// multiError2 represents an error with two causes.
type multiError2 struct {
	// msg is the error message for this error.
	msg string
	// err1 and err2 are non-nil errors.
	err1, err2 error
}

func (m *multiError2) Error() string {
	return fmt.Sprintf("%s (multiple errors): %v, %v", m.msg, m.err1, m.err2)
}

// wrap2 wraps two errors with an additional message.
// Returns nil if there was no error.
func wrap2(err1, err2 error, msg string) error {
	switch {
	case err1 == nil && err2 == nil:
		return nil
	case err2 == nil:
		return wrap(err1, msg)
	case err1 == nil:
		return wrap(err2, msg)
	default:
		return &multiError2{
			msg:  msg,
			err1: err1,
			err2: err2,
		}
	}
}

// multiError3 represents an error with three causes.
type multiError3 struct {
	// msg is the error message for this error.
	msg string
	// err1, err2, and err3 are non-nil errors.
	err1, err2, err3 error
}

func (m *multiError3) Error() string {
	return fmt.Sprintf("%s (multiple errors): %v, %v, %v", m.msg, m.err1, m.err2, m.err3)
}

// wrap3 wraps three errors with an additional message.
// Returns nil if there was no error.
func wrap3(err1, err2, err3 error, msg string) error {
	switch {
	case err1 == nil && err2 == nil && err3 == nil:
		return nil
	case err1 == nil:
		return wrap2(err2, err3, msg)
	case err2 == nil:
		return wrap2(err1, err3, msg)
	case err3 == nil:
		return wrap2(err1, err2, msg)
	default:
		return &multiError3{
			msg:  msg,
			err1: err1,
			err2: err2,
			err3: err3,
		}
	}
}
