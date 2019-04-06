package binq

import (
	"github.com/pkg/errors"
	"strings"
)

// multiError collapses multiple errors into a single error.
func multiError(msg string, errs ...error) error {
	nonNilErrors := make([]string, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			nonNilErrors = append(nonNilErrors, err.Error())
		}
	}
	switch len(nonNilErrors) {
	case 0:
		return nil
	case 1:
		return errors.Errorf("%s: %s", msg, nonNilErrors[0])
	default:
		return errors.New(msg + " (multiple errors): " + strings.Join(nonNilErrors, ", "))
	}
}
