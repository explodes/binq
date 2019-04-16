package binqtree

import (
	"fmt"
	"github.com/pkg/errors"
	"runtime"
	"strings"
)

const (
	debug = false

	makeAssertions     = debug
	logDebugStatements = false
)

// _assert will panic if a test fails.
func _assert(test bool, format string, args ...interface{}) {
	if !makeAssertions {
		return
	}
	if test {
		return
	}
	panic(errors.Errorf(format, args...))
}

func _debug(format string, args ...interface{}) {
	if !logDebugStatements {
		return
	}
	fmt.Print("DEBUG: ")
	fmt.Printf(format, args...)
	fmt.Println()
}

// callerName returns the name of the function that called the function calling callerName.
func callerName() string {
	if !makeAssertions {
		// This code should only be used with assertions.
		return "assertions-disabled"
	}
	fpcs := make([]uintptr, 1)
	// Skip 2 levels to get the caller
	n := runtime.Callers(3, fpcs)
	if n == 0 {
		return "no-caller"
	}

	caller := runtime.FuncForPC(fpcs[0] - 1)
	if caller == nil {
		return "caller was nil"
	}

	name := caller.Name()
	s := strings.Split(name, ".")
	return s[len(s)-1]
}
