package db3

import (
	"fmt"
	"github.com/pkg/errors"
	"os"
	"runtime"
	"strconv"
	"strings"
)

const (
	debug = true

	makeAssertions     = debug
	logDebugStatements = false

	envBranchMaxCellsEnv = "BRANCH_MAX_KEYS"
)

var logOverride = false

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

func _setMaxKeysPerBranchOverride(n int) error {
	if !debug {
		panic("not available outside of debug mode")
	}
	if err := os.Setenv(envBranchMaxCellsEnv, strconv.Itoa(n)); err != nil {
		panic(err)
	}
	logOverride = true
	return nil
}

func _maxKeysPerBranchOverride() cellptr {
	const (
		defaultBranchNodeMaxCells = cellptr(branchNodeMaxCells)
	)
	if debug {
		if s, ok := os.LookupEnv(envBranchMaxCellsEnv); ok {
			override, err := strconv.Atoi(s)
			if err != nil {
				panic(wrap(err, fmt.Sprintf("unable to %s as numeric", envBranchMaxCellsEnv)))
			}
			if logDebugStatements {
				if logOverride {
					fmt.Printf("%s override: %d\n", envBranchMaxCellsEnv, override)
					logOverride = false
				}
			}
			return cellptr(override)
		}
	}
	return cellptr(defaultBranchNodeMaxCells)
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
