package binq

import (
	"encoding/binary"
	"github.com/pkg/errors"
)

type u64le uint64
type u64be uint64
type u32le uint32
type u32be uint32
type u16le uint16
type u16be uint16
type u8 uint8

var uintTypes = []ReturnType{
	ReturnType_RETURN_TYPE_BOOL,
	ReturnType_RETURN_TYPE_U8,
	ReturnType_RETURN_TYPE_U16,
	ReturnType_RETURN_TYPE_U32,
	ReturnType_RETURN_TYPE_U64,
}

// TestType is an interface for benchmarks are unit tests.
type TestType interface {
	Helper()
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
}

// matchesAny creates a Matcher that matches any data.
func matchesAny() MatcherFunc {
	return MatchAnything
}

// matchesErr creates a Matcher that returns an error.
func matchesErr() MatcherFunc {
	return func(bytes []byte) (bool, error) {
		return false, errors.New("always fails")
	}
}

// matchesNone creates a Matcher that never matches any data.
func matchesNone() MatcherFunc {
	return MatchNothing
}

// makeBytes encodes data into a byte slice.
func makeBytes(t TestType, objs ...interface{}) []byte {
	t.Helper()

	var out []byte
	for _, obj := range objs {
		var b []byte
		switch val := obj.(type) {
		case []byte:
			b = val
		case string:
			b = []byte(val)
		case u64le:
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(val))
			b = buf
		case u64be:
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, uint64(val))
			b = buf
		case uint64:
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, val)
			b = buf
		case u32le:
			buf := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf, uint32(val))
			b = buf
		case u32be:
			buf := make([]byte, 4)
			binary.BigEndian.PutUint32(buf, uint32(val))
			b = buf
		case uint32:
			buf := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf, val)
			b = buf
		case u16le:
			buf := make([]byte, 2)
			binary.LittleEndian.PutUint16(buf, uint16(val))
			b = buf
		case u16be:
			buf := make([]byte, 2)
			binary.BigEndian.PutUint16(buf, uint16(val))
			b = buf
		case uint16:
			buf := make([]byte, 2)
			binary.LittleEndian.PutUint16(buf, val)
			b = buf
		case u8:
			b = []byte{byte(val)}
		case uint8:
			b = []byte{byte(val)}
		default:
			t.Fatal(errors.Errorf("cannot serialize bytes of %T", val))
		}
		out = append(out, b...)
	}
	return out
}
