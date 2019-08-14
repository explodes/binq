package db3

import (
	"encoding/binary"
	"github.com/pkg/errors"
)


type testType interface {
	Helper()
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
}

var _ DataSizer = dataSizer{}

type dataSizer struct {
	size uint16
}

func (d dataSizer) DataSize() uint16 {
	return d.size
}

func must(t testType, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func makeBytes(t testType, objs ...interface{}) []byte {
	t.Helper()

	var out []byte
	for _, obj := range objs {
		var b []byte
		switch val := obj.(type) {
		case []byte:
			b = val
		case string:
			b = []byte(val)
		case uint64:
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, val)
			b = buf
		case uint32:
			buf := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf, val)
			b = buf
		case uint16:
			buf := make([]byte, 2)
			binary.LittleEndian.PutUint16(buf, val)
			b = buf
		case uint8:
			b = []byte{byte(val)}
		default:
			t.Fatal(errors.Errorf("cannot serialize bytes of %T", val))
		}
		out = append(out, b...)
	}
	return out
}
