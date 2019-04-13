package db2

import (
	"github.com/stretchr/testify/assert"
	"runtime"
	"testing"
	"unsafe"
)

func TestSentinelValue_packUnpack(t *testing.T) {

	sentinel := newSentinelValue(t, 0x56)
	packed := sentinel.toBytes(t)
	unpacked := parseSentinelValue(t, packed)
	fuck := *(*[sentinelValueSize]byte)(unsafe.Pointer(unpacked))


	assert.Equal(t, packed, fuck[:])
	assert.Equal(t, sentinel, unpacked)

	runtime.KeepAlive(sentinel)
	runtime.KeepAlive(packed)
	runtime.KeepAlive(unpacked)

}
