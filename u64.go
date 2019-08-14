package binq

import "encoding/binary"

// GetU64le gets the little-endian uint64 value in the byte slice.
func GetU64le(bytes []byte) (interface{}, error) {
	if len(bytes) < 8 {
		return uint64(0), ErrBytesTooSmall
	}
	bytesValue := binary.LittleEndian.Uint64(bytes)
	return bytesValue, nil
}

// GetU64be gets the little-endian uint64 value in the byte slice.
func GetU64be(bytes []byte) (interface{}, error) {
	if len(bytes) < 8 {
		return uint64(0), ErrBytesTooSmall
	}
	bytesValue := binary.BigEndian.Uint64(bytes)
	return bytesValue, nil
}

// GetU32le gets the little-endian uint32 value in the byte slice.
func GetU32le(bytes []byte) (interface{}, error) {
	if len(bytes) < 4 {
		return uint32(0), ErrBytesTooSmall
	}
	bytesValue := binary.LittleEndian.Uint32(bytes)
	return bytesValue, nil
}

// GetU32be gets the little-endian uint32 value in the byte slice.
func GetU32be(bytes []byte) (interface{}, error) {
	if len(bytes) < 4 {
		return uint32(0), ErrBytesTooSmall
	}
	bytesValue := binary.BigEndian.Uint32(bytes)
	return bytesValue, nil
}

// GetU16le gets the little-endian uint16 value in the byte slice.
func GetU16le(bytes []byte) (interface{}, error) {
	if len(bytes) < 2 {
		return uint16(0), ErrBytesTooSmall
	}
	bytesValue := binary.LittleEndian.Uint16(bytes)
	return bytesValue, nil
}

// GetU16be gets the little-endian uint16 value in the byte slice.
func GetU16be(bytes []byte) (interface{}, error) {
	if len(bytes) < 2 {
		return uint16(0), ErrBytesTooSmall
	}
	bytesValue := binary.BigEndian.Uint16(bytes)
	return bytesValue, nil
}

// GetU8 gets the little-endian uint8 value in the byte slice.
func GetU8(bytes []byte) (interface{}, error) {
	if len(bytes) < 1 {
		return uint8(0), ErrBytesTooSmall
	}
	bytesValue := bytes[0]
	return bytesValue, nil
}
