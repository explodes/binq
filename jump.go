package binq

import (
	"github.com/pkg/errors"
)

var (
	// ErrOffsetOutOfRange indicates that a computed jump offset was out of range of the bytes data.
	ErrJumpOffsetOutOfRange = errors.New("jump offset out of range")
)

// Jumper is an interface for jumping to a position within data.
type Jumper interface {
	// Jump returns the bytes at some position this jumper should jump to.
	Jump([]byte) ([]byte, error)
}

var _ Jumper = (JumperFunc)(nil)

// JumperFunc is a Jumper composed of a single function.
type JumperFunc func([]byte) ([]byte, error)

// Jump satisfies the Jumper interface.
func (f JumperFunc) Jump(b []byte) ([]byte, error) {
	return f(b)
}

// WithJump creates a Matcher for data at position that is jumped to.
func WithJump(jumper Jumper, matcher Matcher) MatcherFunc {
	return func(bytes []byte) (bool, error) {
		jumped, err := jumper.Jump(bytes)
		if err != nil {
			return false, wrap(err, "unable to jump")
		}
		matched, err := matcher.Match(jumped)
		if err != nil {
			return false, wrap(err, "unable to run matcher")
		}
		return matched, nil
	}
}

// JumpOffset creates a Jumper that will jump to an absolute offset.
func JumpOffset(offset uint64) JumperFunc {
	return func(bytes []byte) ([]byte, error) {
		newBytes, err := jumpOffset64(offset, bytes)
		if err != nil {
			return nil, wrap(err, "unable to jump to address")
		}
		return newBytes, nil
	}
}

// JumpToU64le creates a Jumper that decodes a little-endian uint64
// jumpAddress at an offset and jumps to that position.
func JumpToU64le(offset uint64) JumperFunc {
	return func(bytes []byte) ([]byte, error) {
		// Jump to the position with our jump address.
		jumpedBytes, err := jumpOffset64(offset, bytes)
		if err != nil {
			return nil, wrap(err, "unable to jump to address")
		}
		// Decode the jump address.
		jumpAddr, err := GetU64le(jumpedBytes)
		if err != nil {
			return nil, wrap(err, "unable to decode uint64 jump address")
		}
		// Jump to that position in the original bytes.
		newBytes, err := jumpOffset64(jumpAddr.(uint64), bytes)
		if err != nil {
			return nil, wrap(err, "unable to jump to address")
		}
		return newBytes, nil
	}
}

// JumpToU64be creates a Jumper that decodes a big-endian uint64
// jumpAddress at an offset and jumps to that position.
func JumpToU64be(offset uint64) JumperFunc {
	return func(bytes []byte) ([]byte, error) {
		// Jump to the position with our jump address.
		jumpedBytes, err := jumpOffset64(offset, bytes)
		if err != nil {
			return nil, wrap(err, "unable to jump to address")
		}
		// Decode the jump address.
		jumpAddr, err := GetU64be(jumpedBytes)
		if err != nil {
			return nil, wrap(err, "unable to decode uint64 jump address")
		}
		// Jump to that position in the original bytes.
		newBytes, err := jumpOffset64(jumpAddr.(uint64), bytes)
		if err != nil {
			return nil, wrap(err, "unable to jump to address")
		}
		return newBytes, nil
	}
}

// JumpToU32le creates a Jumper that decodes a little-endian uint32
// jumpAddress at an offset and jumps to that position.
func JumpToU32le(offset uint64) JumperFunc {
	return func(bytes []byte) ([]byte, error) {
		// Jump to the position with our jump address.
		jumpedBytes, err := jumpOffset64(offset, bytes)
		if err != nil {
			return nil, wrap(err, "unable to jump to address")
		}
		// Decode the jump address.
		jumpAddr, err := GetU32le(jumpedBytes)
		if err != nil {
			return nil, wrap(err, "unable to decode uint32 jump address")
		}
		// Jump to that position in the original bytes.
		newBytes, err := jumpOffset32(jumpAddr.(uint32), bytes)
		if err != nil {
			return nil, wrap(err, "unable to jump to address")
		}
		return newBytes, nil
	}
}

// JumpToU32be creates a Jumper that decodes a big-endian uint32
// jumpAddress at an offset and jumps to that position.
func JumpToU32be(offset uint64) JumperFunc {
	return func(bytes []byte) ([]byte, error) {
		// Jump to the position with our jump address.
		jumpedBytes, err := jumpOffset64(offset, bytes)
		if err != nil {
			return nil, wrap(err, "unable to jump to address")
		}
		// Decode the jump address.
		jumpAddr, err := GetU32be(jumpedBytes)
		if err != nil {
			return nil, wrap(err, "unable to decode uint32 jump address")
		}
		// Jump to that position in the original bytes.
		newBytes, err := jumpOffset32(jumpAddr.(uint32), bytes)
		if err != nil {
			return nil, wrap(err, "unable to jump to address")
		}
		return newBytes, nil
	}
}

// JumpToU16le creates a Jumper that decodes a little-endian uint16
// jumpAddress at an offset and jumps to that position.
func JumpToU16le(offset uint64) JumperFunc {
	return func(bytes []byte) ([]byte, error) {
		// Jump to the position with our jump address.
		jumpedBytes, err := jumpOffset64(offset, bytes)
		if err != nil {
			return nil, wrap(err, "unable to jump to address")
		}
		// Decode the jump address.
		jumpAddr, err := GetU16le(jumpedBytes)
		if err != nil {
			return nil, wrap(err, "unable to decode uint16 jump address")
		}
		// Jump to that position in the original bytes.
		newBytes, err := jumpOffset16(jumpAddr.(uint16), bytes)
		if err != nil {
			return nil, wrap(err, "unable to jump to address")
		}
		return newBytes, nil
	}
}

// JumpToU16be creates a Jumper that decodes a big-endian uint16
// jumpAddress at an offset and jumps to that position.
func JumpToU16be(offset uint64) JumperFunc {
	return func(bytes []byte) ([]byte, error) {
		// Jump to the position with our jump address.
		jumpedBytes, err := jumpOffset64(offset, bytes)
		if err != nil {
			return nil, wrap(err, "unable to jump to address")
		}
		// Decode the jump address.
		jumpAddr, err := GetU16be(jumpedBytes)
		if err != nil {
			return nil, wrap(err, "unable to decode uint16 jump address")
		}
		// Jump to that position in the original bytes.
		newBytes, err := jumpOffset16(jumpAddr.(uint16), bytes)
		if err != nil {
			return nil, wrap(err, "unable to jump to address")
		}
		return newBytes, nil
	}
}

// JumpToU8 creates a Jumper that decodes a big-endian uint8
// jumpAddress at an offset and jumps to that position.
func JumpToU8(offset uint64) JumperFunc {
	return func(bytes []byte) ([]byte, error) {
		// Jump to the position with our jump address.
		jumpedBytes, err := jumpOffset64(offset, bytes)
		if err != nil {
			return nil, wrap(err, "unable to jump to address")
		}
		// Decode the jump address.
		jumpAddr, err := GetU8(jumpedBytes)
		if err != nil {
			return nil, wrap(err, "unable to decode uint8 jump address")
		}
		// Jump to that position in the original bytes.
		newBytes, err := jumpOffset8(jumpAddr.(uint8), bytes)
		if err != nil {
			return nil, wrap(err, "unable to jump to address")
		}
		return newBytes, nil
	}
}

// jumpOffset64 jumps data to a given offset.
func jumpOffset64(offset uint64, bytes []byte) ([]byte, error) {
	if uint64(len(bytes)) < offset {
		return nil, ErrJumpOffsetOutOfRange
	}
	return bytes[offset:], nil
}

// jumpOffset32 jumps data to a given offset.
func jumpOffset32(offset uint32, bytes []byte) ([]byte, error) {
	if len(bytes) < int(offset) {
		return nil, ErrJumpOffsetOutOfRange
	}
	return bytes[offset:], nil
}

// jumpOffset16 jumps data to a given offset.
func jumpOffset16(offset uint16, bytes []byte) ([]byte, error) {
	if len(bytes) < int(offset) {
		return nil, ErrJumpOffsetOutOfRange
	}
	return bytes[offset:], nil
}

// jumpOffset8 jumps data to a given offset.
func jumpOffset8(offset uint8, bytes []byte) ([]byte, error) {
	if len(bytes) < int(offset) {
		return nil, ErrJumpOffsetOutOfRange
	}
	return bytes[offset:], nil
}
