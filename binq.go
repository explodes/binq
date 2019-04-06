package binq

import (
	"bytes"
	"github.com/explodes/mfile"
	"github.com/pkg/errors"
	"strings"
	"unsafe"
)

const (
	MaxKeySize = 1024

	binqHeaderSize = int(unsafe.Sizeof(binqHeader{}))
	binqEntrySize  = int(unsafe.Sizeof(binqEntry{}))

	// growBuffer is how much extra to grow the file when new space is required
	growBuffer = 10 * (binqEntrySize + 1024)

	magic   = uint32(0x514e4942) // ASCII: BINQ
	version = 1
)

type binqHeader struct {
	// magic is a unique identifier for binq files
	magic uint32

	//version is a file version code
	version uint16

	// eod is the pointer to the first index after the last byte of data.
	eod uintptr

	// headEntry is the pointer to the last entry in the file.
	headEntry uintptr

	// tailEntry is the pointer to the last entry in the file.
	tailEntry uintptr

	// _reserved is an unused block reserved for future use.
	_reserved [128]byte
}

type binqEntry struct {
	key     [MaxKeySize]byte
	prev    uintptr
	next    uintptr
	dataPtr uintptr
	dataLen uintptr
}

type File struct {
	file *mfile.File
}

func Open(path string) (*File, error) {
	file, err := mfile.Open(path, binqHeaderSize+growBuffer)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open binq file")
	}
	bf := &File{
		file: file,
	}
	header := bf.header()
	if header.magic == 0 {
		// If the magic number of the file is 0, we
		// assume that this is a new file.
		header.magic = magic
		header.version = version
		header.eod = uintptr(binqHeaderSize) + 1
	} else if header.magic != magic {
		// If the magic number is not 0 and not the right
		// number, this isn't the right kind of file.
		return nil, errors.New("invalid file: mismatched magic number")
	}
	return bf, nil
}

// header acquires the header at the current location of our memory mapped file.
// Since the location may be moved around, a fresh location must be acquired each time.
func (b *File) header() *binqHeader {
	return (*binqHeader)(b.file.DataPtr())
}

func (b *File) Put(key []byte, value []byte) error {
	if len(key) > MaxKeySize {
		return errors.New("key too large")
	}

	// Resize the file to be large enough to hold our new entry and value.
	if err := b.ensureSpace(binqEntrySize + len(value)); err != nil {
		return err
	}

	header := b.header()

	// Add our entry to the current end of our data and add the value right after our entry.
	entryPtr := header.eod
	valuePtr := header.eod + uintptr(binqEntrySize)
	valueLen := uintptr(len(value))
	valueEnd := valuePtr + valueLen

	entry := (*binqEntry)(b.file.DataAt(entryPtr))
	copy(entry.key[:], key)
	entry.dataPtr = valuePtr
	entry.dataLen = valueLen
	entry.prev = header.tailEntry

	valueBuf := b.file.BytesAt(valuePtr, len(value))
	copy(valueBuf, value)

	var tailEntrySyncErr error

	if header.headEntry != 0 {
		// This is not our first entry.
		// Set the tail entry's next pointer to the new entry.
		prevEntryPtr := header.tailEntry
		(*binqEntry)(b.file.DataAt(prevEntryPtr)).next = entryPtr
		tailEntrySyncErr = b.file.SyncRange(int64(prevEntryPtr), int64(binqHeaderSize))
	} else {
		// This is our first entry.
		// Set our first and last entry pointers to the first entry.
		header.headEntry = entryPtr
	}
	// Point our EOD marker to after the new value.
	header.tailEntry = entryPtr
	header.eod = valueEnd + 1

	// Sync our results.
	headerSyncErr := b.file.SyncRange(0, int64(binqHeaderSize))
	entrySyncErr := b.file.SyncRange(int64(entryPtr), int64(binqEntrySize)+int64(len(value)))
	return multiError("failed to sync data", headerSyncErr, tailEntrySyncErr, entrySyncErr)
}

func (b *File) Get(key []byte) ([]byte, error) {
	header := b.header()
	ptr := header.headEntry
	for ptr != 0 {
		entry := (*binqEntry)(b.file.DataAt(ptr))
		if bytes.Equal(entry.key[:len(key)], key) {
			return b.file.BytesAt(entry.dataPtr, int(entry.dataLen)), nil
		}
		ptr = entry.next
	}
	return nil, errors.New("key not found")
}

func (b *File) Close() error {
	return b.file.Close()
}

func (b *File) ensureSpace(n int) error {
	header := b.header()
	free := b.file.Len() - int(header.eod)
	if free >= n {
		return nil
	}
	err := b.file.Resize(binqHeaderSize + int(header.eod) + n + growBuffer)
	if err != nil {
		return errors.Wrap(err, "unable to grow file")
	}
	return nil
}

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
