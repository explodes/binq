package binq

import (
	"bytes"
	"context"
	"github.com/explodes/mfile"
	"github.com/pkg/errors"
	"unsafe"
)

const (
	// MaxKeySize is the largest key size allowed.
	// Attempting to put a value with a key larger than this size is an error.
	MaxKeySize = 256

	// growBuffer is how much extra to grow the file when new space is required
	growBuffer = 10 * (binqEntrySize + 1024)

	// magic is a magic number used for file identification.
	magic = uint32(0x514e4942) // ASCII: BINQ, little-endian
	// version is the version marker in file, used for upgrading.
	version = 1

	binqHeaderSize = int(unsafe.Sizeof(binqHeader{}))
	binqEntrySize  = int(unsafe.Sizeof(binqEntry{}))
)

var (
	// ErrNotFound indicates that a key was not found.
	ErrNotFound = errors.New("not found")
)

// binqHeader is the struct located at the beginning of a binq file.
type binqHeader struct {
	// magic is a unique identifier for binq files
	magic uint32

	//version is a file version code
	version uint16

	// eod is the pointer to the first index after the last byte of data.
	eod uintptr

	// headEntry is the pointer to the last entry in the file.
	headEntry uintptr

	// _reserved is an unused block reserved for future use.
	_reserved [128]byte
}

// binqEntry is an entry in the binq database. Its structure is like that of a linked list.
type binqEntry struct {
	// key is the key for this entry.
	key [MaxKeySize]byte
	// keyLen is the actual length of the key.
	keyLen uintptr
	// next is the location in the file of the next entry.
	next uintptr
	// dataPtr is location in the file of the data this entry maps to.
	dataPtr uintptr
	// dataLen is the length of the data that this entry points to.
	dataLen uintptr
}

// File is a binq database file and its supported operations.
// It should be closed after use.
type File struct {
	file *mfile.File
}

// Open opens a binq database at the given file path. If the database does not exist,
// a new one is created.
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

// Put stores a key and value in the database. If the key is
// already present, it will be overwritten.
func (b *File) Put(ctx context.Context, key []byte, value []byte) error {
	if len(key) > MaxKeySize {
		return errors.New("key too large")
	}

	// Resize the file to be large enough to hold our new entry and value.
	if err := b.ensureSpace(binqEntrySize + len(value)); err != nil {
		return err
	}

	// Get a fresh header for this action.
	// The header may be moved around by the Go runtime or OS,
	// so we need to retrieve it every time.
	header := b.header()

	prevEntry, prevEntryPtr, equalKey := b.findParent(header, key)
	// We don't allow entering the same key twice.
	if equalKey {
		// Overwrite the value if there is room, Otherwise append it.
		valueLen := uintptr(len(value))
		var valuePtr uintptr

		// Get the location of our new data.
		if prevEntry.dataLen <= valueLen {
			// We can overwrite the value.
			valuePtr = prevEntry.dataPtr
		} else {
			// We must append the data.
			// We have already reserved the space, so we can start at the end
			// if the previous value slot isn't large enough.
			valuePtr = header.eod + 1
		}

		b.putData(valuePtr, value)
		valueSyncErr := b.syncValue(valuePtr, valueLen)
		prevEntry.dataPtr = valuePtr
		prevEntry.dataLen = valueLen
		entrySyncErr := b.syncEntry(prevEntryPtr)

		return multiError("error syncing value", valueSyncErr, entrySyncErr)
	}

	// Get the pointer to the next entry so that we can insert our new entry between prevEntry and
	// prevEntry's following entry.
	var nextEntryPtr uintptr
	if prevEntry != nil {
		nextEntryPtr = prevEntry.next
	}

	// Add our entry to the current end of our data and add the value right after our entry.
	entryPtr := header.eod + 1
	valuePtr := header.eod + 1 + uintptr(binqEntrySize)
	valueLen := uintptr(len(value))
	valueEnd := valuePtr + valueLen

	b.putEntry(entryPtr, key, valuePtr, valueLen, nextEntryPtr)
	b.putData(valuePtr, value)

	var prevEntrySyncErr error

	if prevEntry == nil {
		// This is our first entry.
		// Set our first and last entry pointers to the first entry.
		header.headEntry = entryPtr
	} else {
		// This is not our first entry.
		// Set the tail entry's next pointer to the new entry.
		prevEntry.next = entryPtr
		prevEntrySyncErr = b.syncEntry(prevEntryPtr)
	}
	// Point our EOD marker to the end of the new data.
	header.eod = valueEnd

	// Sync our results.
	headerSyncErr := b.syncHeader()
	entrySyncErr := b.syncEntryAndData(entryPtr, valueLen)
	return multiError("failed to sync data", headerSyncErr, prevEntrySyncErr, entrySyncErr)
}

// putData writes data to the end of the file and returns the offset to which is written
// as well as the length of the data that was written.
func (b *File) putData(offset uintptr, value []byte) {
	valueBuf := b.file.BytesAt(offset, len(value))
	copy(valueBuf, value)
}

// putEntry writes an entry to given position.
func (b *File) putEntry(offset uintptr, key []byte, valuePtr, valueLen, next uintptr) *binqEntry {
	entry := (*binqEntry)(b.file.DataAt(offset))
	copy(entry.key[:], key)
	entry.keyLen = uintptr(len(key))
	entry.dataPtr = valuePtr
	entry.dataLen = valueLen
	entry.next = next
	return entry
}

func (b *File) syncEntry(offset uintptr) error {
	return b.file.SyncRange(int64(offset), int64(binqEntrySize))
}

func (b *File) syncEntryAndData(offset, valueLen uintptr) error {
	return b.file.SyncRange(int64(offset), int64(binqEntrySize)+int64(valueLen))
}

func (b *File) syncValue(offset, valueLen uintptr) error {
	return b.file.SyncRange(int64(offset), int64(valueLen))
}

func (b *File) syncHeader() error {
	return b.file.SyncRange(0, int64(binqHeaderSize))
}

// findParent finds the parent entry to a key, the entry whose key is the
// largest key lexicographically smaller than the given key.
func (b *File) findParent(header *binqHeader, key []byte) (entry *binqEntry, offset uintptr, equalKey bool) {
	var parent *binqEntry
	var parentPtr uintptr

	ptr := header.headEntry
	for ptr != 0 {
		entry := (*binqEntry)(b.file.DataAt(ptr))
		entryKey := entry.key[:entry.keyLen]
		cmp := bytes.Compare(key, entryKey)
		if cmp == 0 {
			// This entry's key matches.
			return entry, ptr, true
		} else if cmp < 0 {
			// Our new key is less than this entry's key.
			// The entry prior is the correct parent.
			return parent, parentPtr, false
		}
		// Our previous entry is our current parent candidate.
		parent = entry
		parentPtr = ptr
		// Our new key comes after this entry. Our search continues.
		// Advance our pointer.
		ptr = entry.next
	}

	// We didn't find any entries with a larger key. The last parent is our target.
	return parent, parentPtr, false
}

// Get gets the value for a given key.
func (b *File) Get(ctx context.Context, key []byte) ([]byte, error) {
	header := b.header()
	ptr := header.headEntry
	for ptr != 0 {
		entry := (*binqEntry)(b.file.DataAt(ptr))
		entryKey := entry.key[:entry.keyLen]
		// Keys are sorted. If the current key is larger that our target
		// key we can stop the search.
		if cmp := bytes.Compare(entryKey, key); cmp == 0 {
			return b.file.BytesAt(entry.dataPtr, int(entry.dataLen)), nil
		} else if cmp > 0 {
			break
		}
		ptr = entry.next
	}
	return nil, ErrNotFound
}

// Scan scans the database until it is told to stop.
func (b *File) Scan(ctx context.Context, handler func(key, value []byte) (stop bool)) {
	header := b.header()
	ptr := header.headEntry
	for ptr != 0 {
		entry := (*binqEntry)(b.file.DataAt(ptr))
		key := entry.key[:entry.keyLen]
		value := b.file.BytesAt(entry.dataPtr, int(entry.dataLen))
		if handler(key, value) {
			break
		}
		ptr = entry.next
	}
}

// Close closes this file.
func (b *File) Close() error {
	return b.file.Close()
}

// ensureSpace expands the file on disk with room to add new entries and values
// with an arbitrary additional amount of space to avoid repeatedly expanding the file.
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
