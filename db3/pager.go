package db3

import (
	"github.com/pkg/errors"
	"io"
	"syscall"
)

const (
	PageSize = 4096
)

type Page [PageSize]byte

type PagePointer = uint32

type Pager struct {
	fd         int
	fileLength uint32
	pages      []*Page
	numPages   PagePointer
}

func OpenPager(path string, mode int, perm uint32) (*Pager, error) {
	fd, err := syscall.Open(path, mode, perm)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open file")
	}
	fileLength, err := syscall.Seek(fd, 0, io.SeekEnd)
	if err != nil {
		return nil, errors.Wrap(err, "unable to seek file")
	}
	if fileLength%PageSize != 0 {
		return nil, errors.New("file corruption: pager file is not a whole number of pages")
	}
	p := &Pager{
		fd:         fd,
		fileLength: uint32(fileLength),
		numPages:   PagePointer(fileLength / PageSize),
	}
	return p, nil
}

func (p *Pager) GetPage(pageIndex PagePointer) (*Page, error) {
	if pageIndex >= PagePointer(len(p.pages)) {
		newPages := make([]*Page, pageIndex+1)
		if p.pages != nil {
			copy(newPages, p.pages)
		}
		p.pages = newPages
	}
	if p.pages[pageIndex] == nil {
		// Cache miss. Allocate memory and load from file.
		page := new(Page)
		numPages := p.fileLength / PageSize
		// We might save a partial page at the end of the file
		if p.fileLength%PageSize > 0 {
			numPages++
		}
		if pageIndex <= numPages {
			// This page was already on disk.
			// Seek to its position and read the page.
			if _, err := syscall.Seek(p.fd, int64(pageIndex)*int64(PageSize), io.SeekStart); err != nil {
				return nil, errors.Wrap(err, "error seeking to read position")
			}
			if _, err := syscall.Read(p.fd, page[:]); err != nil {
				return nil, errors.Wrap(err, "error reading file")
			}
		}
		p.pages[pageIndex] = page
		if pageIndex >= p.numPages {
			p.numPages = pageIndex + 1
		}
	}
	return p.pages[pageIndex], nil
}

// GetUnusedPageNum returns the next available page.
func (p *Pager) GetUnusedPageNum() (PagePointer, error) {
	// Until we start recycling free pages, new pages will always go
	// onto the end of the database file.
	return p.numPages, nil
}

// NumPages returns the number of pages on disk.
func (p *Pager) NumPages() PagePointer {
	return p.numPages
}

func (p *Pager) Flush(pageIndex PagePointer, sync bool) error {
	if pageIndex >= PagePointer(len(p.pages)) {
		return errors.New("tried to sync page out of range")
	}
	if p.pages[pageIndex] == nil {
		return errors.New("tried to flush nil page")
	}
	offset := int64(pageIndex) * int64(PageSize)
	if _, err := syscall.Seek(p.fd, offset, io.SeekStart); err != nil {
		return errors.Wrap(err, "error seeking to flush position")
	}
	if _, err := syscall.Write(p.fd, p.pages[pageIndex][:]); err != nil {
		return errors.Wrap(err, "error writing page")
	}
	if sync {
		if err := syscall.SyncFileRange(p.fd, offset, PageSize, 0); err != nil {
			return errors.Wrap(err, "error syncing page")
		}
	}
	return nil
}

func (p *Pager) sync1(pageIndex PagePointer) error {
	return wrap(p.Flush(pageIndex, true), "sync error")
}

func (p *Pager) sync2(pageIndex1, pageIndex2 PagePointer) error {
	return wrap2(
		p.Flush(pageIndex1, true),
		p.Flush(pageIndex2, true),
		"sync error")
}

func (p *Pager) sync3(pageIndex1, pageIndex2, pageIndex3 PagePointer) error {
	return wrap3(
		p.Flush(pageIndex1, true),
		p.Flush(pageIndex2, true),
		p.Flush(pageIndex3, true),
		"sync error")
}

func (p *Pager) Close() error {
	return syscall.Close(p.fd)
}
