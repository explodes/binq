package binqtree

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestPager(t *testing.T) {
	const (
		pageIndex1, offset1, magic1 = 1, 2, byte(3)
		pageIndex2, offset2, magic2 = 4, 5, byte(6)
	)
	file := NewTempFile(t)
	defer file.Delete()

	// Create a new pager, write our values to our pages.
	func() {

		pager, err := OpenPager(file.FullPath(), os.O_RDWR|os.O_CREATE, userReadWrite)
		must(t, err)
		defer func() {
			must(t, pager.Close())
		}()

		assert.Equal(t, uint32(0), pager.NumPages())

		page1, err := pager.GetPage(pageIndex1)
		must(t, err)
		page1[offset1] = magic1
		must(t, pager.Flush(pageIndex1, true))

		assert.Equal(t, uint32(pageIndex1+1), pager.NumPages())

		page2, err := pager.GetPage(pageIndex2)
		must(t, err)
		page2[offset2] = magic2
		must(t, pager.Flush(pageIndex2, true))

		assert.Equal(t, uint32(pageIndex2+1), pager.NumPages())
	}()

	// Open the pager, read our values from our pages.
	func() {
		pager, err := OpenPager(file.FullPath(), os.O_RDONLY, userReadWrite)
		must(t, err)
		defer func() {
			must(t, pager.Close())
		}()

		assert.Equal(t, uint32(pageIndex2+1), pager.NumPages())

		page1, err := pager.GetPage(pageIndex1)
		must(t, err)
		if !assert.Equal(t, page1[offset1], magic1) {
			t.Fatal("unexpected value")
		}

		page2, err := pager.GetPage(pageIndex2)
		must(t, err)
		if !assert.Equal(t, page2[offset2], magic2) {
			t.Fatal("unexpected value")
		}

		// We've opened this pager in read-only mode, so this is useless, really.
		// But for testing let's make sure that the next page available follows
		// our highest used page.
		nextPage, err := pager.GetUnusedPageNum()
		must(t, err)
		assert.Equal(t, uint32(pageIndex2+1), nextPage)
	}()

}
