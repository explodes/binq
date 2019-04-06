package binq

import (
	"encoding/hex"
	"io"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"
)

type TempFile struct {
	t    *testing.T
	name string
}

func NewTempFile(t *testing.T) *TempFile {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	randBuf := make([]byte, 64)
	_, err := rng.Read(randBuf)
	if err != nil {
		t.Fatal(err)
	}
	fileName := hex.EncodeToString(randBuf)
	filePath := path.Join(os.TempDir(), fileName)
	return &TempFile{
		t:    t,
		name: filePath,
	}
}

func (t *TempFile) Name() string {
	return t.name
}

func (t *TempFile) Delete() {
	t.t.Helper()
	err := os.Remove(t.name)
	if err != nil && !os.IsNotExist(err) {
		t.t.Error(err)
	}
}

func mustOpenBinq(t *testing.T, path string) *File {
	t.Helper()
	bq, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return bq
}

func mustClose(t *testing.T, c io.Closer) {
	t.Helper()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}
