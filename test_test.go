package binq

import (
	"context"
	"encoding/hex"
	"io"
	"math/rand"
	"os"
	"path"
	"time"
)

const (
	testContextTimeout = 10 * time.Second
)

type TempFile struct {
	t    testType
	name string
}

type testType interface {
	Helper()
	Fatal(args ...interface{})
	Error(args ...interface{})
}

func NewTempFile(t testType) *TempFile {
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

func mustOpenBinq(t testType, path string) *File {
	t.Helper()
	bq, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return bq
}

func mustClose(t testType, c io.Closer) {
	t.Helper()
	must(t, c.Close())
}

func must(t testType, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func testContext() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), testContextTimeout)
	return ctx
}
