package db3

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"path"
	"time"
)

const (
	userReadWrite = 0600
)

type TempFile struct {
	t    testType
	name string
}

func NewTempFile(t testType) *TempFile {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	randBuf := make([]byte, 8)
	_, err := rng.Read(randBuf)
	if err != nil {
		t.Fatal(err)
	}
	fileName := fmt.Sprintf("db2_test_%d_%s", time.Now().UnixNano(), hex.EncodeToString(randBuf))
	filePath := path.Join(os.TempDir(), fileName)
	return &TempFile{
		t:    t,
		name: filePath,
	}
}

func (t *TempFile) FullPath() string {
	return t.name
}

func (t *TempFile) Delete() {
	t.t.Helper()
	err := os.Remove(t.name)
	if err != nil && !os.IsNotExist(err) {
		t.t.Error(err)
	}
}
