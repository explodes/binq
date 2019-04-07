package binq

import "testing"

func BenchmarkFile_OpenPutGetClose(b *testing.B) {
	var (
		key   = []byte("key")
		value = []byte("value")
	)
	for i := 0; i < b.N; i++ {
		func() {
			temp := NewTempFile(b)
			defer temp.Delete()

			bq := mustOpenBinq(b, temp.Name())
			defer mustClose(b, bq)

			ctx := testContext()
			must(b, bq.Put(ctx, key, value))
			_, err := bq.Get(ctx, key)
			must(b, err)
		}()
	}
}
