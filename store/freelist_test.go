package store_test

import (
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	store "github.com/hannahhoward/go-storethehash/store"
	"github.com/stretchr/testify/require"
)

func TestFLPut(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "sth")
	require.NoError(t, err)
	flPath := filepath.Join(tempDir, "storethehash.free")
	fl, err := store.OpenFreeList(flPath)
	require.NoError(t, err)

	blks := generateFreeListEntries(100)
	for _, blk := range blks {
		err := fl.Put(blk)
		require.NoError(t, err)
	}

	outstandingWork := fl.OutstandingWork()
	expectedStorage := 100 * (store.SizeBytesLen + store.OffBytesLen)
	require.Equal(t, store.Work(expectedStorage), outstandingWork)
	work, err := fl.Flush()
	require.NoError(t, err)
	require.Equal(t, store.Work(expectedStorage), work)
	err = fl.Sync()
	require.NoError(t, err)

	// Skip header
	file, err := os.Open(flPath)
	require.NoError(t, err)
	iter := store.NewFreeListIter(file)
	for _, expectedBlk := range blks {
		blk, err := iter.Next()
		require.NoError(t, err)
		require.Equal(t, expectedBlk.Size, blk.Size)
		require.Equal(t, expectedBlk.Offset, blk.Offset)
	}
	_, err = iter.Next()
	require.EqualError(t, err, io.EOF.Error())
}

func generateFreeListEntries(n int) []store.Block {
	blks := make([]store.Block, 0)
	for i := 0; i < n; i++ {
		blks = append(blks, store.Block{
			Size:   store.Size(rand.Int31()),
			Offset: store.Position(rand.Int63()),
		})
	}
	return blks
}
