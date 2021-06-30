package cidprimary_test

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	store "github.com/hannahhoward/go-storethehash/store"
	cidprimary "github.com/hannahhoward/go-storethehash/store/primary/cid"
	"github.com/hannahhoward/go-storethehash/store/testutil"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

// This test is about making sure that inserts into an empty bucket result in a key that is trimmed
// to a single byte.

func TestIndexPut(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "sth")
	require.NoError(t, err)
	primaryPath := filepath.Join(tempDir, "storethehash.primary")
	primaryStorage, err := cidprimary.OpenCIDPrimary(primaryPath)
	require.NoError(t, err)

	blks := testutil.GenerateBlocksOfSize(5, 100)
	expectedOffset := store.Position(0)
	for _, blk := range blks {
		expectedSize := len(blk.Cid().Bytes()) + len(blk.RawData())
		loc, err := primaryStorage.Put(blk.Cid().Bytes(), blk.RawData())
		require.NoError(t, err)
		require.Equal(t, expectedOffset, loc.Offset)
		require.Equal(t, store.Size(expectedSize), loc.Size)
		expectedOffset += cidprimary.CIDSizePrefix + store.Position(expectedSize)
	}

	outstandingWork := primaryStorage.OutstandingWork()
	require.Equal(t, store.Work(expectedOffset), outstandingWork)
	work, err := primaryStorage.Flush()
	require.NoError(t, err)
	require.Equal(t, store.Work(expectedOffset), work)
	err = primaryStorage.Sync()
	require.NoError(t, err)

	// Skip header
	file, err := os.Open(primaryPath)
	require.NoError(t, err)
	iter := cidprimary.NewCIDPrimaryIter(file)
	for _, expectedBlk := range blks {
		key, value, err := iter.Next()
		require.NoError(t, err)
		_, c, err := cid.CidFromBytes(key)
		require.NoError(t, err)
		blk, err := blocks.NewBlockWithCid(value, c)
		require.NoError(t, err)
		require.True(t, expectedBlk.Cid().Equals(blk.Cid()))
		require.Equal(t, expectedBlk.RawData(), blk.RawData())
	}
	_, _, err = iter.Next()
	require.EqualError(t, err, io.EOF.Error())
}

func TestIndexGetEmptyIndex(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "sth")
	require.NoError(t, err)
	primaryPath := filepath.Join(tempDir, "storethehash.primary")
	primaryStorage, err := cidprimary.OpenCIDPrimary(primaryPath)
	require.NoError(t, err)

	key, value, err := primaryStorage.Get(store.Block{
		Offset: 0,
		Size:   50,
	})
	require.Nil(t, key)
	require.Nil(t, value)
	require.Error(t, err)
}

func TestIndexGet(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "sth")
	require.NoError(t, err)
	primaryPath := filepath.Join(tempDir, "storethehash.primary")
	primaryStorage, err := cidprimary.OpenCIDPrimary(primaryPath)
	require.NoError(t, err)

	// load blocks
	blks := testutil.GenerateBlocksOfSize(5, 100)
	var locs []store.Block
	for _, blk := range blks {
		loc, err := primaryStorage.Put(blk.Cid().Bytes(), blk.RawData())
		require.NoError(t, err)
		locs = append(locs, loc)
	}

	// should fetch from memory before flush
	for i, loc := range locs {
		expectedBlk := blks[i]
		key, value, err := primaryStorage.Get(loc)
		require.NoError(t, err)
		_, c, err := cid.CidFromBytes(key)
		require.NoError(t, err)
		blk, err := blocks.NewBlockWithCid(value, c)
		require.NoError(t, err)
		require.True(t, expectedBlk.Cid().Equals(blk.Cid()))
		require.Equal(t, expectedBlk.RawData(), blk.RawData())
	}

	// should fetch from disk after flush
	_, err = primaryStorage.Flush()
	require.NoError(t, err)
	err = primaryStorage.Sync()
	require.NoError(t, err)

	for i, loc := range locs {
		expectedBlk := blks[i]
		key, value, err := primaryStorage.Get(loc)
		require.NoError(t, err)
		_, c, err := cid.CidFromBytes(key)
		require.NoError(t, err)
		blk, err := blocks.NewBlockWithCid(value, c)
		require.NoError(t, err)
		require.True(t, expectedBlk.Cid().Equals(blk.Cid()))
		require.Equal(t, expectedBlk.RawData(), blk.RawData())
	}
}
