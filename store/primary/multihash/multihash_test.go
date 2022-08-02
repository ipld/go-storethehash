package mhprimary_test

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mhprimary "github.com/ipld/go-storethehash/store/primary/multihash"
	"github.com/ipld/go-storethehash/store/testutil"
	"github.com/ipld/go-storethehash/store/types"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// This test is about making sure that inserts into an empty bucket result in a key that is trimmed
// to a single byte.

func TestIndexPut(t *testing.T) {
	tempDir := t.TempDir()
	primaryPath := filepath.Join(tempDir, "storethehash.primary")
	primaryStorage, err := mhprimary.OpenMultihashPrimary(primaryPath)
	require.NoError(t, err)

	blks := testutil.GenerateBlocksOfSize(5, 100)
	expectedOffset := types.Position(0)
	for _, blk := range blks {
		expectedSize := len(blk.Cid().Hash()) + len(blk.RawData())
		loc, err := primaryStorage.Put(blk.Cid().Hash(), blk.RawData())
		require.NoError(t, err)
		require.Equal(t, expectedOffset, loc.Offset)
		require.Equal(t, types.Size(expectedSize), loc.Size)
		expectedOffset += mhprimary.SizePrefix + types.Position(expectedSize)
	}

	outstandingWork := primaryStorage.OutstandingWork()
	require.Equal(t, types.Work(expectedOffset), outstandingWork)
	work, err := primaryStorage.Flush()
	require.NoError(t, err)
	require.Equal(t, types.Work(expectedOffset), work)
	err = primaryStorage.Sync()
	require.NoError(t, err)

	// Skip header
	file, err := os.Open(primaryPath)
	t.Cleanup(func() { file.Close() })
	require.NoError(t, err)
	iter := mhprimary.NewMultihashPrimaryIter(file)
	for _, expectedBlk := range blks {
		key, value, err := iter.Next()
		require.NoError(t, err)
		c := cid.NewCidV1(cid.Raw, mh.Multihash(key))
		blk, err := blocks.NewBlockWithCid(value, c)
		require.NoError(t, err)
		require.True(t, expectedBlk.Cid().Equals(blk.Cid()))
		require.Equal(t, expectedBlk.RawData(), blk.RawData())
	}
	_, _, err = iter.Next()
	require.EqualError(t, err, io.EOF.Error())

	err = primaryStorage.Close()
	require.NoError(t, err)
}

func TestIndexGetEmptyIndex(t *testing.T) {
	tempDir := t.TempDir()
	primaryPath := filepath.Join(tempDir, "storethehash.primary")
	primaryStorage, err := mhprimary.OpenMultihashPrimary(primaryPath)
	require.NoError(t, err)
	defer primaryStorage.Close()

	key, value, err := primaryStorage.Get(types.Block{
		Offset: 0,
		Size:   50,
	})
	require.Nil(t, key)
	require.Nil(t, value)
	require.Error(t, err)
}

func TestIndexGet(t *testing.T) {
	tempDir := t.TempDir()
	primaryPath := filepath.Join(tempDir, "storethehash.primary")
	primaryStorage, err := mhprimary.OpenMultihashPrimary(primaryPath)
	require.NoError(t, err)

	// load blocks
	blks := testutil.GenerateBlocksOfSize(5, 100)
	var locs []types.Block
	for _, blk := range blks {
		loc, err := primaryStorage.Put(blk.Cid().Hash(), blk.RawData())
		require.NoError(t, err)
		locs = append(locs, loc)
	}

	// should fetch from memory before flush
	for i, loc := range locs {
		expectedBlk := blks[i]
		key, value, err := primaryStorage.Get(loc)
		require.NoError(t, err)
		c := cid.NewCidV1(cid.Raw, mh.Multihash(key))
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
		c := cid.NewCidV1(cid.Raw, mh.Multihash(key))
		blk, err := blocks.NewBlockWithCid(value, c)
		require.NoError(t, err)
		require.True(t, expectedBlk.Cid().Equals(blk.Cid()))
		require.Equal(t, expectedBlk.RawData(), blk.RawData())
	}

	err = primaryStorage.Close()
	require.NoError(t, err)
}

func TestFlushRace(t *testing.T) {
	const goroutines = 64
	tempDir := t.TempDir()
	primaryPath := filepath.Join(tempDir, "storethehash.primary")
	primaryStorage, err := mhprimary.OpenMultihashPrimary(primaryPath)
	require.NoError(t, err)

	// load blocks
	blks := testutil.GenerateBlocksOfSize(5, 100)
	var locs []types.Block
	for _, blk := range blks {
		loc, err := primaryStorage.Put(blk.Cid().Hash(), blk.RawData())
		require.NoError(t, err)
		locs = append(locs, loc)
	}

	start := make(chan struct{})
	errs := make(chan error)
	for n := 0; n < goroutines; n++ {
		go func() {
			<-start
			_, err := primaryStorage.Flush()
			errs <- err
		}()
	}
	close(start)
	for n := 0; n < goroutines; n++ {
		err := <-errs
		require.NoError(t, err)
	}

	require.NoError(t, primaryStorage.Close())
}
