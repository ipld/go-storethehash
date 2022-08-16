package store_test

import (
	"context"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-cid"
	store "github.com/ipld/go-storethehash/store"
	"github.com/ipld/go-storethehash/store/freelist"
	cidprimary "github.com/ipld/go-storethehash/store/primary/cid"
	"github.com/ipld/go-storethehash/store/testutil"
	"github.com/ipld/go-storethehash/store/types"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func initStore(t *testing.T, dir string, immutable bool) (*store.Store, error) {
	indexPath := filepath.Join(dir, "storethehash.index")
	dataPath := filepath.Join(dir, "storethehash.data")
	primary, err := cidprimary.Open(dataPath)
	if err != nil {
		return nil, err
	}
	store, err := store.OpenStore(context.Background(), indexPath, primary, immutable)
	if err != nil {
		_ = primary.Close()
		return nil, err
	}
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store, nil
}

func TestUpdate(t *testing.T) {
	t.Run("when not immutable", func(t *testing.T) {
		tempDir := t.TempDir()
		s, err := initStore(t, tempDir, false)
		require.NoError(t, err)
		blks := testutil.GenerateBlocksOfSize(2, 100)

		t.Logf("Putting a new block")
		err = s.Put(blks[0].Cid().Bytes(), blks[0].RawData())
		require.NoError(t, err)
		value, found, err := s.Get(blks[0].Cid().Bytes())
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, value, blks[0].RawData())

		t.Logf("Overwrite same key with different value")
		err = s.Put(blks[0].Cid().Bytes(), blks[1].RawData())
		require.NoError(t, err)
		value, found, err = s.Get(blks[0].Cid().Bytes())
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, value, blks[1].RawData())

		t.Logf("Overwrite same key with same value")
		err = s.Put(blks[0].Cid().Bytes(), blks[1].RawData())
		require.Error(t, err, types.ErrKeyExists.Error())
		value, found, err = s.Get(blks[0].Cid().Bytes())
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, value, blks[1].RawData())

		s.Flush()

		// Start iterator
		flPath := filepath.Join(tempDir, "storethehash.index.free")
		file, err := os.Open(flPath)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, file.Close()) })

		iter := freelist.NewFreeListIter(file)
		// Check freelist for the only update. Should be the first position
		blk, err := iter.Next()
		require.Equal(t, blk.Offset, types.Position(0))
		require.NoError(t, err)
		// Check that is the last
		_, err = iter.Next()
		require.EqualError(t, err, io.EOF.Error())
	})
	t.Run("when immutable", func(t *testing.T) {
		tempDir := t.TempDir()
		s, err := initStore(t, tempDir, true)
		require.NoError(t, err)
		blks := testutil.GenerateBlocksOfSize(2, 100)

		t.Logf("Putting a new block")
		err = s.Put(blks[0].Cid().Bytes(), blks[0].RawData())
		require.NoError(t, err)
		value, found, err := s.Get(blks[0].Cid().Bytes())
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, value, blks[0].RawData())

		t.Logf("Overwrite same key with different value")
		err = s.Put(blks[0].Cid().Bytes(), blks[1].RawData())
		require.Error(t, err, types.ErrKeyExists.Error())
		value, found, err = s.Get(blks[0].Cid().Bytes())
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, value, blks[0].RawData())

		t.Logf("Overwrite same key with same value")
		err = s.Put(blks[0].Cid().Bytes(), blks[1].RawData())
		require.Error(t, err, types.ErrKeyExists.Error())
		value, found, err = s.Get(blks[0].Cid().Bytes())
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, value, blks[0].RawData())

		s.Flush()

		// Start iterator
		flPath := filepath.Join(tempDir, "storethehash.index.free")
		file, err := os.Open(flPath)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, file.Close()) })

		iter := freelist.NewFreeListIter(file)
		// Check freelist -- no updates
		_, err = iter.Next()
		require.EqualError(t, err, io.EOF.Error())
	})
	t.Run("when immutable with nearly identical CIDs", func(t *testing.T) {
		tempDir := t.TempDir()
		s, err := initStore(t, tempDir, true)
		require.NoError(t, err)
		block1 := make([]byte, 100)
		_, _ = rand.Read(block1)

		cid1, err := cid.V1Builder{
			Codec:    cid.Raw,
			MhType:   multihash.IDENTITY,
			MhLength: -1,
		}.Sum([]byte("Hello I am a cid multihash, the absolute #1"))
		require.NoError(t, err)

		cid2, err := cid.V1Builder{
			Codec:    cid.Raw,
			MhType:   multihash.IDENTITY,
			MhLength: -1,
		}.Sum([]byte("Hello I am a cid multihash, the absolute #2"))
		require.NoError(t, err)

		t.Logf("Putting a new block")
		err = s.Put(cid1.Bytes(), block1)
		require.NoError(t, err)
		value, found, err := s.Get(cid1.Bytes())
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, value, block1)

		t.Logf("Overwrite similar key with same value")
		err = s.Put(cid2.Bytes(), block1)
		require.NoError(t, err)
		value, found, err = s.Get(cid2.Bytes())
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, value, block1)

		s.Flush()

		// Start iterator
		flPath := filepath.Join(tempDir, "storethehash.index.free")
		file, err := os.Open(flPath)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, file.Close()) })

		iter := freelist.NewFreeListIter(file)
		// Check freelist -- no updates
		_, err = iter.Next()
		require.EqualError(t, err, io.EOF.Error())
	})
}

func TestRemove(t *testing.T) {
	tempDir := t.TempDir()
	s, err := initStore(t, tempDir, false)
	require.NoError(t, err)
	blks := testutil.GenerateBlocksOfSize(2, 100)

	t.Logf("Putting blocks")
	err = s.Put(blks[0].Cid().Bytes(), blks[0].RawData())
	require.NoError(t, err)
	err = s.Put(blks[1].Cid().Bytes(), blks[1].RawData())
	require.NoError(t, err)

	t.Logf("Removing the first block")
	removed, err := s.Remove(blks[0].Cid().Bytes())
	require.NoError(t, err)
	require.True(t, removed)

	t.Logf("Checking if the block has been removed successfully")
	value, found, err := s.Get(blks[1].Cid().Bytes())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, value, blks[1].RawData())
	_, found, err = s.Get(blks[0].Cid().Bytes())
	require.NoError(t, err)
	require.False(t, found)

	t.Logf("Trying to remove non-existing key")
	removed, err = s.Remove(blks[0].Cid().Bytes())
	require.NoError(t, err)
	require.False(t, removed)

	s.Flush()

	// Start iterator
	flPath := filepath.Join(tempDir, "storethehash.index.free")
	file, err := os.Open(flPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })

	iter := freelist.NewFreeListIter(file)
	// Check freelist for the only removal. Should be the first position
	blk, err := iter.Next()
	require.Equal(t, blk.Offset, types.Position(0))
	require.NoError(t, err)
	// Check that is the last
	_, err = iter.Next()
	require.EqualError(t, err, io.EOF.Error())
}

func TestRecoverBadKey(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "storethehash.index")
	dataPath := filepath.Join(tmpDir, "storethehash.data")
	primary, err := cidprimary.Open(dataPath)
	require.NoError(t, err)
	s, err := store.OpenStore(context.Background(), indexPath, primary, false)
	require.NoError(t, err)

	t.Logf("Putting blocks")
	blks := testutil.GenerateBlocksOfSize(1, 100)
	err = s.Put(blks[0].Cid().Bytes(), blks[0].RawData())
	require.NoError(t, err)

	// Close store and remove primary.
	require.NoError(t, s.Close())
	err = os.Remove(dataPath)
	require.NoError(t, err)

	// Open store again.
	primary, err = cidprimary.Open(dataPath)
	require.NoError(t, err)
	s, err = store.OpenStore(context.Background(), indexPath, primary, false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	// Put data.
	err = s.Put(blks[0].Cid().Bytes(), blks[0].RawData())
	require.NoError(t, err)

	// Get data.
	value, found, err := s.Get(blks[0].Cid().Bytes())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, value, blks[0].RawData())
}
