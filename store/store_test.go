package store_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/ipld/go-storethehash/store"
	"github.com/ipld/go-storethehash/store/freelist"
	cidprimary "github.com/ipld/go-storethehash/store/primary/cid"
	"github.com/ipld/go-storethehash/store/testutil"
	"github.com/ipld/go-storethehash/store/types"
	"github.com/stretchr/testify/require"
)

const defaultIndexSizeBits = uint8(24)
const defaultBurstRate = 4 * 1024 * 1024
const defaultSyncInterval = time.Second

func initStore(t *testing.T, dir string) (*store.Store, error) {
	indexPath := filepath.Join(dir, "storethehash.index")
	dataPath := filepath.Join(dir, "storethehash.data")
	primary, err := cidprimary.OpenCIDPrimary(dataPath)
	if err != nil {
		return nil, err
	}
	return store.OpenStore(indexPath, primary, defaultIndexSizeBits, defaultSyncInterval, defaultBurstRate)
}

func TestUpdate(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "sth")
	require.NoError(t, err)
	s, err := initStore(t, tempDir)
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
	iter := freelist.NewFreeListIter(file)
	// Check freelist for the only update. Should be the first position
	blk, err := iter.Next()
	require.Equal(t, blk.Offset, types.Position(0))
	require.NoError(t, err)
	// Check that is the last
	_, err = iter.Next()
	require.EqualError(t, err, io.EOF.Error())
}

func TestRemove(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "sth")
	require.NoError(t, err)
	s, err := initStore(t, tempDir)
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
	fmt.Println(flPath)
	file, err := os.Open(flPath)
	require.NoError(t, err)
	iter := freelist.NewFreeListIter(file)
	// Check freelist for the only removal. Should be the first position
	blk, err := iter.Next()
	require.Equal(t, blk.Offset, types.Position(0))
	require.NoError(t, err)
	// Check that is the last
	_, err = iter.Next()
	require.EqualError(t, err, io.EOF.Error())
}
