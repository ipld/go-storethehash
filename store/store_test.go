package store_test

import (
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	store "github.com/hannahhoward/go-storethehash/store"
	cidprimary "github.com/hannahhoward/go-storethehash/store/primary/cid"
	"github.com/hannahhoward/go-storethehash/store/testutil"
	"github.com/stretchr/testify/require"
)

const defaultIndexSizeBits = uint8(24)
const defaultBurstRate = 4 * 1024 * 1024
const defaultSyncInterval = time.Second

func initStore(t *testing.T) (*store.Store, error) {
	tempDir, err := ioutil.TempDir("", "sth")
	require.NoError(t, err)
	indexPath := filepath.Join(tempDir, "storethehash.index")
	dataPath := filepath.Join(tempDir, "storethehash.data")
	primary, err := cidprimary.OpenCIDPrimary(dataPath)
	if err != nil {
		return nil, err
	}
	store, err := store.OpenStore(indexPath, primary, defaultIndexSizeBits, defaultSyncInterval, defaultBurstRate)
	if err != nil {
		return nil, err
	}
	return store, nil
}

func TestUpdate(t *testing.T) {
	s, err := initStore(t)
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
	require.NoError(t, err)
	value, found, err = s.Get(blks[0].Cid().Bytes())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, value, blks[1].RawData())

}
