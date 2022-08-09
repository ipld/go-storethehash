package mhprimary_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/ipld/go-storethehash/store"
	"github.com/ipld/go-storethehash/store/freelist"
	mhprimary "github.com/ipld/go-storethehash/store/primary/multihash"
	"github.com/ipld/go-storethehash/store/testutil"
	"github.com/ipld/go-storethehash/store/types"
	"github.com/stretchr/testify/require"
)

func TestGC(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, "storethehash.index")
	dataPath := filepath.Join(tempDir, "storethehash.data")
	freeListPath := indexPath + ".free"

	t.Logf("Creating store in directory %s\n", tempDir)

	freeList, err := freelist.Open(freeListPath)
	require.NoError(t, err)

	primary, err := mhprimary.Open(dataPath, 1024, freeList)
	require.NoError(t, err)
	store, err := store.OpenStore(ctx, indexPath, primary, freeList, 0, 0, time.Minute, 10240, time.Hour, false)
	require.NoError(t, err)
	defer store.Close()

	blks := testutil.GenerateBlocksOfSize(9, 250)

	t.Logf("Inserting %d samples\n", len(blks))
	duplicates := 0
	for _, blk := range blks {
		if err = store.Put(blk.Cid().Hash(), blk.RawData()); err != nil {
			if errors.Is(err, types.ErrKeyExists) {
				duplicates++
				continue
			}
			t.Fatalf("Failed to insert: %s", err.Error())
		}
	}
	t.Logf("Skipped %d duplicates\n", duplicates)

	t.Logf("Finding first 4 blocks")
	for i := 0; i < 4; i++ {
		blk := blks[i]
		removed, err := store.Remove(blk.Cid().Hash())
		require.NoError(t, err)
		require.True(t, removed)
		t.Logf("Removed block %d: %s", i, blk.Cid())
	}

	err = store.Flush()
	require.NoError(t, err)

	primary0 := dataPath + ".0"
	primary1 := dataPath + ".1"
	primary2 := dataPath + ".2"

	require.FileExists(t, primary0)
	require.FileExists(t, primary1)
	require.FileExists(t, primary2)

	t.Logf("Running primary GC")
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	fileCount, err := store.PrimaryGC(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, fileCount)

	// Check that first primary file was deleted.
	require.NoFileExists(t, primary0)
	// Check that other primary files are present.
	require.FileExists(t, primary1)
	require.FileExists(t, primary2)

	t.Logf("Running primary GC with not additional removals")
	fileCount, err = store.PrimaryGC(ctx)
	require.NoError(t, err)
	require.Zero(t, fileCount)

	// Check that other primary files are still present.
	require.FileExists(t, primary1)
	require.FileExists(t, primary2)

	// Remove one more block
	blk := blks[4]
	removed, err := store.Remove(blk.Cid().Hash())
	require.NoError(t, err)
	require.True(t, removed)

	t.Logf("Removed block %d: %s", 4, blk.Cid())
	err = store.Flush()
	require.NoError(t, err)

	t.Logf("Running primary GC")
	fileCount, err = store.PrimaryGC(ctx)
	require.NoError(t, err)
	require.Zero(t, fileCount)

	// Check that other primary files are present.
	require.FileExists(t, primary1)
	require.FileExists(t, primary2)

	// Put .1 file into low-use condition.
	for i := 5; i < 7; i++ {
		blk := blks[i]
		removed, err := store.Remove(blk.Cid().Hash())
		require.NoError(t, err)
		require.True(t, removed)
		t.Logf("Removed block %d: %s", i, blk.Cid())
	}

	err = store.Flush()
	require.NoError(t, err)

	t.Logf("Running primary GC on low-use file to evaporate remaining record")
	fileCount, err = store.PrimaryGC(ctx)
	require.NoError(t, err)
	require.Zero(t, fileCount)

	// GC should have relocated record, but not removed old file yet.
	require.FileExists(t, primary1)
	require.FileExists(t, primary2)

	err = store.Flush()
	require.NoError(t, err)

	t.Logf("Running primary GC on low-use file to remove file")
	fileCount, err = store.PrimaryGC(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, fileCount)

	// GC should have cleaned up evaporated low-use file.
	require.NoFileExists(t, primary1)
	require.FileExists(t, primary2)

}