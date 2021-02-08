package storethehash_test

import (
	"context"
	"errors"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hannahhoward/go-storethehash"
	store "github.com/hannahhoward/go-storethehash/internal"
	"github.com/hannahhoward/go-storethehash/internal/testutil"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/stretchr/testify/require"
)

func TestParallelism(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rand.Seed(time.Now().Unix())
	tempDir, err := ioutil.TempDir("", "sth")
	require.NoError(t, err)
	indexPath := filepath.Join(tempDir, "storethehash.index")
	dataPath := filepath.Join(tempDir, "storethehash.data")

	t.Logf("Creating store in directory %s\n", tempDir)
	bs, err := storethehash.OpenHashedBlockstore(indexPath, dataPath)
	require.NoError(t, err)
	bs.Start()
	defer bs.Shutdown(ctx)

	blks := testutil.GenerateBlocksOfSize(500, 100)

	t.Logf("Inserting %d samples\n", len(blks))
	duplicates := 0
	for _, blk := range blks {
		if err := bs.Put(blk); err != nil {
			if errors.Is(err, store.ErrKeyExists) {
				duplicates++
				continue
			}
			t.Fatalf("Failed to insert: %s", err.Error())
		}
	}
	t.Logf("Skipped %d duplicates\n", duplicates)

	t.Logf("Finding random blks")
	for i := 0; i < len(blks)/25; i++ {
		expectedBlk := blks[rand.Intn(len(blks))]
		blk, err := bs.Get(expectedBlk.Cid())
		require.NoError(t, err)
		require.True(t, expectedBlk.Cid().Equals(blk.Cid()))
		require.Equal(t, expectedBlk.RawData(), blk.RawData())
		t.Logf("Found %s", blk.Cid())
	}

	t.Logf("Running some concurrent inserts and fetches")

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(7)

	outputErrors := make(chan error, 7)

	newBlks := testutil.GenerateBlocksOfSize(5000, 100)

	for i := 0; i < 2; i++ {
		go func(ctx context.Context, wg *sync.WaitGroup) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					outputErrors <- nil
					return
				default:
				}
				for i := 0; i < 500; i++ {
					blk := newBlks[rand.Intn(len(newBlks))]
					if err := bs.Put(blk); err != nil && !errors.Is(err, store.ErrKeyExists) {
						t.Logf("Failed to insert: %v\n", err)
						outputErrors <- err
						return
					}
				}
				t.Logf("Wrote 500 records")
				time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)

			}
		}(ctx, &wg)
	}

	for i := 0; i < 5; i++ {
		go func(tx context.Context, wg *sync.WaitGroup) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					outputErrors <- nil
					return
				default:
				}
				for i := 0; i < 500; i++ {
					expectedBlk := newBlks[rand.Intn(len(newBlks))]
					_, err := bs.Get(expectedBlk.Cid())
					if err != nil && !errors.Is(err, bstore.ErrNotFound) {
						t.Logf("Failed to read: %v\n", err)
						outputErrors <- err
						return
					}
				}
				t.Logf("Read 500 records")
				time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			}
		}(ctx, &wg)
	}

	wg.Wait()
	for i := 0; i < 7; i++ {
		err := <-outputErrors
		require.NoError(t, err)
	}
}
