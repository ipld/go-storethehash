package storethehash_test

import (
	"context"
	"errors"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-storethehash"
	"github.com/ipld/go-storethehash/store/testutil"
	"github.com/ipld/go-storethehash/store/types"
	"github.com/stretchr/testify/require"
)

func TestParallelism(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rng := rand.New(rand.NewSource(1413))
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, "storethehash.index")
	dataPath := filepath.Join(tempDir, "storethehash.data")

	t.Logf("Creating store in directory %s\n", tempDir)
	bs, err := storethehash.OpenHashedBlockstore(ctx, indexPath, dataPath)
	require.NoError(t, err)
	bs.Start()
	defer bs.Close()

	blks := testutil.GenerateBlocksOfSize(500, 100)

	t.Logf("Inserting %d samples\n", len(blks))
	duplicates := 0
	for _, blk := range blks {
		if err := bs.Put(context.Background(), blk); err != nil {
			if errors.Is(err, types.ErrKeyExists) {
				duplicates++
				continue
			}
			t.Fatalf("Failed to insert: %s", err.Error())
		}
	}
	t.Logf("Skipped %d duplicates\n", duplicates)

	t.Logf("Finding random blks")
	for i := 0; i < len(blks)/25; i++ {
		expectedBlk := blks[rng.Intn(len(blks))]
		blk, err := bs.Get(context.Background(), expectedBlk.Cid())
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
		go func(ctx context.Context, wg *sync.WaitGroup, i int) {
			rng := rand.New(rand.NewSource(int64(i)))
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					outputErrors <- nil
					return
				default:
				}
				for i := 0; i < 500; i++ {
					blk := newBlks[rng.Intn(len(newBlks))]
					if err := bs.Put(context.Background(), blk); err != nil && !errors.Is(err, types.ErrKeyExists) {
						t.Logf("Failed to insert cid %v: %v\n", blk.Cid().String(), err)
						outputErrors <- err
						return
					}
				}
				t.Logf("Wrote 500 records")
				time.Sleep(time.Duration(rng.Intn(100)) * time.Millisecond)
			}
		}(ctx, &wg, i)
	}

	for i := 0; i < 5; i++ {
		go func(tx context.Context, wg *sync.WaitGroup, i int) {
			rng := rand.New(rand.NewSource(int64(i)))
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					outputErrors <- nil
					return
				default:
				}
				for i := 0; i < 500; i++ {
					expectedBlk := newBlks[rng.Intn(len(newBlks))]
					_, err := bs.Get(context.Background(), expectedBlk.Cid())
					if err != nil && !errors.Is(err, ipld.ErrNotFound{}) {
						t.Logf("Failed to read: %v\n", err)
						outputErrors <- err
						return
					}
				}
				t.Logf("Read 500 records")
				time.Sleep(time.Duration(rng.Intn(100)) * time.Millisecond)
			}
		}(ctx, &wg, i)
	}

	wg.Wait()
	for i := 0; i < 7; i++ {
		err := <-outputErrors
		require.NoError(t, err)
	}
}
