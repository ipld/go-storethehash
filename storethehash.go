package storethehash

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	ipld "github.com/ipfs/go-ipld-format"
	store "github.com/ipld/go-storethehash/store"
	mhprimary "github.com/ipld/go-storethehash/store/primary/multihash"
	"github.com/ipld/go-storethehash/store/types"
)

type errorType string

func (e errorType) Error() string {
	return string(e)
}

// ErrNotSupported indicates and error that is not supported because this store is append only
const ErrNotSupported = errorType("Operation not supported")

// HashedBlockstore is a blockstore that uses a simple hash table and two files to write
type HashedBlockstore struct {
	store      *store.Store
	hashOnRead bool
}

// OpenHashedBlockstore opens a HashedBlockstore with the default index size
func OpenHashedBlockstore(ctx context.Context, indexPath string, dataPath string, options ...store.Option) (*HashedBlockstore, error) {
	primary, err := mhprimary.Open(dataPath)
	if err != nil {
		return nil, err
	}
	store, err := store.OpenStore(ctx, indexPath, primary, true, options...)
	if err != nil {
		return nil, err
	}
	return &HashedBlockstore{store, false}, nil
}

// DeleteBlock is not supported for this store
func (bs *HashedBlockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	_, err := bs.store.Remove(c.Hash())
	return err
}

// Has indicates if a block is present in a block store
func (bs *HashedBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	return bs.store.Has(c.Hash())
}

// Get returns a block
func (bs *HashedBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	value, found, err := bs.store.Get(c.Hash())
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ipld.ErrNotFound{Cid: c}
	}
	// if hash on read is enabled, rehash and compare blocks
	if bs.hashOnRead {
		newCid, err := c.Prefix().Sum(value)
		if err != nil {
			return nil, err
		}
		if !newCid.Equals(c) {
			return nil, blocks.ErrWrongHash
		}
	}
	return blocks.NewBlockWithCid(value, c)
}

// GetSize returns the CIDs mapped BlockSize
func (bs *HashedBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	// unoptimized implementation for now
	size, found, err := bs.store.GetSize(c.Hash())
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, ipld.ErrNotFound{Cid: c}
	}
	return int(size), nil
}

// Put puts a given block to the underlying datastore
func (bs *HashedBlockstore) Put(ctx context.Context, blk blocks.Block) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	err := bs.store.Put(blk.Cid().Hash(), blk.RawData())
	// suppress key exist error because this is not expected behavior for a blockstore
	if err == types.ErrKeyExists {
		return nil
	}
	return err
}

// PutMany puts a slice of blocks at the same time using batching
// capabilities of the underlying datastore whenever possible.
func (bs *HashedBlockstore) PutMany(ctx context.Context, blks []blocks.Block) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	for _, blk := range blks {
		err := bs.store.Put(blk.Cid().Hash(), blk.RawData())
		// suppress key exist error because this is not expected behavior for a blockstore
		if err != nil && err != types.ErrKeyExists {
			return err
		}
	}
	return nil
}

// AllKeysChan returns a channel from which
// the CIDs in the Blockstore can be read. It should respect
// the given context, closing the channel if it becomes Done.
func (bs *HashedBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, ErrNotSupported
}

// HashOnRead specifies if every read block should be
// rehashed to make sure it matches its CID.
func (bs *HashedBlockstore) HashOnRead(enabled bool) {
	bs.hashOnRead = true
}

func (bs *HashedBlockstore) Start() {
	bs.store.Start()
}

func (bs *HashedBlockstore) Close() {
	bs.store.Close()
}

var _ bstore.Blockstore = &HashedBlockstore{}

// ErrOutOfBounds indicates the bucket index was greater than the number of bucks
const ErrOutOfBounds = types.ErrOutOfBounds

// ErrIndexTooLarge indicates the maximum supported bucket size is 32-bits
const ErrIndexTooLarge = types.ErrIndexTooLarge

const ErrKeyTooShort = types.ErrKeyTooShort

const ErrKeyExists = types.ErrKeyExists

type ErrIndexWrongBitSize = types.ErrIndexWrongBitSize
