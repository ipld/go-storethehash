package storethehash

import (
	"context"
	"time"

	store "github.com/hannahhoward/go-storethehash/store"
	cidprimary "github.com/hannahhoward/go-storethehash/store/primary/cid"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
)

type errorType string

func (e errorType) Error() string {
	return string(e)
}

// ErrNotSupported indicates and error that is not supported because this store is append only
const ErrNotSupported = errorType("Operation not supported")

// HashedBlockstore is a blockstore that uses a simple hash table and two files to write
type HashedBlockstore struct {
	store *store.Store
}

const defaultIndexSizeBits = uint8(24)
const defaultBurstRate = 4 * 1024 * 1024
const defaultSyncInterval = time.Second

type configOptions struct {
	indexSizeBits uint8
	syncInterval  time.Duration
	burstRate     store.Work
}

type Option func(*configOptions)

func IndexBitSize(indexBitSize uint8) Option {
	return func(co *configOptions) {
		co.indexSizeBits = indexBitSize
	}
}

func SyncInterval(syncInterval time.Duration) Option {
	return func(co *configOptions) {
		co.syncInterval = syncInterval
	}
}

func BurstRate(burstRate uint64) Option {
	return func(co *configOptions) {
		co.burstRate = store.Work(burstRate)
	}
}

// OpenHashedBlockstore opens a HashedBlockstore with the default index size
func OpenHashedBlockstore(indexPath string, dataPath string, options ...Option) (*HashedBlockstore, error) {
	co := configOptions{
		indexSizeBits: defaultIndexSizeBits,
		syncInterval:  defaultSyncInterval,
		burstRate:     defaultBurstRate,
	}
	for _, option := range options {
		option(&co)
	}
	primary, err := cidprimary.OpenCIDPrimary(dataPath)
	if err != nil {
		return nil, err
	}
	store, err := store.OpenStore(indexPath, primary, co.indexSizeBits, co.syncInterval, co.burstRate)
	if err != nil {
		return nil, err
	}
	return &HashedBlockstore{store}, nil
}

// DeleteBlock is not supported for this store
func (bs *HashedBlockstore) DeleteBlock(_ cid.Cid) error {
	return ErrNotSupported
}

// Has indicates if a block is present in a block store
func (bs *HashedBlockstore) Has(c cid.Cid) (bool, error) {
	return bs.store.Has(c.Bytes())
}

// Get returns a block
func (bs *HashedBlockstore) Get(c cid.Cid) (blocks.Block, error) {
	value, found, err := bs.store.Get(c.Bytes())
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, bstore.ErrNotFound
	}
	return blocks.NewBlockWithCid(value, c)
}

// GetSize returns the CIDs mapped BlockSize
func (bs *HashedBlockstore) GetSize(c cid.Cid) (int, error) {
	// unoptimized implementation for now
	size, found, err := bs.store.GetSize(c.Bytes())
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, bstore.ErrNotFound
	}
	return int(size), nil
}

// Put puts a given block to the underlying datastore
func (bs *HashedBlockstore) Put(blk blocks.Block) error {
	err := bs.store.Put(blk.Cid().Bytes(), blk.RawData())
	// suppress key exist error because this is not expected behavior for a blockstore
	if err == store.ErrKeyExists {
		return nil
	}
	return err
}

// PutMany puts a slice of blocks at the same time using batching
// capabilities of the underlying datastore whenever possible.
func (bs *HashedBlockstore) PutMany(blks []blocks.Block) error {
	for _, blk := range blks {
		err := bs.store.Put(blk.Cid().Bytes(), blk.RawData())
		// suppress key exist error because this is not expected behavior for a blockstore
		if err != nil && err != store.ErrKeyExists {
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
}

func (bs *HashedBlockstore) Start() {
	bs.store.Start()
}

func (bs *HashedBlockstore) Close() {
	bs.store.Close()
}

var _ bstore.Blockstore = &HashedBlockstore{}

// ErrOutOfBounds indicates the bucket index was greater than the number of bucks
const ErrOutOfBounds = store.ErrOutOfBounds

// ErrIndexTooLarge indicates the maximum supported bucket size is 32-bits
const ErrIndexTooLarge = store.ErrIndexTooLarge

const ErrKeyTooShort = store.ErrKeyTooShort

const ErrKeyExists = store.ErrKeyExists

type ErrIndexWrongBitSize = store.ErrIndexWrongBitSize
