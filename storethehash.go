package storethehash

import (
	"context"

	store "github.com/hannahhoward/go-storethehash/internal"
	cidprimary "github.com/hannahhoward/go-storethehash/internal/primary/cid"
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

// DefaultIndexSizeBits is the default size for in memory portion of the index
const DefaultIndexSizeBits = uint8(24)

// OpenHashedBlockstore opens a HashedBlockstore with the default index size
func OpenHashedBlockstore(indexPath string, dataPath string) (*HashedBlockstore, error) {
	return OpenHashedBlockstoreBitSize(indexPath, dataPath, DefaultIndexSizeBits)
}

// OpenHashedBlockstoreBitSize opens a HashedBlockstore with the specified index size
func OpenHashedBlockstoreBitSize(indexPath string, dataPath string, indexSizeBits uint8) (*HashedBlockstore, error) {
	primary, err := cidprimary.OpenCIDPrimary(dataPath)
	if err != nil {
		return nil, err
	}
	store, err := store.OpenStore(indexPath, primary, indexSizeBits)
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
	if err != nil {
		return err
	}
	return bs.store.Flush()
}

// PutMany puts a slice of blocks at the same time using batching
// capabilities of the underlying datastore whenever possible.
func (bs *HashedBlockstore) PutMany(blks []blocks.Block) error {
	for _, blk := range blks {
		err := bs.store.Put(blk.Cid().Bytes(), blk.RawData())
		if err != nil {
			return err
		}
	}
	return bs.store.Flush()
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

// CopyInto is a special method to mass copy one store into another
func (bs *HashedBlockstore) CopyInto(other *HashedBlockstore) error {
	err := bs.store.CopyInto(other.store)
	if err != nil {
		return err
	}
	return other.store.Flush()
}

var _ bstore.Blockstore = &HashedBlockstore{}
