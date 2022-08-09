package inmemory

import (
	"fmt"
	"io"
	"time"

	"github.com/ipld/go-storethehash/store/primary"
	"github.com/ipld/go-storethehash/store/types"
)

//! In-memory primary storage implementation.
//!
//! It's using a vector of tuples containing the key-value pairs.

type InMemory struct {
	pairs [][2][]byte
	ident string
}

func NewInmemory(data [][2][]byte) *InMemory {
	return &InMemory{
		pairs: data,
		ident: fmt.Sprint("inmemory-", time.Now().UnixNano()),
	}
}

func (im *InMemory) Get(blk types.Block) (key []byte, value []byte, err error) {
	max := len(im.pairs)
	if blk.Offset >= types.Position(max) {
		return nil, nil, types.ErrOutOfBounds
	}
	val := im.pairs[blk.Offset]
	return val[0], val[1], nil
}

func (im *InMemory) Put(key []byte, value []byte) (blk types.Block, err error) {
	pos := len(im.pairs)
	im.pairs = append(im.pairs, [2][]byte{key, value})
	return types.Block{Offset: types.Position(pos), Size: 1}, nil
}

func (im *InMemory) Flush() (types.Work, error) {
	return 0, nil
}

func (im *InMemory) Sync() error {
	return nil
}

func (im *InMemory) Close() error {
	return nil
}

func (im *InMemory) OutstandingWork() types.Work {
	return 0
}

func (im *InMemory) IndexKey(key []byte) ([]byte, error) {
	return key, nil
}

func (im *InMemory) GetIndexKey(blk types.Block) ([]byte, error) {
	key, _, err := im.Get(blk)
	if err != nil {
		return nil, err
	}
	return im.IndexKey(key)
}

func (im *InMemory) Iter() (primary.PrimaryStorageIter, error) {
	return &inMemoryIter{im, 0}, nil
}

type inMemoryIter struct {
	im  *InMemory
	idx int
}

func (imi *inMemoryIter) Next() ([]byte, []byte, error) {
	key, value, err := imi.im.Get(types.Block{Offset: types.Position(imi.idx)})
	if err == types.ErrOutOfBounds {
		return nil, nil, io.EOF
	}
	imi.idx++
	return key, value, nil
}

func (im *InMemory) StorageSize() (int64, error) {
	return 0, nil
}

// Ident always returns a different value per InMemory instance because the
// existing index is invalid if used with a new InMemory instance.
func (im *InMemory) Ident() string {
	return im.ident
}

var _ primary.PrimaryStorage = &InMemory{}
