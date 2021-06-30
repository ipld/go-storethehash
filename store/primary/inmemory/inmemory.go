package inmemory

import (
	"io"

	store "github.com/hannahhoward/go-storethehash/store"
)

//! In-memory primary storage implementation.
//!
//! It's using a vector of tuples containing the key-value pairs.

type InMemory [][2][]byte

func NewInmemory(data [][2][]byte) *InMemory {
	value := InMemory(data)
	return &value
}

func (im *InMemory) Get(blk store.Block) (key []byte, value []byte, err error) {
	max := len(*im)
	if blk.Offset >= store.Position(max) {
		return nil, nil, store.ErrOutOfBounds
	}
	val := (*im)[blk.Offset]
	return val[0], val[1], nil
}

func (im *InMemory) Put(key []byte, value []byte) (blk store.Block, err error) {
	pos := len(*im)
	*im = append(*im, [2][]byte{key, value})
	return store.Block{Offset: store.Position(pos), Size: 1}, nil
}

func (im *InMemory) Flush() (store.Work, error) {
	return 0, nil
}

func (im *InMemory) Sync() error {
	return nil
}

func (im *InMemory) Close() error {
	return nil
}

func (im *InMemory) OutstandingWork() store.Work {
	return 0
}

func (im *InMemory) IndexKey(key []byte) ([]byte, error) {
	return key, nil
}

func (im *InMemory) GetIndexKey(blk store.Block) ([]byte, error) {
	key, _, err := im.Get(blk)
	if err != nil {
		return nil, err
	}
	return im.IndexKey(key)
}

func (im *InMemory) Iter() (store.PrimaryStorageIter, error) {
	return &inMemoryIter{im, 0}, nil
}

type inMemoryIter struct {
	im  *InMemory
	idx int
}

func (imi *inMemoryIter) Next() ([]byte, []byte, error) {
	key, value, err := imi.im.Get(store.Block{Offset: store.Position(imi.idx)})
	if err == store.ErrOutOfBounds {
		return nil, nil, io.EOF
	}
	imi.idx++
	return key, value, nil
}

var _ store.PrimaryStorage = &InMemory{}
