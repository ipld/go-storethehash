package store

import (
	"io"

	"github.com/ipld/go-storethehash/store/index"
)

// Iterator iterates keys and values. Any write to the store potentially
// invalidates the iterator and may cause values to be missed or seen again.
type Iterator struct {
	index     *index.Index
	indexIter *index.Iterator
}

// NewIterator creates a new store iterator.
func (s *Store) NewIterator() *Iterator {
	_ = s.Flush()
	return &Iterator{
		index:     s.index,
		indexIter: s.index.NewIterator(),
	}
}

// Next returns the next key and value. Returns io.EOF error when done.
func (it *Iterator) Next() ([]byte, []byte, error) {
	for {
		rec, done, err := it.indexIter.Next()
		if err != nil {
			return nil, nil, err
		}
		if done {
			return nil, nil, io.EOF
		}

		// Get the key and value stored in primary to see if it is the same
		// (index only stores prefixes).
		key, value, err := it.index.Primary.Get(rec.Block)
		if err != nil || key == nil {
			// Record no longer there, skip.
			continue
		}

		return key, value, nil
	}
}
