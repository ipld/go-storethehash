package store

import (
	"bytes"
	"io"
	"sync"
)

type Store struct {
	index   *Index
	writeLk sync.Mutex
}

func OpenStore(path string, primary PrimaryStorage, indexSizeBits uint8) (*Store, error) {
	index, err := OpenIndex(path, primary, indexSizeBits)
	if err != nil {
		return nil, err
	}
	return &Store{index: index}, nil
}

func (s *Store) Get(key []byte) ([]byte, bool, error) {
	indexKey, err := s.index.Primary.IndexKey(key)
	if err != nil {
		return nil, false, err
	}
	fileOffset, found, err := s.index.Get(indexKey)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	primaryKey, value, err := s.index.Primary.Get(fileOffset)
	if err != nil {
		return nil, false, err
	}

	// The index stores only prefixes, hence check if the given key fully matches the
	// key that is stored in the primary storage before returning the actual value.
	if bytes.Compare(key, primaryKey) == 0 {
		return value, true, nil
	} else {
		return nil, false, nil
	}
}

func (s *Store) Put(key []byte, value []byte) error {
	s.writeLk.Lock()
	defer s.writeLk.Unlock()
	fileOffset, err := s.index.Primary.Put(key, value)
	if err != nil {
		return err
	}
	indexKey, err := s.index.Primary.IndexKey(key)
	if err != nil {
		return err
	}
	return s.index.Put(indexKey, fileOffset)
}

func (s *Store) Flush() error {
	s.writeLk.Lock()
	defer s.writeLk.Unlock()
	if err := s.index.Flush(); err != nil {
		return err
	}
	return s.index.Primary.Flush()
}

func (s *Store) Has(key []byte) (bool, error) {
	indexKey, err := s.index.Primary.IndexKey(key)
	if err != nil {
		return false, err
	}
	_, found, err := s.index.Get(indexKey)
	return found, err
}

func (s *Store) GetSize(key []byte) (Size, bool, error) {
	indexKey, err := s.index.Primary.IndexKey(key)
	if err != nil {
		return 0, false, err
	}
	blk, found, err := s.index.Get(indexKey)
	if err != nil {
		return 0, false, err
	}
	if !found {
		return 0, false, nil
	}
	return blk.Size - Size(len(key)), true, nil
}

func (s *Store) CopyInto(other *Store) error {
	s.writeLk.Lock()
	defer s.writeLk.Unlock()
	iter, err := s.index.Primary.Iter()
	if err != nil {
		return err
	}
	for {
		key, value, err := iter.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := other.Put(key, value); err != nil {
			return err
		}
	}
}
