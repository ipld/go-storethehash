package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-storethehash/store/freelist"
	"github.com/ipld/go-storethehash/store/index"
	"github.com/ipld/go-storethehash/store/primary"
	mhprimary "github.com/ipld/go-storethehash/store/primary/multihash"
	"github.com/ipld/go-storethehash/store/types"
)

const DefaultBurstRate = 4 * 1024 * 1024

var log = logging.Logger("storethehash")

type Store struct {
	index    *index.Index
	freelist *freelist.FreeList

	indexGC   GC
	primaryGC GC

	stateLk sync.RWMutex
	open    bool
	running bool
	err     error

	rateLk    sync.RWMutex
	rate      float64 // rate at which data can be flushed
	burstRate types.Work
	lastFlush time.Time

	closed       chan struct{}
	closing      chan struct{}
	flushNow     chan struct{}
	syncInterval time.Duration
	immutable    bool
}

type GC interface {
	SignalUpdate()
	Close()
	Cycle(context.Context) (int, error)
}

// OpenStore opens the index and freelist and returns a Store with the given
// primary.
//
// Specifying 0 for indexSizeBits and indexFileSize results in using their
// default values. A gcInterval of 0 disables garbage collection.
func OpenStore(ctx context.Context, path string, primary primary.PrimaryStorage, freeList *freelist.FreeList, indexSizeBits uint8, indexFileSize uint32, syncInterval time.Duration, burstRate types.Work, gcInterval time.Duration, immutable bool) (*Store, error) {
	idx, err := index.Open(ctx, path, primary, indexSizeBits, indexFileSize)
	if err != nil {
		return nil, err
	}

	store := &Store{
		lastFlush:    time.Now(),
		index:        idx,
		freelist:     freeList,
		open:         true,
		running:      false,
		syncInterval: syncInterval,
		burstRate:    burstRate,
		closed:       make(chan struct{}),
		closing:      make(chan struct{}),
		flushNow:     make(chan struct{}, 1),
		immutable:    immutable,
	}

	if gcInterval == 0 {
		log.Warn("Index and primary garbage collection disabled")
	} else {
		store.indexGC = index.NewGC(idx, gcInterval)

		mp, ok := primary.(*mhprimary.MultihashPrimary)
		if ok && mp != nil {
			store.primaryGC = mhprimary.NewGC(mp, freeList, gcInterval, idx.Update)
		}
	}

	return store, nil
}

func (s *Store) Start() {
	s.stateLk.Lock()
	running := s.running
	s.running = true
	s.stateLk.Unlock()
	if !running {
		go s.run()
	}
}

func (s *Store) IndexGC(ctx context.Context) (int, error) {
	if s.indexGC == nil {
		return 0, errors.New("index garbage collection not enabled")
	}
	return s.indexGC.Cycle(ctx)
}

func (s *Store) PrimaryGC(ctx context.Context) (int, error) {
	if s.primaryGC == nil {
		return 0, errors.New("primary garbage collection not enabled")
	}
	return s.primaryGC.Cycle(ctx)
}

func (s *Store) run() {
	defer close(s.closed)
	d := time.NewTicker(s.syncInterval)

	for {
		select {
		case <-s.flushNow:
			if err := s.Flush(); err != nil {
				s.setErr(err)
			}
		case <-s.closing:
			d.Stop()
			select {
			case <-d.C:
			default:
			}
			return
		case <-d.C:
			select {
			case s.flushNow <- struct{}{}:
			default:
				// Already signaled by write, do not need another flush.
			}
		}
	}
}

// Close stops store goroutines and calls Close on the index, primary, and
// freelist. This flushes any outstanding work and buffered data to their
// files.
func (s *Store) Close() error {
	s.stateLk.Lock()
	open := s.open
	s.open = false

	if !open {
		s.stateLk.Unlock()
		return nil
	}

	running := s.running
	s.running = false
	s.stateLk.Unlock()

	if running {
		close(s.closing)
		<-s.closed
	}

	cerr := s.Err()

	if s.indexGC != nil {
		s.indexGC.Close()
	}
	if s.primaryGC != nil {
		s.primaryGC.Close()
	}

	err := s.index.Close()
	if err != nil {
		cerr = err
	}
	if err = s.index.Primary.Close(); err != nil {
		cerr = err
	}
	if err = s.freelist.Close(); err != nil {
		cerr = err
	}

	return cerr
}

func (s *Store) Get(key []byte) ([]byte, bool, error) {
	err := s.Err()
	if err != nil {
		return nil, false, err
	}

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

	primaryKey, value, err := s.getPrimaryKeyData(fileOffset, indexKey)
	if err != nil {
		return nil, false, err
	}
	if primaryKey == nil {
		return nil, false, nil
	}

	return value, true, nil
}

func (s *Store) Err() error {
	s.stateLk.RLock()
	defer s.stateLk.RUnlock()
	return s.err
}

func (s *Store) setErr(err error) {
	s.stateLk.Lock()
	s.err = err
	s.stateLk.Unlock()
}

func (s *Store) Put(key []byte, value []byte) error {
	err := s.Err()
	if err != nil {
		return err
	}

	// Get the key in primary storage
	indexKey, err := s.index.Primary.IndexKey(key)
	if err != nil {
		return err
	}
	// See if the key already exists and get offset
	prevOffset, found, err := s.index.Get(indexKey)
	if err != nil {
		return err
	}
	// If found, get the key and value stored in primary to see if it is the
	// same (index only stores prefixes).
	var storedKey []byte
	var storedVal []byte
	var cmpKey bool
	if found {
		storedKey, storedVal, err = s.getPrimaryKeyData(prevOffset, indexKey)
		if err != nil {
			return err
		}
		// We need to compare to the resulting indexKey for the storedKey.
		// Two keys may point to same IndexKey (i.e. two CIDS same multihash),
		// and they need to be treated as the same key.
		if storedKey != nil {
			// if we're not accepting updates, this is the point we bail --
			// the identical key is in primary storage, we don't do update operations
			if s.immutable {
				return types.ErrKeyExists
			}
			cmpKey = true
		}
		if bytes.Equal(value, storedVal) {
			// Trying to put the same value in an existing key, so ok to
			// directly return.
			//
			// NOTE: How many times is this going to happen. Can this step be
			// removed? This is still needed for the blockstore and that is
			// ErrKeyExists is returned..
			return types.ErrKeyExists
		}
	}

	// We are ready now to start putting/updating the value in the key.
	// Put value in primary storage first. In primary storage we put
	// the key, not the indexKey. The storage knows how to manage the key
	// under the hood while the index is primary storage-agnostic.
	fileOffset, err := s.index.Primary.Put(key, value)
	if err != nil {
		return err
	}

	// If the key being set is not found, or the stored key is not equal
	// (even if same prefix is shared @index), we put the key without updates
	if !cmpKey {
		if err = s.index.Put(indexKey, fileOffset); err != nil {
			return err
		}
	} else {
		// If the key exists and the one stored is the one we are trying
		// to put this is an update.
		// if found && bytes.Compare(key, storedKey) == 0 {
		if err = s.index.Update(indexKey, fileOffset); err != nil {
			return err
		}
		// Add outdated data in primary storage to freelist
		if err = s.freelist.Put(prevOffset); err != nil {
			return err
		}
	}

	s.flushTick()

	return nil
}

func (s *Store) Remove(key []byte) (bool, error) {
	err := s.Err()
	if err != nil {
		return false, err
	}

	// Get the key in primary storage
	indexKey, err := s.index.Primary.IndexKey(key)
	if err != nil {
		return false, err
	}
	// See if the key already exists and get offset
	offset, found, err := s.index.Get(indexKey)
	if err != nil {
		return false, err
	}

	// If not found it means there's nothing to remove.
	// Return false with no error
	if !found {
		return false, nil
	}

	// If found, get the key and value stored in primary to see if it is the
	// same (index only stores prefixes).
	storedKey, _, err := s.getPrimaryKeyData(offset, indexKey)
	if err != nil {
		return false, err
	}
	if storedKey == nil {
		// The indexKey does not exist and there is nothing to remove.
		return false, nil
	}

	removed, err := s.index.Remove(storedKey)
	if err != nil {
		return false, err
	}
	if removed {
		// Mark slot in freelist
		err = s.freelist.Put(offset)
		if err != nil {
			return false, err
		}
	}

	s.flushTick()
	return removed, nil
}

func (s *Store) getPrimaryKeyData(blk types.Block, indexKey []byte) ([]byte, []byte, error) {
	// Get the key and value stored in primary to see if it is the same (index
	// only stores prefixes).
	storedKey, storedValue, err := s.index.Primary.Get(blk)
	if err != nil {
		// Log the error reading the primary, since no error is returned if the
		// bad index is successfully deleted.
		log.Errorw("Error reading primary, removing bad index", "err", err)
		// The offset returned from the index is not usable, so delete the
		// index entry regardless of which key in indexes. It is not safe to
		// put this offset onto the free list, since it may be an invalid
		// location in the primary.
		if _, err = s.index.Remove(indexKey); err != nil {
			return nil, nil, fmt.Errorf("error removing unusable key: %w", err)
		}
		s.flushTick()
		return nil, nil, nil
	}

	// Compare the indexKey with the storedKey read from the primary. This
	// determines if the indexKey was stored or if some other key with the same
	// prefix was stored.
	storedKey, err = s.index.Primary.IndexKey(storedKey)
	if err != nil {
		return nil, nil, err
	}

	// The index stores only prefixes, hence check if the given key fully
	// matches the key that is stored in the primary storage before returning
	// the actual value.
	if !bytes.Equal(indexKey, storedKey) {
		return nil, nil, nil
	}

	return storedKey, storedValue, nil
}

func (s *Store) flushTick() {
	now := time.Now()
	s.rateLk.Lock()
	if s.rate == 0 {
		s.rateLk.Unlock()
		return
	}
	elapsed := now.Sub(s.lastFlush)
	// TODO: move this Outstanding calculation into Pool?
	work := s.index.OutstandingWork() + s.index.Primary.OutstandingWork() + s.freelist.OutstandingWork()
	rate := math.Ceil(float64(work) / elapsed.Seconds())
	flushNow := rate > s.rate && work > s.burstRate
	s.rateLk.Unlock()

	if flushNow {
		select {
		case s.flushNow <- struct{}{}:
		default:
			// Already signaled, but flush not yet started.  No need to wait to
			// signal again since the existing unread signal guarantees the
			// change will be written.
		}
	}
}

func (s *Store) commit() (types.Work, error) {
	primaryWork, err := s.index.Primary.Flush()
	if err != nil {
		return 0, err
	}
	indexWork, err := s.index.Flush()
	if err != nil {
		return 0, err
	}
	flWork, err := s.freelist.Flush()
	if err != nil {
		return 0, err
	}
	// finalize disk writes
	if err = s.index.Primary.Sync(); err != nil {
		return 0, err
	}
	if err = s.index.Sync(); err != nil {
		return 0, err
	}
	if err = s.freelist.Sync(); err != nil {
		return 0, err
	}

	// Tell the index garbage collectors there may be some garbage.
	if indexWork != 0 && s.indexGC != nil {
		s.indexGC.SignalUpdate()
	}
	if flWork != 0 && s.primaryGC != nil {
		s.primaryGC.SignalUpdate()
	}

	return primaryWork + indexWork + flWork, nil
}

func (s *Store) outstandingWork() bool {
	return s.index.OutstandingWork()+s.index.Primary.OutstandingWork() > 0
}

// Flush writes outstanding work and buffered data to the primary, index, and
// freelist files. It then syncs these files to permanent storage.
func (s *Store) Flush() error {
	lastFlush := time.Now()

	s.rateLk.Lock()
	s.lastFlush = lastFlush
	s.rateLk.Unlock()

	if !s.outstandingWork() {
		return nil
	}

	work, err := s.commit()
	if err != nil {
		return err
	}

	now := time.Now()
	elapsed := now.Sub(lastFlush)
	rate := math.Ceil(float64(work) / elapsed.Seconds())

	if work > types.Work(s.burstRate) {
		s.rateLk.Lock()
		s.rate = rate
		s.rateLk.Unlock()
	}

	return nil
}

func (s *Store) Has(key []byte) (bool, error) {
	err := s.Err()
	if err != nil {
		return false, err
	}
	indexKey, err := s.index.Primary.IndexKey(key)
	if err != nil {
		return false, err
	}
	blk, found, err := s.index.Get(indexKey)
	if !found || err != nil {
		return false, err
	}

	// The index stores only prefixes, hence check if the given key fully matches the
	// key that is stored in the primary storage before returning the actual value.
	// TODO: avoid second lookup
	primaryIndexKey, err := s.index.Primary.GetIndexKey(blk)
	if err != nil {
		return false, err
	}

	return bytes.Equal(indexKey, primaryIndexKey), nil
}

func (s *Store) GetSize(key []byte) (types.Size, bool, error) {
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

	// The index stores only prefixes, hence check if the given key fully matches the
	// key that is stored in the primary storage before returning the actual value.
	// TODO: avoid second lookup
	primaryIndexKey, err := s.index.Primary.GetIndexKey(blk)
	if err != nil {
		return 0, false, err
	}

	if !bytes.Equal(indexKey, primaryIndexKey) {
		return 0, false, nil
	}
	return blk.Size - types.Size(len(key)), true, nil
}

// IndexStorageSize returns the storage used by the index files.
func (s *Store) IndexStorageSize() (int64, error) {
	return s.index.StorageSize()
}

// PrimaryStorageSize returns the storage used by the primary storage files.
func (s *Store) PrimaryStorageSize() (int64, error) {
	return s.index.Primary.StorageSize()
}

// FreelistStorageSize returns the storage used by the freelist files.
func (s *Store) FreelistStorageSize() (int64, error) {
	return s.freelist.StorageSize()
}

// StorageSize returns the storage used by the index, primary, and freelist files.
func (s *Store) StorageSize() (int64, error) {
	isize, err := s.index.StorageSize()
	if err != nil {
		return 0, err
	}
	psize, err := s.index.Primary.StorageSize()
	if err != nil {
		return 0, err
	}
	fsize, err := s.freelist.StorageSize()
	if err != nil {
		return 0, err
	}
	return isize + psize + fsize, nil
}
