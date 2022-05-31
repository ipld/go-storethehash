package store

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-storethehash/store/freelist"
	"github.com/ipld/go-storethehash/store/index"
	"github.com/ipld/go-storethehash/store/primary"
	"github.com/ipld/go-storethehash/store/types"
)

const DefaultBurstRate = 4 * 1024 * 1024

var log = logging.Logger("storethehash")

type Store struct {
	index    *index.Index
	freelist *freelist.FreeList

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
}

func OpenStore(path string, primary primary.PrimaryStorage, indexSizeBits uint8, syncInterval time.Duration, burstRate types.Work, gcInterval time.Duration) (*Store, error) {
	index, err := index.OpenIndex(path, primary, indexSizeBits, gcInterval)
	if err != nil {
		return nil, err
	}
	freelist, err := freelist.OpenFreeList(path + ".free")
	if err != nil {
		return nil, err
	}
	store := &Store{
		lastFlush:    time.Now(),
		index:        index,
		freelist:     freelist,
		open:         true,
		running:      false,
		syncInterval: syncInterval,
		burstRate:    burstRate,
		closed:       make(chan struct{}),
		closing:      make(chan struct{}),
		flushNow:     make(chan struct{}, 1),
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

func (s *Store) run() {
	defer close(s.closed)
	d := time.NewTicker(s.syncInterval)

	for {
		select {
		case <-s.flushNow:
			s.Flush()
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

	var err error
	if s.outstandingWork() {
		if _, err = s.commit(); err != nil {
			s.setErr(err)
		}
	}

	if err = s.Err(); err != nil {
		return err
	}

	if err = s.index.Close(); err != nil {
		return err
	}

	if err = s.index.Primary.Close(); err != nil {
		return err
	}

	if err = s.freelist.Close(); err != nil {
		return err
	}

	return nil
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
		// bad index is sucessfully deleted.
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

	// Compare to the indexKey with the storedKey read from the primary. This
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
	return primaryWork + indexWork + flWork, nil
}

func (s *Store) outstandingWork() bool {
	return s.index.OutstandingWork()+s.index.Primary.OutstandingWork() > 0
}

func (s *Store) Flush() {
	lastFlush := time.Now()

	s.rateLk.Lock()
	s.lastFlush = lastFlush
	s.rateLk.Unlock()

	if !s.outstandingWork() {
		return
	}

	work, err := s.commit()
	if err != nil {
		s.setErr(err)
		return
	}

	now := time.Now()
	elapsed := now.Sub(lastFlush)
	rate := math.Ceil(float64(work) / elapsed.Seconds())

	if work > types.Work(s.burstRate) {
		s.rateLk.Lock()
		s.rate = rate
		s.rateLk.Unlock()
	}
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

// IndexStorageSize returns the storage used by the index files.  This includs
// the `.info` file and the `.free` file and does not include primary storage.
func (s *Store) IndexStorageSize() (int64, error) {
	return s.index.StorageSize()
}
