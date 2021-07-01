package store

import (
	"bytes"
	"math"
	"sync"
	"time"

	"github.com/hannahhoward/go-storethehash/store/freelist"
	"github.com/hannahhoward/go-storethehash/store/index"
	"github.com/hannahhoward/go-storethehash/store/primary"
	"github.com/hannahhoward/go-storethehash/store/types"
)

const DefaultBurstRate = 4 * 1024 * 1024

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

	closing      chan struct{}
	syncInterval time.Duration
}

func OpenStore(path string, primary primary.PrimaryStorage, indexSizeBits uint8, syncInterval time.Duration, burstRate types.Work) (*Store, error) {
	index, err := index.OpenIndex(path, primary, indexSizeBits)
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
		closing:      make(chan struct{}),
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
	d := time.NewTicker(s.syncInterval)

	for {
		select {

		case <-s.closing:
			d.Stop()
			select {
			case <-d.C:
			default:
			}
			return

		case <-d.C:
			s.Flush()
		}
	}
}

func (s *Store) Close() error {
	s.stateLk.Lock()
	open := s.open
	s.open = false
	s.stateLk.Unlock()

	if !open {
		return nil
	}

	s.stateLk.Lock()
	running := s.running
	s.running = false
	s.stateLk.Unlock()

	if running {
		close(s.closing)
	}

	if s.outstandingWork() {
		if _, err := s.commit(); err != nil {
			s.setErr(err)
		}
	}

	if err := s.Err(); err != nil {
		return err
	}

	if err := s.index.Close(); err != nil {
		return err
	}

	if err := s.index.Primary.Close(); err != nil {
		return err
	}

	return nil
}

func (s *Store) Get(key []byte) ([]byte, bool, error) {
	if err := s.Err(); err != nil {
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
	primaryKey, value, err := s.index.Primary.Get(fileOffset)
	if err != nil {
		return nil, false, err
	}

	// The index stores only prefixes, hence check if the given key fully matches the
	// key that is stored in the primary storage before returning the actual value.
	if bytes.Compare(key, primaryKey) != 0 {
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
	if err := s.Err(); err != nil {
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
	// If found get the key and value stored in primary to see if it is the same
	// (index only stores prefixes)
	var storedKey []byte
	var storedVal []byte
	if found {
		storedKey, storedVal, err = s.index.Primary.Get(prevOffset)
		if err != nil {
			return err
		}
	}

	cmpKey := bytes.Equal(key, storedKey)

	if cmpKey && bytes.Equal(value, storedVal) {
		// We are trying to put the same value in an existing key,
		// we can directly return
		// NOTE: How many times is going to happen this. Can we save ourselves
		// this step? We can't in the case of the blockstore and that is why we
		// return an ErrKeyExists.
		return types.ErrKeyExists
	}

	// We are ready now to start putting/updating the value in the key.
	// Put value in primary storage first
	fileOffset, err := s.index.Primary.Put(key, value)
	if err != nil {
		return err
	}

	// If the key being set is not found, or the stored key is not equal
	// (even if same prefix is shared @index), we put the key without updates
	if !found || !cmpKey {
		if err := s.index.Put(indexKey, fileOffset); err != nil {
			return err
		}
	} else {
		// If the key exists and the one stored is the one we are trying
		// to put this is an update.
		// if found && bytes.Compare(key, storedKey) == 0 {
		if err := s.index.Update(indexKey, fileOffset); err != nil {
			return err
		}
	}

	// Add outdated data in primary storage to freelist
	err = s.freelist.Put(prevOffset)
	if err != nil {
		return err
	}

	now := time.Now()
	s.rateLk.Lock()
	elapsed := now.Sub(s.lastFlush)
	// TODO: move this Outstanding calculation into Pool?
	work := s.index.OutstandingWork() + s.index.Primary.OutstandingWork() + s.freelist.OutstandingWork()
	rate := math.Ceil(float64(work) / elapsed.Seconds())
	sleep := s.rate > 0 && rate > s.rate && work > s.burstRate
	s.rateLk.Unlock()

	if sleep {
		time.Sleep(25 * time.Millisecond)
	}

	return nil
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
	// finalize disk writes
	if err := s.index.Primary.Sync(); err != nil {
		return 0, err
	}
	if err := s.index.Sync(); err != nil {
		return 0, err
	}
	return primaryWork + indexWork, nil
}

func (s *Store) outstandingWork() bool {
	return s.index.OutstandingWork()+s.index.Primary.OutstandingWork() > 0
}
func (s *Store) Flush() {

	s.rateLk.Lock()
	s.lastFlush = time.Now()
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
	s.rateLk.Lock()
	elapsed := now.Sub(s.lastFlush)
	rate := math.Ceil(float64(work) / elapsed.Seconds())
	if work > types.Work(s.burstRate) {
		s.rate = rate
	}
	s.rateLk.Unlock()
}

func (s *Store) Has(key []byte) (bool, error) {
	if err := s.Err(); err != nil {
		return false, err
	}
	indexKey, err := s.index.Primary.IndexKey(key)
	if err != nil {
		return false, err
	}
	blk, found, err := s.index.Get(indexKey)
	if !found {
		return false, nil
	}

	// The index stores only prefixes, hence check if the given key fully matches the
	// key that is stored in the primary storage before returning the actual value.
	// TODO: avoid second lookup
	primaryIndexKey, err := s.index.Primary.GetIndexKey(blk)
	if err != nil {
		return false, err
	}

	return bytes.Compare(indexKey, primaryIndexKey) == 0, nil
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

	if bytes.Compare(indexKey, primaryIndexKey) != 0 {
		return 0, false, nil
	}
	return blk.Size - types.Size(len(key)), true, nil
}
