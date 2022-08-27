package store

import (
	"time"

	"github.com/ipld/go-storethehash/store/types"
)

const (
	defaultIndexSizeBits = uint8(24)
	defaultIndexFileSize = uint32(1024 * 1024 * 1024)
	defaultBurstRate     = 4 * 1024 * 1024
	defaultSyncInterval  = time.Second
	defaultGCInterval    = 30 * time.Minute
	defaultGCTimeLimit   = 5 * time.Minute
	defaultGCScanFree    = true
)

type config struct {
	indexSizeBits uint8
	indexFileSize uint32
	syncInterval  time.Duration
	burstRate     types.Work
	gcInterval    time.Duration
	gcTimeLimit   time.Duration
	gcScanFree    bool
}

type Option func(*config)

// apply applies the given options to this config.
func (c *config) apply(opts []Option) {
	for _, opt := range opts {
		opt(c)
	}
}

// IndexBitSize is the number of bits in an index prefix.
func IndexBitSize(indexBitSize uint8) Option {
	return func(c *config) {
		c.indexSizeBits = indexBitSize
	}
}

// IndexFileSize is the maximum offset an index record can have within an
// individual index file, before the record must be stored in another file.
func IndexFileSize(indexFileSize uint32) Option {
	return func(c *config) {
		c.indexFileSize = indexFileSize
	}
}

// SyncInterval determines how frequently changes are flushed to disk.
func SyncInterval(syncInterval time.Duration) Option {
	return func(c *config) {
		c.syncInterval = syncInterval
	}
}

// BurstRate specifies how much unwritten data can accumulate before causing
// data to be flushed to disk.
func BurstRate(burstRate uint64) Option {
	return func(c *config) {
		c.burstRate = types.Work(burstRate)
	}
}

// GCInterval is the amount of time to wait between GC cycles.
func GCInterval(gcInterval time.Duration) Option {
	return func(c *config) {
		c.gcInterval = gcInterval
	}
}

// GCTimeLimit is the maximum amount of time that a GC cycle may run.
func GCTimeLimit(gcTimeLimit time.Duration) Option {
	return func(c *config) {
		c.gcTimeLimit = gcTimeLimit
	}
}

// GCFreeScan enables a fast scan of files to find any that are not reverenced
// by any index buckets.
func GCScanFree(scanFree bool) Option {
	return func(c *config) {
		c.gcScanFree = scanFree
	}
}
