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
)

type config struct {
	indexSizeBits uint8
	indexFileSize uint32
	syncInterval  time.Duration
	burstRate     types.Work
	gcInterval    time.Duration
	gcTimeLimit   time.Duration
}

type Option func(*config)

func IndexBitSize(indexBitSize uint8) Option {
	return func(c *config) {
		c.indexSizeBits = indexBitSize
	}
}

func IndexFileSize(indexFileSize uint32) Option {
	return func(c *config) {
		c.indexFileSize = indexFileSize
	}
}

func SyncInterval(syncInterval time.Duration) Option {
	return func(c *config) {
		c.syncInterval = syncInterval
	}
}

func BurstRate(burstRate uint64) Option {
	return func(c *config) {
		c.burstRate = types.Work(burstRate)
	}
}

func GCInterval(gcInterval time.Duration) Option {
	return func(c *config) {
		c.gcInterval = gcInterval
	}
}

func GCTimeLimit(gcTimeLimit time.Duration) Option {
	return func(c *config) {
		c.gcTimeLimit = gcTimeLimit
	}
}
