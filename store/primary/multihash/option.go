package mhprimary

import (
	"time"
)

const (
	defaultPrimaryFileSize = uint32(1024 * 1024 * 1024)
	defaultGCInterval      = 45 * time.Minute
	defaultGCTimeLimit     = 5 * time.Minute
)

type config struct {
	primaryFileSize uint32
	gcInterval      time.Duration
	gcTimeLimit     time.Duration
}

type Option func(*config)

// apply applies the given options to this config.
func (c *config) apply(opts []Option) {
	for _, opt := range opts {
		opt(c)
	}
}

// PrimaryFileSize is the maximum offset a primary record can have within an
// individual primary file, before the record must be stored in another file.
func PrimaryFileSize(fileSize uint32) Option {
	return func(c *config) {
		c.primaryFileSize = fileSize
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
