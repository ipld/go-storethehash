package store_test

import (
	"testing"

	store "github.com/hannahhoward/go-storethehash/internal"
	"github.com/stretchr/testify/require"
)

func TestNewBuckets(t *testing.T) {
	var bucketBits uint8 = 24
	buckets, err := store.NewBuckets(bucketBits)
	require.NoError(t, err)
	require.Equal(t, len(buckets), 1<<bucketBits)
}

func TestNewBucketsError(t *testing.T) {
	var bucketBits uint8 = 64
	_, err := store.NewBuckets(bucketBits)
	require.EqualError(t, err, store.ErrIndexTooLarge.Error())
}

func TestPut(t *testing.T) {
	var bucketBits uint8 = 3
	buckets, err := store.NewBuckets(bucketBits)
	require.NoError(t, err)
	err = buckets.Put(3, 54321)
	require.NoError(t, err)
	value, err := buckets.Get(3)
	require.NoError(t, err)
	require.Equal(t, store.Position(54321), value)
}

func TestPutError(t *testing.T) {
	var bucketBits uint8 = 3
	buckets, err := store.NewBuckets(bucketBits)
	require.NoError(t, err)
	err = buckets.Put(333, 54321)
	require.EqualError(t, err, store.ErrOutOfBounds.Error())
}

func TestGet(t *testing.T) {
	var bucketBits uint8 = 3
	buckets, err := store.NewBuckets(bucketBits)
	value, err := buckets.Get(3)
	require.NoError(t, err)
	require.Equal(t, store.Position(0), value)
	err = buckets.Put(3, 54321)
	require.NoError(t, err)
	value, err = buckets.Get(3)
	require.NoError(t, err)
	require.Equal(t, store.Position(54321), value)
}

func TestGetErrir(t *testing.T) {
	var bucketBits uint8 = 3
	buckets, err := store.NewBuckets(bucketBits)
	require.NoError(t, err)
	_, err = buckets.Get(333)
	require.EqualError(t, err, store.ErrOutOfBounds.Error())
}
