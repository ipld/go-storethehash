package store_test

import (
	"testing"

	store "github.com/hannahhoward/go-storethehash/internal"
	"github.com/stretchr/testify/require"
)

func TestFirstNonCommonByte(t *testing.T) {
	require.Equal(t, store.FirstNonCommonByte([]byte{0}, []byte{1}), 0)
	require.Equal(t, store.FirstNonCommonByte([]byte{0}, []byte{0}), 1)
	require.Equal(t, store.FirstNonCommonByte([]byte{0, 1, 2, 3}, []byte{0}), 1)
	require.Equal(t, store.FirstNonCommonByte([]byte{0}, []byte{0, 1, 2, 3}), 1)
	require.Equal(t, store.FirstNonCommonByte([]byte{0, 1, 2}, []byte{0, 1, 2, 3}), 3)
	require.Equal(t, store.FirstNonCommonByte([]byte{0, 1, 2, 3}, []byte{0, 1, 2}), 3)
	require.Equal(t, store.FirstNonCommonByte([]byte{3, 2, 1, 0}, []byte{0, 1, 2}), 0)
	require.Equal(t, store.FirstNonCommonByte([]byte{0, 1, 1, 0}, []byte{0, 1, 2}), 2)
	require.Equal(t,
		store.FirstNonCommonByte([]byte{180, 9, 113, 0}, []byte{180, 0, 113, 0}),
		1,
	)
}
