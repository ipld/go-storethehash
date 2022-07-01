package index

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	mhprimary "github.com/ipld/go-storethehash/store/primary/multihash"
	"github.com/stretchr/testify/require"
)

func TestGC(t *testing.T) {
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, filepath.Base(testIndexPath))

	// Copy test file.
	err := copyFile(testIndexPath, indexPath)
	require.NoError(t, err)

	dataPath := filepath.Join(tempDir, "storethehash.data")
	primary, err := mhprimary.OpenMultihashPrimary(dataPath)
	require.NoError(t, err)

	idx, err := OpenIndex(indexPath, primary, 24, 1024, 0)
	require.NoError(t, err)
	defer idx.Close()

	// All index files in use, so gc should not remove any files.
	count, err := idx.gc(context.Background())
	require.NoError(t, err)
	require.Equal(t, count, 0)

	require.NoError(t, idx.Close())

	// Copy the first two files as the last two files so that the indexes in
	// them are associated with the last files.
	err = copyFile(indexPath+".0", fmt.Sprintf("%s.%d", indexPath, idx.fileNum+1))
	require.NoError(t, err)
	err = copyFile(indexPath+".1", fmt.Sprintf("%s.%d", indexPath, idx.fileNum+2))
	require.NoError(t, err)

	// Open the index with the duplicated files.
	idx, err = OpenIndex(indexPath, primary, 24, 1024, 0)
	require.NoError(t, err)
	defer idx.Close()

	require.False(t, idx.gcCheckpoint)

	// GC should now remove the first 2 files only.
	count, err = idx.gc(context.Background())
	require.NoError(t, err)
	require.Equal(t, count, 2)

	require.True(t, idx.gcCheckpoint)

	// Another GC should not remove files.
	count, err = idx.gc(context.Background())
	require.NoError(t, err)
	require.Equal(t, count, 0)

	// Check that first file is .2 and last file is .24
	header, err := readHeader(idx.headerPath)
	require.NoError(t, err)
	require.Equal(t, header.FirstFile, 2)
	require.Equal(t, idx.fileNum, 24)
}

func copyFile(src, dst string) error {
	fin, err := os.Open(src)
	if err != nil {
		return err
	}
	defer fin.Close()

	fout, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer fout.Close()

	_, err = io.Copy(fout, fin)
	return err
}
