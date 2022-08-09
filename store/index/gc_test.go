package index

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipld/go-storethehash/store/freelist"
	mhprimary "github.com/ipld/go-storethehash/store/primary/multihash"
	"github.com/stretchr/testify/require"
)

func TestGC(t *testing.T) {
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, filepath.Base(testIndexPath))

	// Copy test file.
	err := copyFile(testIndexPath, indexPath)
	require.NoError(t, err)

	freeList, err := freelist.Open(testIndexPath + ".free")
	require.NoError(t, err)

	dataPath := filepath.Join(tempDir, "storethehash.data")
	primary, err := mhprimary.Open(dataPath, 0, freeList)
	require.NoError(t, err)
	defer primary.Close()

	idx, err := Open(context.Background(), indexPath, primary, 24, 1024)
	require.NoError(t, err)
	defer idx.Close()
	gc := &indexGC{
		index: idx,
	}

	// All index files in use, so gc should not remove any files.
	count, err := gc.Cycle(context.Background())
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
	idx, err = Open(context.Background(), indexPath, primary, 24, 1024)
	require.NoError(t, err)
	defer idx.Close()
	gc = &indexGC{
		index: idx,
	}

	require.False(t, gc.checkpoint)

	// GC should now remove the first 2 files only.
	count, err = gc.Cycle(context.Background())
	require.NoError(t, err)
	require.Equal(t, count, 2)

	require.True(t, gc.checkpoint)

	// Another GC should not remove files.
	count, err = gc.Cycle(context.Background())
	require.NoError(t, err)
	require.Equal(t, count, 0)

	// Check that first file is .2 and last file is .24
	header, err := readHeader(idx.headerPath)
	require.NoError(t, err)
	require.Equal(t, header.FirstFile, uint32(2))
	require.Equal(t, idx.fileNum, uint32(24))
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
