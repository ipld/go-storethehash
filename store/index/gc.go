package index

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("storethehash/index")

// gcInterval is how often to run garbage collection.
const gcInterval = 30 * time.Minute

// garbageCollector is a goroutine that runs periodically to search for and
// remove stale index files.
func (i *Index) garbageCollector() {
	defer close(i.gcDone)

	var gcDone chan struct{}
	hasUpdate := true

	// Run 1st GC 1 minute adter startup.
	t := time.NewTimer(time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		select {
		case _, ok := <-i.updateSig:
			if !ok {
				// Channel closed; shutting down.
				cancel()
				if gcDone != nil {
					<-gcDone
				}
				return
			}
			hasUpdate = true
		case <-t.C:
			if !hasUpdate {
				// Nothing new, keep waiting.
				t.Reset(gcInterval)
				continue
			}

			gcDone = make(chan struct{})
			go func() {
				defer close(gcDone)
				log.Infow("GC started")
				fileCount, err := i.gc(ctx)
				if err != nil {
					log.Errorw("GC failed", "err", err)
					return
				}
				if fileCount == 0 {
					log.Info("GC finished, no index files to remove")
				} else {
					log.Infow("GC finished, removed index files", "fileCount", fileCount)
				}
			}()
		case <-gcDone:
			gcDone = nil
			hasUpdate = false
			// Finished the checkpoint update, cleanup and reset timer.
			t.Reset(gcInterval)
		}
	}
}

// gc searches for and removes stale index files. Returns the number of files removed.
func (i *Index) gc(ctx context.Context) (int, error) {
	header, err := readHeader(i.headerPath)
	if err != nil {
		return 0, err
	}
	fileNum := header.FirstFile

	var count int
	for {
		if fileNum == i.fileNum {
			// Do not try to GC the current index file.
			break
		}
		indexPath := indexFileName(i.basePath, fileNum)
		stale, err := i.gcIndexFile(ctx, fileNum, indexPath)
		if err != nil {
			return 0, err
		}
		if !stale {
			break
		}
		fileNum++
		header.FirstFile = fileNum
		err = writeHeader(i.headerPath, header)
		if err != nil {
			return 0, err
		}
		// If updating index info ok, then remove stale index file.
		err = os.Remove(indexPath)
		if err != nil {
			return 0, err
		}
		count++
	}

	return count, nil
}

// gcIndexFile scans a single index file, checking if any of the entries are in
// buckets that use this file.  If no buckets are using this file for any of
// the entries, then there are no more active entries and the file can be
// deleted.
func (i *Index) gcIndexFile(ctx context.Context, fileNum uint32, indexPath string) (bool, error) {
	file, err := openFileForScan(indexPath)
	if err != nil {
		return false, err
	}
	defer file.Close()

	inBuf := bufio.NewReader(file)
	sizeBuffer := make([]byte, SizePrefixSize)
	scratch := make([]byte, 256)
	for {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if _, err = io.ReadFull(inBuf, sizeBuffer); err != nil {
			if err == io.EOF {
				// Finished reading entire index.
				break
			}
			return false, err
		}
		size := binary.LittleEndian.Uint32(sizeBuffer)
		if int(size) > len(scratch) {
			scratch = make([]byte, size)
		}
		data := scratch[:size]
		if _, err = io.ReadFull(inBuf, data); err != nil {
			if err == io.EOF {
				// The data has not been written yet, or the file is corrupt.
				// Take the data we are able to use and move on.
				break
			}
			return false, fmt.Errorf("error reading data from index: %w", err)
		}

		bucketPrefix := BucketIndex(binary.LittleEndian.Uint32(data))
		i.bucketLk.Lock()
		bucketPos, err := i.buckets.Get(bucketPrefix)
		i.bucketLk.Unlock()
		if err != nil {
			return false, err
		}
		ok, fnum := bucketPosToFileNum(bucketPos)
		if ok && fnum == fileNum {
			// This index file is in use by the bucket, so no GC for this file.
			return false, nil
		}
	}

	return true, nil
}
