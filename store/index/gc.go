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

type indexGC struct {
	index     *Index
	updateSig chan struct{}
	done      chan struct{}

	// Checkpoint is the last bucket index still in use by first file.
	checkpoint  bool
	bucketIndex BucketIndex
}

func NewGC(index *Index, gcInterval time.Duration) *indexGC {
	igc := &indexGC{
		index:     index,
		updateSig: make(chan struct{}, 1),
		done:      make(chan struct{}),
	}

	go igc.run(gcInterval)

	return igc
}

func (gc *indexGC) SignalUpdate() {
	// Send signal to tell GC there are updates.
	select {
	case gc.updateSig <- struct{}{}:
	default:
	}
}

func (gc *indexGC) Close() {
	close(gc.updateSig)
	<-gc.done
}

// run is a goroutine that runs periodically to search for and remove stale
// index files. It runs every gcInterval, if there have been any index updates.
func (gc *indexGC) run(gcInterval time.Duration) {
	defer close(gc.done)

	var gcDone chan struct{}
	hasUpdate := true

	// Run 1st GC 1 minute after startup.
	t := time.NewTimer(time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		select {
		case _, ok := <-gc.updateSig:
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
				fileCount, err := gc.cycle(ctx)
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

// cycle searches for and removes stale index files. Returns the number of
// unused index files that were removed.
func (gc *indexGC) cycle(ctx context.Context) (int, error) {
	header, err := readHeader(gc.index.headerPath)
	if err != nil {
		return 0, err
	}
	fileNum := header.FirstFile

	// Before scanning the index files, check if the first index file is still
	// in use by the bucket index last seen using it.
	if gc.checkpoint {
		inUse, err := gc.bucketInFile(gc.bucketIndex, fileNum)
		if err != nil {
			gc.checkpoint = false
			return 0, err
		}
		if inUse {
			// First index file still used to store info.
			return 0, nil
		}
		// Checkpoint bucket checked.
		gc.checkpoint = false
	}

	var count int
	for {
		if fileNum == gc.index.fileNum {
			// Do not try to GC the current index file.
			break
		}
		indexPath := indexFileName(gc.index.basePath, fileNum)
		stale, err := gc.collectIndexFile(ctx, fileNum, indexPath)
		if err != nil {
			return 0, err
		}
		if !stale {
			break
		}
		fileNum++
		header.FirstFile = fileNum
		err = writeHeader(gc.index.headerPath, header)
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

// collectIndexFile scans a single index file, checking if any of the entries
// are in buckets that use this file. If no buckets are using this file for any
// of the entries, then there are no more active entries and the file can be
// deleted.
func (gc *indexGC) collectIndexFile(ctx context.Context, fileNum uint32, indexPath string) (bool, error) {
	file, err := openFileForScan(indexPath)
	if err != nil {
		return false, err
	}
	defer file.Close()

	inBuf := bufio.NewReader(file)
	sizeBuffer := make([]byte, sizePrefixSize)
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
		inUse, err := gc.bucketInFile(bucketPrefix, fileNum)
		if err != nil {
			return false, err
		}
		if inUse {
			// This index file is in use by the bucket, so no GC for this file.
			gc.checkpoint = true
			gc.bucketIndex = bucketPrefix
			return false, nil
		}
	}

	return true, nil
}

func (gc *indexGC) bucketInFile(bucketPrefix BucketIndex, fileNum uint32) (bool, error) {
	gc.index.bucketLk.Lock()
	bucketPos, err := gc.index.buckets.Get(bucketPrefix)
	gc.index.bucketLk.Unlock()
	if err != nil {
		return false, err
	}
	ok, fnum := bucketPosToFileNum(bucketPos, gc.index.maxFileSize)
	if ok && fnum == fileNum {
		return true, nil
	}
	return false, nil
}
