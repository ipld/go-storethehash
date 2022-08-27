package index

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-storethehash/store/types"
)

var log = logging.Logger("storethehash/index")

// garbageCollector is a goroutine that runs periodically to search for and
// remove stale index files. It runs every gcInterval, if there have been any
// index updates.
func (index *Index) garbageCollector(interval, timeLimit time.Duration, scanUnused bool) {
	defer close(index.gcDone)

	var gcDone chan struct{}
	hasUpdate := true

	// Run 1st GC 1 minute after startup.
	t := time.NewTimer(time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		select {
		case _, ok := <-index.updateSig:
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
				t.Reset(interval)
				continue
			}

			gcDone = make(chan struct{})
			go func() {
				defer close(gcDone)
				gcCtx := ctx
				if timeLimit != 0 {
					var cancel context.CancelFunc
					gcCtx, cancel = context.WithTimeout(ctx, timeLimit)
					defer cancel()
				}

				log.Infow("GC started")
				fileCount, err := index.gc(gcCtx, scanUnused)
				if err != nil {
					switch err {
					case context.DeadlineExceeded:
						log.Infow("GC stopped at time limit", "limit", timeLimit)
					case context.Canceled:
						log.Info("GC canceled")
					default:
						log.Errorw("GC failed", "err", err)
					}
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
			t.Reset(interval)
		}
	}
}

// gc searches for and removes stale index files. Returns the number of unused
// index files that were removed.
func (index *Index) gc(ctx context.Context, scanFree bool) (int, error) {
	var count int
	var err error

	if scanFree {
		count, err = index.freeFreeFiles(ctx)
		if err != nil {
			return 0, err
		}
	}

	header, err := readHeader(index.headerPath)
	if err != nil {
		return 0, err
	}

	index.flushLock.Lock()
	lastFileNum := index.fileNum
	index.flushLock.Unlock()

	for fileNum := header.FirstFile; fileNum < lastFileNum; fileNum++ {
		indexPath := indexFileName(index.basePath, fileNum)

		stale, err := index.gcIndexFile(ctx, fileNum, indexPath)
		if err != nil {
			return 0, err
		}
		if !stale {
			continue
		}
		if header.FirstFile == fileNum {
			header.FirstFile++
			err = writeHeader(index.headerPath, header)
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
	}
	return count, nil
}

func (index *Index) freeFreeFiles(ctx context.Context) (int, error) {
	busySet := make(map[uint32]struct{})
	maxFileSize := index.maxFileSize

	var i int
	end := 1 << index.sizeBits
	tmpBuckets := make([]types.Position, 4096)
	for i < end {
		index.bucketLk.RLock()
		i += copy(tmpBuckets, index.buckets[i:])
		index.bucketLk.RUnlock()
		for _, offset := range tmpBuckets {
			ok, fileNum := bucketPosToFileNum(offset, maxFileSize)
			if ok {
				busySet[fileNum] = struct{}{}
			}
		}
	}

	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	header, err := readHeader(index.headerPath)
	if err != nil {
		return 0, fmt.Errorf("cannot read index header: %w", err)
	}

	var rmCount int
	basePath := index.basePath

	index.flushLock.Lock()
	lastFileNum := index.fileNum
	index.flushLock.Unlock()

	for fileNum := header.FirstFile; fileNum < lastFileNum; fileNum++ {
		if _, busy := busySet[fileNum]; busy {
			continue
		}
		indexPath := indexFileName(basePath, fileNum)

		if fileNum == header.FirstFile {
			header.FirstFile++
			err = writeHeader(index.headerPath, header)
			if err != nil {
				return 0, fmt.Errorf("cannot write index header: %w", err)
			}

			err = os.Remove(indexPath)
			if err != nil {
				log.Errorw("Error removing index file", "err", err, "file", indexPath)
				continue
			}
			log.Infow("Removed unused index file", "file", indexPath)
			rmCount++
			continue
		}

		fi, err := os.Stat(indexPath)
		if err != nil {
			log.Errorw("Cannot stat index file", "err", err, "file", indexPath)
			continue
		}
		if fi.Size() == 0 {
			continue
		}

		err = os.Truncate(indexPath, 0)
		if err != nil {
			log.Errorw("Error truncating index file", "err", err, "file", indexPath)
			continue
		}
		log.Infow("Emptied unused index file", "file", indexPath)

		if ctx.Err() != nil {
			break
		}
	}

	return rmCount, ctx.Err()
}

// gcIndexFile scans a single index file, checking if any of the entries are in
// buckets that use this file. If no buckets are using this file for any of the
// entries, then there are no more active entries and the file can be deleted.
func (index *Index) gcIndexFile(ctx context.Context, fileNum uint32, indexPath string) (bool, error) {
	fi, err := os.Stat(indexPath)
	if err != nil {
		return false, fmt.Errorf("cannot stat index file: %w", err)
	}
	if fi.Size() == 0 {
		// File is empty, so OK to delete if it is first file.
		return true, nil
	}

	file, err := os.OpenFile(indexPath, os.O_RDWR, 0644)
	if err != nil {
		return false, err
	}
	defer file.Close()

	var freedCount, mergedCount int
	var freeAtSize uint32
	var busyAt, freeAt int64
	freeAt = -1
	busyAt = -1

	sizeBuf := make([]byte, sizePrefixSize)
	scratch := make([]byte, 256)
	var pos int64
	for {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if _, err = file.ReadAt(sizeBuf, pos); err != nil {
			if err == io.EOF {
				// Finished reading entire index.
				break
			}
			return false, err
		}

		size := binary.LittleEndian.Uint32(sizeBuf)
		if size&deletedBit != 0 {
			// Record is already deleted.
			size ^= deletedBit
			if freeAt > busyAt {
				// Previous record free, so merge this record into the last.
				freeAtSize += sizePrefixSize + size
				binary.LittleEndian.PutUint32(sizeBuf, freeAtSize|deletedBit)
				_, err = file.WriteAt(sizeBuf, freeAt)
				if err != nil {
					return false, fmt.Errorf("cannot write to index file %s: %w", file.Name(), err)
				}
				mergedCount++
			} else {
				// Previous record was not free, so mark new free position.
				freeAt = pos
				freeAtSize = size
			}
			pos += sizePrefixSize + int64(size)
			continue
		}

		if int(size) > len(scratch) {
			scratch = make([]byte, size)
		}
		data := scratch[:size]
		if _, err = file.ReadAt(data, pos+sizePrefixSize); err != nil {
			if err == io.EOF {
				// The data has not been written yet, or the file is corrupt.
				// Take the data we are able to use and move on.
				break
			}
			return false, fmt.Errorf("error reading data from index: %w", err)
		}

		bucketPrefix := BucketIndex(binary.LittleEndian.Uint32(data))
		inUse, err := index.busy(bucketPrefix, pos+sizePrefixSize, fileNum)
		if err != nil {
			return false, err
		}
		if inUse {
			// Record is in use.
			busyAt = pos
		} else {
			// Record is free.
			if freeAt > busyAt {
				// Merge this free record into the last
				freeAtSize += sizePrefixSize + size
				binary.LittleEndian.PutUint32(sizeBuf, freeAtSize|deletedBit)
				_, err = file.WriteAt(sizeBuf, freeAt)
				if err != nil {
					return false, fmt.Errorf("cannot write to index file %s: %w", file.Name(), err)
				}
				mergedCount++
			} else {
				// Mark the record as deleted by setting the highest bit in the size. That
				// bit is otherwise unused since the maximum filesize is 2^30.
				binary.LittleEndian.PutUint32(sizeBuf, size|deletedBit)
				_, err = file.WriteAt(sizeBuf, pos)
				if err != nil {
					return false, fmt.Errorf("cannot write to index file %s: %w", file.Name(), err)
				}

				freeAt = pos
				freeAtSize = size
			}
			freedCount++
		}
		pos += sizePrefixSize + int64(size)
	}

	fileName := filepath.Base(file.Name())
	log.Infow("Marked index records as free", "freed", freedCount, "merged", mergedCount, "file", fileName)

	// If there is a span of free records at end of file, truncate file.
	if freeAt > busyAt {
		// End of primary is free.
		if err = file.Truncate(freeAt); err != nil {
			return false, fmt.Errorf("failed to truncate index file: %w", err)
		}
		log.Infow("Removed free records from end of index file", "file", fileName, "at", freeAt, "bytes", freeAtSize)
		if freeAt == 0 {
			return true, nil
		}
	}

	return false, nil
}

func (index *Index) busy(bucketPrefix BucketIndex, localPos int64, fileNum uint32) (bool, error) {
	index.bucketLk.RLock()
	bucketPos, err := index.buckets.Get(bucketPrefix)
	index.bucketLk.RUnlock()
	if err != nil {
		return false, err
	}
	localPosInBucket, fileNumInBucket := localizeBucketPos(bucketPos, index.maxFileSize)
	if fileNum == fileNumInBucket && localPos == int64(localPosInBucket) {
		return true, nil
	}
	return false, nil
}
