package mhprimary

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-storethehash/store/freelist"
	"github.com/ipld/go-storethehash/store/types"
)

var log = logging.Logger("storethehash/mhprimary")

type primaryGC struct {
	freeList    *freelist.FreeList
	primary     *MultihashPrimary
	updateSig   chan struct{}
	done        chan struct{}
	cycleLock   sync.Mutex
	updateIndex UpdateIndexFunc
}

type UpdateIndexFunc func([]byte, types.Block) error

func newGC(primary *MultihashPrimary, freeList *freelist.FreeList, gcInterval, gcTimeLimit time.Duration) *primaryGC {
	gc := &primaryGC{
		freeList:  freeList,
		primary:   primary,
		updateSig: make(chan struct{}, 1),
		done:      make(chan struct{}),
	}

	go gc.run(gcInterval, gcTimeLimit)

	return gc
}

func (gc *primaryGC) SignalUpdate() {
	// Send signal to tell GC there are updates.
	select {
	case gc.updateSig <- struct{}{}:
	default:
	}
}

func (gc *primaryGC) close() {
	gc.cycleLock.Lock()
	defer gc.cycleLock.Unlock()

	if gc.primary != nil {
		close(gc.updateSig)
		<-gc.done
		gc.primary = nil
	}
}

func (gc *primaryGC) setUpdateIndex(updateIndex UpdateIndexFunc) {
	gc.cycleLock.Lock()
	defer gc.cycleLock.Unlock()
	gc.updateIndex = updateIndex
}

// run is a goroutine that runs periodically to search for and
// remove stale index files. It runs every gcInterval, if there have been any
// primary updates.
func (gc *primaryGC) run(gcInterval, gcTimeLimit time.Duration) {
	defer close(gc.done)

	var gcDone chan struct{}
	hasUpdate := true

	// Run 1st GC 5 minute after startup.
	t := time.NewTimer(15 * time.Second) //5 * time.Minute)

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
				// Nothing new, nothing to evaporate, keep waiting.
				t.Reset(gcInterval)
				continue
			}

			gcDone = make(chan struct{})
			go func() {
				defer close(gcDone)
				gcCtx := ctx
				if gcTimeLimit != 0 {
					var cancel context.CancelFunc
					gcCtx, cancel = context.WithTimeout(ctx, gcTimeLimit)
					defer cancel()
				}

				log.Infow("GC started")
				count, err := gc.gc(gcCtx)
				if err != nil {
					switch err {
					case context.DeadlineExceeded:
						log.Infow("GC stopped at time limit", "limit", gcTimeLimit)
					case context.Canceled:
						log.Info("GC canceled")
					default:
						log.Errorw("GC failed", "err", err)
					}
					return
				}
				log.Infow("GC finished", "removedFiles", count)
			}()
		case <-gcDone:
			gcDone = nil
			hasUpdate = false
			// Finished the checkpoint update, cleanup and reset timer.
			t.Reset(gcInterval)
		}
	}
}

// gc searches for and removes stale index files. Returns the number of unused
// index files that were removed.
func (gc *primaryGC) gc(ctx context.Context) (int, error) {
	gc.cycleLock.Lock()
	defer gc.cycleLock.Unlock()

	affectedSet, err := processFreeList(ctx, gc.freeList, gc.primary.basePath, gc.primary.maxFileSize)
	if err != nil {
		return 0, fmt.Errorf("cannot process freelist: %w", err)
	}

	header, err := readHeader(gc.primary.headerPath)
	if err != nil {
		return 0, fmt.Errorf("cannot read primary header: %w", err)
	}

	delete(affectedSet, gc.primary.fileNum)
	gcFiles := make([]uint32, 0, len(affectedSet)+int(gc.primary.fileNum-header.FirstFile))
	for fileNum := range affectedSet {
		gcFiles = append(gcFiles, fileNum)
	}

	// GC all but the current primary file, starting with the affected list.
	for fileNum := header.FirstFile; fileNum != gc.primary.fileNum; fileNum++ {
		if _, ok := affectedSet[fileNum]; ok {
			continue
		}
		gcFiles = append(gcFiles, fileNum)
	}

	var delCount int
	for _, fileNum := range gcFiles {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if fileNum == gc.primary.fileNum {
			continue
		}
		filePath := primaryFileName(gc.primary.basePath, fileNum)

		dead, err := gc.reapRecords(ctx, fileNum)
		if err != nil {
			return 0, fmt.Errorf("cannot reap records in primary file %s: %w", filePath, err)
		}

		if dead && fileNum == header.FirstFile {
			header.FirstFile++
			if err = writeHeader(gc.primary.headerPath, header); err != nil {
				return 0, fmt.Errorf("cannot write header: %w", err)
			}
			if err = os.Remove(filePath); err != nil {
				return 0, fmt.Errorf("cannot remove primary file %s: %w", filePath, err)
			}
			log.Infow("Removed stale primary file", "file", filepath.Base(filePath))
			delCount++
		}
	}

	return delCount, nil
}

// reapRecords removes empty records from the end of the file. If the file is
// empty, then returns true to indicate the file can be deleted.
func (gc *primaryGC) reapRecords(ctx context.Context, fileNum uint32) (bool, error) {
	file, err := os.OpenFile(primaryFileName(gc.primary.basePath, fileNum), os.O_RDWR, 0644)
	if err != nil {
		return false, fmt.Errorf("cannot open primary file: %w", err)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return false, fmt.Errorf("cannot stat primary file: %w", err)
	}
	fileName := filepath.Base(file.Name())
	if fi.Size() == 0 {
		// File was already truncated to 0 size, but was not yet removed.
		log.Debugw("Primary file is already empty", "file", fileName)
		return true, nil
	}

	var mergedCount int
	var busyAt, freeAt, prevBusyAt int64
	var busySize, prevBusySize, totalBusy, totalFree int64
	var freeAtSize uint32
	freeAt = -1
	busyAt = -1
	prevBusyAt = -1

	// See if any entries can be merged.
	sizeBuf := make([]byte, sizePrefixSize)
	var pos int64
	for {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if _, err = file.ReadAt(sizeBuf, pos); err != nil {
			if err == io.EOF {
				// Finished reading entire primary.
				break
			}
			return false, err
		}
		size := binary.LittleEndian.Uint32(sizeBuf)

		if size&deletedBit != 0 {
			size ^= deletedBit
			// If previous record is free.
			if freeAt > busyAt {
				// Merge this free record into the last
				freeAtSize += sizePrefixSize + size
				if freeAtSize >= deletedBit {
					log.Warn("Records are too large to merge")
					freeAt = pos
					freeAtSize = size
				} else {
					binary.LittleEndian.PutUint32(sizeBuf, freeAtSize|deletedBit)
					_, err = file.WriteAt(sizeBuf, freeAt)
					if err != nil {
						return false, fmt.Errorf("cannot write to index file %s: %w", file.Name(), err)
					}
					mergedCount++
				}
			} else {
				// Previous record was not free, so mark new free position.
				freeAt = pos
				freeAtSize = size
			}
			totalFree += int64(size)
		} else {
			// Record is in use.
			prevBusyAt = busyAt
			prevBusySize = busySize
			busyAt = pos
			busySize = int64(size)
			totalBusy += busySize
		}

		pos += sizePrefixSize + int64(size)
	}

	log.Infow("Merged free index records", "merged", mergedCount, "file", fileName)

	// If there is a span of free records at end of file, truncate file.
	if freeAt > busyAt {
		// End of primary is free.
		if err = file.Truncate(freeAt); err != nil {
			return false, err
		}
		log.Infow("Removed free records from end of primary file", "file", fileName, "at", freeAt, "bytes", freeAtSize)

		if freeAt == 0 {
			// Entire primary is free.
			return true, nil
		}
	}

	// If only known busy location was freed, but file is not empty, then start
	// over next gc cycle.
	if busyAt == -1 {
		return false, nil
	}

	// No ability to move primary records without being able to update index.
	if gc.updateIndex == nil {
		return false, nil
	}

	// If 75% or more of the records in the file are free, rewrite the last 2
	// records. that are still in use, into a later primary. This will allow
	// low-use primary files to evaporate over time.
	log.Debugf("%s free=%d busy=%d", fileName, totalFree, totalBusy)
	if 4*totalFree >= 3*(totalFree+totalBusy) {
		scratch := make([]byte, 1024)

		for busyAt >= 0 {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}

			// Read the record data.
			if _, err = file.ReadAt(sizeBuf, busyAt); err != nil {
				return false, fmt.Errorf("cannot read record size: %w", err)
			}
			size := binary.LittleEndian.Uint32(sizeBuf)
			if int(size) > len(scratch) {
				scratch = make([]byte, size)
			}
			data := scratch[:size]
			if _, err = file.ReadAt(data, busyAt+sizePrefixSize); err != nil {
				return false, fmt.Errorf("cannot read record data: %w", err)
			}
			// Extract key and value from record data.
			key, val, err := readNode(data)
			if err != nil {
				return false, fmt.Errorf("cannot extract key and value from record: %w", err)
			}
			// Get the index key for the record key.
			indexKey, err := gc.primary.IndexKey(key)
			if err != nil {
				return false, fmt.Errorf("cannot get index key for record key: %w", err)
			}
			// Store the key and value in the primary.
			fileOffset, err := gc.primary.Put(key, val)
			if err != nil {
				return false, fmt.Errorf("cannot put new primary record: %w", err)
			}
			// Update the index with the new primary location.
			if err = gc.updateIndex(indexKey, fileOffset); err != nil {
				return false, fmt.Errorf("cannot update index with new record location: %w", err)
			}
			log.Infow("Moved record from end of low-use file", "from", fileName)

			// Do not truncate file here, because moved record may not be
			// written yet. Instead put moved record onto freelist and let next
			// GC cycle process freelist and delete this record.

			// Add outdated data in primary storage to freelist
			offset := absolutePrimaryPos(types.Position(busyAt), fileNum, gc.primary.maxFileSize)
			blk := types.Block{Size: types.Size(busySize), Offset: types.Position(offset)}
			if err = gc.freeList.Put(blk); err != nil {
				return false, fmt.Errorf("cannot put old record location into freelist: %w", err)
			}

			busyAt = prevBusyAt
			busySize = prevBusySize
			prevBusyAt = -1
		}
	}

	return false, nil
}

// processFreeList reads the freelist and marks the locations in primary files
// as dead by setting the deleted bit in the record size field.
func processFreeList(ctx context.Context, freeList *freelist.FreeList, basePath string, maxFileSize uint32) (map[uint32]struct{}, error) {
	flPath, err := freeList.ToGC()
	if err != nil {
		return nil, fmt.Errorf("cannot get freelist gc file: %w", err)
	}

	fi, err := os.Stat(flPath)
	if err != nil {
		return nil, fmt.Errorf("cannot stat freelist gc file: %w", err)
	}

	var affectedSet map[uint32]struct{}

	// If the freelist size is non-zero, then process its records.
	if fi.Size() != 0 {
		log.Infof("Applying freelist to primary storage")
		affectedSet = make(map[uint32]struct{})
		startTime := time.Now()

		flFile, err := os.OpenFile(flPath, os.O_RDONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("error opening freelist gc file: %w", err)
		}
		defer flFile.Close()

		var count int
		flIter := freelist.NewIterator(bufio.NewReader(flFile))
		for {
			free, err := flIter.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, fmt.Errorf("error reading freelist: %w", err)
			}

			// Mark dead location with tombstone bit in the record's size data.
			localPos, fileNum := localizePrimaryPos(free.Offset, maxFileSize)
			err = deleteRecord(localPos, fileNum, free.Size, basePath)
			if err != nil {
				log.Errorw("Cannot mark primary record deleted", "err", err)
				continue
			}
			affectedSet[fileNum] = struct{}{}
			count++
		}
		flFile.Close()
		log.Infow("Marked primary records from freelist as deleted", "count", count, "elapsed", time.Since(startTime))
	}

	if err = os.Remove(flPath); err != nil {
		return nil, fmt.Errorf("error removing freelist: %w", err)
	}

	return affectedSet, nil
}

func deleteRecord(localPos types.Position, fileNum uint32, size types.Size, basePath string) error {
	file, err := os.OpenFile(primaryFileName(basePath, fileNum), os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("cannot open primary file %s: %w", file.Name(), err)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return fmt.Errorf("cannot stat primary file %s: %w", file.Name(), err)
	}
	if localPos > types.Position(fi.Size()) {
		return fmt.Errorf("freelist record has out-of-range primary offset, offset=%d, fileSize=%d", localPos, fi.Size())
	}

	sizeBuf := make([]byte, sizePrefixSize)
	if _, err = file.ReadAt(sizeBuf, int64(localPos)); err != nil {
		return err
	}
	recSize := binary.LittleEndian.Uint32(sizeBuf)
	if recSize&deletedBit != 0 {
		// Already deleted
		return nil
	}

	if types.Size(recSize) != size {
		return fmt.Errorf("record size (%d) in primary %s does not match size in freelist (%d), pos=%d", recSize, file.Name(), size, localPos)
	}

	// Mark the record as deleted by setting the highest bit in the size. This
	// assumes that the record size is < 2^31.
	binary.LittleEndian.PutUint32(sizeBuf, recSize|deletedBit)
	_, err = file.WriteAt(sizeBuf, int64(localPos))
	if err != nil {
		return fmt.Errorf("cannot write to primary file %s: %w", file.Name(), err)
	}

	return nil
}
