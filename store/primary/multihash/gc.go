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
	freeList  *freelist.FreeList
	primary   *MultihashPrimary
	updateSig chan struct{}
	done      chan struct{}
	cycleLock sync.Mutex
	lowUse    map[uint32]struct{}

	updateIndex UpdateIndexFunc

	// GC stats
	cycleCount     int64
	bytesCollected int64
}

type UpdateIndexFunc func([]byte, types.Block) error

func NewGC(primary *MultihashPrimary, freeList *freelist.FreeList, gcInterval time.Duration, updateIndex UpdateIndexFunc) *primaryGC {
	gc := &primaryGC{
		freeList:  freeList,
		primary:   primary,
		updateSig: make(chan struct{}, 1),
		done:      make(chan struct{}),
		lowUse:    make(map[uint32]struct{}),

		updateIndex: updateIndex,
	}

	go gc.run(gcInterval)

	return gc
}

func (gc *primaryGC) SignalUpdate() {
	// Send signal to tell GC there are updates.
	select {
	case gc.updateSig <- struct{}{}:
	default:
	}
}

func (gc *primaryGC) Close() {
	close(gc.updateSig)
	<-gc.done
}

// run is a goroutine that runs periodically to search for and
// remove stale index files. It runs every gcInterval, if there have been any
// primary updates.
func (gc *primaryGC) run(gcInterval time.Duration) {
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
			if !hasUpdate && len(gc.lowUse) == 0 {
				// Nothing new, nothing to evaporate, keep waiting.
				t.Reset(gcInterval)
				continue
			}

			gcDone = make(chan struct{})
			go func() {
				defer close(gcDone)
				log.Infow("GC started")
				fileCount, err := gc.Cycle(ctx)
				if err != nil {
					log.Errorw("GC failed", "err", err)
					return
				}
				if fileCount == 0 {
					log.Infow("GC finished, no primary files to remove", "bytesCollected", gc.bytesCollected, "gcCycles", gc.cycleCount)
				} else {
					log.Infow("GC finished, removed primary files", "fileCount", fileCount, "bytesCollected", gc.bytesCollected, "gcCycles", gc.cycleCount)
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

// gc searches for and removes stale index files. Returns the number of unused
// index files that were removed.
func (gc *primaryGC) Cycle(ctx context.Context) (int, error) {
	gc.cycleLock.Lock()
	defer gc.cycleLock.Unlock()

	gc.cycleCount++

	flCount, err := processFreeList(ctx, gc.freeList, gc.primary.basePath, gc.primary.maxFileSize)
	if err != nil {
		return 0, fmt.Errorf("cannot process freelist: %w", err)
	}

	header, err := readHeader(gc.primary.headerPath)
	if err != nil {
		return 0, fmt.Errorf("cannot read primary header: %w", err)
	}

	var delCount int

	// If no new freelist entries, and not first run, evaporate the low use files.
	if flCount == 0 && gc.cycleCount != 1 {
		for fileNum := range gc.lowUse {
			deleted, err := gc.reapFile(ctx, fileNum, &header)
			if err != nil {
				fileName := primaryFileName(gc.primary.basePath, fileNum)
				return 0, fmt.Errorf("cannot reap low-use primary file %s: %w", fileName, err)
			}
			if deleted {
				delete(gc.lowUse, fileNum)
				delCount++
			}
		}
		return delCount, nil
	}

	// Try to GC all but the current primary file.
	for fileNum := header.FirstFile; fileNum != gc.primary.fileNum; fileNum++ {
		deleted, err := gc.reapFile(ctx, fileNum, &header)
		if err != nil {
			fileName := primaryFileName(gc.primary.basePath, fileNum)
			return 0, fmt.Errorf("cannot reap primary file %s: %w", fileName, err)
		}
		if deleted {
			delete(gc.lowUse, fileNum)
			delCount++
		}
	}

	return delCount, nil
}

func (gc *primaryGC) reapFile(ctx context.Context, fileNum uint32, header *Header) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	dead, err := gc.reapRecords(ctx, fileNum)
	if err != nil {
		return false, fmt.Errorf("cannot reap dead primary records: %w", err)
	}

	if dead && fileNum == header.FirstFile {
		header.FirstFile++
		err = writeHeader(gc.primary.headerPath, *header)
		if err != nil {
			return false, fmt.Errorf("cannot write header: %w", err)
		}
		fileName := primaryFileName(gc.primary.basePath, fileNum)

		err = os.Remove(fileName)
		if err != nil {
			return false, fmt.Errorf("cannot remove primary file %s: %w", filepath.Base(fileName), err)
		}
		log.Infow("Removed stale primary file", "file", filepath.Base(fileName))
		return true, nil
	}

	return false, nil
}

// reapRecords removes empty records from the end of the file. If the file is
// empty, then returns true to indicate the file can be deleted.
func (gc *primaryGC) reapRecords(ctx context.Context, fileNum uint32) (bool, error) {
	file, err := os.OpenFile(primaryFileName(gc.primary.basePath, fileNum), os.O_RDWR, 0644)
	if err != nil {
		return false, fmt.Errorf("cannot open primary file: %w", err)
	}
	//defer file.Seek(0, io.SeekEnd)
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

	var freeCount, busyCount int
	var busyAt, freeAt, prevBusyAt int64
	var busySize, prevBusySize int64
	freeAt = -1
	busyAt = -1
	prevBusyAt = -1

	// See if any entries can be truncated
	inBuf := bufio.NewReader(file)
	sizeBuffer := make([]byte, sizePrefixSize)
	scratch := make([]byte, 256)
	var pos int64
	for {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if _, err = io.ReadFull(inBuf, sizeBuffer); err != nil {
			if err == io.EOF {
				// Finished reading entire primary.
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
			return false, fmt.Errorf("error reading data from primary: %w", err)
		}

		if allZeros(data) {
			// Record is deleted.
			freeCount++
			// Only update freeAt if there is a busy record after it.
			if busyAt >= freeAt {
				freeAt = pos
				if busyAt == freeAt {
					busyAt = prevBusyAt
					busySize = prevBusySize
				}
			}
		} else {
			// Record is in use.
			busyCount++
			prevBusyAt = busyAt
			prevBusySize = busySize
			busyAt = pos
			busySize = int64(size)
		}

		pos += sizePrefixSize + int64(size)
	}

	if freeAt > busyAt {
		sizeBefore := fi.Size()
		// End of primary is free.
		if err = file.Truncate(freeAt); err != nil {
			return false, err
		}
		collected := sizeBefore - freeAt
		log.Infow("Truncated primary file", "bytesCollected", collected, "file", fileName)
		gc.bytesCollected += collected

		if freeAt == 0 {
			// Entire primary is free.
			return true, nil
		}
		if busyAt != -1 {
			_, err = file.Seek(busyAt, io.SeekStart)
			if err != nil {
				return false, fmt.Errorf("cannot seek primary: %w", err)
			}
		}
	}

	// If only known busy location was freed, but file is not empty, then start
	// over next gc cycle.
	if busyAt == -1 {
		return false, nil
	}

	// If more that 75% of the records in the file are free, rewrite the last 2
	// records. that are still in use, into a later primary. This will allow
	// low-use primary files to evaporate over time.
	log.Debugf("%s freecount=%d busycount=%d", fileName, freeCount, busyCount)
	if 4*freeCount >= 3*(freeCount+busyCount) {
		for busyAt >= 0 {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}

			// Read the record data.
			if _, err = file.ReadAt(sizeBuffer, busyAt); err != nil {
				return false, fmt.Errorf("cannot read record size: %w", err)
			}
			size := binary.LittleEndian.Uint32(sizeBuffer)
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

		gc.lowUse[fileNum] = struct{}{}
	}

	return false, nil
}

func allZeros(data []byte) bool {
	for i := range data {
		if data[i] != 0 {
			return false
		}
	}
	return true
}

// processFreeList reads the freelist and marks the locations in primary
// files as dead by zeroing the data of the dead record.
func processFreeList(ctx context.Context, freeList *freelist.FreeList, basePath string, maxFileSize uint32) (int, error) {
	flPath, err := freeList.ToGC()
	if err != nil {
		return 0, fmt.Errorf("cannot get freelist gc file: %w", err)
	}

	fi, err := os.Stat(flPath)
	if err != nil {
		return 0, fmt.Errorf("cannot stat freelist gc file: %w", err)
	}

	// If the freelist size is non-zero, then process its records.
	var count int
	if fi.Size() != 0 {
		log.Infof("Applying freelist to primary storage")

		flFile, err := os.OpenFile(flPath, os.O_RDONLY, 0644)
		if err != nil {
			return 0, fmt.Errorf("error opening freelist gc file: %w", err)
		}
		defer flFile.Close()

		total := int(fi.Size() / (types.OffBytesLen + types.SizeBytesLen))
		startTime := time.Now()
		flIter := freelist.NewIter(bufio.NewReader(flFile))
		for {
			free, err := flIter.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				return 0, fmt.Errorf("error reading freelist: %w", err)
			}

			// Mark dead location with tombstone by zeroing the record's data.
			err = zeroRecord(free.Offset, free.Size, basePath, maxFileSize)
			if err != nil {
				log.Errorw("Cannot zero primary record", "err", err)
				continue
			}
			count++

			// Log every 5 minutes, do time check every 2^20 records.
			if count&1024*1024-1 == 0 && time.Since(startTime) >= 5*time.Minute {
				log.Infof("Processed %d of %d freelist records: %d%% done", count, total, 100*count/total)
				startTime = time.Now()
			}
		}
		log.Infow("Marked primary records from freelist as deleted", "count", count)
		flFile.Close()
	}

	if err = os.Remove(flPath); err != nil {
		return 0, fmt.Errorf("error removing freelist: %w", err)
	}

	return count, nil
}

func zeroRecord(pos types.Position, size types.Size, basePath string, maxFileSize uint32) error {
	var localPos types.Position
	var fileName string
	if maxFileSize == 0 {
		// Not split into separate files.
		localPos = pos
		fileName = basePath
	} else {
		var fileNum uint32
		localPos, fileNum = localizePrimaryPos(pos, maxFileSize)
		fileName = primaryFileName(basePath, fileNum)
	}

	file, err := os.OpenFile(fileName, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("cannot open primary file %s: %w", fileName, err)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return fmt.Errorf("cannot stat primary file %s: %w", fileName, err)
	}
	if localPos > types.Position(fi.Size()) {
		return fmt.Errorf("freelist record has out-of-range primary offset, offset=%d, fileSize=%d", localPos, fi.Size())
	}

	sizeBuffer := make([]byte, sizePrefixSize)
	if _, err = file.ReadAt(sizeBuffer, int64(localPos)); err != nil {
		return err
	}
	recSize := binary.LittleEndian.Uint32(sizeBuffer)
	if types.Size(recSize) != size {
		return fmt.Errorf("record size (%d) in primary %s does not match size in freelist (%d), pos=%d", recSize, fileName, size, localPos)
	}

	zeros := make([]byte, size)
	_, err = file.WriteAt(zeros, int64(localPos+sizePrefixSize))
	if err != nil {
		return fmt.Errorf("cannot write to primary file %s: %w", fileName, err)
	}

	log.Debugw("Zeroed record", "file", fileName, "pos", localPos, "size", size)
	return nil
}
