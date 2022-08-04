package mhprimary

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
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

	busyAt map[uint32]int64

	updateIndex UpdateIndexFunc
}

type UpdateIndexFunc func([]byte, types.Block) error

func NewGC(primary *MultihashPrimary, freeList *freelist.FreeList, gcInterval time.Duration, updateIndex UpdateIndexFunc) *primaryGC {
	gc := &primaryGC{
		freeList:  freeList,
		primary:   primary,
		updateSig: make(chan struct{}, 1),
		done:      make(chan struct{}),

		busyAt: make(map[uint32]int64),

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

// gc searches for and removes stale index files. Returns the number of unused
// index files that were removed.
func (gc *primaryGC) cycle(ctx context.Context) (int, error) {
	err := gc.processFreeList(ctx)
	if err != nil {
		return 0, fmt.Errorf("cannot process freelist: %w", err)
	}

	header, err := readHeader(gc.primary.headerPath)
	if err != nil {
		return 0, fmt.Errorf("cannot read primary header: %w", err)
	}
	fileNum := header.FirstFile

	var count int
	for {
		if fileNum == gc.primary.fileNum {
			// Do not try to GC the current primary file.
			break
		}
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		primaryPath := primaryFileName(gc.primary.basePath, fileNum)
		dead, err := gc.reapFile(ctx, fileNum, primaryPath)
		if err != nil {
			return 0, err
		}
		if dead && fileNum == header.FirstFile {
			header.FirstFile = fileNum + 1
			err = writeHeader(gc.primary.headerPath, header)
			if err != nil {
				return 0, err
			}
			err = os.Remove(primaryPath)
			if err != nil {
				return 0, err
			}
			count++
		}
		fileNum++
	}

	return count, nil
}

// reapFile removes empty entries from the end of the file. If the file is
// empty, then returns true to indicate the file can be deleted.
func (gc *primaryGC) reapFile(ctx context.Context, fileNum uint32, primaryPath string) (bool, error) {
	file, err := os.Open(primaryPath)
	if err != nil {
		return false, fmt.Errorf("cannot open primary file: %w", err)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return false, fmt.Errorf("cannot stat primary file: %w", err)
	}
	if fi.Size() == 0 {
		return true, nil
	}

	busyAt, ok := gc.busyAt[fileNum]
	if ok {
		_, err = file.Seek(busyAt, os.SEEK_SET)
		if err != nil {
			return false, err
		}
	}

	var freeCount, busyCount int
	var freeAt, prevBusyAt int64
	freeAt = -1
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
			freeCount++
			if busyAt > freeAt {
				freeAt = pos
				if busyAt == freeAt {
					busyAt = prevBusyAt
				}
			}
		} else {
			busyCount++
			prevBusyAt = busyAt
			busyAt = pos
		}

		pos += sizePrefixSize + int64(size)
	}

	delete(gc.busyAt, fileNum)

	if freeAt > busyAt {
		// End of primary is free.
		if err = file.Truncate(freeAt); err != nil {
			return false, err
		}
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

	// If less than 25% of the records in the file are still used, rewrite the
	// last 2 record still in use into a later primary. This will allow low-use
	// primary files to evaporate over time.
	if freeCount > busyCount*4 {
		for {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}

			// Read the record data.
			if _, err = file.ReadAt(sizeBuffer, busyAt); err != nil {
				return false, err
			}
			size := binary.LittleEndian.Uint32(sizeBuffer)
			data := scratch[:size]
			if _, err = file.ReadAt(data, busyAt+sizePrefixSize); err != nil {
				return false, err
			}
			// Extract key and value from record data.
			key, val, err := readNode(data)
			if err != nil {
				return false, err
			}
			// Get the index key for the record key.
			indexKey, err := gc.primary.IndexKey(key)
			if err != nil {
				return false, err
			}
			// Store the key and value in the primary.
			fileOffset, err := gc.primary.Put(key, val)
			if err != nil {
				return false, err
			}
			// Update the index with the new primary location.
			if err = gc.updateIndex(indexKey, fileOffset); err != nil {
				return false, err
			}
			// Remove the old record data from this primary file.
			if err = file.Truncate(busyAt); err != nil {
				return false, err
			}
			if busyAt <= 0 {
				break
			}
			busyAt = prevBusyAt
			prevBusyAt = -1
		}
		// Return true if primary is free.
		return busyAt == 0, nil
	}

	// Remember the last busy location so that next GC reap starts looking
	// there instead of rescanning whole file.
	gc.busyAt[fileNum] = busyAt

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
func (gc *primaryGC) processFreeList(ctx context.Context) error {
	flPath, err := gc.freeList.Rotate()
	if err != nil {
		return fmt.Errorf("cannot rotate freelist: %w", err)
	}

	flFile, err := os.OpenFile(flPath, os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening freelist: %w", err)
	}
	defer flFile.Close()

	flIter := freelist.NewFreeListIter(flFile)
	for {
		free, err := flIter.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("error reading freelist: %w", err)
		}

		// Mark dead location with tombstone by zeroing the record's data.
		err = gc.primary.ZeroRecord(free.Offset, free.Size)
		if err != nil {
			return fmt.Errorf("gc cannot zero primary record: %w", err)
		}

	}
	flFile.Close()
	err = os.Remove(flPath)
	if err != nil {
		return fmt.Errorf("error removing freelist: %w", err)
	}

	return nil
}
