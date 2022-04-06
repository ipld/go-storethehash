package index

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-storethehash/store/types"
)

var log = logging.Logger("storethehash/index")

const (
	// checkpointInterval is how often to advance the checkpoint.
	checkpointInterval = 10 * time.Second //2 * time.Minute
	// compactPercentThreshold is percent of the index that must be eliminated
	// by the checkpoint for compaction to take place.
	compactPercentThreshold = 3
)

// checkpointer is a goroutine that waits for a signal to update the index
// checkpoint, and looks for the next checkpoint if it has not been too soon,
// `checkpointInterval`, since the last time.  The search runs and advances the
// checkpoint until it has been running for `checkpointRuntime` or until it
// finds the next live index entry.  The search runs in a separate goroutine so
// that this function can continue reading the channel used to signal updates.
func (i *Index) checkpointer() {
	var cpDone chan struct{}
	hasUpdate := true
	t := time.NewTimer(checkpointInterval)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		select {
		case _, ok := <-i.cpUpdate:
			if !ok {
				// Channel closed; shutting down.
				cancel()
				if cpDone != nil {
					<-cpDone
				}
				return
			}
			hasUpdate = true
		case <-t.C:
			if !hasUpdate {
				// Nothing new, keep waiting.
				t.Reset(checkpointInterval)
				continue
			}

			cpDone = make(chan struct{})
			go func() {
				defer close(cpDone)
				cp, err := i.updateCheckpoint(ctx)
				if err != nil {
					log.Errorw("Failed to update checkpoint", "err", err)
					return
				}

				// See if the checkpoint reduces the size enough to make it
				// worth it. Reducing the size of the index does not not
				// improve runtime performance. It only reduces storage space
				// and helps startup time, but does cause a pause in operation
				// while compaction is done.
				if checkpointAtThreshold(cp, int64(i.length)) {
					log.Infow("Checkpoint reached compaction threshold", "percent", compactPercentThreshold,
						"checkpoint", cp, "total", i.length)
					start := time.Now()
					if err = i.compactIndex(); err != nil {
						log.Errorw("Failed to compact index", "err", err)
					}
					log.Debugw("Compaction time", "elapsed", time.Since(start))
				}
			}()
		case <-cpDone:
			cpDone = nil
			// Finished the checkpoint update, cleanup and reset timer.
			t.Reset(checkpointInterval)
		}
	}
}

func checkpointAtThreshold(checkpoint, totalSize int64) bool {
	if checkpoint == 0 || totalSize == 0 {
		return false
	}
	return (checkpoint*100)/int64(totalSize) >= compactPercentThreshold
}

// updateCheckpoint attempts to advance the checkpoint from its previous
// location to the first entry in use. The checkpoint is returned.
func (i *Index) updateCheckpoint(ctx context.Context) (int64, error) {
	checkpoint, err := readCheckpoint(i.cpPath)
	if err != nil {
		return 0, fmt.Errorf("could not read checkpoint: %w", err)
	}
	prevCheckpoint := checkpoint

	file, err := openFileForScan(i.file.Name())
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var iterPos int64
	if checkpoint == 0 {
		_, bytesRead, err := ReadHeader(file)
		if err != nil {
			return 0, err
		}
		iterPos = int64(bytesRead)
	} else {
		file.Seek(checkpoint, 0)
		iterPos = checkpoint
	}

	inBuf := bufio.NewReader(file)
	sizeBuffer := make([]byte, SizePrefixSize)
	scratch := make([]byte, 256)
	var count int
	for {
		if ctx.Err() != nil {
			// If context canceled, return what was found so far.
			log.Debugw("Ending search for next checkpoint", "reason", ctx.Err())
			break
		}
		if _, err = io.ReadFull(inBuf, sizeBuffer); err != nil {
			if err == io.EOF {
				// Finished reading entire index.
				break
			}
			return 0, err
		}
		size := binary.LittleEndian.Uint32(sizeBuffer)

		pos := iterPos + int64(SizePrefixSize)
		iterPos = pos + int64(size)
		if int(size) > len(scratch) {
			scratch = make([]byte, size)
		}
		data := scratch[:size]
		if _, err = io.ReadFull(inBuf, data); err != nil {
			if err == io.EOF {
				// The data was not been written yet, or the file is corrupt.
				// Take the data we are able to use and move on.
				break
			}
			return 0, fmt.Errorf("error reading data from index: %w", err)
		}

		bucketPrefix := BucketIndex(binary.LittleEndian.Uint32(data))
		i.bucketLk.Lock()
		bucketPos, err := i.buckets.Get(bucketPrefix)
		i.bucketLk.Unlock()
		if err != nil {
			return 0, err
		}
		if int64(bucketPos) == pos {
			// Index item in use; do not advance checkpoint.
			break
		}
		// Advance checkpoint to next uninspected item.
		checkpoint = iterPos
		// Write the checkpoint every 1e6 entries.
		count++
		if count == 1000000 {
			count = 0
			if err = writeCheckpoint(i.cpPath, checkpoint); err != nil {
				return 0, err
			}
		}
	}
	if count != 0 {
		if err = writeCheckpoint(i.cpPath, checkpoint); err != nil {
			return 0, err
		}
	}

	if checkpoint == prevCheckpoint {
		log.Debug("No new checkpoint")
	} else {
		log.Debugw("Advanced checkpoint", "bytes", checkpoint-prevCheckpoint)
	}

	return checkpoint, nil
}

func checkpointPath(path string) string {
	return path + "_cp"
}

func readCheckpoint(cpPath string) (int64, error) {
	data, err := os.ReadFile(cpPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	if len(data) == 0 {
		return 0, nil
	}
	return strconv.ParseInt(string(data), 10, 64)
}

func writeCheckpoint(cpPath string, checkpoint int64) error {
	posStr := strconv.FormatInt(checkpoint, 10)
	return os.WriteFile(cpPath, []byte(posStr), 0666)
}

// compactIndex copies the existing index file to a new file, omitting all data
// that the checkpoint skips over, and rebuilds the buckets from the new file.
//
// If is process is done is two parts. The first part above is done while data
// is still being writen to the index, so that the index can continue to
// operate while most of the data is copied and buckets rebuilt.  The second
// part is to lock the index, blocking its operation. Then copy any additional
// data from the index to the new file and read additional buckets.  Finally
// the new file replaces the old index and index is unlocked.
func (i *Index) compactIndex() error {
	start := time.Now()
	indexPath := i.file.Name()

	// Read the checkpoint.
	checkpoint, err := readCheckpoint(i.cpPath)
	if err != nil {
		return fmt.Errorf("could not read checkpoint: %w", err)
	}
	// If no checkpoint, then no compaction.
	if checkpoint == 0 {
		return nil
	}

	if _, err = i.Flush(); err != nil {
		return err
	}
	if err = i.Sync(); err != nil {
		return err
	}

	file, err := openFileForScan(indexPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create new index file.
	tmp, err := os.Create(indexPath + ".tmp")
	if err != nil {
		return err
	}
	defer tmp.Close()

	// Copy header from old to new index file and set position within index to
	// read from.
	if _, err := copyHeader(tmp, file); err != nil {
		return fmt.Errorf("cannot copy index header: %w", err)
	}

	// Skip everything up to the checkpoint.
	if _, err = file.Seek(checkpoint, 0); err != nil {
		return fmt.Errorf("cannot seek to checkpoint: %w", err)
	}

	// Copy the remaining index into the new file.
	if _, err = io.Copy(tmp, file); err != nil {
		return fmt.Errorf("cannot copy index to new file: %w", err)
	}

	if err = tmp.Sync(); err != nil {
		return fmt.Errorf("cannot sync new index file: %w", err)
	}

	// Scan the new index file.
	buckets, sizeBuckets, err := scanIndex(tmp.Name(), i.sizeBits, 0, nil, nil)
	if err != nil {
		return fmt.Errorf("failed scanning index: %w", err)
	}

	log.Debugf("Creating compacted index and scanned buckets", "elapsed", time.Since(start))

	start = time.Now()

	// Lock the index.
	i.gcMutex.Lock()
	defer i.gcMutex.Unlock()

	// Flush any new index data.
	if err = i.writer.Flush(); err != nil {
		return err
	}
	if err = i.file.Sync(); err != nil {
		return err
	}

	// Copy any additional index data into the new file.
	n, err := io.Copy(tmp, file)
	if err != nil {
		return fmt.Errorf("cannot copy index to new file: %w", err)
	}
	file.Close()

	// Make sure the new index is written.
	if err = tmp.Sync(); err != nil {
		return fmt.Errorf("cannot sync new index file: %w", err)
	}
	tmp.Close()

	// If additional data written to new index, scan only new data.
	if n != 0 {
		_, _, err = scanIndex(tmp.Name(), i.sizeBits, n, buckets, sizeBuckets)
		if err != nil {
			return fmt.Errorf("failed scanning index: %w", err)
		}
	}

	// Remove checkpoint file.
	if err = os.Remove(i.cpPath); err != nil {
		return fmt.Errorf("cannot remove checkpoint file: %w", err)
	}

	// Replace old index file with new.
	if err = os.Rename(tmp.Name(), file.Name()); err != nil {
		return fmt.Errorf("failed to replace old index file: %w", err)
	}

	newFile, err := openFileRandom(indexPath, os.O_RDWR|os.O_APPEND|os.O_EXCL)
	if err != nil {
		return err
	}

	fi, err := newFile.Stat()
	if err != nil {
		return err
	}

	i.file.Close()
	i.writer.Reset(newFile)

	i.buckets = buckets
	i.sizeBuckets = sizeBuckets
	i.file = newFile
	i.length += types.Position(fi.Size())

	log.Debugf("Finalized compacted index and buckets", "elapsed", time.Since(start))
	return nil
}

func copyHeader(dst io.Writer, src io.Reader) (int64, error) {
	sizeBuffer := make([]byte, SizePrefixSize)
	_, err := io.ReadFull(src, sizeBuffer)
	if err != nil {
		return 0, err
	}
	headerSize := binary.LittleEndian.Uint32(sizeBuffer)
	headerBytes := make([]byte, headerSize)
	if _, err = io.ReadFull(src, headerBytes); err != nil {
		return 0, err
	}
	if _, err = dst.Write(sizeBuffer); err != nil {
		return 0, err
	}
	if _, err = dst.Write(headerBytes); err != nil {
		return 0, err
	}
	return SizePrefixSize + int64(headerSize), nil
}
