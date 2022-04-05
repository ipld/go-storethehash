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
	checkpointInterval = 2 * time.Minute
	// checkpointRuntime is the maximum time to run checkpoint advancement if
	// it does not find a current entry to stop on.
	checkpointRuntime = time.Minute
	// compactPercentThreshold is percent of the index that must be eliminated
	// by the checkpoint for compaction to take place.
	compactPercentThreshold = 33
)

// checkpointer is a goroutine that waits for a signal to update the index
// checkpoint, and looks for the next checkpoint if it has not been too soon,
// `checkpointInterval`, since the last time.  The search runs and advances the
// checkpoint until it has been running for `checkpointRuntime` or until it
// finds the next live index entry.  The search runs in a separate goroutine so
// that this function can continue reading the channel used to signal updates.
func (i *Index) checkpointer() {
	cpPath := checkpointPath(i.file.Name())
	cpDone := make(chan struct{})
	var cancel context.CancelFunc
	hasUpdate := true
	t := time.NewTimer(checkpointInterval)

	for {
		select {
		case _, ok := <-i.cpUpdate:
			if !ok {
				// Channel closed; shutting down.
				if cancel != nil {
					cancel()
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

			go func() {
				defer func() {
					cpDone <- struct{}{}
				}()
				var ctx context.Context
				ctx, cancel = context.WithTimeout(context.Background(), checkpointRuntime)
				cp, err := i.updateCheckpoint(ctx, cpPath)
				if err != nil {
					log.Errorw("Failed to update checkpoint", "err", err)
					return
				}

				// Only compact if checkpoint update finished and did not timeout.
				if ctx.Err() == nil {
					// See if the checkpoint reduces the size enough to make it worth it.
					if (cp*100)/int64(i.length) >= compactPercentThreshold {
						log.Infow("Checkpoint reached compaction threshold", "percent", compactPercentThreshold,
							"checkpoint", cp, "total", i.length)
						start := time.Now()
						if err = i.runtimeCompactIndex(); err != nil {
							log.Errorw("Failed to compact index", "err", err)
						}
						log.Debugw("Compaction time", "elapsed", time.Since(start))
					}
				}
			}()
		case <-cpDone:
			// Finished the checkpoint update, cleanup and reset timer.
			cancel()
			cancel = nil
			hasUpdate = false
			t.Reset(checkpointInterval)
		}
	}
}

// updateCheckpoint attempts to advance the checkpoint from its previous
// location. The checkpoint is returned.
func (i *Index) updateCheckpoint(ctx context.Context, cpPath string) (int64, error) {
	checkpoint, err := readCheckpoint(cpPath)
	if err != nil {
		return 0, fmt.Errorf("could not read checkpoint: %w", err)
	}

	newCheckpoint, err := i.nextCheckpoint(ctx, cpPath, checkpoint)
	if err != nil {
		return 0, fmt.Errorf("error finding next checkpoint: %w", err)
	}

	if newCheckpoint == checkpoint {
		log.Debug("no new checkpoint")
		return checkpoint, nil
	}

	if err = writeCheckpoint(cpPath, newCheckpoint); err != nil {
		return 0, err
	}

	log.Debugw("Advanced checkpoint", "from", checkpoint, "to", newCheckpoint)
	return newCheckpoint, nil
}

// nextCheckpoint scans the index starting at the specified checkpoint, and
// returns the location of the first index entry that is still in use.
func (i *Index) nextCheckpoint(ctx context.Context, cpPath string, checkpoint int64) (int64, error) {
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
// that the checkpoint skips over.
func compactIndex(path string, indexSizeBits uint8) error {
	// Read the checkpoint.
	cpPath := checkpointPath(path)
	checkpoint, err := readCheckpoint(cpPath)
	if err != nil {
		return fmt.Errorf("could not read checkpoint: %w", err)
	}
	// If no checkpoint, then no compaction.
	if checkpoint == 0 {
		return nil
	}

	file, err := openFileForScan(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create new index file.
	tmp, err := os.Create(path + ".tmp")
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

	// Make sure the new index is written before removing the old one.
	if err = tmp.Sync(); err != nil {
		return fmt.Errorf("cannot sync new index file: %w", err)
	}

	tmp.Close()
	file.Close()

	// New index does not include anything skipped by the checkpoint, so remove checkpoint file.
	if err = os.Remove(cpPath); err != nil {
		return fmt.Errorf("cannot remove checkpoint file: %w", err)
	}

	// Replace old index file with new.
	if err = os.Rename(tmp.Name(), file.Name()); err != nil {
		return fmt.Errorf("failed to replace old checkpoint file: %w", err)
	}

	return nil
}

// runtimeCompactIndex compacts the index during runtime by locking out any
// modifications to the index and changing to use the new index, and rebuilding
// the buckets, after compaction.
func (i *Index) runtimeCompactIndex() error {
	indexPath := i.file.Name()

	i.gcMutex.Lock()
	defer i.gcMutex.Unlock()

	err := i.writer.Flush()
	if err != nil {
		return err
	}

	if err = i.file.Sync(); err != nil {
		return err
	}

	if err = compactIndex(indexPath, i.sizeBits); err != nil {
		return err
	}

	buckets, sizeBuckets, err := scanIndex(indexPath, i.sizeBits)
	if err != nil {
		return fmt.Errorf("failed scanning index: %w", err)
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
