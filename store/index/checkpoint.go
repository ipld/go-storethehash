package index

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/ipld/go-storethehash/store/types"
)

const (
	// Try to advance the checkpoint this often.
	checkpointInterval = time.Minute
	// Compact when this percent of entries is deleted.
	compactPercentThreshold = 10
)

func (i *Index) checkpointer() {
	cpPath := checkpointPath(i.file.Name())
	hasUpdate := true
	cpDone := make(chan struct{})
	var cancel context.CancelFunc
	t := time.NewTimer(checkpointInterval)

	for {
		select {
		case _, ok := <-i.cpUpdate:
			if !ok {
				if cancel != nil {
					cancel()
					<-cpDone
				}
				return
			}
			hasUpdate = true
		case <-t.C:
			if hasUpdate {
				var ctx context.Context
				ctx, cancel = context.WithCancel(context.Background())
				go func() {
					if err := i.updateCheckpoint(ctx, cpPath); err != nil {
						fmt.Fprintln(os.Stderr, "failed to update checkpoint:", err)
					}
					cpDone <- struct{}{}
				}()
			} else {
				t.Reset(checkpointInterval)
			}
		case <-cpDone:
			cancel()
			cancel = nil
			t.Reset(checkpointInterval)
			hasUpdate = false
		}
	}
}

func (i *Index) updateCheckpoint(ctx context.Context, cpPath string) error {
	checkpoint, err := readCheckpoint(cpPath)
	if err != nil {
		return err
	}

	newCheckpoint, err := i.nextCheckpoint(ctx, cpPath, checkpoint)
	if err != nil {
		return err
	}
	if newCheckpoint == checkpoint {
		return nil
	}

	return writeCheckpoint(cpPath, newCheckpoint)
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
			return 0, ctx.Err()
		}
		if _, err = io.ReadFull(inBuf, sizeBuffer); err != nil {
			if err == io.EOF {
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
				// The file is corrupt. Though it's not a problem, just take
				// the data we are able to use and move on.
				if _, err = file.Seek(0, 2); err != nil {
					return 0, err
				}
				break
			}
			return 0, err
		}

		bucketPrefix := BucketIndex(binary.LittleEndian.Uint32(data))
		i.bucketLk.Lock()
		bucketPos, err := i.buckets.Get(bucketPrefix)
		i.bucketLk.Unlock()
		if err != nil {
			return 0, err
		}
		if int64(bucketPos) == pos {
			// First position in use. Do not advance checkpoint.
			break
		}
		// Advance checkpoint
		checkpoint = iterPos
	}
	return checkpoint, nil
}

func compactIndex(path string, indexSizeBits uint8) error {
	file, err := openFileForScan(path)
	if err != nil {
		return err
	}
	defer file.Close()

	header, bytesRead, err := ReadHeader(file)
	if err != nil {
		return err
	}
	if header.BucketsBits != indexSizeBits {
		return types.ErrIndexWrongBitSize{header.BucketsBits, indexSizeBits}
	}

	iterPos := int64(bytesRead)

	cpPath := checkpointPath(path)
	checkpoint, err := readCheckpoint(cpPath)
	if err != nil {
		return err
	}
	// If there is a checkpoint, skip everything up to the checkpoint.
	if checkpoint != 0 {
		if _, err = file.Seek(checkpoint, 0); err != nil {
			return err
		}
		iterPos = checkpoint
	}

	buckets, err := NewBuckets(indexSizeBits)
	if err != nil {
		return err
	}

	inBuf := bufio.NewReaderSize(file, 256*1024)
	sizeBuffer := make([]byte, SizePrefixSize)
	scratch := make([]byte, 256)
	var count, del int

	// First iteration determines buckets still in use.
	for {
		if _, err = io.ReadFull(inBuf, sizeBuffer); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		size := binary.LittleEndian.Uint32(sizeBuffer)

		pos := iterPos + SizePrefixSize
		iterPos = pos + int64(size)
		if int(size) > len(scratch) {
			scratch = make([]byte, size)
		}
		data := scratch[:size]
		if _, err = io.ReadFull(inBuf, data); err != nil {
			if err == io.EOF {
				// The file is corrupt. Though it's not a problem, just take
				// the data we are able to use and move on.
				if _, err = file.Seek(0, 2); err != nil {
					return err
				}
				break
			}
			return err
		}

		bucketPrefix := BucketIndex(binary.LittleEndian.Uint32(data))
		prevPos, err := buckets.Get(bucketPrefix)
		if err != nil {
			return err
		}
		// If there is a previous position being replaced, then count a delete.
		if prevPos != 0 {
			del++
		}
		if err = buckets.Put(bucketPrefix, types.Position(pos)); err != nil {
			return err
		}
		count++
	}
	if count == 0 {
		// Enpty index.
		return nil
	}
	if del == count {
		return errors.New("compact failed, no data to keep")
	}

	var removeToCheckpoint bool

	// Nothing or not enough to compact.
	if del == 0 || ((del*100)/count) < compactPercentThreshold {
		fi, err := file.Stat()
		if err != nil {
			return err
		}
		if (checkpoint*100)/fi.Size() < compactPercentThreshold {
			// Not enought to compact.
			return nil
		}
		// Remove everything up to checkpoint.
		removeToCheckpoint = true
	}

	tmp, err := os.Create(path + ".tmp")
	if err != nil {
		return err
	}
	defer tmp.Close()

	// Go back to the begining of the index file.
	if _, err = file.Seek(0, 0); err != nil {
		return err
	}
	inBuf.Reset(file)

	// Copy header from old to new index file and set position within index to
	// read from.
	iterPos, err = copyHeader(tmp, file)
	if err != nil {
		return err
	}
	if checkpoint != 0 {
		if _, err = file.Seek(checkpoint, 0); err != nil {
			return err
		}
		iterPos = checkpoint
	}

	if removeToCheckpoint {
		_, err := io.Copy(tmp, inBuf)
		if err != nil {
			return err
		}
	} else {
		outBuf := bufio.NewWriter(tmp)
		del = 0

		// Second iteration writes only buckets still in use to new index file.
		for {
			_, err = io.ReadFull(inBuf, sizeBuffer)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			size := binary.LittleEndian.Uint32(sizeBuffer)

			pos := iterPos + SizePrefixSize
			iterPos = pos + int64(size)
			data := scratch[:size]
			_, err = io.ReadFull(inBuf, data)
			if err != nil {
				if err == io.EOF {
					// The file is corrupt. Though it's not a problem, just take
					// the data we are able to use and move on.
					if _, err = file.Seek(0, 2); err != nil {
						return err
					}
					break
				}
				return err
			}

			bucketPrefix := BucketIndex(binary.LittleEndian.Uint32(data))
			bucketPos, err := buckets.Get(bucketPrefix)
			if err != nil {
				return err
			}
			// If the current position is in bucket, then the entry is current.
			if int64(bucketPos) == pos {
				outBuf.Write(sizeBuffer)
				outBuf.Write(data)
			} else {
				del++
			}
		}
		outBuf.Flush()
	}

	if err = tmp.Sync(); err != nil {
		return err
	}
	tmp.Close()
	file.Close()

	// New index does not include anything skipped by the checkpoint, so reset
	// the checkpoint.
	if err = writeCheckpoint(cpPath, 0); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}

	// Replace old index file with new.
	return os.Rename(tmp.Name(), file.Name())
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
