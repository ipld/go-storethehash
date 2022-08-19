package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-storethehash/store/freelist"
	"github.com/ipld/go-storethehash/store/index"
	"github.com/ipld/go-storethehash/store/primary"
	cidprimary "github.com/ipld/go-storethehash/store/primary/cid"
	mhprimary "github.com/ipld/go-storethehash/store/primary/multihash"
	"github.com/ipld/go-storethehash/store/types"
	"github.com/multiformats/go-multihash"
)

const (
	// sizePrefixSize is the number of bytes used for the size prefix of a
	// record list.
	sizePrefixSize = 4

	// deletedBit is the highest order bit in the uint32 size part of a file
	// record, and when set, indicates that the record is deleted. This means
	// that record sizes must be less than 2^31.
	deletedBit = uint32(1 << 31)
)

func main() {
	var (
		dir          string
		indexBits    int
		keep         bool
		primaryType  string
		showIndex    bool
		undoFreelist bool
	)
	flag.StringVar(&dir, "dir", "", "valuestore directory")
	flag.IntVar(&indexBits, "bits", 0, "bit prefix length for index")
	flag.BoolVar(&keep, "keep", false, "no not replace original valuestore")
	flag.StringVar(&primaryType, "primary", "mh", "primary type {mh, cid}")
	flag.BoolVar(&showIndex, "show-index", false, "show index records and exit")
	flag.BoolVar(&undoFreelist, "undo-freelist", false, "undo applying freelist to primary")
	flag.Parse()

	if dir == "" {
		fmt.Fprintln(os.Stderr, "missing dir")
		os.Exit(1)
	}

	indexPath := filepath.Join(dir, "storethehash.index")
	if showIndex {
		err := lookIndexFile(indexPath, 0)
		if err != nil {
			fmt.Fprintln(os.Stderr, "could not scan index file", err)
			os.Exit(1)
		}
		return
	}

	primaryPath := filepath.Join(dir, "storethehash.data")
	freelistPath := indexPath + ".free"

	if undoFreelist {
		err := applyFreeList(context.TODO(), freelistPath, primaryPath, true)
		if err != nil {
			fmt.Fprintln(os.Stderr, "could not apply freelist to primary", err)
			os.Exit(1)
		}
		return
	}

	if indexBits == 0 {
		fmt.Fprintln(os.Stderr, "missing bits")
		os.Exit(1)
	}
	if indexBits > 32 || indexBits < 8 {
		fmt.Fprintln(os.Stderr, "bits must be between 8 and 32")
		os.Exit(1)
	}

	err := rebuildIndex(context.Background(), indexPath, primaryPath, freelistPath, uint8(indexBits), keep, primaryType)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func rebuildIndex(ctx context.Context, indexPath, primaryPath, freelistPath string, indexSizeBits uint8, keep bool, primaryType string) error {
	switch primaryType {
	case "mh", "cid":
	case "multihash":
		primaryType = "mh"
	default:
		return fmt.Errorf("unsupported primary type: %s", primaryType)
	}

	oldDir := filepath.Dir(primaryPath)
	newDir := oldDir + ".new"
	if err := os.Mkdir(newDir, 0750); err != nil {
		return err
	}

	// Before editing any file, check that there is enough memory to start the index.
	newIndexPath := filepath.Join(newDir, filepath.Base(indexPath))
	idx, err := index.Open(ctx, newIndexPath, nil, indexSizeBits, 0, 10*time.Minute, 3*time.Minute)
	if err != nil {
		return err
	}
	idx.Close()

	// Applying the freelist is necessary to prevent putting deleted data back
	// into the index.
	err = applyFreeList(ctx, freelistPath, primaryPath, false)
	if err != nil {
		return fmt.Errorf("could not apply freelist to primary: %w", err)
	}

	newPrimaryPath := filepath.Join(newDir, filepath.Base(primaryPath))
	err = copyPrimary(ctx, primaryPath, newPrimaryPath, keep, primaryType)
	if err != nil {
		return fmt.Errorf("could not copy and records from old to new primary: %w", err)
	}

	log.Println("Rebuilding index from new primary")

	var primaryStore primary.PrimaryStorage
	switch primaryType {
	case "mh":
		primaryStore, err = mhprimary.Open(newPrimaryPath)
	case "cid":
		primaryStore, err = cidprimary.Open(newPrimaryPath)
	}
	if err != nil {
		return fmt.Errorf("cannot open %s primary at %s: %w", primaryType, newPrimaryPath, err)
	}

	idx, err = index.Open(ctx, newIndexPath, primaryStore, indexSizeBits, 0, 0, 0)
	if err != nil {
		return err
	}
	defer idx.Close()

	inFile, err := os.Open(newPrimaryPath)
	if err != nil {
		return fmt.Errorf("cannot open old primary file: %w", err)
	}
	defer inFile.Close()

	fi, err := inFile.Stat()
	if err != nil {
		return fmt.Errorf("cannot stat old primary file: %w", err)
	}
	inSize := fi.Size()
	if inSize == 0 {
		// File was already truncated to 0 size, but was not yet removed.
		fmt.Fprintln(os.Stderr, "Primary file is empty", "file:", newPrimaryPath)
		return nil
	}

	percentIncr := int64(1)
	nextPercent := percentIncr
	var count int

	sizeBuf := make([]byte, sizePrefixSize)
	scratch := make([]byte, 1024)
	var pos int64
	for {
		if _, err = inFile.ReadAt(sizeBuf, pos); err != nil {
			if err == io.EOF {
				// Finished reading entire primary.
				break
			}
			return err
		}
		size := binary.LittleEndian.Uint32(sizeBuf)

		if size&deletedBit != 0 {
			pos += sizePrefixSize + int64(size^deletedBit)
			continue
		}

		if int(size) > len(scratch) {
			scratch = make([]byte, size)
		}
		data := scratch[:size]

		if _, err = inFile.ReadAt(data, pos+sizePrefixSize); err != nil {
			return fmt.Errorf("cannot read record data: %w", err)
		}

		var key []byte
		if primaryType == "mh" {
			br := bytes.NewReader(data)
			mhr := multihash.NewReader(br)
			h, err := mhr.ReadMultihash()
			if err != nil {
				return fmt.Errorf("error reading multihash from data: %w", err)
			}
			key = h
		} else {
			_, c, err := cid.CidFromBytes(data)
			if err != nil {
				return fmt.Errorf("error reading cid from data: %w", err)
			}
			key = c.Bytes()
		}

		// Get the key in primary storage
		indexKey, err := primaryStore.IndexKey(key)
		if err != nil {
			return err
		}

		// The offset points to the start of the record, which is the size
		// field. The size given in the block should be what is encoded in the
		// record's size field.
		blk := types.Block{Offset: types.Position(pos), Size: types.Size(size)}
		if err = idx.Put(indexKey, blk); err != nil {
			return err
		}

		pos += sizePrefixSize + int64(size)

		count++

		if count&1023 == 0 {
			_, err := idx.Flush()
			if err != nil {
				return fmt.Errorf("error flushing index: %w", err)
			}
		}

		percent := 100 * pos / inSize
		if percent >= nextPercent {
			log.Printf("Indexed %d primary records, %d%% complete\n", count, percent)
			for nextPercent <= percent {
				nextPercent += percentIncr
			}
		}
	}
	idx.Close()
	primaryStore.Close()

	var finalDir string
	if keep {
		finalDir = newDir
	} else {
		log.Println("Removing", oldDir)
		if err = os.RemoveAll(oldDir); err != nil {
			return err
		}
		log.Println("Renaming", newDir, "to", oldDir)
		if err = os.Rename(newDir, oldDir); err != nil {
			return err
		}
		finalDir = oldDir
	}
	log.Println("Done rebuilding valuestore in", finalDir)

	return nil
}

func copyPrimary(ctx context.Context, primaryPath, newPrimaryPath string, keep bool, primaryType string) error {
	log.Println("Copying primary and removing deleted records")
	var err error
	var inFile *os.File
	if keep {
		inFile, err = os.OpenFile(primaryPath, os.O_RDWR, 0644)
	} else {
		inFile, err = os.Open(primaryPath)
	}
	if err != nil {
		return fmt.Errorf("cannot open old primary file: %w", err)
	}
	defer inFile.Close()

	outFile, err := os.Create(newPrimaryPath)
	if err != nil {
		return fmt.Errorf("cannot open new primary file: %w", err)
	}
	defer outFile.Close()
	log.Println("Created new primary at", newPrimaryPath)

	fi, err := inFile.Stat()
	if err != nil {
		return fmt.Errorf("cannot stat old primary file: %w", err)
	}
	inSize := fi.Size()
	if inSize == 0 {
		fmt.Fprintln(os.Stderr, "Primary file is empty", "file:", primaryPath)
		return nil
	}

	percentIncr := int64(10)
	nextPercent := percentIncr
	var count int

	writer := bufio.NewWriter(outFile)
	sizeBuf := make([]byte, sizePrefixSize)
	scratch := make([]byte, 1024)
	var pos int64
	for {
		if _, err = inFile.ReadAt(sizeBuf, pos); err != nil {
			if err == io.EOF {
				// Finished reading entire primary.
				break
			}
			return err
		}
		size := binary.LittleEndian.Uint32(sizeBuf)

		if size&deletedBit != 0 {
			size ^= deletedBit
			// If keeping the original files, restore the deleted marker.
			if keep {
				binary.LittleEndian.PutUint32(sizeBuf, size)
				_, err = inFile.WriteAt(sizeBuf, pos)
				if err != nil {
					return fmt.Errorf("cannot write to primary file %s: %w", inFile.Name(), err)
				}
			}
			pos += sizePrefixSize + int64(size)
			continue
		}
		if _, err = writer.Write(sizeBuf); err != nil {
			return err
		}
		pos += sizePrefixSize

		if int(size) > len(scratch) {
			scratch = make([]byte, size)
		}
		data := scratch[:size]

		if _, err = inFile.ReadAt(data, pos); err != nil {
			return fmt.Errorf("cannot read record data: %w", err)
		}

		_, err := writer.Write(data)
		if err != nil {
			return err
		}

		pos += int64(size)

		count++
		percent := 100 * pos / inSize
		if percent >= nextPercent {
			log.Printf("Copied %d records to new primary, %d%% complete\n", count, percent)
			for nextPercent <= percent {
				nextPercent += percentIncr
			}
		}
	}
	if err = writer.Flush(); err != nil {
		return err
	}
	outFile.Close()

	log.Println("Finished copying primary and removing deleted records")
	return nil
}

// applyFreeList reads the freelist and marks the locations in the old primary file
// as dead by setting the deleted bit in the record size field.
func applyFreeList(ctx context.Context, flPath, primaryPath string, undo bool) error {
	fi, err := os.Stat(flPath)
	if err != nil {
		return fmt.Errorf("cannot stat freelist gc file: %w", err)
	}
	flSize := fi.Size()
	if flSize == 0 {
		return nil
	}

	var count int
	log.Print("Applying freelist to primary storage")

	flFile, err := os.OpenFile(flPath, os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening freelist gc file: %w", err)
	}
	defer flFile.Close()

	primaryFile, err := os.OpenFile(primaryPath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("cannot open primary file %s: %w", primaryPath, err)
	}
	defer primaryFile.Close()

	fi, err = primaryFile.Stat()
	if err != nil {
		return fmt.Errorf("cannot stat primary file %s: %w", primaryFile.Name(), err)
	}
	primarySize := fi.Size()

	total := int(flSize / (types.OffBytesLen + types.SizeBytesLen))
	flIter := freelist.NewIterator(bufio.NewReader(flFile))
	sizeBuf := make([]byte, sizePrefixSize)
	percentIncr := 10
	nextPercent := percentIncr
	for {
		free, err := flIter.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("error reading freelist: %w", err)
		}

		offset := int64(free.Offset)

		if offset > primarySize {
			fmt.Fprintln(os.Stderr, "freelist record has out-of-range primary offset", "offset:", offset, "fileSize:", primarySize)
			continue // skip bad freelist entry
		}

		if _, err = primaryFile.ReadAt(sizeBuf, offset); err != nil {
			return err
		}
		recSize := binary.LittleEndian.Uint32(sizeBuf)
		if (recSize & (deletedBit - 1)) != uint32(free.Size) {
			fmt.Fprintln(os.Stderr, "Record size in primary does not match size in freelist", "primaryRecordSize", recSize, "freelistRecordSize", free.Size, "file", flFile.Name(), "offset", offset)
		}

		if undo {
			if recSize&deletedBit == 0 {
				// already undeleted
				continue
			}
			// Mark the record as not-deleted by clearing the highest bit in the
			// size. This assumes that the record size is < 2^31.
			recSize ^= deletedBit
		} else {
			if recSize&deletedBit != 0 {
				// Already deleted.
				continue
			}
			// Mark the record as deleted by setting the highest bit in the
			// size. This assumes that the record size is < 2^31.
			recSize |= deletedBit
		}

		binary.LittleEndian.PutUint32(sizeBuf, recSize)
		_, err = primaryFile.WriteAt(sizeBuf, int64(offset))
		if err != nil {
			return fmt.Errorf("cannot write to primary file %s: %w", flFile.Name(), err)
		}

		count++

		// Log every 5 minutes, do time check every 2^20 records.
		percent := 100 * count / total
		if percent >= nextPercent {
			log.Printf("Processed %d of %d freelist records: %d%% done", count, total, percent)
			nextPercent += percentIncr
		}
	}
	var op string
	if undo {
		op = "not deleted"
	} else {
		op = "deleted"
	}
	log.Printf("Marked %d primary records from freelist as %s", count, op)
	return nil
}

func lookIndexFile(basePath string, fileNum uint32) error {
	maxFileSize := uint32(1024 * 1024 * 1024)
	indexPath := fmt.Sprintf("%s.%d", basePath, fileNum)

	file, err := os.Open(indexPath)
	if err != nil {
		return err
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return fmt.Errorf("cannot stat index file: %w", err)
	}
	if fi.Size() == 0 {
		return nil
	}

	sizeBuffer := make([]byte, sizePrefixSize)
	scratch := make([]byte, 256)
	var pos int64
	for {
		if _, err = file.ReadAt(sizeBuffer, pos); err != nil {
			if err == io.EOF {
				// Finished reading entire index.
				break
			}
			return err
		}
		pos += sizePrefixSize

		size := binary.LittleEndian.Uint32(sizeBuffer)
		if size&deletedBit != 0 {
			// Record is deleted, so skip.
			pos += int64(size ^ deletedBit)
			continue
		}

		if int(size) > len(scratch) {
			scratch = make([]byte, size)
		}
		data := scratch[:size]
		if _, err = file.ReadAt(data, pos); err != nil {
			return err
		}
		bucketIndex := binary.LittleEndian.Uint32(data)
		bucketPos := localPosToBucketPos(pos, fileNum, maxFileSize)
		fmt.Printf("bucket[%08d] --> %d (size=%d) data: %v\n", bucketIndex, bucketPos, len(data), data)
		rl := index.NewRecordList(data)
		ri := rl.Iter()
		for !ri.Done() {
			rec := ri.Next()
			fmt.Printf("  rec: %+v\n", rec)
		}

		pos += int64(size)
	}
	return nil
}

func localPosToBucketPos(pos int64, fileNum, maxFileSize uint32) types.Position {
	// Valid position must be non-zero, at least sizePrefixSize.
	if pos == 0 {
		panic("invalid local offset")
	}
	// fileNum is a 32bit value and will wrap at 4GiB, So 4294967296 is the
	// maximum number of index files possible.
	return types.Position(fileNum)*types.Position(maxFileSize) + types.Position(pos)
}
