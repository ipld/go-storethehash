package mhprimary

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ipld/go-storethehash/store/freelist"
	"github.com/ipld/go-storethehash/store/types"
)

type IndexRemapper struct {
	firstFile   uint32
	maxFileSize uint32
	sizes       []int64
}

func (cp *MultihashPrimary) NewIndexRemapper() (*IndexRemapper, error) {
	header, err := readHeader(cp.headerPath)
	if err != nil {
		return nil, err
	}

	var sizes []int64
	for fileNum := header.FirstFile; fileNum <= cp.fileNum; fileNum++ {
		fi, err := os.Stat(primaryFileName(cp.basePath, fileNum))
		if err != nil {
			if os.IsNotExist(err) {
				break
			}
			return nil, err
		}
		sizes = append(sizes, fi.Size())
	}

	return &IndexRemapper{
		firstFile:   header.FirstFile,
		maxFileSize: cp.maxFileSize,
		sizes:       sizes,
	}, nil
}

func (ir *IndexRemapper) RemapOffset(pos types.Position) (types.Position, error) {
	fileNum := ir.firstFile
	for _, size := range ir.sizes {
		if pos < types.Position(size) {
			break
		}
		pos -= types.Position(size)
		fileNum++
	}
	if pos >= types.Position(ir.maxFileSize) {
		return 0, fmt.Errorf("cannot convert out-of-range primary position: %d", pos)
	}

	return absolutePrimaryPos(pos, fileNum, ir.maxFileSize), nil
}

func upgradePrimary(ctx context.Context, filePath, headerPath string, maxFileSize uint32, freeList *freelist.FreeList) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	_, err := os.Stat(headerPath)
	if !os.IsNotExist(err) {
		// Header already exists, do nothing.
		return false, nil
	}

	if freeList != nil {
		// Instead of remapping all the primary offsets in the freelist, call
		// the garbage collector function to process the freelist and make the
		// primary records deleted. This is safer because it can be re-applied
		// if there is a failure during this phase.
		_, err := processFreeList(ctx, freeList, filePath, 0)
		if err != nil {
			return false, fmt.Errorf("could not apply freelist to primary: %w", err)
		}
	}

	log.Infow("Upgrading primary storage and splitting into separate files", "newVersion", PrimaryVersion, "fileSize", maxFileSize)
	inFile, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// No primary to upgrade.
			return false, nil
		}
		return false, err
	}
	defer inFile.Close()

	fileNum, err := chunkOldPrimary(ctx, inFile, filePath, int64(maxFileSize))
	if err != nil {
		return false, err
	}
	inFile.Close()

	if err = writeHeader(headerPath, newHeader(maxFileSize)); err != nil {
		return false, err
	}

	if err = os.Remove(filePath); err != nil {
		return false, err
	}

	log.Infow("Replaced old primary with multiple files", "replaced", filePath, "files", fileNum)
	log.Infof("Upgraded primary from version 0 to %d", PrimaryVersion)
	return true, nil
}

func chunkOldPrimary(ctx context.Context, file *os.File, name string, fileSizeLimit int64) (uint32, error) {
	fi, err := file.Stat()
	if err != nil {
		return 0, err
	}
	if fi.Size() == 0 {
		return 0, nil
	}

	var fileNum uint32
	outName := primaryFileName(name, fileNum)
	outFile, err := createFileAppend(outName)
	if err != nil {
		return 0, err
	}
	writer := bufio.NewWriter(outFile)

	sizeBuf := make([]byte, sizePrefixSize)
	var written int64
	var count int
	var pos int64
	scratch := make([]byte, 1024)

	for {
		_, err = file.ReadAt(sizeBuf, pos)
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		size := binary.LittleEndian.Uint32(sizeBuf)
		if _, err = writer.Write(sizeBuf); err != nil {
			outFile.Close()
			return 0, err
		}
		pos += sizePrefixSize

		del := false
		if size&deletedBit != 0 {
			size ^= deletedBit
			del = true
		}

		if int(size) > len(scratch) {
			scratch = make([]byte, size)
		}
		data := scratch[:size]

		if !del {
			if _, err = file.ReadAt(data, pos); err != nil {
				return 0, fmt.Errorf("cannot read record data: %w", err)
			}
		}
		_, err := writer.Write(data)
		if err != nil {
			outFile.Close()
			return 0, err
		}
		pos += int64(size)

		written += sizePrefixSize + int64(size)
		if written >= fileSizeLimit {
			if err = writer.Flush(); err != nil {
				return 0, err
			}
			outFile.Close()
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			log.Infow("Upgrade created primary file", "file", filepath.Base(outName))
			fileNum++
			outName = primaryFileName(name, fileNum)
			outFile, err = createFileAppend(outName)
			if err != nil {
				if os.IsNotExist(err) {
					break
				}
				return 0, err
			}
			writer.Reset(outFile)
			written = 0
		}
		count++
	}
	if written != 0 {
		if err = writer.Flush(); err != nil {
			return 0, err
		}
		log.Infow("Upgrade created primary file", "file", filepath.Base(outName))

	}
	outFile.Close()
	return fileNum, nil
}

func createFileAppend(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_TRUNC, 0644)
}
