package index

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/ipld/go-storethehash/store/types"
)

func upgradeIndex(name, headerPath string) error {
	inFile, err := os.Open(name)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer inFile.Close()

	version, bucketBits, headerSize, err := readOldHeader(inFile)
	if err != nil {
		return err
	}
	if version != 2 {
		return fmt.Errorf("cannot convert unknown header version: %d", version)
	}

	checkpoint, err := findCheckpoint(inFile, bucketBits, headerSize)
	if err != nil {
		return err
	}
	log.Infof("Skipped %d bytes of unused index data", checkpoint-headerSize)

	_, err = inFile.Seek(checkpoint, 0)
	if err != nil {
		return err
	}

	fileNum, err := chunkOldIndex(inFile, name, maxFileSize)
	if err != nil {
		return err
	}
	inFile.Close()

	if err = writeHeader(headerPath, newHeader(bucketBits)); err != nil {
		return err
	}

	if err = os.Remove(name); err != nil {
		return err
	}

	log.Infow("Replaced old index with multiple files", "replaced", name, "files", fileNum)
	log.Infof("Upgraded index from version 2 to %d", IndexVersion)
	return nil
}

func readOldHeader(file *os.File) (byte, byte, int64, error) {
	headerSizeBuffer := make([]byte, SizePrefixSize)
	_, err := io.ReadFull(file, headerSizeBuffer)
	if err != nil {
		return 0, 0, 0, err
	}
	headerSize := binary.LittleEndian.Uint32(headerSizeBuffer)
	headerBytes := make([]byte, headerSize)
	_, err = io.ReadFull(file, headerBytes)
	if err != nil {
		return 0, 0, 0, err
	}
	version := headerBytes[0]
	bucketBits := headerBytes[1]

	return version, bucketBits, int64(SizePrefixSize + headerSize), nil
}

func chunkOldIndex(file *os.File, name string, fileSizeLimit int64) (uint32, error) {
	var fileNum uint32
	outName := indexFileName(name, fileNum)
	outFile, err := createFileAppend(outName)
	if err != nil {
		return 0, err
	}
	writer := bufio.NewWriter(outFile)
	reader := bufio.NewReaderSize(file, 32*1024)

	sizeBuffer := make([]byte, SizePrefixSize)
	var written int64
	for {
		_, err = io.ReadFull(reader, sizeBuffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		size := binary.LittleEndian.Uint32(sizeBuffer)
		if _, err = writer.Write(sizeBuffer); err != nil {
			outFile.Close()
			return 0, err
		}
		n, err := io.CopyN(writer, reader, int64(size))
		if err != nil {
			outFile.Close()
			return 0, err
		}
		if n != int64(size) {
			writer.Flush()
			outFile.Close()
			return 0, fmt.Errorf("count not read complete entry from index")
		}
		written += SizePrefixSize + int64(size)
		if written > fileSizeLimit {
			if err = writer.Flush(); err != nil {
				return 0, err
			}
			outFile.Close()
			log.Infof("Upgrade created index file %s", outName)
			fileNum++
			outName = indexFileName(name, fileNum)
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
	}
	if written != 0 {
		if err = writer.Flush(); err != nil {
			return 0, err
		}
		log.Infof("Upgrade created index file %s", outName)

	}
	outFile.Close()
	return fileNum, nil
}

func createFileAppend(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_TRUNC, 0644)
}

func findCheckpoint(file *os.File, indexSizeBits uint8, headerSize int64) (int64, error) {
	_, err := file.Seek(headerSize, 0)
	if err != nil {
		return 0, err
	}

	buckets, err := NewBuckets(indexSizeBits)
	if err != nil {
		return 0, err
	}

	log.Info("Scanning old index")
	inBuf := bufio.NewReaderSize(file, 32*1024)
	iterPos := headerSize
	sizeBuffer := make([]byte, SizePrefixSize)
	scratch := make([]byte, 256)
	for {
		_, err = io.ReadFull(inBuf, sizeBuffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		size := binary.LittleEndian.Uint32(sizeBuffer)

		pos := iterPos + SizePrefixSize
		iterPos = pos + int64(size)
		if int(size) > len(scratch) {
			scratch = make([]byte, size)
		}
		data := scratch[:size]
		_, err = io.ReadFull(inBuf, data)
		if err != nil {
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
		err = buckets.Put(bucketPrefix, types.Position(pos))
		if err != nil {
			return 0, err
		}
	}

	log.Info("Skipping unused data in old index")
	if _, err = file.Seek(headerSize, 0); err != nil {
		return 0, err
	}

	inBuf.Reset(file)
	iterPos = headerSize
	checkpoint := iterPos
	for {
		_, err := io.ReadFull(inBuf, sizeBuffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		size := binary.LittleEndian.Uint32(sizeBuffer)

		pos := iterPos + int64(SizePrefixSize)
		iterPos = pos + int64(size)
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
		bucketPos, err := buckets.Get(bucketPrefix)
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
