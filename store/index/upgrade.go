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

	version, bucketBits, _, err := readOldHeader(inFile)
	if err != nil {
		return err
	}
	if version != 2 {
		return fmt.Errorf("cannot convert unknown header version")
	}

	fileNum, err := chunkOldIndex(inFile, name)
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

func readOldHeader(file *os.File) (byte, byte, types.Position, error) {
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

	return version, bucketBits, types.Position(SizePrefixSize + headerSize), nil
}

func chunkOldIndex(file *os.File, name string) (uint32, error) {
	var fileNum uint32
	outName := indexFileName(name, fileNum)
	outFile, err := openNewFileAppend(outName)
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
		if written > indexFileSizeLimit {
			if err = writer.Flush(); err != nil {
				return 0, err
			}
			outFile.Close()
			// Write this to stdout so that a human user knows that the indexer
			// is busy upgrading, without including this with in log output.
			fmt.Println("Created index file", outName)
			fileNum++
			outName = indexFileName(name, fileNum)
			outFile, err = openNewFileAppend(outName)
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
		outFile.Close()
		fmt.Println("Created index file", outName)
	}
	return fileNum, nil
}
