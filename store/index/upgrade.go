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
		return fmt.Errorf("cannot convert unknown header version: %d", version)
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

func readOldHeader(file *os.File) (byte, byte, types.Position, error) {
	headerSizeBuffer := make([]byte, sizePrefixSize)
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

	return version, bucketBits, types.Position(sizePrefixSize + headerSize), nil
}

func chunkOldIndex(file *os.File, name string, fileSizeLimit int64) (uint32, error) {
	var fileNum uint32
	outName := indexFileName(name, fileNum)
	outFile, err := createFileAppend(outName)
	if err != nil {
		return 0, err
	}
	writer := bufio.NewWriterSize(outFile, indexBufferSize)
	reader := bufio.NewReaderSize(file, indexBufferSize)

	sizeBuffer := make([]byte, sizePrefixSize)
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
		written += sizePrefixSize + int64(size)
		if written >= fileSizeLimit {
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
