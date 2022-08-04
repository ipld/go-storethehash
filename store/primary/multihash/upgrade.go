package mhprimary

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

func upgradePrimary(ctx context.Context, name, headerPath string, maxFileSize uint32) (int, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	_, err := os.Stat(headerPath)
	if !os.IsNotExist(err) {
		// Header already exists, do nothing.
		return 0, nil
	}

	inFile, err := os.Open(name)
	if err != nil {
		if os.IsNotExist(err) {
			// No primary to upgrade.
			return 0, nil
		}
		return 0, err
	}
	defer inFile.Close()

	fileNum, err := chunkOldPrimary(ctx, inFile, name, int64(maxFileSize))
	if err != nil {
		return 0, err
	}
	inFile.Close()

	if err = writeHeader(headerPath, newHeader(maxFileSize)); err != nil {
		return 0, err
	}

	if err = os.Remove(name); err != nil {
		return 0, err
	}

	log.Infow("Replaced old primary with multiple files", "replaced", name, "files", fileNum)
	log.Infof("Upgraded primary from version 0 to %d", PrimaryVersion)
	return int(fileNum), nil
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
	reader := bufio.NewReader(file)

	sizeBuffer := make([]byte, sizePrefixSize)
	var written int64
	var count int
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
			return 0, fmt.Errorf("count not read complete entry from primary")
		}
		written += sizePrefixSize + int64(size)
		if written >= fileSizeLimit {
			if err = writer.Flush(); err != nil {
				return 0, err
			}
			outFile.Close()
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			log.Infof("Upgrade created primary file %s", outName)
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
		log.Infof("Upgrade created primary file %s", outName)

	}
	outFile.Close()
	return fileNum, nil
}

func createFileAppend(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_TRUNC, 0644)
}
