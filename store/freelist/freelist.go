package freelist

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/ipld/go-storethehash/store/types"
)

const CIDSizePrefix = 4

// A primary storage that is CID aware.
type FreeList struct {
	file            *os.File
	writer          *bufio.Writer
	outstandingWork types.Work
	blockPool       []types.Block
	poolLk          sync.RWMutex
	flushLock       sync.Mutex
}

const (
	// blockBufferSize is the size of I/O buffers. If has the same size as the
	// linux pipe size.
	blockBufferSize = 16 * 4096
	// blockPoolSize is the size of the freelist cache.
	blockPoolSize = 1024
)

func Open(path string) (*FreeList, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return &FreeList{
		file:      file,
		writer:    bufio.NewWriterSize(file, blockBufferSize),
		blockPool: make([]types.Block, 0, blockPoolSize),
	}, nil
}

func (cp *FreeList) Put(blk types.Block) error {
	cp.poolLk.Lock()
	defer cp.poolLk.Unlock()
	cp.blockPool = append(cp.blockPool, blk)
	// Offset = 8bytes + Size = 4bytes = 12 Bytes
	cp.outstandingWork += types.Work(types.SizeBytesLen + types.OffBytesLen)
	return nil
}

func (cp *FreeList) flushBlock(blk types.Block) (types.Work, error) {
	sizeBuf := make([]byte, types.SizeBytesLen)
	offBuf := make([]byte, types.OffBytesLen)
	// NOTE: If Position or Size types change, this needs to change.
	binary.LittleEndian.PutUint64(offBuf, uint64(blk.Offset))
	binary.LittleEndian.PutUint32(sizeBuf, uint32(blk.Size))
	// We append offset to size in free list
	if _, err := cp.writer.Write(offBuf); err != nil {
		return 0, err
	}
	if _, err := cp.writer.Write(sizeBuf); err != nil {
		return 0, err
	}
	return types.Work(types.SizeBytesLen + types.OffBytesLen), nil
}

// Flush writes outstanding work and buffered data to the freelist file.
func (cp *FreeList) Flush() (types.Work, error) {
	cp.flushLock.Lock()
	defer cp.flushLock.Unlock()

	cp.poolLk.Lock()
	if len(cp.blockPool) == 0 {
		cp.poolLk.Unlock()
		return 0, nil
	}
	blocks := cp.blockPool
	cp.blockPool = make([]types.Block, 0, blockPoolSize)
	cp.outstandingWork = 0
	cp.poolLk.Unlock()

	// The pool lock is released allowing Put to write to nextPool. The
	// flushLock is still held, preventing concurrent flushes from changing the
	// pool or accessing writer.

	var work types.Work
	for _, record := range blocks {
		blockWork, err := cp.flushBlock(record)
		if err != nil {
			return 0, err
		}
		work += blockWork
	}
	err := cp.writer.Flush()
	if err != nil {
		return 0, fmt.Errorf("cannot flush data to freelist file %s: %w", cp.file.Name(), err)
	}

	return work, nil
}

// Sync commits the contents of the freelist file to disk. Flush should be
// called before calling Sync.
func (cp *FreeList) Sync() error {
	return cp.file.Sync()
}

// Close calls Flush to write work and data to the freelist file, and then
// closes the file.
func (cp *FreeList) Close() error {
	_, err := cp.Flush()
	if err != nil {
		cp.file.Close()
		return err
	}
	return cp.file.Close()
}

func (cp *FreeList) OutstandingWork() types.Work {
	cp.poolLk.RLock()
	defer cp.poolLk.RUnlock()
	return cp.outstandingWork
}

func (cp *FreeList) Iter() (*Iterator, error) {
	return NewIter(cp.file), nil
}

func NewIter(reader *os.File) *Iterator {
	return &Iterator{reader, 0}
}

type Iterator struct {
	reader *os.File
	pos    types.Position
}

func (cpi *Iterator) Next() (*types.Block, error) {
	sizeBuf := make([]byte, types.SizeBytesLen)
	offBuf := make([]byte, types.OffBytesLen)
	_, err := cpi.reader.ReadAt(offBuf, int64(cpi.pos))
	if err != nil {
		return nil, err
	}
	cpi.pos += types.OffBytesLen
	offset := binary.LittleEndian.Uint64(offBuf)

	_, err = cpi.reader.ReadAt(sizeBuf, int64(cpi.pos))
	if err != nil {
		return nil, err
	}
	cpi.pos += types.SizeBytesLen
	size := binary.LittleEndian.Uint32(sizeBuf)
	return &types.Block{Size: types.Size(size), Offset: types.Position(offset)}, nil
}

// StorageSize returns bytes of storage used by the freelist.
func (fl *FreeList) StorageSize() (int64, error) {
	fi, err := fl.file.Stat()
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	return fi.Size(), nil
}

func (cp *FreeList) Rotate() (string, error) {
	fileName := cp.file.Name()
	workFilePath := fileName + ".work"
	_, err := os.Stat(workFilePath)
	if !os.IsNotExist(err) {
		if err != nil {
			return "", err
		}
		return workFilePath, nil
	}

	_, err = cp.Flush()
	if err != nil {
		return "", err
	}

	cp.poolLk.RLock()
	defer cp.poolLk.RUnlock()

	cp.file.Close()
	err = os.Rename(fileName, workFilePath)
	if err != nil {
		return "", err
	}

	cp.file, err = os.OpenFile(fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return "", err
	}
	cp.writer.Reset(cp.file)

	return workFilePath, nil
}
