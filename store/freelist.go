package store

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

const CIDSizePrefix = 4

// A primary storage that is CID aware.
type FreeList struct {
	file              *os.File
	writer            *bufio.Writer
	outstandingWork   Work
	curPool, nextPool blockPool
	poolLk            sync.RWMutex
}

const blockBufferSize = 32 * 4096
const blockPoolSize = 1024

type blockPool struct {
	blocks []Block
}

func newBlockPool() blockPool {
	return blockPool{
		blocks: make([]Block, 0, blockPoolSize),
	}
}

func OpenFreeList(path string) (*FreeList, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return &FreeList{
		file:     file,
		writer:   bufio.NewWriterSize(file, blockBufferSize),
		curPool:  newBlockPool(),
		nextPool: newBlockPool(),
	}, nil
}

func (cp *FreeList) Put(blk Block) error {
	cp.poolLk.Lock()
	defer cp.poolLk.Unlock()
	cp.nextPool.blocks = append(cp.nextPool.blocks, blk)
	// Offset = 8bytes + Size = 4bytes = 12 Bytes
	cp.outstandingWork += Work(SizeBytesLen + OffBytesLen)
	return nil
}

func (cp *FreeList) flushBlock(blk Block) (Work, error) {
	sizeBuf := make([]byte, SizeBytesLen)
	offBuf := make([]byte, OffBytesLen)
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
	return Work(SizeBytesLen + OffBytesLen), nil
}

func (cp *FreeList) commit() (Work, error) {
	cp.poolLk.Lock()
	nextPool := cp.curPool
	cp.curPool = cp.nextPool
	cp.nextPool = nextPool
	cp.outstandingWork = 0
	cp.poolLk.Unlock()
	if len(cp.curPool.blocks) == 0 {
		return 0, nil
	}
	var work Work
	for _, record := range cp.curPool.blocks {
		blockWork, err := cp.flushBlock(record)
		if err != nil {
			return 0, err
		}
		work += blockWork
	}
	return work, nil
}

func (cp *FreeList) Flush() (Work, error) {
	return cp.commit()
}

func (cp *FreeList) Sync() error {
	if err := cp.writer.Flush(); err != nil {
		return err
	}
	if err := cp.file.Sync(); err != nil {
		return err
	}
	cp.poolLk.Lock()
	defer cp.poolLk.Unlock()
	cp.curPool = newBlockPool()
	return nil
}

func (cp *FreeList) Close() error {
	return cp.file.Close()
}

func (cp *FreeList) OutstandingWork() Work {
	cp.poolLk.RLock()
	defer cp.poolLk.RUnlock()
	return cp.outstandingWork
}
func (cp *FreeList) Iter() (*FreeListIter, error) {
	return NewFreeListIter(cp.file), nil
}

func NewFreeListIter(reader *os.File) *FreeListIter {
	return &FreeListIter{reader, 0}
}

type FreeListIter struct {
	reader *os.File
	pos    Position
}

func (cpi *FreeListIter) Next() (*Block, error) {
	sizeBuf := make([]byte, SizeBytesLen)
	offBuf := make([]byte, OffBytesLen)
	_, err := cpi.reader.ReadAt(offBuf, int64(cpi.pos))
	if err != nil {
		return nil, err
	}
	cpi.pos += OffBytesLen
	offset := binary.LittleEndian.Uint64(offBuf)

	_, err = cpi.reader.ReadAt(sizeBuf, int64(cpi.pos))
	if err != nil {

		return nil, err
	}
	cpi.pos += SizeBytesLen
	size := binary.LittleEndian.Uint32(sizeBuf)
	return &Block{Size: Size(size), Offset: Position(offset)}, nil
}
