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
	file              *os.File
	writer            *bufio.Writer
	outstandingWork   types.Work
	curPool, nextPool blockPool
	poolLk            sync.RWMutex
}

const (
	// blockBufferSize is the size of I/O buffers. If has the same size as the
	// linux pipe size.
	blockBufferSize = 16 * 4096
	// blockPoolSize is the size of the freelist cache.
	blockPoolSize = 1024
)

type blockPool struct {
	blocks []types.Block
}

func newBlockPool() blockPool {
	return blockPool{
		blocks: make([]types.Block, 0, blockPoolSize),
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

func (cp *FreeList) Put(blk types.Block) error {
	cp.poolLk.Lock()
	defer cp.poolLk.Unlock()
	cp.nextPool.blocks = append(cp.nextPool.blocks, blk)
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

func (cp *FreeList) commit() (types.Work, error) {
	cp.poolLk.Lock()
	nextPool := cp.curPool
	cp.curPool = cp.nextPool
	cp.nextPool = nextPool
	cp.outstandingWork = 0
	cp.poolLk.Unlock()
	if len(cp.curPool.blocks) == 0 {
		return 0, nil
	}
	var work types.Work
	for _, record := range cp.curPool.blocks {
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

func (cp *FreeList) Flush() (types.Work, error) {
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

func (cp *FreeList) OutstandingWork() types.Work {
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
	pos    types.Position
}

func (cpi *FreeListIter) Next() (*types.Block, error) {
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
