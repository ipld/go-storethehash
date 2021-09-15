package mhprimary

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"

	"github.com/ipld/go-storethehash/store/primary"
	"github.com/ipld/go-storethehash/store/types"
	"github.com/multiformats/go-multihash"
	mh "github.com/multiformats/go-multihash"
)

const SizePrefix = 4

// A primary storage that is multihash aware.
type MultihashPrimary struct {
	file              *os.File
	writer            *bufio.Writer
	length            types.Position
	outstandingWork   types.Work
	curPool, nextPool blockPool
	poolLk            sync.RWMutex
}

const blockBufferSize = 32 * 4096
const blockPoolSize = 1024

type blockRecord struct {
	key   []byte
	value []byte
}
type blockPool struct {
	refs   map[types.Block]int
	blocks []blockRecord
}

func newBlockPool() blockPool {
	return blockPool{
		refs:   make(map[types.Block]int, blockPoolSize),
		blocks: make([]blockRecord, 0, blockPoolSize),
	}
}

func OpenMultihashPrimary(path string) (*MultihashPrimary, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	length, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		return nil, err
	}
	return &MultihashPrimary{
		file:     file,
		writer:   bufio.NewWriterSize(file, blockBufferSize),
		length:   types.Position(length),
		curPool:  newBlockPool(),
		nextPool: newBlockPool(),
	}, nil
}

func (cp *MultihashPrimary) getCached(blk types.Block) ([]byte, []byte, error) {
	cp.poolLk.RLock()
	defer cp.poolLk.RUnlock()
	idx, ok := cp.nextPool.refs[blk]
	if ok {
		br := cp.nextPool.blocks[idx]
		return br.key, br.value, nil
	}
	idx, ok = cp.curPool.refs[blk]
	if ok {
		br := cp.curPool.blocks[idx]
		return br.key, br.value, nil
	}
	if blk.Offset >= cp.length {
		return nil, nil, types.ErrOutOfBounds
	}
	return nil, nil, nil
}

func (cp *MultihashPrimary) Get(blk types.Block) (key []byte, value []byte, err error) {
	key, value, err = cp.getCached(blk)
	if err != nil {
		return
	}
	if key != nil && value != nil {
		return
	}
	read := make([]byte, int(blk.Size+4))
	cp.file.ReadAt(read, int64(blk.Offset))
	h, value, err := readNode(read[4:])
	return h, value, err
}

// readNode extracts the multihash from the data read and splits key and value.
func readNode(data []byte) (mh.Multihash, []byte, error) {
	c, n, err := readMh(data)
	if err != nil {
		return mh.Multihash{}, nil, err
	}

	return c, data[n:], nil
}

func readMh(buf []byte) (mh.Multihash, int, error) {
	br := bytes.NewReader(buf)
	mhr := mh.NewReader(br)
	h, err := mhr.ReadMultihash()
	if err != nil {
		return mh.Multihash{}, 0, err
	}

	return h, len(buf) - br.Len(), nil
}
func (cp *MultihashPrimary) Put(key []byte, value []byte) (types.Block, error) {
	cp.poolLk.Lock()
	defer cp.poolLk.Unlock()
	length := cp.length
	size := len(key) + len(value)
	cp.length += SizePrefix + types.Position(size)
	blk := types.Block{Offset: length, Size: types.Size(size)}
	cp.nextPool.refs[blk] = len(cp.nextPool.blocks)
	cp.nextPool.blocks = append(cp.nextPool.blocks, blockRecord{key, value})
	cp.outstandingWork += types.Work(SizePrefix + size)
	return blk, nil
}

func (cp *MultihashPrimary) flushBlock(key []byte, value []byte) (types.Work, error) {
	size := len(key) + len(value)
	sizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBuf, uint32(size))
	if _, err := cp.writer.Write(sizeBuf); err != nil {
		return 0, err
	}
	if _, err := cp.writer.Write(key); err != nil {
		return 0, err
	}
	if _, err := cp.writer.Write(value); err != nil {
		return 0, err
	}
	return types.Work(size + SizePrefix), nil
}

func (cp *MultihashPrimary) IndexKey(key []byte) ([]byte, error) {
	// This is a sanity-check to see if it really is a multihash
	decoded, err := multihash.Decode(mh.Multihash(key))
	if err != nil {
		return nil, err
	}
	return decoded.Digest, nil
}

func (cp *MultihashPrimary) GetIndexKey(blk types.Block) ([]byte, error) {
	key, _, err := cp.Get(blk)
	if err != nil {
		return nil, err
	}
	return cp.IndexKey(key)
}

func (cp *MultihashPrimary) commit() (types.Work, error) {
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
		blockWork, err := cp.flushBlock(record.key, record.value)
		if err != nil {
			return 0, err
		}
		work += blockWork
	}
	return work, nil
}

func (cp *MultihashPrimary) Flush() (types.Work, error) {
	return cp.commit()
}

func (cp *MultihashPrimary) Sync() error {
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

func (cp *MultihashPrimary) Close() error {
	return cp.file.Close()
}

func (cp *MultihashPrimary) OutstandingWork() types.Work {
	cp.poolLk.RLock()
	defer cp.poolLk.RUnlock()
	return cp.outstandingWork
}
func (cp *MultihashPrimary) Iter() (primary.PrimaryStorageIter, error) {
	return NewMultihashPrimaryIter(cp.file), nil
}

func NewMultihashPrimaryIter(reader *os.File) *MultihashPrimaryIter {
	return &MultihashPrimaryIter{reader, 0}
}

type MultihashPrimaryIter struct {
	reader *os.File
	pos    types.Position
}

func (cpi *MultihashPrimaryIter) Next() ([]byte, []byte, error) {
	sizeBuff := make([]byte, SizePrefix)
	_, err := cpi.reader.ReadAt(sizeBuff, int64(cpi.pos))
	if err != nil {

		return nil, nil, err
	}
	cpi.pos += SizePrefix
	size := binary.LittleEndian.Uint32(sizeBuff)
	read := make([]byte, int(size))
	_, err = cpi.reader.ReadAt(read, int64(cpi.pos))
	cpi.pos += types.Position(size)
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, nil, err
	}
	h, value, err := readNode(read)
	return h, value, err
}

var _ primary.PrimaryStorage = &MultihashPrimary{}
