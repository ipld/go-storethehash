package cidprimary

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-storethehash/store/primary"
	"github.com/ipld/go-storethehash/store/types"
	"github.com/multiformats/go-multihash"
)

const CIDSizePrefix = 4

// A primary storage that is CID aware.
type CIDPrimary struct {
	file              *os.File
	writer            *bufio.Writer
	length            types.Position
	outstandingWork   types.Work
	curPool, nextPool blockPool
	poolLk            sync.RWMutex
	flushLock         sync.Mutex
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

func Open(path string) (*CIDPrimary, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	length, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		return nil, err
	}
	return &CIDPrimary{
		file:     file,
		writer:   bufio.NewWriterSize(file, blockBufferSize),
		length:   types.Position(length),
		curPool:  newBlockPool(),
		nextPool: newBlockPool(),
	}, nil
}

func (cp *CIDPrimary) getCached(blk types.Block) ([]byte, []byte, error) {
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

func (cp *CIDPrimary) Get(blk types.Block) ([]byte, []byte, error) {
	key, value, err := cp.getCached(blk)
	if err != nil {
		return nil, nil, err
	}
	if key != nil && value != nil {
		return key, value, nil
	}
	read := make([]byte, CIDSizePrefix+int(blk.Size))
	if _, err = cp.file.ReadAt(read, int64(blk.Offset)); err != nil {
		return nil, nil, fmt.Errorf("error reading data from cid primary: %w", err)
	}
	c, value, err := readNode(read[4:])
	if err != nil {
		return nil, nil, err
	}
	return c.Bytes(), value, nil
}

// readNode extracts the Cid from the data read and splits key and value.
func readNode(data []byte) (cid.Cid, []byte, error) {
	n, c, err := cid.CidFromBytes(data)
	if err != nil {
		return cid.Cid{}, nil, fmt.Errorf("error reading cid from data: %w", err)
	}

	return c, data[n:], nil
}

func (cp *CIDPrimary) Put(key []byte, value []byte) (types.Block, error) {
	cp.poolLk.Lock()
	defer cp.poolLk.Unlock()
	length := cp.length
	size := len(key) + len(value)
	cp.length += CIDSizePrefix + types.Position(size)
	blk := types.Block{Offset: length, Size: types.Size(size)}
	cp.nextPool.refs[blk] = len(cp.nextPool.blocks)
	cp.nextPool.blocks = append(cp.nextPool.blocks, blockRecord{key, value})
	cp.outstandingWork += types.Work(size + CIDSizePrefix)
	return blk, nil
}

func (cp *CIDPrimary) flushBlock(key []byte, value []byte) (types.Work, error) {
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
	return types.Work(CIDSizePrefix + size), nil
}

func (cp *CIDPrimary) IndexKey(key []byte) ([]byte, error) {
	// A CID is stored, but the index only contains the digest (the actual hash) of the CID.
	_, c, err := cid.CidFromBytes(key)
	if err != nil {
		return nil, err
	}
	decoded, err := multihash.Decode([]byte(c.Hash()))
	if err != nil {
		return nil, err
	}
	return decoded.Digest, nil
}

func (cp *CIDPrimary) GetIndexKey(blk types.Block) ([]byte, error) {
	key, _, err := cp.Get(blk)
	if err != nil {
		return nil, err
	}
	return cp.IndexKey(key)
}

// Flush writes outstanding work and buffered data to the primary file.
func (cp *CIDPrimary) Flush() (types.Work, error) {
	// Only one Flush at a time, otherwise the 2nd Flush can swap the pools
	// while the 1st Flush is still reading the pool being flushed. That could
	// cause the pool being read by the 1st Flush to be written to
	// concurrently.
	cp.flushLock.Lock()
	defer cp.flushLock.Unlock()

	cp.poolLk.Lock()
	// If no new data, then nothing to do.
	if len(cp.nextPool.blocks) == 0 {
		cp.poolLk.Unlock()
		return 0, nil
	}
	cp.curPool = cp.nextPool
	cp.nextPool = newBlockPool()
	cp.outstandingWork = 0
	cp.poolLk.Unlock()

	var work types.Work
	for _, record := range cp.curPool.blocks {
		blockWork, err := cp.flushBlock(record.key, record.value)
		if err != nil {
			return 0, err
		}
		work += blockWork
	}
	err := cp.writer.Flush()
	if err != nil {
		return 0, fmt.Errorf("cannot flush data to primary file %s: %w", cp.file.Name(), err)
	}

	return work, nil
}

// Sync commits the contents of the primary file to disk. Flush should be
// called before calling Sync.
func (cp *CIDPrimary) Sync() error {
	return cp.file.Sync()
}

// Close calls Flush to write work and data to the primary file, and then
// closes the file.
func (cp *CIDPrimary) Close() error {
	_, err := cp.Flush()
	if err != nil {
		cp.file.Close()
		return err
	}
	return cp.file.Close()
}

func (cp *CIDPrimary) OutstandingWork() types.Work {
	cp.poolLk.RLock()
	defer cp.poolLk.RUnlock()
	return cp.outstandingWork
}
func (cp *CIDPrimary) Iter() (primary.PrimaryStorageIter, error) {
	return NewIter(cp.file), nil
}

func NewIter(reader *os.File) *Iterator {
	return &Iterator{reader, 0}
}

type Iterator struct {
	reader *os.File
	pos    types.Position
}

func (cpi *Iterator) Next() ([]byte, []byte, error) {
	sizeBuff := make([]byte, CIDSizePrefix)
	_, err := cpi.reader.ReadAt(sizeBuff, int64(cpi.pos))
	if err != nil {

		return nil, nil, err
	}
	cpi.pos += CIDSizePrefix
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
	c, value, err := readNode(read)
	return c.Bytes(), value, err
}

func (cp *CIDPrimary) StorageSize() (int64, error) {
	fi, err := cp.file.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

var _ primary.PrimaryStorage = &CIDPrimary{}
