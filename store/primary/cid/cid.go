package cidprimary

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"sync"

	"github.com/ipfs/go-cid"
	util "github.com/ipld/go-car/util"
	"github.com/multiformats/go-multihash"

	store "github.com/hannahhoward/go-storethehash/store"
)

const CIDSizePrefix = 4

// A primary storage that is CID aware.
type CIDPrimary struct {
	file              *os.File
	writer            *bufio.Writer
	length            store.Position
	outstandingWork   store.Work
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
	refs   map[store.Block]int
	blocks []blockRecord
}

func newBlockPool() blockPool {
	return blockPool{
		refs:   make(map[store.Block]int, blockPoolSize),
		blocks: make([]blockRecord, 0, blockPoolSize),
	}
}

func OpenCIDPrimary(path string) (*CIDPrimary, error) {
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
		length:   store.Position(length),
		curPool:  newBlockPool(),
		nextPool: newBlockPool(),
	}, nil
}

func (cp *CIDPrimary) getCached(blk store.Block) ([]byte, []byte, error) {
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
		return nil, nil, store.ErrOutOfBounds
	}
	return nil, nil, nil
}

func (cp *CIDPrimary) Get(blk store.Block) (key []byte, value []byte, err error) {
	key, value, err = cp.getCached(blk)
	if err != nil {
		return
	}
	if key != nil && value != nil {
		return
	}
	read := make([]byte, CIDSizePrefix+int(blk.Size))
	cp.file.ReadAt(read, int64(blk.Offset))
	c, value, err := readNode(read[4:])
	return c.Bytes(), value, err
}

// readNode extracts the Cid from the data read and splits key and value.
func readNode(data []byte) (cid.Cid, []byte, error) {
	c, n, err := util.ReadCid(data)
	if err != nil {
		return cid.Cid{}, nil, err
	}

	return c, data[n:], nil
}

func (cp *CIDPrimary) Put(key []byte, value []byte) (store.Block, error) {
	cp.poolLk.Lock()
	defer cp.poolLk.Unlock()
	length := cp.length
	size := len(key) + len(value)
	cp.length += CIDSizePrefix + store.Position(size)
	blk := store.Block{Offset: length, Size: store.Size(size)}
	cp.nextPool.refs[blk] = len(cp.nextPool.blocks)
	cp.nextPool.blocks = append(cp.nextPool.blocks, blockRecord{key, value})
	cp.outstandingWork += store.Work(size + CIDSizePrefix)
	return blk, nil
}

func (cp *CIDPrimary) flushBlock(key []byte, value []byte) (store.Work, error) {
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
	return store.Work(CIDSizePrefix + size), nil
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

func (cp *CIDPrimary) GetIndexKey(blk store.Block) ([]byte, error) {
	key, _, err := cp.Get(blk)
	if err != nil {
		return nil, err
	}
	return cp.IndexKey(key)
}

func (cp *CIDPrimary) commit() (store.Work, error) {
	cp.poolLk.Lock()
	nextPool := cp.curPool
	cp.curPool = cp.nextPool
	cp.nextPool = nextPool
	cp.outstandingWork = 0
	cp.poolLk.Unlock()
	if len(cp.curPool.blocks) == 0 {
		return 0, nil
	}
	var work store.Work
	for _, record := range cp.curPool.blocks {
		blockWork, err := cp.flushBlock(record.key, record.value)
		if err != nil {
			return 0, err
		}
		work += blockWork
	}
	return work, nil
}

func (cp *CIDPrimary) Flush() (store.Work, error) {
	return cp.commit()
}

func (cp *CIDPrimary) Sync() error {
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

func (cp *CIDPrimary) Close() error {
	return cp.file.Close()
}

func (cp *CIDPrimary) OutstandingWork() store.Work {
	cp.poolLk.RLock()
	defer cp.poolLk.RUnlock()
	return cp.outstandingWork
}
func (cp *CIDPrimary) Iter() (store.PrimaryStorageIter, error) {
	return NewCIDPrimaryIter(cp.file), nil
}

func NewCIDPrimaryIter(reader *os.File) *CIDPrimaryIter {
	return &CIDPrimaryIter{reader, 0}
}

type CIDPrimaryIter struct {
	reader *os.File
	pos    store.Position
}

func (cpi *CIDPrimaryIter) Next() ([]byte, []byte, error) {
	sizeBuff := make([]byte, CIDSizePrefix)
	_, err := cpi.reader.ReadAt(sizeBuff, int64(cpi.pos))
	if err != nil {

		return nil, nil, err
	}
	cpi.pos += CIDSizePrefix
	size := binary.LittleEndian.Uint32(sizeBuff)
	read := make([]byte, int(size))
	_, err = cpi.reader.ReadAt(read, int64(cpi.pos))
	cpi.pos += store.Position(size)
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, nil, err
	}
	c, value, err := readNode(read)
	return c.Bytes(), value, err
}

var _ store.PrimaryStorage = &CIDPrimary{}
