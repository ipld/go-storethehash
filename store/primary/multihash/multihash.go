package mhprimary

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ipld/go-storethehash/store/filecache"
	"github.com/ipld/go-storethehash/store/freelist"
	"github.com/ipld/go-storethehash/store/primary"
	"github.com/ipld/go-storethehash/store/types"
	"github.com/multiformats/go-multihash"
)

const (
	// PrimaryVersion is stored in the header data to indicate how to interpret
	// primary data.
	PrimaryVersion = 1

	// defaultMaxFileSize is largest the max file size is allowed to be.
	defaultMaxFileSize = uint32(1024 * 1024 * 1024)

	// blockBufferSize is the size of primary I/O buffers. If has the same size
	// as the linux pipe size.
	blockBufferSize = 16 * 4096
	// blockPoolSize is the size of the primary cache.
	blockPoolSize = 1024

	// sizePrefixSize is the number of bytes used for the size prefix of a
	// record list.
	sizePrefixSize = 4
	SizePrefix     = 4

	deletedBit = uint32(1 << 31)
)

// A primary storage that is multihash aware.
type MultihashPrimary struct {
	basePath          string
	file              *os.File
	headerPath        string
	maxFileSize       uint32
	writer            *bufio.Writer
	outstandingWork   types.Work
	curPool, nextPool blockPool
	poolLk            sync.RWMutex
	flushLock         sync.Mutex
	fileCache         *filecache.FileCache

	// fileNum and length track flushed data.
	fileNum uint32
	length  types.Position

	// recFileNum and recPos track where each record will be written when they
	// are flushed to disk.
	recFileNum uint32
	recPos     types.Position

	// gc is the garbage collector for the primary.
	gc      *primaryGC
	gcMutex sync.Mutex
	closed  bool
}

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

// Open opens the multihash primary storage file. The primary is created if
// there is no existing primary at the specified path. If there is an older
// version primary, then it is automatically upgraded.
func Open(path string, freeList *freelist.FreeList, fileCache *filecache.FileCache, maxFileSize uint32) (*MultihashPrimary, error) {
	headerPath := filepath.Clean(path) + ".info"

	if maxFileSize == 0 {
		maxFileSize = defaultMaxFileSize
	} else if maxFileSize > defaultMaxFileSize {
		return nil, fmt.Errorf("maximum primary file size cannot exceed %d", defaultMaxFileSize)
	}

	var lastPrimaryNum uint32
	header, err := readHeader(headerPath)
	if os.IsNotExist(err) {
		// If header does not exist, then upgrade primary.
		lastPrimaryNum, err = upgradePrimary(context.Background(), path, headerPath, maxFileSize, freeList)
		if err != nil {
			return nil, fmt.Errorf("error upgrading primary: %w", err)
		}

		// Header does not exist, so create new one.
		header = newHeader(maxFileSize)
		if err = writeHeader(headerPath, header); err != nil {
			return nil, err
		}
	} else {
		if err != nil {
			return nil, err
		}

		if header.MaxFileSize != maxFileSize {
			return nil, types.ErrPrimaryWrongFileSize{header.MaxFileSize, maxFileSize}
		}

		// Find last primary file.
		lastPrimaryNum, err = findLastPrimary(path, header.FirstFile)
		if err != nil {
			return nil, err
		}
	}

	file, err := os.OpenFile(primaryFileName(path, lastPrimaryNum), os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	length, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	mp := &MultihashPrimary{
		basePath:    path,
		file:        file,
		fileCache:   fileCache,
		headerPath:  headerPath,
		maxFileSize: maxFileSize,
		writer:      bufio.NewWriterSize(file, blockBufferSize),
		curPool:     newBlockPool(),
		nextPool:    newBlockPool(),

		fileNum: lastPrimaryNum,
		length:  types.Position(length),

		recFileNum: lastPrimaryNum,
		recPos:     types.Position(length),
	}

	return mp, nil
}

func (mp *MultihashPrimary) StartGC(freeList *freelist.FreeList, interval, timeLimit time.Duration, updateIndex UpdateIndexFunc) {
	if freeList == nil || interval == 0 {
		return
	}

	mp.gcMutex.Lock()
	defer mp.gcMutex.Unlock()

	// If GC already started, then do nothing.
	if mp.gc != nil {
		return
	}

	mp.gc = newGC(mp, freeList, interval, timeLimit, updateIndex)
}

func (mp *MultihashPrimary) GC(ctx context.Context, lowUsePercent int64) (int, error) {
	mp.gcMutex.Lock()
	gc := mp.gc
	mp.gcMutex.Unlock()

	if gc == nil {
		return 0, errors.New("gc disabled")
	}

	return gc.gc(ctx, 0, lowUsePercent)
}

func (cp *MultihashPrimary) FileSize() uint32 {
	return cp.maxFileSize
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
	if blk.Offset >= absolutePrimaryPos(cp.recPos, cp.recFileNum, cp.maxFileSize) {
		return nil, nil, fmt.Errorf("error getting cached multihashed primary: %w", types.ErrOutOfBounds)
	}
	return nil, nil, nil
}

func (cp *MultihashPrimary) Get(blk types.Block) ([]byte, []byte, error) {
	key, value, err := cp.getCached(blk)
	if err != nil {
		return nil, nil, err
	}
	if key != nil && value != nil {
		return key, value, nil
	}

	localPos, fileNum := localizePrimaryPos(blk.Offset, cp.maxFileSize)

	file, err := cp.fileCache.Open(primaryFileName(cp.basePath, fileNum))
	if err != nil {
		return nil, nil, err
	}
	defer cp.fileCache.Close(file)

	read := make([]byte, int(blk.Size+4))
	if _, err = file.ReadAt(read, int64(localPos)); err != nil {
		return nil, nil, fmt.Errorf("error reading data from multihash primary: %w", err)
	}
	if binary.LittleEndian.Uint32(read[:4])&deletedBit != 0 {
		return nil, nil, nil
	}

	return readNode(read[4:])
}

// readNode extracts the multihash from the data read and splits key and value.
func readNode(data []byte) (multihash.Multihash, []byte, error) {
	c, n, err := readMh(data)
	if err != nil {
		return multihash.Multihash{}, nil, err
	}

	return c, data[n:], nil
}

func readMh(buf []byte) (multihash.Multihash, int, error) {
	br := bytes.NewReader(buf)
	mhr := multihash.NewReader(br)
	h, err := mhr.ReadMultihash()
	if err != nil {
		return multihash.Multihash{}, 0, fmt.Errorf("error reading multihash from data: %w", err)
	}

	return h, len(buf) - br.Len(), nil
}

// Put adds a new pending blockRecord to the pool and returns a Block that
// contains the location that the block will occupy in the primary. The
// returned primary location must be an absolute position across all primary
// files.
func (cp *MultihashPrimary) Put(key []byte, value []byte) (types.Block, error) {
	recSize := int64(len(key) + len(value))
	dataSize := sizePrefixSize + recSize

	cp.poolLk.Lock()
	defer cp.poolLk.Unlock()

	if cp.recPos >= types.Position(cp.maxFileSize) {
		cp.recFileNum++
		cp.recPos = 0
	}

	// Tell index the location that this record will be writtten.
	absRecPos := absolutePrimaryPos(cp.recPos, cp.recFileNum, cp.maxFileSize)
	blk := types.Block{Offset: absRecPos, Size: types.Size(recSize)}

	cp.recPos += types.Position(dataSize)

	cp.nextPool.refs[blk] = len(cp.nextPool.blocks)
	cp.nextPool.blocks = append(cp.nextPool.blocks, blockRecord{key, value})
	cp.outstandingWork += types.Work(dataSize)
	return blk, nil
}

func (cp *MultihashPrimary) flushBlock(key []byte, value []byte) (types.Work, error) {
	if cp.length >= types.Position(cp.maxFileSize) {
		fileNum := cp.fileNum + 1
		primaryPath := primaryFileName(cp.basePath, fileNum)
		// If the primary file being opened already exists then fileNum has
		// wrapped and there are max uint32 of in]dex files. This means that
		// maxFileSize is set far too small or GC is disabled.
		if _, err := os.Stat(primaryPath); !os.IsNotExist(err) {
			return 0, fmt.Errorf("creating primary file overwrites existing, check file size, gc and path (maxFileSize=%d) (path=%s)", cp.maxFileSize, primaryPath)
		}

		file, err := os.OpenFile(primaryPath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o644)
		if err != nil {
			return 0, fmt.Errorf("cannot open new primary file %s: %w", primaryPath, err)
		}
		if err = cp.writer.Flush(); err != nil {
			return 0, fmt.Errorf("cannot write to primary file %s: %w", cp.file.Name(), err)
		}

		cp.file.Close()
		cp.writer.Reset(file)
		cp.file = file
		cp.fileNum = fileNum
		cp.length = 0
	}

	size := len(key) + len(value)
	sizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBuf, uint32(size))

	_, err := cp.writer.Write(sizeBuf)
	if err != nil {
		return 0, err
	}
	if _, err = cp.writer.Write(key); err != nil {
		return 0, err
	}
	if _, err = cp.writer.Write(value); err != nil {
		return 0, err
	}

	writeSize := size + sizePrefixSize
	cp.length += types.Position(writeSize)

	return types.Work(writeSize), nil
}

func (cp *MultihashPrimary) IndexKey(key []byte) ([]byte, error) {
	// This is a sanity-check to see if it really is a multihash
	decoded, err := multihash.Decode(multihash.Multihash(key))
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
	if key == nil {
		return nil, nil
	}
	return cp.IndexKey(key)
}

// Flush writes outstanding work and buffered data to the primary file.
func (cp *MultihashPrimary) Flush() (types.Work, error) {
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

	// The pool lock is released allowing Put to write to nextPool. The
	// flushLock is still held, preventing concurrent flushes from changing the
	// pools or accessing writer.

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
func (mp *MultihashPrimary) Sync() error {
	mp.flushLock.Lock()
	defer mp.flushLock.Unlock()
	return mp.file.Sync()
}

// Close calls Flush to write work and data to the primary file, and then
// closes the file.
func (mp *MultihashPrimary) Close() error {
	mp.gcMutex.Lock()
	if mp.closed {
		mp.gcMutex.Unlock()
		return nil
	}
	if mp.gc != nil {
		mp.gc.close()
	}
	mp.gcMutex.Unlock()

	mp.fileCache.Clear()

	_, err := mp.Flush()
	if err != nil {
		mp.file.Close()
		return err
	}

	return mp.file.Close()
}

func (cp *MultihashPrimary) OutstandingWork() types.Work {
	cp.poolLk.RLock()
	defer cp.poolLk.RUnlock()
	return cp.outstandingWork
}

type Iterator struct {
	// The index data we are iterating over
	file *os.File
	// The current position within the index
	pos int64
	// The base index file path
	base string
	// The current index file number
	fileNum uint32
}

func (cp *MultihashPrimary) Iter() (primary.PrimaryStorageIter, error) {
	header, err := readHeader(cp.headerPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	return NewIterator(cp.basePath, header.FirstFile), nil
}

func NewIterator(basePath string, fileNum uint32) *Iterator {
	return &Iterator{
		base:    basePath,
		fileNum: fileNum,
	}
}

func (iter *Iterator) Next() ([]byte, []byte, error) {
	if iter == nil {
		return nil, nil, nil
	}

	if iter.file == nil {
		file, err := os.OpenFile(primaryFileName(iter.base, iter.fileNum), os.O_RDONLY, 0644)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, nil, io.EOF
			}
			return nil, nil, err
		}
		iter.file = file
		iter.pos = 0
	}

	var size uint32
	sizeBuf := make([]byte, sizePrefixSize)
	for {
		_, err := iter.file.ReadAt(sizeBuf, iter.pos)
		if err != nil {
			iter.file.Close()
			if err == io.EOF {
				iter.file = nil
				iter.fileNum++
				return iter.Next()
			}
			return nil, nil, err
		}
		size = binary.LittleEndian.Uint32(sizeBuf)
		if size&deletedBit != 0 {
			iter.pos += sizePrefixSize + int64(size^deletedBit)
		} else {
			break
		}
	}
	pos := iter.pos + sizePrefixSize
	data := make([]byte, size)
	_, err := iter.file.ReadAt(data, pos)
	if err != nil {
		iter.file.Close()
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, nil, err
	}

	iter.pos += int64(sizePrefixSize + size)
	return readNode(data)
}

func (iter *Iterator) Close() error {
	if iter.file == nil {
		return nil
	}
	return iter.file.Close()
}

// StorageSize returns bytes of storage used by the primary files.
func (cp *MultihashPrimary) StorageSize() (int64, error) {
	header, err := readHeader(cp.headerPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	fi, err := os.Stat(cp.headerPath)
	if err != nil {
		return 0, err
	}
	size := fi.Size()

	fileNum := header.FirstFile
	for {
		primaryName := primaryFileName(cp.basePath, fileNum)

		// Get size of primary file.
		fi, err = os.Stat(primaryName)
		if err != nil {
			if os.IsNotExist(err) {
				break
			}
			return 0, err
		}
		size += fi.Size()

		fileNum++
	}
	return size, nil
}

func primaryFileName(basePath string, fileNum uint32) string {
	return fmt.Sprintf("%s.%d", basePath, fileNum)
}

func primaryPosToFileNum(pos types.Position, maxFileSize uint32) (bool, uint32) {
	// Primary pos 0 means there is no data in the primary, so indicate empty.
	if pos == 0 {
		return false, 0
	}
	// The start of the entry determines which is file is used.
	return true, uint32(pos / types.Position(maxFileSize))
}

// localizePrimaryPos decodes a position into a local primary offset and file number.
func localizePrimaryPos(pos types.Position, maxFileSize uint32) (types.Position, uint32) {
	ok, fileNum := primaryPosToFileNum(pos, maxFileSize)
	if !ok {
		// Return 0 local pos to indicate empty bucket.
		return 0, 0
	}
	// Subtract file offset to get pos within its local file.
	localPos := pos - (types.Position(fileNum) * types.Position(maxFileSize))
	return localPos, fileNum
}

func absolutePrimaryPos(localPos types.Position, fileNum, maxFileSize uint32) types.Position {
	return types.Position(maxFileSize)*types.Position(fileNum) + localPos
}

func findLastPrimary(basePath string, fileNum uint32) (uint32, error) {
	var lastFound uint32
	for {
		_, err := os.Stat(primaryFileName(basePath, fileNum))
		if err != nil {
			if os.IsNotExist(err) {
				break
			}
			return 0, err
		}
		lastFound = fileNum
		fileNum++
	}
	return lastFound, nil
}

var _ primary.PrimaryStorage = &MultihashPrimary{}
