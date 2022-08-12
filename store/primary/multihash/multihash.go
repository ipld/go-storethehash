package mhprimary

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/ipld/go-storethehash/store/freelist"
	"github.com/ipld/go-storethehash/store/primary"
	"github.com/ipld/go-storethehash/store/types"
	"github.com/multiformats/go-multihash"
)

const (
	// PrimaryVersion is stored in the header data to indicate how to interpret
	// primary data.
	PrimaryVersion = 1

	// maxFileSizeLimit is largest the max file size is allowed to be.
	maxFileSizeLimit = 1024 * 1024 * 1024

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

// Header contains information about the primary. This is actually stored in a
// separate ".info" file, but is the first file read when the index is opened.
type Header struct {
	// A version number in case we change the header
	Version int
	// MaxFileSize is the size limit of each index file. This cannot be greater
	// than 4GiB.
	MaxFileSize uint32
	// First index file number
	FirstFile uint32
}

func newHeader(maxFileSize uint32) Header {
	return Header{
		Version:     PrimaryVersion,
		MaxFileSize: maxFileSize,
	}
}

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

	// fileNum and length track flushed data.
	fileNum uint32
	length  types.Position

	// recFileNum and recPos track where each record will be written.
	recFileNum uint32
	recPos     types.Position
}

type blockRecord struct {
	key   []byte
	value []byte
}
type blockPool struct {
	refs   map[types.Block]int
	blocks []blockRecord
}

var _ primary.PrimaryStorage = &MultihashPrimary{}

func newBlockPool() blockPool {
	return blockPool{
		refs:   make(map[types.Block]int, blockPoolSize),
		blocks: make([]blockRecord, 0, blockPoolSize),
	}
}

// Open opens the multihash primary storage file. The primary is created if
// there is no existing primary at the specified path. If there is an older
// version primary, then it is automatically upgraded.
//
// Specifying 0 for maxFileSize results in using the default value.
func Open(path string, maxFileSize uint32, freeList *freelist.FreeList) (*MultihashPrimary, error) {
	headerPath := filepath.Clean(path) + ".info"

	if maxFileSize == 0 {
		// Use the maximum size limit as the default.
		maxFileSize = maxFileSizeLimit
	} else if maxFileSize > maxFileSizeLimit {
		return nil, fmt.Errorf("maximum file size cannot exceed %d", maxFileSizeLimit)
	}

	_, err := upgradePrimary(context.Background(), path, headerPath, maxFileSize, freeList)
	if err != nil {
		return nil, err
	}

	var lastPrimaryNum uint32
	header, err := readHeader(headerPath)
	if os.IsNotExist(err) {
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

	return &MultihashPrimary{
		basePath:    path,
		file:        file,
		headerPath:  headerPath,
		maxFileSize: maxFileSize,
		writer:      bufio.NewWriterSize(file, blockBufferSize),
		curPool:     newBlockPool(),
		nextPool:    newBlockPool(),

		fileNum: lastPrimaryNum,
		length:  types.Position(length),

		recFileNum: lastPrimaryNum,
		recPos:     types.Position(length),
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

	file, err := os.Open(primaryFileName(cp.basePath, fileNum))
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	read := make([]byte, int(blk.Size+4))
	if _, err = file.ReadAt(read, int64(localPos)); err != nil {
		return nil, nil, fmt.Errorf("error reading data from multihash primary: %w", err)
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
func (cp *MultihashPrimary) Sync() error {
	return cp.file.Sync()
}

// Close calls Flush to write work and data to the primary file, and then
// closes the file.
func (cp *MultihashPrimary) Close() error {
	_, err := cp.Flush()
	if err != nil {
		cp.file.Close()
		return err
	}
	return cp.file.Close()
}

func (cp *MultihashPrimary) OutstandingWork() types.Work {
	cp.poolLk.RLock()
	defer cp.poolLk.RUnlock()
	return cp.outstandingWork
}

type Iterator struct {
	// The index data we are iterating over
	reader io.ReadCloser
	// The current position within the index
	pos types.Position
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

	return NewIter(cp.basePath, header.FirstFile), nil
}

func NewIter(basePath string, fileNum uint32) *Iterator {
	return &Iterator{
		base:    basePath,
		fileNum: fileNum,
	}
}

func (iter *Iterator) Next() ([]byte, []byte, error) {
	if iter == nil {
		return nil, nil, nil
	}

	if iter.reader == nil {
		file, err := os.OpenFile(primaryFileName(iter.base, iter.fileNum), os.O_RDONLY, 0644)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, nil, io.EOF
			}
			return nil, nil, err
		}
		iter.reader = file
		iter.pos = 0
	}

	size, err := readSizePrefix(iter.reader)
	if err != nil {
		iter.reader.Close()
		if err == io.EOF {
			iter.reader = nil
			iter.fileNum++
			return iter.Next()
		}
		return nil, nil, err
	}

	data := make([]byte, size)
	_, err = io.ReadFull(iter.reader, data)
	if err != nil {
		iter.reader.Close()
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, nil, err
	}

	iter.pos += types.Position(sizePrefixSize) + types.Position(size)

	return readNode(data)
}

func (iter *Iterator) Close() error {
	if iter.reader == nil {
		return nil
	}
	return iter.reader.Close()
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

// Ident includes file size, since changing this would invalidate the index.
func (cp *MultihashPrimary) Ident() string {
	return fmt.Sprint("multihash-", cp.maxFileSize)
}

// Only reads the size prefix of the data and returns it.
func readSizePrefix(reader io.Reader) (uint32, error) {
	sizeBuffer := make([]byte, sizePrefixSize)
	_, err := io.ReadFull(reader, sizeBuffer)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(sizeBuffer), nil
}

func readHeader(filePath string) (Header, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return Header{}, err
	}

	var header Header
	err = json.Unmarshal(data, &header)
	if err != nil {
		return Header{}, err
	}

	return header, nil
}

func writeHeader(headerPath string, header Header) error {
	data, err := json.Marshal(&header)
	if err != nil {
		return err
	}

	return os.WriteFile(headerPath, data, 0o666)
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

func (cp *MultihashPrimary) GetFileInfo() (uint32, uint32, []int64, error) {
	header, err := readHeader(cp.headerPath)
	if err != nil {
		return 0, 0, nil, err
	}
	sizes, err := cp.getFileSizes(header.FirstFile)
	if err != nil {
		return 0, 0, nil, err
	}
	return header.FirstFile, cp.maxFileSize, sizes, nil
}

func (cp *MultihashPrimary) getFileSizes(fileNum uint32) ([]int64, error) {
	var sizes []int64
	for {
		fi, err := os.Stat(primaryFileName(cp.basePath, fileNum))
		if err != nil {
			if os.IsNotExist(err) {
				break
			}
			return nil, err
		}
		sizes = append(sizes, fi.Size())
		fileNum++
	}
	return sizes, nil
}
