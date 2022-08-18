package index

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
	"time"

	"github.com/ipld/go-storethehash/store/primary"
	"github.com/ipld/go-storethehash/store/types"
)

/* An append-only log [`recordlist`]s.

The format of that append only log is:

```text
    |                    Repeated                 |
    |                                             |
    |         4 bytes        |  Variable size | … |
    | Size of the Recordlist |   Recordlist   | … |
```
*/

// In-memory buckets are used to track the location of records within the index
// files. The buckets map a bit-prefix to a bucketPos value. The bucketPos
// encodes both the index file number and the record offset within that file.
// If 1GiB is the maximum size for a file, then the local data offset is kept
// in the first GiB worth of bits (30) of the bucketPos. The file number is
// kept in the bits above that. It is necessary for the file number to wrap
// before it reaches a value greater than the number of bits available to
// record it in the buckerPos. This results in a trade-off between allowing
// larger files or allowing more files, but with the same overall maximum
// storage.
//
// With a 1GiB local offset taking the first 30 bits of a 64 bit number, that
// leaves 34 bits left to encode the file number. Instead of having logic to
// wrap the file number at the largest value allowed by the available bits, the
// file number is represented as a 32-bit value that always wraps at 2^32.
//
// Since the file number wraps 2^32 this means there can never be more than
// 2^32 active index files. This also means that maxFileSize should never be
// greater than 2^32. Using a maxFileSize of 2^30, the default, and a 32-bit
// file number, results in 2 bits unused in the bucketPos address space. With a
// smaller maxFileSize more bits would be unused.
//
// Smaller values for maxFileSize result in more files needed to hold the
// index, but also more granular GC. A value too small risks running out of
// inodes on the file system, and a value too large means that there is more
// stale data that GC cannot remove. Using a 1GiB index file size limit offers
// a good balance, and this value should not be changed (other than for
// testing) by more than a factor of 4.

const (
	// IndexVersion is stored in the header data to indicate how to interpret
	// index data.
	IndexVersion = 3

	// defaultIndexSizeBits is the default number of bits in an index prefix.
	defaultIndexSizeBits = uint8(24)

	// defaultMaxFileSize is the default size at which to start a new file.
	defaultMaxFileSize = 1024 * 1024 * 1024

	// sizePrefixSize is the number of bytes used for the size prefix of a
	// record list.
	sizePrefixSize = 4

	// indexBufferSize is the size of I/O buffers. If has the same size as the
	// linux pipe size.
	indexBufferSize = 16 * 4096

	// bucketPoolSize is the bucket cache size.
	bucketPoolSize = 1024

	// deletedBit is the highest order bit in the uint32 size part of an index
	// file record, and when set, indicates that the index record is deleted.
	// Since index files cannot exceed a size of 2^30, this bit is otherwise
	// unused.
	deletedBit = uint32(1 << 31)
)

// stripBucketPrefix removes the prefix that is used for the bucket.
//
// The first bits of a key are used to determine the bucket to put the key
// into. This function removes those bytes. Only bytes that are fully covered
// by the bits are removed. E.g. a bit value of 19 will remove only 2 bytes,
// whereas 24 bits removes 3 bytes.
func stripBucketPrefix(key []byte, bits byte) []byte {
	prefixLen := int(bits / 8)
	if len(key) < prefixLen {
		return nil
	}
	return key[prefixLen:]
}

// Header contains information about the index. This is actually stored in a
// separate ".info" file, but is the first file read when the index is opened.
type Header struct {
	// A version number in case we change the header
	Version int
	// The number of bits used to determine the in-memory buckets
	BucketsBits byte
	// MaxFileSize is the size limit of each index file. This cannot be greater
	// than 4GiB.
	MaxFileSize uint32
	// First index file number
	FirstFile uint32
}

func newHeader(bucketsBits byte, maxFileSize uint32) Header {
	return Header{
		Version:     IndexVersion,
		BucketsBits: bucketsBits,
		MaxFileSize: maxFileSize,
	}
}

type Index struct {
	sizeBits          uint8
	maxFileSize       uint32
	buckets           Buckets
	sizeBuckets       SizeBuckets
	file              *os.File
	fileNum           uint32
	headerPath        string
	writer            *bufio.Writer
	Primary           primary.PrimaryStorage
	bucketLk          sync.RWMutex
	flushLock         sync.Mutex
	outstandingWork   types.Work
	curPool, nextPool bucketPool
	length            types.Position
	basePath          string
	updateSig         chan struct{}

	gcDone chan struct{}
}

type bucketPool map[BucketIndex][]byte

// Open opens the index for the given primary. The index is created if there is
// no existing index at the specified path. If there is an older version index,
// then it is automatically upgraded.
//
// Specifying 0 for indexSizeBits and maxFileSize results in using their
// default values. A gcInterval of 0 disables garbage collection.
func Open(ctx context.Context, path string, primary primary.PrimaryStorage, indexSizeBits uint8, maxFileSize uint32, gcInterval, gcTimeLimit time.Duration) (*Index, error) {
	var file *os.File
	headerPath := filepath.Clean(path) + ".info"

	if indexSizeBits == 0 {
		indexSizeBits = defaultIndexSizeBits
	}
	if maxFileSize == 0 {
		maxFileSize = defaultMaxFileSize
	} else if maxFileSize > defaultMaxFileSize {
		return nil, fmt.Errorf("maximum file size cannot exceed %d", defaultMaxFileSize)
	}

	err := upgradeIndex(ctx, path, headerPath, maxFileSize)
	if err != nil {
		return nil, err
	}

	buckets, err := NewBuckets(indexSizeBits)
	if err != nil {
		return nil, err
	}
	sizeBuckets, err := NewSizeBuckets(indexSizeBits)
	if err != nil {
		return nil, err
	}

	var lastIndexNum uint32
	header, err := readHeader(headerPath)
	if os.IsNotExist(err) {
		header = newHeader(indexSizeBits, maxFileSize)
		if err = writeHeader(headerPath, header); err != nil {
			return nil, err
		}
	} else {
		if err != nil {
			return nil, err
		}

		if header.BucketsBits != indexSizeBits {
			return nil, types.ErrIndexWrongBitSize{header.BucketsBits, indexSizeBits}
		}

		if header.MaxFileSize != maxFileSize {
			return nil, types.ErrIndexWrongFileSize{header.MaxFileSize, maxFileSize}
		}

		err = loadBucketState(ctx, path, buckets, sizeBuckets, maxFileSize)
		if err != nil {
			log.Warnw("Could not load bucket state, scanning index file", "err", err)
			lastIndexNum, err = scanIndex(ctx, path, header.FirstFile, buckets, sizeBuckets, maxFileSize)
			if err != nil {
				return nil, err
			}
		} else {
			lastIndexNum, err = findLastIndex(path, header.FirstFile)
			if err != nil {
				return nil, fmt.Errorf("could not find most recent index file: %w", err)
			}
		}
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	file, err = openFileAppend(indexFileName(path, lastIndexNum))
	if err != nil {
		return nil, err
	}

	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}

	idx := &Index{
		sizeBits:    indexSizeBits,
		maxFileSize: maxFileSize,
		buckets:     buckets,
		sizeBuckets: sizeBuckets,
		file:        file,
		fileNum:     lastIndexNum,
		headerPath:  headerPath,
		writer:      bufio.NewWriterSize(file, indexBufferSize),
		Primary:     primary,
		curPool:     make(bucketPool, bucketPoolSize),
		nextPool:    make(bucketPool, bucketPoolSize),
		length:      types.Position(fi.Size()),
		basePath:    path,
	}

	if gcInterval == 0 {
		log.Warn("Index garbage collection disabled")
	} else {
		idx.updateSig = make(chan struct{}, 1)
		idx.gcDone = make(chan struct{})
		go idx.garbageCollector(gcInterval, gcTimeLimit)
	}

	return idx, nil
}

func indexFileName(basePath string, fileNum uint32) string {
	return fmt.Sprintf("%s.%d", basePath, fileNum)
}

func savedBucketsName(basePath string) string {
	return basePath + ".buckets"
}

func scanIndex(ctx context.Context, basePath string, fileNum uint32, buckets Buckets, sizeBuckets SizeBuckets, maxFileSize uint32) (uint32, error) {
	var lastFileNum uint32
	for {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		err := scanIndexFile(ctx, basePath, fileNum, buckets, sizeBuckets, maxFileSize)
		if err != nil {
			if os.IsNotExist(err) {
				break
			}
			return 0, fmt.Errorf("error scanning index file %q: %w", indexFileName(basePath, fileNum), err)
		}
		lastFileNum = fileNum
		fileNum++
	}
	return lastFileNum, nil
}

// StorageSize returns bytes of storage used by the index files.
func (idx *Index) StorageSize() (int64, error) {
	header, err := readHeader(idx.headerPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	fi, err := os.Stat(idx.headerPath)
	if err != nil {
		return 0, err
	}
	size := fi.Size()

	fileNum := header.FirstFile
	for {
		fi, err = os.Stat(indexFileName(idx.basePath, fileNum))
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

func scanIndexFile(ctx context.Context, basePath string, fileNum uint32, buckets Buckets, sizeBuckets SizeBuckets, maxFileSize uint32) error {
	indexPath := indexFileName(basePath, fileNum)

	// This is a single sequential read across the index file.
	file, err := openFileForScan(indexPath)
	if err != nil {
		return err
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return fmt.Errorf("cannot stat index file: %w", err)
	}
	if fi.Size() == 0 {
		return nil
	}

	sizeBuffer := make([]byte, sizePrefixSize)
	scratch := make([]byte, 256)
	var pos int64
	var i int
	for {
		if _, err = file.ReadAt(sizeBuffer, pos); err != nil {
			if err == io.EOF {
				// Finished reading entire index.
				break
			}
			if err == io.ErrUnexpectedEOF {
				log.Errorw("Unexpected EOF scanning index", "file", indexPath)
				file.Close()
				// Cut off incomplete data
				e := os.Truncate(indexPath, pos)
				if e != nil {
					log.Errorw("Error truncating file", "err", e, "file", indexPath)
				}
				break
			}
			return err
		}
		pos += sizePrefixSize

		size := binary.LittleEndian.Uint32(sizeBuffer)
		if size&deletedBit != 0 {
			// Record is deleted, so skip.
			pos += int64(size ^ deletedBit)
			continue
		}

		if int(size) > len(scratch) {
			scratch = make([]byte, size)
		}
		data := scratch[:size]
		if _, err = file.ReadAt(data, pos); err != nil {
			if err == io.ErrUnexpectedEOF || err == io.EOF {
				// The file is corrupt since the expected data could not be
				// read. Take the usable data and move on.
				log.Errorw("Unexpected EOF scanning index record", "file", indexPath)
				file.Close()
				// Cut off incomplete data
				e := os.Truncate(indexPath, pos-sizePrefixSize)
				if e != nil {
					log.Errorw("Error truncating file", "err", e, "file", indexPath)
				}
				break
			}
			return err
		}

		i++
		if i&1023 == 0 && ctx.Err() != nil {
			return ctx.Err()
		}

		bucketPrefix := BucketIndex(binary.LittleEndian.Uint32(data))
		err = buckets.Put(bucketPrefix, localPosToBucketPos(pos, fileNum, maxFileSize))
		if err != nil {
			return err
		}
		err = sizeBuckets.Put(bucketPrefix, types.Size(len(data)))
		if err != nil {
			return err
		}

		pos += int64(size)
	}
	log.Infof("Scanned %s", indexPath)
	return nil
}

// Put puts a key together with a file offset into the index.
//
// The key needs to be a cryptographically secure hash that is at least 4 bytes
// long.
func (idx *Index) Put(key []byte, location types.Block) error {
	// Get record list and bucket index
	bucket, err := idx.getBucketIndex(key)
	if err != nil {
		return err
	}
	idx.bucketLk.Lock()
	defer idx.bucketLk.Unlock()
	records, err := idx.getRecordsFromBucket(bucket)
	if err != nil {
		return err
	}

	// The key does not need the prefix that was used to find the right
	// bucket. For simplicity only full bytes are trimmed off.
	indexKey := stripBucketPrefix(key, idx.sizeBits)

	// No records stored in that bucket yet
	var newData []byte
	if records == nil {
		// As it's the first key a single byte is enough as it does not need to
		// be distinguished from other keys.
		trimmedIndexKey := indexKey[:1]
		newData = EncodeKeyPosition(KeyPositionPair{trimmedIndexKey, location})
	} else {
		// Read the record list from disk and insert the new key
		pos, prevRecord, has := records.FindKeyPosition(indexKey)

		if has && bytes.HasPrefix(indexKey, prevRecord.Key) {
			// The previous key is fully contained in the current key. We need to read the full
			// key from the main data file in order to retrieve a key that is distinguishable
			// from the one that should get inserted.
			fullPrevKey, err := idx.Primary.GetIndexKey(prevRecord.Block)
			if err != nil {
				return fmt.Errorf("error reading previous key from primary: %w", err)
			}
			// The index key has already removed the prefix that is used to determine the
			// bucket. Do the same for the full previous key.
			prevKey := stripBucketPrefix(fullPrevKey, idx.sizeBits)
			if prevKey == nil {
				// The previous key, read from the primary, was bad. This means
				// that the data in the primary at prevRecord.Bucket is not
				// good, or that data in the index is bad and prevRecord.Bucket
				// has a wrong location in the primary.  Log the error with
				// diagnostic information.
				cached, indexOffset, _, fileNum, err := idx.readBucketInfo(bucket)
				if err != nil {
					log.Errorw("Cannot read bucket", "err", err)
				} else {
					msg := "Read bad pevious key data, too short"
					if cached == nil {
						log.Errorw(msg, "offset", indexOffset, "size", indexFileName(idx.basePath, fileNum))
					} else {
						log.Error(msg)
					}
				}
				// Either way, the previous key record is not usable, so
				// overwrite it with a record for the new key.  Use the same
				// key in the index record as the previous record, since the
				// previous key is being replaced so there is no need to
				// differentiate old from new.
				//
				// This results in the data for the previous keys being lost,
				// but it may not have been present in the first place, in which
				// case that was the cause of this problem.
				newData = records.PutKeys([]KeyPositionPair{{prevRecord.Key, location}}, prevRecord.Pos, pos)
				idx.outstandingWork += types.Work(len(newData) + BucketPrefixSize + sizePrefixSize)
				idx.nextPool[bucket] = newData
				return nil
			}

			keyTrimPos := firstNonCommonByte(indexKey, prevKey)
			// Only store the new key if it doesn't exist yet.
			if keyTrimPos >= len(indexKey) {
				return nil
			}

			trimmedPrevKey := prevKey
			if keyTrimPos < len(prevKey) {
				trimmedPrevKey = prevKey[:keyTrimPos+1]
			} else {
				// trimmedPrevKey should always be a prefix. since it is not
				// here, collect some diagnostic logs.
				cached, indexOffset, _, fileNum, err := idx.readBucketInfo(bucket)
				if err != nil {
					log.Errorw("Cannot read bucket", "err", err)
				} else {
					msg := "Read bad pevious key data"
					if cached == nil {
						log.Errorw(msg, "offset", indexOffset, "size", indexFileName(idx.basePath, fileNum))
					} else {
						log.Error(msg)
					}
				}
			}
			trimmedIndexKey := indexKey[:keyTrimPos+1]
			var keys []KeyPositionPair

			// Replace the existing previous key (which is too short) with a
			// new one and also insert the new key.
			if bytes.Compare(trimmedPrevKey, trimmedIndexKey) == -1 {
				keys = []KeyPositionPair{
					{trimmedPrevKey, prevRecord.Block},
					{trimmedIndexKey, location},
				}
			} else {
				keys = []KeyPositionPair{
					{trimmedIndexKey, location},
					{trimmedPrevKey, prevRecord.Block},
				}
			}
			newData = records.PutKeys(keys, prevRecord.Pos, pos)
			// There is no need to do anything with the next key as the next key is
			// already guaranteed to be distinguishable from the new key as it was already
			// distinguishable from the previous key.
		} else {
			// The previous key is not fully contained in the key that should get inserted.
			// Hence we only need to trim the new key to the smallest one possible that is
			// still distinguishable from the previous (in case there is one) and next key
			// (in case there is one).
			prevRecordNonCommonBytePos := 0
			if has {
				prevRecordNonCommonBytePos = firstNonCommonByte(indexKey, prevRecord.Key)
			}
			// The new record will not be the last record.
			nextRecordNonCommonBytePos := 0
			if pos < records.Len() {
				// In order to determine the minimal key size, we need to get
				// the next key as well.
				nextRecord := records.ReadRecord(pos)
				nextRecordNonCommonBytePos = firstNonCommonByte(indexKey, nextRecord.Key)
			}

			// Minimum prefix of the key that is different in at least one byte
			// from the previous as well as the next key.
			minPrefix := max(
				prevRecordNonCommonBytePos,
				nextRecordNonCommonBytePos,
			)

			// We cannot trim beyond the key length.
			keyTrimPos := min(minPrefix, len(indexKey)-1)

			trimmedIndexKey := indexKey[:keyTrimPos+1]
			newData = records.PutKeys([]KeyPositionPair{{trimmedIndexKey, location}}, pos, pos)
		}
	}
	idx.outstandingWork += types.Work(len(newData) + BucketPrefixSize + sizePrefixSize)
	idx.nextPool[bucket] = newData
	return nil
}

// Update updates a key together with a file offset into the index.
func (idx *Index) Update(key []byte, location types.Block) error {
	// Get record list and bucket index
	bucket, err := idx.getBucketIndex(key)
	if err != nil {
		return err
	}
	idx.bucketLk.Lock()
	defer idx.bucketLk.Unlock()
	records, err := idx.getRecordsFromBucket(bucket)
	if err != nil {
		return err
	}

	// The key does not need the prefix that was used to find its bucket. For
	// simplicity only full bytes are trimmed off.
	indexKey := stripBucketPrefix(key, idx.sizeBits)

	var newData []byte
	// If no records are stored in that bucket yet, it means there is no key to
	// be updated.
	if records == nil {
		return fmt.Errorf("no records found in index, unable to update key")
	} else {
		// Read the record list to find the key and position.
		r := records.GetRecord(indexKey)
		if r == nil {
			return fmt.Errorf("key to update not found in index")
		}
		// We want to overwrite the key so no need to do anything else.
		// Update key in position.
		newData = records.PutKeys([]KeyPositionPair{{r.Key, location}}, r.Pos, r.NextPos())
	}

	idx.outstandingWork += types.Work(len(newData) + BucketPrefixSize + sizePrefixSize)
	idx.nextPool[bucket] = newData
	return nil
}

// Remove removes a key from the index.
func (idx *Index) Remove(key []byte) (bool, error) {
	// Get record list and bucket index
	bucket, err := idx.getBucketIndex(key)
	if err != nil {
		return false, err
	}
	idx.bucketLk.Lock()
	defer idx.bucketLk.Unlock()
	records, err := idx.getRecordsFromBucket(bucket)
	if err != nil {
		return false, err
	}

	// The key does not need the prefix that was used to find its bucket. For
	// simplicity only full bytes are trimmed off.
	indexKey := stripBucketPrefix(key, idx.sizeBits)

	var newData []byte
	// If no records are stored in that bucket yet, it means there is no key to
	// be removed.
	if records == nil {
		// No records in index. Nothing to remove.
		return false, nil
	}

	// Read the record list to find the key and its position.
	r := records.GetRecord(indexKey)
	if r == nil {
		// The record does not exist. Nothing to remove.
		return false, nil
	}

	// Remove key from record.
	newData = records.PutKeys([]KeyPositionPair{}, r.Pos, r.NextPos())
	// NOTE: We are removing the key without changing any keys. If we want
	// to optimize for storage we need to check the keys with the same prefix
	// and see if any of them can be shortened. This process will be similar
	// to finding where to put a new key.

	idx.outstandingWork += types.Work(len(newData) + BucketPrefixSize + sizePrefixSize)
	idx.nextPool[bucket] = newData
	return true, nil
}

func (idx *Index) getBucketIndex(key []byte) (BucketIndex, error) {
	if len(key) < 4 {
		return 0, types.ErrKeyTooShort
	}

	// Determine which bucket a key falls into. Use the first few bytes of they
	// key for it and interpret them as a little-endian integer.
	prefix := BucketIndex(binary.LittleEndian.Uint32(key))
	var leadingBits BucketIndex = (1 << idx.sizeBits) - 1
	return prefix & leadingBits, nil
}

// getRecordsFromBucket returns the recordList and bucket the key belongs to.
func (idx *Index) getRecordsFromBucket(bucket BucketIndex) (RecordList, error) {
	// Get the index file offset of the record list the key is in.
	cached, indexOffset, recordListSize, fileNum, err := idx.readBucketInfo(bucket)
	if err != nil {
		return nil, fmt.Errorf("error reading bucket info: %w", err)
	}
	var records RecordList
	if cached != nil {
		records = NewRecordListRaw(cached)
	} else {
		records, err = idx.readDiskBucket(indexOffset, recordListSize, fileNum)
		if err != nil {
			return nil, fmt.Errorf("error reading index records from disk: %w", err)
		}
	}
	return records, nil
}

func (idx *Index) flushBucket(bucket BucketIndex, newData []byte) (types.Block, types.Work, error) {
	if idx.length >= types.Position(idx.maxFileSize) {
		fileNum := idx.fileNum + 1
		indexPath := indexFileName(idx.basePath, fileNum)
		// If the index file being opened already exists then fileNum has
		// wrapped and there are max uint32 of index files. This means that
		// maxFileSize is set far too small or GC is disabled.
		if _, err := os.Stat(indexPath); !os.IsNotExist(err) {
			log.Warnw("Creating index file overwrites existing. Check that file size limit is not too small resulting in too many files.",
				"maxFileSize", idx.maxFileSize, "indexPath", indexPath)
		}
		file, err := openFileAppend(indexPath)
		if err != nil {
			return types.Block{}, 0, fmt.Errorf("cannot open new index file %s: %w", indexPath, err)
		}
		if err = idx.writer.Flush(); err != nil {
			return types.Block{}, 0, fmt.Errorf("cannot write to index file %s: %w", idx.file.Name(), err)
		}
		idx.file.Close()
		idx.writer.Reset(file)
		idx.file = file
		idx.fileNum = fileNum
		idx.length = 0
	}

	// Write new data to disk. The record list is prefixed with the bucket they
	// are in. This is needed in order to reconstruct the in-memory buckets
	// from the index itself.
	newDataSize := make([]byte, sizePrefixSize)
	binary.LittleEndian.PutUint32(newDataSize, uint32(len(newData))+uint32(BucketPrefixSize))
	_, err := idx.writer.Write(newDataSize)
	if err != nil {
		return types.Block{}, 0, err
	}

	bucketPrefixBuffer := make([]byte, BucketPrefixSize)
	binary.LittleEndian.PutUint32(bucketPrefixBuffer, uint32(bucket))
	if _, err = idx.writer.Write(bucketPrefixBuffer); err != nil {
		return types.Block{}, 0, err
	}

	if _, err = idx.writer.Write(newData); err != nil {
		return types.Block{}, 0, err
	}
	length := idx.length
	toWrite := types.Position(len(newData) + BucketPrefixSize + sizePrefixSize)
	idx.length += toWrite
	// Fsyncs are expensive, so do not do them here; do in explicit Sync().

	// Keep the reference to the stored data in the bucket.
	return types.Block{
		Offset: localPosToBucketPos(int64(length+sizePrefixSize), idx.fileNum, idx.maxFileSize),
		Size:   types.Size(len(newData) + BucketPrefixSize),
	}, types.Work(toWrite), nil
}

type bucketBlock struct {
	bucket BucketIndex
	blk    types.Block
}

func (idx *Index) readBucketInfo(bucket BucketIndex) ([]byte, types.Position, types.Size, uint32, error) {
	data, ok := idx.nextPool[bucket]
	if ok {
		return data, 0, 0, 0, nil
	}
	data, ok = idx.curPool[bucket]
	if ok {
		return data, 0, 0, 0, nil
	}
	bucketPos, err := idx.buckets.Get(bucket)
	if err != nil {
		return nil, 0, 0, 0, fmt.Errorf("error reading bucket: %w", err)
	}
	recordListSize, err := idx.sizeBuckets.Get(bucket)
	if err != nil {
		return nil, 0, 0, 0, fmt.Errorf("error reading size bucket: %w", err)
	}
	localPos, fileNum := localizeBucketPos(bucketPos, idx.maxFileSize)
	return nil, localPos, recordListSize, fileNum, nil
}

func (idx *Index) readDiskBucket(indexOffset types.Position, recordListSize types.Size, fileNum uint32) (RecordList, error) {
	// indexOffset should never be 0 if there is a bucket, because it is always
	// at lease sizePrefixSize into the stored data.
	if indexOffset == 0 {
		return nil, nil
	}

	file, err := os.Open(indexFileName(idx.basePath, fileNum))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read the record list from disk and get the file offset of that key in
	// the primary storage.
	data := make([]byte, recordListSize)
	if _, err = file.ReadAt(data, int64(indexOffset)); err != nil {
		return nil, err
	}
	return NewRecordList(data), nil
}

// Get the file offset in the primary storage of a key.
func (idx *Index) Get(key []byte) (types.Block, bool, error) {
	// Get record list and bucket index.
	bucket, err := idx.getBucketIndex(key)
	if err != nil {
		return types.Block{}, false, err
	}

	// Here we just need an RLock since there will not be changes over buckets.
	// So, do not use getRecordsFromBucket and instead only wrap this line of
	// code in the RLock.
	idx.bucketLk.RLock()
	cached, indexOffset, recordListSize, fileNum, err := idx.readBucketInfo(bucket)
	idx.bucketLk.RUnlock()
	if err != nil {
		return types.Block{}, false, fmt.Errorf("error reading bucket: %w", err)
	}
	var records RecordList
	if cached != nil {
		records = NewRecordListRaw(cached)
	} else {
		records, err = idx.readDiskBucket(indexOffset, recordListSize, fileNum)
		if err != nil {
			return types.Block{}, false, fmt.Errorf("error reading index records from disk: %w", err)
		}
	}
	if records == nil {
		return types.Block{}, false, nil
	}

	// The key does not need the prefix that was used to find its bucket. For
	// simplicity only full bytes are trimmed off.
	indexKey := stripBucketPrefix(key, idx.sizeBits)

	fileOffset, found := records.Get(indexKey)
	return fileOffset, found, nil
}

// Flush writes outstanding work and buffered data to the current index file
// and updates buckets.
func (idx *Index) Flush() (types.Work, error) {
	// Only one Flush at a time, otherwise the 2nd Flush can swap the pools
	// while the 1st Flush is still reading the pool being flushed. That could
	// cause the pool being read by the 1st Flush to be written to
	// concurrently.
	idx.flushLock.Lock()
	defer idx.flushLock.Unlock()

	idx.bucketLk.Lock()
	// If no new data, then nothing to do.
	if len(idx.nextPool) == 0 {
		idx.bucketLk.Unlock()
		return 0, nil
	}
	idx.curPool = idx.nextPool
	idx.nextPool = make(bucketPool, bucketPoolSize)
	idx.outstandingWork = 0
	idx.bucketLk.Unlock()

	blks := make([]bucketBlock, 0, len(idx.curPool))
	var work types.Work
	for bucket, data := range idx.curPool {
		blk, newWork, err := idx.flushBucket(bucket, data)
		if err != nil {
			return 0, err
		}
		blks = append(blks, bucketBlock{bucket, blk})
		work += newWork
	}
	err := idx.writer.Flush()
	if err != nil {
		return 0, fmt.Errorf("cannot flush data to index file %s: %w", idx.file.Name(), err)
	}
	idx.bucketLk.Lock()
	defer idx.bucketLk.Unlock()
	for _, blk := range blks {
		bucket := blk.bucket
		if err = idx.buckets.Put(bucket, blk.blk.Offset); err != nil {
			return 0, fmt.Errorf("error commiting bucket: %w", err)
		}
		if err = idx.sizeBuckets.Put(bucket, blk.blk.Size); err != nil {
			return 0, fmt.Errorf("error commiting size bucket: %w", err)
		}
	}

	if idx.updateSig != nil {
		// Send signal to tell GC there are updates.
		select {
		case idx.updateSig <- struct{}{}:
		default:
		}
	}

	return work, nil
}

// Sync commits the contents of the current index file to disk. Flush should be
// called before calling Sync.
func (idx *Index) Sync() error {
	return idx.file.Sync()
}

// Close calls Flush to write work and data to the current index file, and then
// closes the file.
func (idx *Index) Close() error {
	if idx.updateSig != nil {
		close(idx.updateSig)
		<-idx.gcDone
		idx.updateSig = nil
	}
	_, err := idx.Flush()
	if err != nil {
		idx.file.Close()
		return err
	}
	if err = idx.file.Close(); err != nil {
		return err
	}
	return idx.saveBucketState()
}
func (idx *Index) saveBucketState() error {
	bucketsFileName := savedBucketsName(idx.basePath)
	bucketsFileNameTemp := bucketsFileName + ".tmp"

	file, err := os.Create(bucketsFileNameTemp)
	if err != nil {
		return err
	}
	writer := bufio.NewWriterSize(file, indexBufferSize)
	buf := make([]byte, types.OffBytesLen+types.SizeBytesLen)

	for i, offset := range idx.buckets {
		binary.LittleEndian.PutUint64(buf, uint64(offset))
		binary.LittleEndian.PutUint32(buf[types.OffBytesLen:], uint32(idx.sizeBuckets[i]))

		_, err = writer.Write(buf)
		if err != nil {
			return err
		}
	}
	if err = writer.Flush(); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}

	log.Debug("Saved bucket state")

	// Only create the file after saving all buckets.
	return os.Rename(bucketsFileNameTemp, bucketsFileName)
}

func loadBucketState(ctx context.Context, basePath string, buckets Buckets, sizeBuckets SizeBuckets, maxFileSize uint32) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	bucketsFileName := savedBucketsName(basePath)
	file, err := os.Open(bucketsFileName)
	if err != nil {
		return err
	}
	defer func() {
		e := file.Close()
		if e != nil {
			log.Error("Error closing saved buckets file", "err", err)
		}
		if e = os.Remove(bucketsFileName); e != nil {
			log.Error("Error removing saved buckets file", "err", err)
		}
	}()

	reader := bufio.NewReaderSize(file, indexBufferSize)
	buf := make([]byte, types.OffBytesLen+types.SizeBytesLen)

	for i := 0; i < len(buckets); i++ {
		// Read offset from bucket state.
		_, err = io.ReadFull(reader, buf)
		if err != nil {
			return err
		}
		buckets[i] = types.Position(binary.LittleEndian.Uint64(buf))
		sizeBuckets[i] = types.Size(binary.LittleEndian.Uint32(buf[types.OffBytesLen:]))
	}

	log.Debug("Loaded saved bucket state")
	return nil
}

func RemoveSavedBuckets(basePath string) error {
	err := os.Remove(savedBucketsName(basePath))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (i *Index) OutstandingWork() types.Work {
	i.bucketLk.RLock()
	defer i.bucketLk.RUnlock()
	return i.outstandingWork
}

// An iterator over index entries.
//
// On each iteration it returns the position of the record within the index
// together with the raw record list data.
type IndexIter struct {
	// The index data we are iterating over
	file *os.File
	// The current position within the index
	pos int64
	// The base index file path
	base string
	// The current index file number
	fileNum uint32
}

func NewIndexIter(basePath string, fileNum uint32) *IndexIter {
	return &IndexIter{
		base:    basePath,
		fileNum: fileNum,
	}
}

func (iter *IndexIter) Next() ([]byte, types.Position, bool, error) {
	if iter.file == nil {
		file, err := openFileForScan(indexFileName(iter.base, iter.fileNum))
		if err != nil {
			if os.IsNotExist(err) {
				return nil, 0, true, nil
			}
			return nil, 0, false, err
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
			return nil, 0, false, err
		}
		size = binary.LittleEndian.Uint32(sizeBuf)
		if size&deletedBit != 0 {
			size ^= deletedBit
			iter.pos += int64(sizePrefixSize + size)
		} else {
			break
		}
	}
	pos := iter.pos + int64(sizePrefixSize)
	data := make([]byte, size)
	_, err := iter.file.ReadAt(data, pos)
	if err != nil {
		iter.file.Close()
		return nil, 0, false, err
	}

	iter.pos += int64(sizePrefixSize + size)
	return data, types.Position(pos), false, nil
}

func (iter *IndexIter) Close() error {
	if iter.file == nil {
		return nil
	}
	return iter.file.Close()
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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// firstNonCommonByte returns the position of the first character that both
// given slices have not in common.
//
// It might return an index that is bigger than the input strings. If one is
// full prefix of the other, the index will be `shorterSlice.len() + 1`, if
// both slices are equal it will be `slice.len() + 1`
func firstNonCommonByte(aa []byte, bb []byte) int {
	smallerLength := min(len(aa), len(bb))
	index := 0
	for ; index < smallerLength; index++ {
		if aa[index] != bb[index] {
			break
		}
	}
	return index
}

func openFileAppend(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
}

func openFileForScan(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_RDONLY, 0644)
}

func bucketPosToFileNum(pos types.Position, maxFileSize uint32) (bool, uint32) {
	// Bucket pos 0 means there is no data in the bucket, so indicate empty bucket.
	if pos == 0 {
		return false, 0
	}
	// The start of the entry, not the position of the record, determines which
	// is file is used.  The record begins sizePrefixSize before pos.  This
	// matters only if pos is slightly after a maxFileSize boundry, but
	// the adjusted position is not.
	return true, uint32((pos - sizePrefixSize) / types.Position(maxFileSize))
}

func localPosToBucketPos(pos int64, fileNum, maxFileSize uint32) types.Position {
	// Valid position must be non-zero, at least sizePrefixSize.
	if pos == 0 {
		panic("invalid local offset")
	}
	// fileNum is a 32bit value and will wrap at 4GiB, So 4294967296 is the
	// maximum number of index files possible.
	return types.Position(fileNum)*types.Position(maxFileSize) + types.Position(pos)
}

// localizeBucketPos decodes a bucketPos into a local pos and file number.
func localizeBucketPos(pos types.Position, maxFileSize uint32) (types.Position, uint32) {
	ok, fileNum := bucketPosToFileNum(pos, maxFileSize)
	if !ok {
		// Return 0 local pos to indicate empty bucket.
		return 0, 0
	}
	// Subtract file offset to get pos within its local file.
	localPos := pos - (types.Position(fileNum) * types.Position(maxFileSize))
	return localPos, fileNum
}

func findLastIndex(basePath string, fileNum uint32) (uint32, error) {
	var lastFound uint32
	for {
		_, err := os.Stat(indexFileName(basePath, fileNum))
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
