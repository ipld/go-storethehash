package index

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/ipld/go-storethehash/store/primary"
	"github.com/ipld/go-storethehash/store/types"
)

/* An append-only log [`recordlist`]s.

The format of that append only log is:

```text
    |                  Once              |                    Repeated                 |
    |                                    |                                             |
    |       4 bytes      | Variable size |         4 bytes        |  Variable size | … |
    | Size of the header |   [`Header`]  | Size of the Recordlist |   Recordlist   | … |
```
*/
const IndexVersion uint8 = 2

// Number of bytes used for the size prefix of a record list.
const SizePrefixSize = 4

// Remove the prefix that is used for the bucket.
//
// The first bits of a key are used to determine the bucket to put the key into. This function
// removes those bytes. Only bytes that are fully covered by the bits are removed. E.g. a bit
// value of 19 will remove only 2 bytes, whereas 24 bits removes 3 bytes.
func StripBucketPrefix(key []byte, bits byte) []byte {
	return key[(bits / 8):]
}

// The header of the index
//
// The serialized header is:
// ```text
//     |         1 byte        |                1 byte               |
//     | Version of the header | Number of bits used for the buckets |
// ```
type Header struct {
	// A version number in case we change the header
	Version byte
	// The number of bits used to determine the in-memory buckets
	BucketsBits byte
}

func NewHeader(bucketsBits byte) Header {
	return Header{IndexVersion, bucketsBits}
}

func FromHeader(h Header) []byte {
	return []byte{h.Version, h.BucketsBits}
}

func FromBytes(bytes []byte) Header {
	return Header{
		Version:     bytes[0],
		BucketsBits: bytes[1],
	}
}

type Index struct {
	sizeBits          uint8
	buckets           Buckets
	sizeBuckets       SizeBuckets
	file              *os.File
	writer            *bufio.Writer
	Primary           primary.PrimaryStorage
	bucketLk          sync.RWMutex
	outstandingWork   types.Work
	curPool, nextPool bucketPool
	length            types.Position
	cpUpdate          chan struct{}
	gcMutex           sync.RWMutex
}

const indexBufferSize = 32 * 4096

type bucketPool map[BucketIndex][]byte

const BucketPoolSize = 1024

// Open and index.
//
// It is created if there is no existing index at that path.
func OpenIndex(path string, primary primary.PrimaryStorage, indexSizeBits uint8) (*Index, error) {
	var file *os.File
	var buckets Buckets
	var sizeBuckets SizeBuckets
	var length types.Position
	stat, err := os.Stat(path)
	if os.IsNotExist(err) {
		header := FromHeader(NewHeader(indexSizeBits))
		headerSize := make([]byte, 4)
		binary.LittleEndian.PutUint32(headerSize, uint32(len(header)))

		file, err = openFileRandom(path, os.O_RDWR|os.O_APPEND|os.O_EXCL|os.O_CREATE)
		if err != nil {
			return nil, err
		}
		if _, err := file.Write(headerSize); err != nil {
			return nil, err
		}
		if _, err = file.Write(header); err != nil {
			return nil, err
		}
		if err := file.Sync(); err != nil {
			return nil, err
		}
		length = types.Position(len(header) + len(headerSize))
		buckets, err = NewBuckets(indexSizeBits)
		if err != nil {
			return nil, err
		}
		sizeBuckets, err = NewSizeBuckets(indexSizeBits)
		if err != nil {
			return nil, err
		}
	} else {
		if err != nil {
			return nil, err
		}
		err = compactIndex(path, indexSizeBits)
		if err != nil {
			return nil, err
		}
		buckets, sizeBuckets, err = scanIndex(path, indexSizeBits)
		if err != nil {
			return nil, err
		}
		file, err = openFileRandom(path, os.O_RDWR|os.O_APPEND|os.O_EXCL)
		if err != nil {
			return nil, err
		}
		length = types.Position(stat.Size())
	}

	idx := &Index{
		sizeBits:    indexSizeBits,
		buckets:     buckets,
		sizeBuckets: sizeBuckets,
		file:        file,
		writer:      bufio.NewWriterSize(file, indexBufferSize),
		Primary:     primary,
		bucketLk:    sync.RWMutex{},
		curPool:     make(bucketPool, BucketPoolSize),
		nextPool:    make(bucketPool, BucketPoolSize),
		length:      length,
		cpUpdate:    make(chan struct{}, 1),
	}
	go idx.checkpointer()
	return idx, nil
}

func scanIndex(path string, indexSizeBits uint8) (Buckets, SizeBuckets, error) {
	// this is a single sequential read across the whole index
	file, err := openFileForScan(path)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = file.Close()
	}()
	header, bytesRead, err := ReadHeader(file)
	if err != nil {
		return nil, nil, err
	}
	if header.BucketsBits != indexSizeBits {
		return nil, nil, types.ErrIndexWrongBitSize{header.BucketsBits, indexSizeBits}
	}
	buckets, err := NewBuckets(indexSizeBits)
	if err != nil {
		return nil, nil, err
	}
	sizeBuckets, err := NewSizeBuckets(indexSizeBits)
	if err != nil {
		return nil, nil, err
	}
	buffered := bufio.NewReaderSize(file, 32*1024)

	// Current position within the index.
	iterPos := types.Position(bytesRead)

	checkpoint, err := readCheckpoint(checkpointPath(path))
	if err != nil {
		return nil, nil, err
	}
	// If there is a checkpoint, skip everything up to the checkpoint.
	if checkpoint != 0 {
		if _, err = file.Seek(checkpoint, 0); err != nil {
			return nil, nil, err
		}
		iterPos = types.Position(checkpoint)
	}

	sizeBuffer := make([]byte, SizePrefixSize)
	scratch := make([]byte, 256)
	for {
		_, err = io.ReadFull(buffered, sizeBuffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, nil, err
		}
		size := binary.LittleEndian.Uint32(sizeBuffer)

		pos := iterPos + types.Position(SizePrefixSize)
		iterPos = pos + types.Position(size)
		if int(size) > len(scratch) {
			scratch = make([]byte, size)
		}
		data := scratch[:size]
		_, err = io.ReadFull(buffered, data)
		if err != nil {
			if err == io.EOF {
				// The file is corrupt. Though it's not a problem, just take
				// the data we are able to use and move on.
				if _, err = file.Seek(0, 2); err != nil {
					return nil, nil, err
				}
				break
			}
			return nil, nil, err
		}

		bucketPrefix := BucketIndex(binary.LittleEndian.Uint32(data))
		err = buckets.Put(bucketPrefix, pos)
		if err != nil {
			return nil, nil, err
		}
		err = sizeBuckets.Put(bucketPrefix, types.Size(len(data)))
		if err != nil {
			return nil, nil, err
		}
	}
	return buckets, sizeBuckets, nil
}

// Put a key together with a file offset into the index.
//
// The key needs to be a cryptographically secure hash and at least 4 bytes long.
func (i *Index) Put(key []byte, location types.Block) error {
	i.gcMutex.RLock()
	defer i.gcMutex.RUnlock()

	// Get record list and bucket index
	bucket, err := i.getBucketIndex(key)
	if err != nil {
		return err
	}
	i.bucketLk.Lock()
	defer i.bucketLk.Unlock()
	records, err := i.getRecordsFromBucket(bucket)
	if err != nil {
		return err
	}

	// The key doesn't need the prefix that was used to find the right bucket. For simplicty
	// only full bytes are trimmed off.
	indexKey := StripBucketPrefix(key, i.sizeBits)

	// No records stored in that bucket yet
	var newData []byte
	if records == nil {
		// As it's the first key a single byte is enough as it doesn't need to be distinguised
		// from other keys.
		trimmedIndexKey := indexKey[:1]
		newData = EncodeKeyPosition(KeyPositionPair{trimmedIndexKey, location})
	} else {
		// Read the record list from disk and insert the new key
		pos, prevRecord, has := records.FindKeyPosition(indexKey)

		if has && bytes.HasPrefix(indexKey, prevRecord.Key) {

			// The previous key is fully contained in the current key. We need to read the full
			// key from the main data file in order to retrieve a key that is distinguishable
			// from the one that should get inserted.
			fullPrevKey, err := i.Primary.GetIndexKey(prevRecord.Block)
			if err != nil {
				return err
			}
			// The index key has already removed the prefix that is used to determine the
			// bucket. Do the same for the full previous key.
			prevKey := StripBucketPrefix(fullPrevKey, i.sizeBits)
			keyTrimPos := FirstNonCommonByte(indexKey, prevKey)
			// Only store the new key if it doesn't exist yet.
			if keyTrimPos >= len(indexKey) {
				return nil
			}

			trimmedPrevKey := prevKey[:keyTrimPos+1]
			trimmedIndexKey := indexKey[:keyTrimPos+1]
			var keys []KeyPositionPair

			// Replace the existing previous key (which is too short) with a new one and
			// also insert the new key.
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
				prevRecordNonCommonBytePos = FirstNonCommonByte(indexKey, prevRecord.Key)
			}
			// The new record won't be the last record
			nextRecordNonCommonBytePos := 0
			if pos < records.Len() {
				// In order to determine the minimal key size, we need to get the next key
				// as well.
				nextRecord := records.ReadRecord(pos)
				nextRecordNonCommonBytePos = FirstNonCommonByte(indexKey, nextRecord.Key)
			}

			// Minimum prefix of the key that is different in at least one byte from the
			// previous as well as the next key.
			minPrefix := max(
				prevRecordNonCommonBytePos,
				nextRecordNonCommonBytePos,
			)

			// We cannot trim beyond the key length
			keyTrimPos := min(minPrefix, len(indexKey)-1)

			trimmedIndexKey := indexKey[:keyTrimPos+1]
			newData = records.PutKeys([]KeyPositionPair{{trimmedIndexKey, location}}, pos, pos)
		}
	}
	i.outstandingWork += types.Work(len(newData) + BucketPrefixSize + SizePrefixSize)
	i.nextPool[bucket] = newData
	return nil
}

// Update a key together with a file offset into the index.
func (i *Index) Update(key []byte, location types.Block) error {
	i.gcMutex.RLock()
	defer i.gcMutex.RUnlock()

	// Get record list and bucket index
	bucket, err := i.getBucketIndex(key)
	if err != nil {
		return err
	}
	i.bucketLk.Lock()
	defer i.bucketLk.Unlock()
	records, err := i.getRecordsFromBucket(bucket)
	if err != nil {
		return err
	}

	// The key doesn't need the prefix that was used to find the right bucket. For simplicty
	// only full bytes are trimmed off.
	indexKey := StripBucketPrefix(key, i.sizeBits)

	var newData []byte
	// If no records stored in that bucket yet it means there is no key
	// to be updated.
	if records == nil {
		return fmt.Errorf("no records found in index. We can't udpate the key")
	} else {
		// Read the record list to find the key and position
		r := records.GetRecord(indexKey)
		if r == nil {
			return fmt.Errorf("key to update not found in index")
		}
		// We want to overwrite the key so no need to do anything else.
		// Update key in position.
		newData = records.PutKeys([]KeyPositionPair{{r.Key, location}}, r.Pos, r.NextPos())
	}

	i.outstandingWork += types.Work(len(newData) + BucketPrefixSize + SizePrefixSize)
	i.nextPool[bucket] = newData
	return nil
}

// Remove a key from index
func (i *Index) Remove(key []byte) (bool, error) {
	i.gcMutex.RLock()
	defer i.gcMutex.RUnlock()

	// Get record list and bucket index
	bucket, err := i.getBucketIndex(key)
	if err != nil {
		return false, err
	}
	i.bucketLk.Lock()
	defer i.bucketLk.Unlock()
	records, err := i.getRecordsFromBucket(bucket)
	if err != nil {
		return false, err
	}

	// The key doesn't need the prefix that was used to find the right bucket. For simplicty
	// only full bytes are trimmed off.
	indexKey := StripBucketPrefix(key, i.sizeBits)

	var newData []byte
	// If no records stored in that bucket yet it means there is no key
	// to be removed.
	if records == nil {
		// No records in index. Nothing to remove.
		return false, nil
	}

	// Read the record list to find the key and position
	r := records.GetRecord(indexKey)
	if r == nil {
		// The record doesn't exist. Nothing to remove
		return false, nil
	}

	// Remove key from record
	newData = records.PutKeys([]KeyPositionPair{}, r.Pos, r.NextPos())
	// NOTE: We are removing the key without changing any keys. If we want
	// to optimize for storage we need to check the keys with the same prefix
	// and see if any of them can be shortened. This process will be similar
	// to finding where to put a new key.

	i.outstandingWork += types.Work(len(newData) + BucketPrefixSize + SizePrefixSize)
	i.nextPool[bucket] = newData
	return true, nil
}

func (i *Index) getBucketIndex(key []byte) (BucketIndex, error) {
	if len(key) < 4 {
		return 0, types.ErrKeyTooShort
	}

	// Determine which bucket a key falls into. Use the first few bytes of they key for it and
	// interpret them as a little-endian integer.
	prefix := BucketIndex(binary.LittleEndian.Uint32(key))
	var leadingBits BucketIndex = (1 << i.sizeBits) - 1
	return prefix & leadingBits, nil
}

// getRecordsFromBucket returns the recordList and bucket the key belongs to.
func (i *Index) getRecordsFromBucket(bucket BucketIndex) (RecordList, error) {
	// Get the index file offset of the record list the key is in.
	cached, indexOffset, recordListSize, err := i.readBucketInfo(bucket)
	if err != nil {
		return nil, err
	}
	var records RecordList
	if cached != nil {
		records = NewRecordListRaw(cached)
	} else {
		records, err = i.readDiskBuckets(bucket, indexOffset, recordListSize)
		if err != nil {
			return nil, err
		}
	}
	return records, nil
}

func (i *Index) flushBucket(bucket BucketIndex, newData []byte) (types.Block, types.Work, error) {
	// Write new data to disk. The record list is prefixed with bucket they are in. This is
	// needed in order to reconstruct the in-memory buckets from the index itself.
	// TODO vmx 2020-11-25: This should be an error and not a panic
	newDataSize := make([]byte, SizePrefixSize)
	binary.LittleEndian.PutUint32(newDataSize, uint32(len(newData))+uint32(BucketPrefixSize))
	if _, err := i.writer.Write(newDataSize); err != nil {
		return types.Block{}, 0, err
	}

	bucketPrefixBuffer := make([]byte, BucketPrefixSize)
	binary.LittleEndian.PutUint32(bucketPrefixBuffer, uint32(bucket))
	if _, err := i.writer.Write(bucketPrefixBuffer); err != nil {
		return types.Block{}, 0, err
	}
	if _, err := i.writer.Write(newData); err != nil {
		return types.Block{}, 0, err
	}
	length := i.length
	toWrite := types.Position(len(newData) + BucketPrefixSize + SizePrefixSize)
	i.length += toWrite
	// Fsyncs are expensive
	//self.file.syncData()?;

	// Keep the reference to the stored data in the bucket
	return types.Block{
		Offset: length + types.Position(SizePrefixSize),
		Size:   types.Size(len(newData) + BucketPrefixSize),
	}, types.Work(toWrite), nil
}

type bucketBlock struct {
	bucket BucketIndex
	blk    types.Block
}

func (i *Index) commit() (types.Work, error) {
	i.bucketLk.Lock()
	nextPool := i.curPool
	i.curPool = i.nextPool
	i.nextPool = nextPool
	i.outstandingWork = 0
	i.bucketLk.Unlock()
	if len(i.curPool) == 0 {
		return 0, nil
	}
	blks := make([]bucketBlock, 0, len(i.curPool))
	var work types.Work
	for bucket, data := range i.curPool {
		blk, newWork, err := i.flushBucket(bucket, data)
		if err != nil {
			return 0, err
		}
		blks = append(blks, bucketBlock{bucket, blk})
		work += newWork
	}
	i.bucketLk.Lock()
	defer i.bucketLk.Unlock()
	for _, blk := range blks {
		bucket := blk.bucket
		pos := blk.blk.Offset
		size := blk.blk.Size
		if err := i.buckets.Put(bucket, pos); err != nil {
			return 0, err
		}
		if err := i.sizeBuckets.Put(bucket, size); err != nil {
			return 0, err
		}
	}
	// Send signal to update index checkpoint.
	i.cpUpdate <- struct{}{}

	return work, nil
}

func (i *Index) readBucketInfo(bucket BucketIndex) ([]byte, types.Position, types.Size, error) {
	data, ok := i.nextPool[bucket]
	if ok {
		return data, 0, 0, nil
	}
	data, ok = i.curPool[bucket]
	if ok {
		return data, 0, 0, nil
	}
	indexOffset, err := i.buckets.Get(bucket)
	if err != nil {
		return nil, 0, 0, err
	}
	recordListSize, err := i.sizeBuckets.Get(bucket)
	if err != nil {
		return nil, 0, 0, err
	}
	return nil, indexOffset, recordListSize, nil
}

func (i *Index) readDiskBuckets(bucket BucketIndex, indexOffset types.Position, recordListSize types.Size) (RecordList, error) {
	if indexOffset == 0 {
		return nil, nil
	}
	// Read the record list from disk and get the file offset of that key in the primary
	// storage.
	data := make([]byte, recordListSize)
	_, err := i.file.ReadAt(data, int64(indexOffset))
	if err != nil {
		return nil, err
	}
	return NewRecordList(data), nil
}

// Get the file offset in the primary storage of a key.
func (i *Index) Get(key []byte) (types.Block, bool, error) {
	i.gcMutex.RLock()
	defer i.gcMutex.RUnlock()

	// Get record list and bucket index
	bucket, err := i.getBucketIndex(key)
	if err != nil {
		return types.Block{}, false, err
	}

	// Here we just nead an RLock, there won't be changes over buckets.
	// This is why we don't use getRecordsFromBuckets to wrap only this
	// line of code in the lock
	i.bucketLk.RLock()
	cached, indexOffset, recordListSize, err := i.readBucketInfo(bucket)
	i.bucketLk.RUnlock()
	if err != nil {
		return types.Block{}, false, err
	}
	var records RecordList
	if cached != nil {
		records = NewRecordListRaw(cached)
	} else {
		records, err = i.readDiskBuckets(bucket, indexOffset, recordListSize)
		if err != nil {
			return types.Block{}, false, err
		}
	}
	if records == nil {
		return types.Block{}, false, nil
	}

	// The key doesn't need the prefix that was used to find the right bucket. For simplicty
	// only full bytes are trimmed off.
	indexKey := StripBucketPrefix(key, i.sizeBits)

	fileOffset, found := records.Get(indexKey)
	return fileOffset, found, nil
}

func (i *Index) Flush() (types.Work, error) {
	i.gcMutex.RLock()
	defer i.gcMutex.RUnlock()

	return i.commit()
}

func (i *Index) Sync() error {
	i.gcMutex.RLock()
	defer i.gcMutex.RUnlock()

	if err := i.writer.Flush(); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	i.bucketLk.Lock()
	i.curPool = make(bucketPool, BucketPoolSize)
	i.bucketLk.Unlock()
	return nil
}

func (i *Index) Close() error {
	close(i.cpUpdate)
	return i.file.Close()
}

func (i *Index) OutstandingWork() types.Work {
	i.bucketLk.RLock()
	defer i.bucketLk.RUnlock()
	return i.outstandingWork
}

// An iterator over index entries.
//
// On each iteration it returns the position of the record within the index together with the raw
// record list data.
type IndexIter struct {
	// The index data we are iterating over
	index io.Reader
	// The current position within the index
	pos types.Position
}

func NewIndexIter(index io.Reader, pos types.Position) *IndexIter {
	return &IndexIter{index, pos}
}

func (iter *IndexIter) Next() ([]byte, types.Position, bool, error) {
	size, err := ReadSizePrefix(iter.index)
	switch err {
	case nil:
		pos := iter.pos + types.Position(SizePrefixSize)
		iter.pos += types.Position(SizePrefixSize) + types.Position(size)
		data := make([]byte, size)
		_, err := io.ReadFull(iter.index, data)
		if err != nil {
			return nil, 0, false, err
		}
		return data, pos, false, nil
	case io.EOF:
		return nil, 0, true, nil
	default:
		return nil, 0, false, err
	}
}

// Only reads the size prefix of the data and returns it.
func ReadSizePrefix(reader io.Reader) (uint32, error) {
	sizeBuffer := make([]byte, SizePrefixSize)
	_, err := io.ReadFull(reader, sizeBuffer)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(sizeBuffer), nil
}

// Returns the headet together with the bytes read.
//
// The bytes read include all the bytes that were read by this function. Hence it also includes
// the 4-byte size prefix of the header besides the size of the header data itself.
func ReadHeader(file *os.File) (Header, types.Position, error) {
	headerSizeBuffer := make([]byte, SizePrefixSize)
	_, err := io.ReadFull(file, headerSizeBuffer)
	if err != nil {
		return Header{}, 0, err
	}
	headerSize := binary.LittleEndian.Uint32(headerSizeBuffer)
	headerBytes := make([]byte, headerSize)
	_, err = io.ReadFull(file, headerBytes)
	if err != nil {
		return Header{}, 0, err
	}
	return FromBytes(headerBytes), types.Position(SizePrefixSize) + types.Position(headerSize), nil
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

// Returns the position of the first character that both given slices have not in common.
//
// It might return an index that is bigger than the input strings. If one is full prefix of the
// other, the index will be `shorterSlice.len() + 1`, if both slices are equal it will be
// `slice.len() + 1`
func FirstNonCommonByte(aa []byte, bb []byte) int {
	smallerLength := min(len(aa), len(bb))
	index := 0
	for ; index < smallerLength; index++ {
		if aa[index] != bb[index] {
			break
		}
	}
	return index
}

func openFile(name string, flag int, perm os.FileMode, advice int) (*os.File, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	/*
		err = unix.Fadvise(int(f.Fd()), 0, 0, advice)
		if err != nil {
			return nil, fmt.Errorf("fadvise: %w", err)
		}
	*/
	return f, nil
}

func openFileRandom(name string, flag int) (*os.File, error) {
	//	return openFile(name, flag, 0o644, unix.FADV_RANDOM)
	return openFile(name, flag, 0o644, 0)
}

func openFileForScan(name string) (*os.File, error) {
	//	return openFile(name, os.O_RDONLY, 0o644, unix.FADV_SEQUENTIAL)
	return openFile(name, os.O_RDONLY, 0o644, 0)
}
