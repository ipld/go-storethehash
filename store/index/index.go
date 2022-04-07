package index

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
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
    |                    Repeated                 |
    |                                             |
    |         4 bytes        |  Variable size | … |
    | Size of the Recordlist |   Recordlist   | … |
```
*/
const indexFileSizeLimit = 1024 * 1024 * 1024
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
//     |         1 byte        |              1 byte             |              4 bytes             |
//     | Version of the header | Number of bits used for buckets | First index file number          |
// ```
type Header struct {
	// A version number in case we change the header
	Version byte
	// The number of bits used to determine the in-memory buckets
	BucketsBits byte
	// First index file number
	FirstFile uint32
}

func NewHeader(bucketsBits byte) Header {
	return Header{
		Version:     IndexVersion,
		BucketsBits: bucketsBits,
	}
}

type Index struct {
	sizeBits          uint8
	buckets           Buckets
	sizeBuckets       SizeBuckets
	fileBuckets       FileBuckets
	file              *os.File
	fileNum           uint32
	headerPath        string
	writer            *bufio.Writer
	Primary           primary.PrimaryStorage
	bucketLk          sync.RWMutex
	outstandingWork   types.Work
	curPool, nextPool bucketPool
	length            types.Position
	basePath          string
	updateSig         chan struct{}
}

const indexBufferSize = 32 * 4096

type bucketPool map[BucketIndex][]byte

const BucketPoolSize = 1024

// Open and index.
//
// It is created if there is no existing index at that path.
func OpenIndex(path string, primary primary.PrimaryStorage, indexSizeBits uint8) (*Index, error) {
	var lastIndexNum uint32
	var lastIndexPath string
	var file *os.File
	headerPath := path + ".info"

	buckets, err := NewBuckets(indexSizeBits)
	if err != nil {
		return nil, err
	}
	sizeBuckets, err := NewSizeBuckets(indexSizeBits)
	if err != nil {
		return nil, err
	}
	fileBuckets, err := NewFileBuckets(indexSizeBits)
	if err != nil {
		return nil, err
	}

	header, err := readHeader(headerPath)
	if os.IsNotExist(err) {
		header = NewHeader(indexSizeBits)
		if err = writeHeader(headerPath, header); err != nil {
			return nil, err
		}
		lastIndexPath = indexFileName(path, 0)
		file, err = openNewFileAppend(lastIndexPath)
		if err != nil {
			return nil, err
		}
	} else {
		if err != nil {
			return nil, err
		}

		if header.BucketsBits != indexSizeBits {
			return nil, types.ErrIndexWrongBitSize{header.BucketsBits, indexSizeBits}
		}

		lastIndexNum, err = scanIndex(path, header.FirstFile, indexSizeBits, buckets, sizeBuckets, fileBuckets)
		if err != nil {
			return nil, err
		}

		lastIndexPath = indexFileName(path, lastIndexNum)
		file, err = openFileAppend(lastIndexPath)
		if err != nil {
			return nil, err
		}

	}

	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}

	idx := &Index{
		sizeBits:    indexSizeBits,
		buckets:     buckets,
		sizeBuckets: sizeBuckets,
		fileBuckets: fileBuckets,
		file:        file,
		fileNum:     lastIndexNum,
		headerPath:  headerPath,
		writer:      bufio.NewWriterSize(file, indexBufferSize),
		Primary:     primary,
		bucketLk:    sync.RWMutex{},
		curPool:     make(bucketPool, BucketPoolSize),
		nextPool:    make(bucketPool, BucketPoolSize),
		length:      types.Position(fi.Size()),
		basePath:    path,
		updateSig:   make(chan struct{}, 1),
	}

	go idx.garbageCollector()

	return idx, nil
}

func indexFileName(basePath string, fileNum uint32) string {
	return fmt.Sprintf("%s.%d", basePath, fileNum)
}

func scanIndex(basePath string, fileNum uint32, indexSizeBits uint8, buckets Buckets, sizeBuckets SizeBuckets, fileBuckets FileBuckets) (uint32, error) {
	if len(buckets) != 1<<indexSizeBits {
		panic("wrong size of buckets")
	}
	if len(sizeBuckets) != 1<<indexSizeBits {
		panic("wrong size of sizeBuckets")
	}
	if len(fileBuckets) != 1<<indexSizeBits {
		panic("wrong size of fileBuckets")
	}

	var lastFileNum uint32
	for {
		err := scanIndexFile(basePath, fileNum, indexSizeBits, buckets, sizeBuckets, fileBuckets)
		if err != nil {
			if os.IsNotExist(err) {
				break
			}
			return 0, err
		}
		lastFileNum = fileNum
		fileNum++
	}
	return lastFileNum, nil
}

func (i *Index) StorageSize() (int64, error) {
	header, err := readHeader(i.headerPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	var size int64
	fi, err := os.Stat(i.headerPath)
	if err != nil {
		return 0, err
	}
	size += fi.Size()

	fi, err = os.Stat(i.basePath + ".free")
	if err != nil {
		if !os.IsNotExist(err) {
			return 0, err
		}
	} else {
		size += fi.Size()
	}

	fileNum := header.FirstFile
	for {
		fi, err = os.Stat(indexFileName(i.basePath, fileNum))
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

func scanIndexFile(basePath string, fileNum uint32, indexSizeBits uint8, buckets Buckets, sizeBuckets SizeBuckets, fileBuckets FileBuckets) error {
	indexPath := indexFileName(basePath, fileNum)

	// This is a single sequential read across the index file.
	file, err := openFileForScan(indexPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()

	buffered := bufio.NewReaderSize(file, 32*1024)
	sizeBuffer := make([]byte, SizePrefixSize)
	scratch := make([]byte, 256)
	var iterPos int64
	for {
		_, err = io.ReadFull(buffered, sizeBuffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		size := binary.LittleEndian.Uint32(sizeBuffer)

		pos := iterPos + SizePrefixSize
		iterPos = pos + int64(size)
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
					return err
				}
				break
			}
			return err
		}

		bucketPrefix := BucketIndex(binary.LittleEndian.Uint32(data))
		err = buckets.Put(bucketPrefix, types.Position(pos))
		if err != nil {
			return err
		}
		err = sizeBuckets.Put(bucketPrefix, types.Size(len(data)))
		if err != nil {
			return err
		}
		err = fileBuckets.Put(bucketPrefix, fileNum)
		if err != nil {
			return err
		}
	}
	fmt.Println("Scanned", indexPath)
	return nil
}

// Put a key together with a file offset into the index.
//
// The key needs to be a cryptographically secure hash and at least 4 bytes long.
func (i *Index) Put(key []byte, location types.Block) error {
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
			keyTrimPos := firstNonCommonByte(indexKey, prevKey)
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
				prevRecordNonCommonBytePos = firstNonCommonByte(indexKey, prevRecord.Key)
			}
			// The new record won't be the last record
			nextRecordNonCommonBytePos := 0
			if pos < records.Len() {
				// In order to determine the minimal key size, we need to get the next key
				// as well.
				nextRecord := records.ReadRecord(pos)
				nextRecordNonCommonBytePos = firstNonCommonByte(indexKey, nextRecord.Key)
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
	cached, indexOffset, recordListSize, fileNum, err := i.readBucketInfo(bucket)
	if err != nil {
		return nil, err
	}
	var records RecordList
	if cached != nil {
		records = NewRecordListRaw(cached)
	} else {
		records, err = i.readDiskBuckets(bucket, indexOffset, recordListSize, fileNum)
		if err != nil {
			return nil, err
		}
	}
	return records, nil
}

func (i *Index) flushBucket(bucket BucketIndex, newData []byte) (types.Block, uint32, types.Work, error) {
	if i.length > indexFileSizeLimit {
		fileNum := i.fileNum + 1
		indexPath := indexFileName(i.basePath, fileNum)
		file, err := openFileAppend(indexPath)
		if err != nil {
			return types.Block{}, 0, 0, err
		}
		i.fileNum = fileNum
		i.writer.Flush()
		i.file.Close()
		i.writer.Reset(file)
		i.file = file
		i.length = 0
	}

	// Write new data to disk. The record list is prefixed with bucket they are in. This is
	// needed in order to reconstruct the in-memory buckets from the index itself.
	// TODO vmx 2020-11-25: This should be an error and not a panic
	newDataSize := make([]byte, SizePrefixSize)
	binary.LittleEndian.PutUint32(newDataSize, uint32(len(newData))+uint32(BucketPrefixSize))
	_, err := i.writer.Write(newDataSize)
	if err != nil {
		return types.Block{}, 0, 0, err
	}

	bucketPrefixBuffer := make([]byte, BucketPrefixSize)
	binary.LittleEndian.PutUint32(bucketPrefixBuffer, uint32(bucket))
	if _, err = i.writer.Write(bucketPrefixBuffer); err != nil {
		return types.Block{}, 0, 0, err
	}

	if _, err = i.writer.Write(newData); err != nil {
		return types.Block{}, 0, 0, err
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
	}, i.fileNum, types.Work(toWrite), nil
}

type bucketBlock struct {
	bucket  BucketIndex
	blk     types.Block
	fileNum uint32
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
		blk, fileNum, newWork, err := i.flushBucket(bucket, data)
		if err != nil {
			return 0, err
		}
		blks = append(blks, bucketBlock{bucket, blk, fileNum})
		work += newWork
	}
	i.bucketLk.Lock()
	defer i.bucketLk.Unlock()
	for _, blk := range blks {
		bucket := blk.bucket
		if err := i.buckets.Put(bucket, blk.blk.Offset); err != nil {
			return 0, err
		}
		if err := i.sizeBuckets.Put(bucket, blk.blk.Size); err != nil {
			return 0, err
		}
		if err := i.fileBuckets.Put(bucket, blk.fileNum); err != nil {
			return 0, err
		}
	}
	// Send signal to update index checkpoint.
	select {
	case i.updateSig <- struct{}{}:
	default:
	}

	return work, nil
}

func (i *Index) readBucketInfo(bucket BucketIndex) ([]byte, types.Position, types.Size, uint32, error) {
	data, ok := i.nextPool[bucket]
	if ok {
		return data, 0, 0, 0, nil
	}
	data, ok = i.curPool[bucket]
	if ok {
		return data, 0, 0, 0, nil
	}
	indexOffset, err := i.buckets.Get(bucket)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	recordListSize, err := i.sizeBuckets.Get(bucket)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	fileNum, err := i.fileBuckets.Get(bucket)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	return nil, indexOffset, recordListSize, fileNum, nil
}

func (i *Index) readDiskBuckets(bucket BucketIndex, indexOffset types.Position, recordListSize types.Size, fileNum uint32) (RecordList, error) {
	if indexOffset == 0 {
		return nil, nil
	}

	file, err := os.Open(indexFileName(i.basePath, fileNum))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read the record list from disk and get the file offset of that key in the primary
	// storage.
	data := make([]byte, recordListSize)
	if _, err = file.ReadAt(data, int64(indexOffset)); err != nil {
		return nil, err
	}
	return NewRecordList(data), nil
}

// Get the file offset in the primary storage of a key.
func (i *Index) Get(key []byte) (types.Block, bool, error) {
	// Get record list and bucket index
	bucket, err := i.getBucketIndex(key)
	if err != nil {
		return types.Block{}, false, err
	}

	// Here we just nead an RLock, there won't be changes over buckets.
	// This is why we don't use getRecordsFromBuckets to wrap only this
	// line of code in the lock
	i.bucketLk.RLock()
	cached, indexOffset, recordListSize, fileNum, err := i.readBucketInfo(bucket)
	i.bucketLk.RUnlock()
	if err != nil {
		return types.Block{}, false, err
	}
	var records RecordList
	if cached != nil {
		records = NewRecordListRaw(cached)
	} else {
		records, err = i.readDiskBuckets(bucket, indexOffset, recordListSize, fileNum)
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
	return i.commit()
}

func (i *Index) Sync() error {
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
	close(i.updateSig)
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
	index io.ReadCloser
	// The current position within the index
	pos types.Position
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
	if iter.index == nil {
		file, err := openFileForScan(indexFileName(iter.base, iter.fileNum))
		if err != nil {
			if os.IsNotExist(err) {
				return nil, 0, true, nil
			}
			return nil, 0, false, err
		}
		iter.index = file
		iter.pos = 0
	}

	size, err := readSizePrefix(iter.index)
	switch err {
	case nil:
		pos := iter.pos + types.Position(SizePrefixSize)
		iter.pos += types.Position(SizePrefixSize) + types.Position(size)
		data := make([]byte, size)
		_, err := io.ReadFull(iter.index, data)
		if err != nil {
			iter.index.Close()
			return nil, 0, false, err
		}
		return data, pos, false, nil
	case io.EOF:
		iter.index.Close()
		iter.index = nil
		iter.fileNum++
		return iter.Next()
	default:
		iter.index.Close()
		return nil, 0, false, err
	}
}

func (iter *IndexIter) Close() error {
	if iter.index == nil {
		return nil
	}
	return iter.index.Close()
}

// Only reads the size prefix of the data and returns it.
func readSizePrefix(reader io.Reader) (uint32, error) {
	sizeBuffer := make([]byte, SizePrefixSize)
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

func openNewFileAppend(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_EXCL, 0644)
}

func openFileForScan(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_RDONLY, 0644)
}
