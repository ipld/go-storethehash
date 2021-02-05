package cidprimary

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"

	"github.com/ipfs/go-cid"
	util "github.com/ipld/go-car/util"
	"github.com/multiformats/go-multihash"

	store "github.com/hannahhoward/go-storethehash/internal"
)

var bufferSize = 1 << 20

const CIDSizePrefix = 4

// A primary storage that is CID aware.
type CIDPrimary struct {
	reader *os.File
	writer *bufio.Writer
	length store.Position
}

func OpenCIDPrimary(path string) (*CIDPrimary, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_EXCL|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	length, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		return nil, err
	}
	return &CIDPrimary{
		reader: file,
		writer: bufio.NewWriterSize(file, bufferSize),
		length: store.Position(length),
	}, nil
}

func (cp *CIDPrimary) Get(blk store.Block) (key []byte, value []byte, err error) {
	read := make([]byte, CIDSizePrefix+int(blk.Size))
	cp.reader.ReadAt(read, int64(blk.Offset))
	c, value, err := readNode(read[4:])
	return c.Bytes(), value, err
}

func readNode(data []byte) (cid.Cid, []byte, error) {
	c, n, err := util.ReadCid(data)
	if err != nil {
		return cid.Cid{}, nil, err
	}

	return c, data[n:], nil
}

func (cp *CIDPrimary) Put(key []byte, value []byte) (blk store.Block, err error) {
	length := cp.length
	size := len(key) + len(value)
	sizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBuf, uint32(size))
	if _, err := cp.writer.Write(sizeBuf); err != nil {
		return store.Block{}, err
	}
	cp.length += CIDSizePrefix

	if _, err := cp.writer.Write(key); err != nil {
		return store.Block{}, err
	}
	cp.length += store.Position(len(key))

	if _, err := cp.writer.Write(value); err != nil {
		return store.Block{}, err
	}
	cp.length += store.Position(len(value))
	return store.Block{Offset: length, Size: store.Size(size)}, nil
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

func (cp *CIDPrimary) Flush() error {
	return cp.writer.Flush()
}

func (cp *CIDPrimary) Iter() (store.PrimaryStorageIter, error) {
	if _, err := cp.reader.Seek(0, os.SEEK_SET); err != nil {
		return nil, err
	}
	return &cidPrimaryIter{cp}, nil
}

type cidPrimaryIter struct {
	cp *CIDPrimary
}

func (cpi *cidPrimaryIter) Next() ([]byte, []byte, error) {
	sizeBuff := make([]byte, CIDSizePrefix)
	_, err := io.ReadFull(cpi.cp.reader, sizeBuff)
	if err != nil {
		return nil, nil, err
	}
	size := binary.LittleEndian.Uint32(sizeBuff)
	read := make([]byte, int(size))
	_, err = io.ReadFull(cpi.cp.reader, read)
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, nil, err
	}
	c, value, err := readNode(read[4:])
	return c.Bytes(), value, err
}

var _ store.PrimaryStorage = &CIDPrimary{}
