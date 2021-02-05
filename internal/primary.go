package store

// Position indicates a position in a file
type Position uint64

type Block struct {
	Offset Position
	Size   Size
}

// PrimaryStorage is an interface for storing and retrieving key value pairs on disk
type PrimaryStorage interface {
	// Returns the key-value pair from the given position.
	Get(blk Block) (key []byte, value []byte, err error)

	// Saves a key-value pair and returns the position it was stored at.
	Put(key []byte, value []byte) (blk Block, err error)

	//  Creates a key that can be used for the index.
	//
	//  The index needs a key which is at least 4 bytes long and contains random bytes (the more
	//  random the better). In case the keys you are storing don't have this property, you can
	//  transform them with this function.
	//
	//  By default it just returns the original key with any changes.
	IndexKey(key []byte) ([]byte, error)

	// Returns the key that is used for the index which is stored at the given position.
	//
	// Note that this key might differ from the key that is actually stored.
	GetIndexKey(blk Block) ([]byte, error)

	Flush() error

	Iter() (PrimaryStorageIter, error)
}

type PrimaryStorageIter interface {
	// Next should return io.EOF when done
	Next() (key []byte, value []byte, err error)
}
