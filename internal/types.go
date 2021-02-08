package store

// Position indicates a position in a file
type Position uint64

type Block struct {
	Offset Position
	Size   Size
}

type Size uint32

type Work uint64
