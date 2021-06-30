package testutil

import (
	"math/rand"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	util "github.com/ipfs/go-ipfs-util"
)

var blockGenerator = blocksutil.NewBlockGenerator()

// RandomBytes returns a byte array of the given size with random values.
func RandomBytes(n int64) []byte {
	data := make([]byte, n)
	_, _ = rand.Read(data)
	return data
}

// GenerateBlocksOfSize generates a series of blocks of the given byte size
func GenerateBlocksOfSize(n int, size int64) []blocks.Block {
	generatedBlocks := make([]blocks.Block, 0, n)
	for i := 0; i < n; i++ {
		data := RandomBytes(size)
		mhash := util.Hash(data)
		c := cid.NewCidV1(cid.Raw, mhash)
		b, _ := blocks.NewBlockWithCid(data, c)
		generatedBlocks = append(generatedBlocks, b)

	}
	return generatedBlocks
}

// GenerateCids produces n content identifiers.
func GenerateCids(n int) []cid.Cid {
	cids := make([]cid.Cid, 0, n)
	for i := 0; i < n; i++ {
		c := blockGenerator.Next().Cid()
		cids = append(cids, c)
	}
	return cids
}
