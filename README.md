# go-storethehash

This is a storage for hashes. Targeted at content addressable systems.

go-storethehash is primarily an index, though it also includes some basic primary storage implementations as well, so that it can be used as a full key-value store.

This was originally ported from go port of [vmx/storethehash](https://github.com/vmx/storethehash#readme)

go-storethehash is a `golang` port of [vmx/storethehash](https://github.com/vmx/storethehash#readme)

## How it Works

Store-the-hash consists of four distinct pieces. The in-memory buckets, the on-disk index, the primary storage, and the freelist.

### Buckets

The buckets are an in-memory data structure that maps small byte ranges (at most 4) to file offsets in the index file. An instance of Store-the-hash is bound to a specific number of bits (at most 32) that are used to determine to which bucket a key belongs to. If you e.g. decide to use 24-bits, then there will be 2^24 = 16m buckets. As file offsets are stored as 64-bit integers the buckets will consume at least 128MiB of memory.

When a new key is inserted, the first few bits (24 in this example) will be used to map it to a bucket. The bytes are interpreted as little-endian. From a key like `[0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07]` we would take the first 3 bytes `[0x00, 0x01, 0x02]` and convert into a 32-bit integer it would be `131328` (`0x020100`). So the file offset of the index would be stored in a bucket at position `131328`.


### Index

The index is an append-only log that maps keys to offsets in the primary storage. Updates are always appended, there are no in-place updates. The index consists of so-called record lists. There is one record list per bucket. Such a list contains all keys that are mapped to one specific bucket.

The index is split across multiple files on disk. By default, each of these files may reach a maximum size of approcimately 1GiB. When all record lists in an index file are no longer referenced by any file offsets in the Bucket, that index file can be deleted.  These unused index files are removed, in order, by the index garbage collector.  

#### Record list

A record list is a sorted list of key-value pairs where the value is an (64-bit integer) offset in the primary storage. Not the full keys are stored, but only parts of them. First, we don't need the prefix that is used to determine the bucket they are in, it's the same for every key within a record list. So the key from above `[0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07]` is trimmed to just `[0x03, 0x04, 0x05, 0x06, 0x07]`. But there is more. Only the parts of the key that is needed to distinguish it from another key are stored. For example if we have the already trimmed key from before `[0x03, 0x04, 0x05, 0x06, 0x07]` and another trimmed key `[0x03, 0x04, 0x08, 0x09, 0x10]`, then only the prefixes until a byte that is not equal are stored. In this case the keys that are stored are `[0x03, 0x04, 0x05]` and `[0x03, 0x04, 0x08]`.

Given the random distribution of the keys, this leads to huge space savings.

### Primary storage

The requirement for the primary storage is that it can return a key and value by a given position. That position will be used in the index to retrieve the actual value for a key.

There are various implementations of a primary storage provided.

- An in-memory storage implementation
- An implementation that is [CID](https://github.com/multiformats/cid/) aware.
- An implementation that is [Multihash](https://github.com/multiformats/multihash) aware.

### Freelist

A freelist is a list of primary storage locations that are no longer used due to updates or deletes. The freelist is maintained is intended for use in primary storage compaction and in reuse of primary storage space. This is not yet implemented.

## Trade-offs

This storage is meant to also work with larger deployments with 100s of millions of keys. There is a trade-off that needs to be made between the index growth and the memory usage. The lower the memory usage the larger the record lists become. There is some more overhead involved but here is an example of the approximate usage if you would have 512m keys.

| Buckets bit size | Number of Buckets | Buckets memory consumption| Avg. keys per record list | Avg. key size (in bytes) | Record list size (key + 8 bytes file offset) |
| -: | ------------: | -------: | --------: | --: | -------: |
|  8 |           256 |    2 KiB | 2_000_000 | <=3 | < 21 MiB |
| 12 |         4_096 |   32 KiB |   125_000 | <=3 | <  2 MiB |
| 16 |        65_536 |  512 KiB |      7813 | <=2 | < 77 KiB |
| 20 |     1_048_576 |    8 MiB |       489 | <=2 | <  5 KiB |
| 24 |    16_777_216 |  128 MiB |        31 |   1 |  < 280 B |
| 28 |   268_435_456 |    2 GiB |         2 |   1 |   < 19 B |
| 32 | 4_294_967_296 |   32 GiB |         1 |   1 |    <10 B |

The index size (compacted) will be around 5 GiB.

## Possible improvements

### Primary Compaction

The primary storage needs compaction and/or reuse of locations used to store old data that has been replaced or removed.

### Concurrency

Reads are blocked by writes when the fall within the same bucket. Data for in index, primary, and freelist are written to memory and flushed to disk asynchronously.

Techniques from [iand/gonudb](https://github.com/iand/gonudb) & [CPPAlliance/nudb](https://github.com/CPPAlliance/nudb) have been ported to support parallelism.

### Deletions

Currently deletions only partially are supported. Items can be removed from the index, but not from the primary in a way that allows recovery of storage space.
