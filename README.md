# HFile implementation in Go
HFile is a KV file format (borrowed from HBase, based on SSTables) used at foursquare as the storage format for our distributed KV store.

HFiles are designed to be simultaneously a) easily to generate incrementally (metadata is flushed at the end) and b) easy to serve / search as-is (thanks to an index), making them friendly to pipelines that regenerate large datasets and need to bulk-load them into production regularly.

# Summary of HFile format

On-disk, an HFile consists of:
  
  - some number of blocks,
  - an index, specifying the offset and length of each block along with its first key
  - a fixed-size, fixed-layout header (or trailer actually) of metadata

Each block contains some number of key-value pairs, each in the form of two 4-byte sizes `i` and `j`, followed by `i` bytes of key and `j` bytes of value.

# Finding data in an Hfile

Callers likely want to construct a `Reader` per file, and a `Scanner` per request.

Scanners are stateful (see below) and are *not* threadsafe.

## Reader

Readers mmap a file, read the trailer to find the index and load the index into memory. When a specific block's data is requested, the reader uses the index's offsets to find its data, decompresses it if needed and returns it.

## Scanner
A scanner looks up a key by binary searching the reader's block index, comparing the `firstKey` until it finds the last block with a starting key less than or equal to the requested key. It then iterates through the key-value pairs of the block until it finds a matching key, or returns nothing if it finds a greater key or the end of the block.

## Sorting requested keys
Scanners keep a reference to the most-recently loaded block, and their position within said block. Subsequent lookups only binary search the remaining blocks, and, when searching the current block, only search forward from the current position. This saves substantial repeated searching, but requires that keys be looked up in strictly ascending order.

Scanners can be `Reset()` to return to their initial state.

Scanners keep track of the last key they looked up, and will error if a lower key is passed to them.

# Authors
- [Dan Harrison](http://github.com/paperstreet)
- [David Taylor](http://github.com/dt)

