// Copyright (C) 2014 Daniel Harrison

package hfile

import (
	"bytes"
	"encoding/binary"
	"log"
)

type Iterator struct {
	hfile          *Reader
	dataBlockIndex int

	block []byte
	pos   int

	buf []byte

	key   []byte
	value []byte
	OrderedOps
}

func NewIterator(r *Reader) *Iterator {
	var buf []byte
	if r.header.compressionCodec > CompressionNone {
		buf = make([]byte, int(float64(r.header.totalUncompressedDataBytes/uint64(len(r.index)))*1.5))
	}

	it := Iterator{r, 0, nil, 0, buf, nil, nil, OrderedOps{nil}}
	return &it
}

func (it *Iterator) Reset() {
	it.dataBlockIndex = 0
	it.block = nil
	it.pos = 0
	it.key = nil
	it.value = nil
	it.ResetState()
}

func (it *Iterator) Seek(key []byte) (bool, error) {
	var err error

	if err = it.CheckIfKeyOutOfOrder(key); err != nil {
		return false, err
	}

	if it.key != nil && After(it.key, key) {
		if it.hfile.Debug {
			log.Println("[Iterator.Seek] already past")
		}
		return true, nil
	}

	blk := it.hfile.FindBlock(it.dataBlockIndex, key)
	if it.hfile.Debug {
		log.Printf("[Iterator.Seek] picked block %d, cur %d\n", blk, it.dataBlockIndex)
		if len(it.hfile.index) > blk+1 {
			log.Printf("[Iterator.Seek] following block starts at: %v\n", it.hfile.index[blk+1].firstKeyBytes)
		} else {
			log.Printf("[Iterator.Seek] (last block)\n")
		}
	}

	if blk != it.dataBlockIndex {
		if it.hfile.Debug {
			log.Println("[Iterator.Seek] new block!")
		}
		it.block = nil
		it.dataBlockIndex = blk
	}

	ok, err := it.Next()
	if err != nil {
		return false, err
	}

	if it.hfile.Debug {
		log.Printf("[Iterator.Seek] looking for %v (starting at %v)\n", key, it.key)
	}

	for ok {
		after := After(key, it.key)
		if it.hfile.Debug {
			log.Printf("[Iterator.Seek] \t %v (%v)\n", it.key, after)
		}

		if err == nil && after {
			ok, err = it.Next()
		} else {
			if it.hfile.Debug {
				log.Printf("[Iterator.Seek] done %v (err %v)\n", it.key, err)
			}
			return ok, err
		}
	}

	if it.hfile.Debug {
		log.Println("[Iterator.Seek] walked off block")
	}

	return ok, err
}

func (it *Iterator) Next() (bool, error) {
	var err error

	it.key = nil
	it.value = nil

	if it.dataBlockIndex >= len(it.hfile.index) {
		return false, nil
	}

	if it.block == nil {
		it.block, err = it.hfile.GetBlockBuf(it.dataBlockIndex, it.buf)
		it.pos = 8
		if err != nil {
			return false, err
		}
	}

	if len(it.block)-it.pos <= 0 {
		it.dataBlockIndex += 1
		it.block = nil
		return it.Next()
	}

	keyLen := int(binary.BigEndian.Uint32(it.block[it.pos : it.pos+4]))
	valLen := int(binary.BigEndian.Uint32(it.block[it.pos+4 : it.pos+8]))

	it.key = it.block[it.pos+8 : it.pos+8+keyLen]
	it.value = it.block[it.pos+8+keyLen : it.pos+8+keyLen+valLen]
	it.pos += keyLen + valLen + 8
	return true, nil
}

func (it *Iterator) Key() []byte {
	ret := make([]byte, len(it.key))
	copy(ret, it.key)
	return ret
}

func (it *Iterator) Value() []byte {
	ret := make([]byte, len(it.value))
	copy(ret, it.value)
	return ret
}

func (it *Iterator) AllForPrfixes(prefixes [][]byte) (map[string][][]byte, error) {
	res := make(map[string][][]byte)

	var err error

	for _, prefix := range prefixes {
		ok := false
		if ok, err = it.Seek(prefix); err != nil {
			return nil, err
		}

		acc := make([][]byte, 0, 1)

		for ok && bytes.HasPrefix(it.key, prefix) {
			prev := it.Key()
			acc = append(acc, it.Value())

			if ok, err = it.Next(); err != nil {
				return nil, err
			}

			if !ok || !bytes.Equal(prev, it.key) {
				res[string(prev)] = acc
				acc = make([][]byte, 0, 1)
			}
		}
	}
	return res, nil
}

func (it *Iterator) Release() {
	it.Reset()
	select {
	case it.hfile.iteratorCache <- it:
	default:
	}
}
