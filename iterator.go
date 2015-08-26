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

	buf []byte
	pos int

	key   []byte
	value []byte
	OrderedOps
}

func (hfile *Reader) NewIterator() *Iterator {
	it := Iterator{hfile, 0, nil, 0, nil, nil, OrderedOps{nil}}
	return &it
}

func (it *Iterator) Seek(key []byte) (bool, error) {
	var err error

	if err = it.CheckIfKeyOutOfOrder(key); err != nil {
		return false, err
	}

	if it.key != nil && After(it.key, key) {
		if it.hfile.debug {
			log.Println("[Iterator.Seek] already past")
		}
		return true, nil
	}

	blk := it.hfile.FindBlock(it.dataBlockIndex, key)
	if it.hfile.debug {
		log.Printf("[Iterator.Seek] picked block %d, cur %d\n", blk, it.dataBlockIndex)
	}

	if blk != it.dataBlockIndex {
		if it.hfile.debug {
			log.Println("[Iterator.Seek] new block!")
		}
		it.buf = nil
		it.dataBlockIndex = blk
	}

	ok, err := it.Next()
	if err != nil {
		return false, err
	}

	if it.hfile.debug {
		log.Printf("[Iterator.Seek] %v (looking for %v)\n", it.Key(), key)
	}

	for ok {
		if it.hfile.debug {
			log.Printf("[Iterator.Seek] %v\n", it.Key())
		}
		after := After(key, it.Key())

		if err == nil && after {
			ok, err = it.Next()
		} else {
			if it.hfile.debug {
				log.Printf("[Iterator.Seek] done %v (err %v)\n", it.Key(), err)
			}
			return ok, err
		}
	}

	if it.hfile.debug {
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

	if it.buf == nil {
		it.buf, err = it.hfile.GetBlockBuf(it.dataBlockIndex, it.buf)
		it.pos = 8
		if err != nil {
			return false, err
		}
	}

	if len(it.buf)-it.pos <= 0 {
		it.dataBlockIndex += 1
		it.buf = nil
		return it.Next()
	}

	keyLen := int(binary.BigEndian.Uint32(it.buf[it.pos : it.pos+4]))
	valLen := int(binary.BigEndian.Uint32(it.buf[it.pos+4 : it.pos+8]))

	it.key = it.buf[it.pos+8 : it.pos+8+keyLen]
	it.value = it.buf[it.pos+8+keyLen : it.pos+8+keyLen+valLen]
	it.pos += keyLen + valLen + 8
	return true, nil
}

func (it *Iterator) Key() []byte {
	return it.key
}

func (it *Iterator) Value() []byte {
	return it.value
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

		for ok && bytes.HasPrefix(it.Key(), prefix) {
			prev := it.Key()
			acc = append(acc, it.Value())

			if ok, err = it.Next(); err != nil {
				return nil, err
			}

			if !ok || !bytes.Equal(prev, it.Key()) {
				res[string(prev)] = acc
				acc = make([][]byte, 0, 1)
			}
		}
	}
	return res, nil
}
