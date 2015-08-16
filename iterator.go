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
	block          *bytes.Reader
	key            []byte
	value          []byte
	OrderedLookups
}

func (hfile *Reader) NewIterator() *Iterator {
	it := Iterator{hfile, 0, nil, nil, nil, OrderedLookups{nil}}
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
		it.block = nil
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

	if it.block == nil {
		it.block, err = it.hfile.GetBlock(it.dataBlockIndex)
		if err != nil {
			return false, err
		}
	}

	if it.block.Len() <= 0 {
		it.dataBlockIndex += 1
		return it.Next()
	}

	var keyLen, valLen uint32
	binary.Read(it.block, binary.BigEndian, &keyLen)
	binary.Read(it.block, binary.BigEndian, &valLen)
	it.key = make([]byte, keyLen)
	it.value = make([]byte, valLen)
	it.block.Read(it.key)
	it.block.Read(it.value)
	return true, nil
}

func (it *Iterator) Key() []byte {
	return it.key
}

func (it *Iterator) Value() []byte {
	return it.value
}

func (it *Iterator) AllForPrfixes(prefixes [][]byte) (map[string][]byte, error) {
	res := make(map[string][]byte)

	var err error

	for _, prefix := range prefixes {
		ok := false
		if ok, err = it.Seek(prefix); err != nil {
			return nil, err
		}

		for ok && bytes.HasPrefix(it.Key(), prefix) {
			res[string(it.Key())] = it.Value()
			if ok, err = it.Next(); err != nil {
				return nil, err
			}
		}
	}
	return res, nil
}
