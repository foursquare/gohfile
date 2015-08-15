// Copyright (C) 2014 Daniel Harrison

package hfile

import (
	"bytes"
	"encoding/binary"
)

type Iterator struct {
	hfile          *Reader
	dataBlockIndex int
	block          *bytes.Reader
	key            []byte
	value          []byte
}

func (hfile *Reader) NewIterator() *Iterator {
	it := Iterator{hfile, 0, nil, nil, nil}
	return &it
}

func (it *Iterator) Next() (bool, error) {
	var err error

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
