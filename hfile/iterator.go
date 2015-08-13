// Copyright (C) 2014 Daniel Harrison

package hfile

import "encoding/binary"

type Iterator struct {
	hfile          *Reader
	dataBlockIndex int
	key            []byte
	value          []byte
}

func (hfile *Reader) NewIterator() *Iterator {
	it := Iterator{hfile, 0, nil, nil}
	return &it
}

func (it *Iterator) Next() bool {
	if it.dataBlockIndex >= len(it.hfile.index) {
		return false
	}
	dataBlock := it.hfile.index[it.dataBlockIndex]
	if dataBlock.buf.Len() <= 0 {
		it.dataBlockIndex += 1
		if it.dataBlockIndex >= len(it.hfile.index) {
			return false
		}
		dataBlock := it.hfile.index[it.dataBlockIndex]
		dataBlock.reset()
		return it.Next()
	}

	var keyLen, valLen uint32
	binary.Read(dataBlock.buf, binary.BigEndian, &keyLen)
	binary.Read(dataBlock.buf, binary.BigEndian, &valLen)
	it.key = make([]byte, keyLen)
	it.value = make([]byte, valLen)
	dataBlock.buf.Read(it.key)
	dataBlock.buf.Read(it.value)
	return true
}

func (it *Iterator) Key() []byte {
	return it.key
}

func (it *Iterator) Value() []byte {
	return it.value
}
