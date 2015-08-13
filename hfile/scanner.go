// Copyright (C) 2014 Daniel Harrison

package hfile

import (
	"bytes"
	"encoding/binary"
	"log"
	"sort"
)

type Scanner struct {
	reader *Reader
}

func NewScanner(r *Reader) Scanner {
	return Scanner{r}
}

func (s *Scanner) findBlock(key []byte) int {
	idx := sort.Search(len(s.reader.index), func(i int) bool {
		return s.reader.index[i].IsAfter(key)
	})

	return idx - 1
}

func (s *Scanner) blockFor(key []byte) (*bytes.Reader, error, bool) {
	idx := s.findBlock(key)
	if idx < 0 {
		return nil, nil, false
	}
	data, err := s.reader.GetBlock(idx)
	return data, err, true
}

func (s *Scanner) GetFirst(key []byte) ([]byte, error, bool) {
	data, err, ok := s.blockFor(key)

	if !ok {
		return nil, err, ok
	}

	value, _, found := getValuesFromBuffer(data, key, true)
	return value, nil, found
}

func (s *Scanner) GetAll(key []byte) ([][]byte, error) {
	data, err, ok := s.blockFor(key)

	if !ok {
		log.Println("no block for key ", key)
		return nil, err
	}

	_, found, _ := getValuesFromBuffer(data, key, true)
	return found, nil
}

func getValuesFromBuffer(buf *bytes.Reader, key []byte, first bool) ([]byte, [][]byte, bool) {
	var acc [][]byte

	for buf.Len() > 0 {
		var keyLen, valLen uint32
		binary.Read(buf, binary.BigEndian, &keyLen)
		binary.Read(buf, binary.BigEndian, &valLen)
		keyBytes := make([]byte, keyLen)
		valBytes := make([]byte, valLen)
		buf.Read(keyBytes)
		buf.Read(valBytes)
		if bytes.Compare(key, keyBytes) == 0 {
			if first {
				return valBytes, nil, true
			} else {
				acc = append(acc, valBytes)
			}
		}
	}

	return nil, acc, len(acc) > 0
}
