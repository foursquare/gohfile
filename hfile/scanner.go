// Copyright (C) 2014 Daniel Harrison

package hfile

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"sort"
)

type Scanner struct {
	reader  *Reader
	idx     int
	buf     *bytes.Reader
	lastKey *[]byte
}

func NewScanner(r *Reader) Scanner {
	return Scanner{r, 0, nil, nil}
}

func (s *Scanner) Reset() {
	s.idx = 0
	s.buf = nil
	s.lastKey = nil
}

func (s *Scanner) findBlock(key []byte) int {
	remaining := len(s.reader.index) - s.idx - 1

	if remaining <= 0 {
		return s.idx // s.cur is the last block, so it is only choice.
	}

	if s.reader.index[s.idx+1].IsAfter(key) {
		return s.idx
	}

	offset := sort.Search(remaining, func(i int) bool {
		return s.reader.index[s.idx+i+1].IsAfter(key)
	})

	return s.idx + offset
}

func (s *Scanner) CheckIfKeyOutOfOrder(key []byte) error {
	if s.lastKey != nil && bytes.Compare(*s.lastKey, key) > 0 {
		return fmt.Errorf("Keys our of order! %v > %v", *s.lastKey, key)
	}
	s.lastKey = &key
	return nil
}

func (s *Scanner) blockFor(key []byte) (*bytes.Reader, error, bool) {
	err := s.CheckIfKeyOutOfOrder(key)
	if err != nil {
		return nil, err, false
	}

	if s.reader.index[s.idx].IsAfter(key) {
		return nil, nil, false
	}

	idx := s.findBlock(key)

	if idx != s.idx || s.buf == nil { // need to load a new block
		data, err := s.reader.GetBlock(idx)
		if err != nil {
			return nil, err, false
		}
		s.idx = idx
		s.buf = data
	}

	return s.buf, nil, true
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

	_, found, _ := getValuesFromBuffer(data, key, false)
	return found, err
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
