package hfile

import (
	"bytes"
	"fmt"
)

type OrderedLookups struct {
	lastKey *[]byte
}

func (s *OrderedLookups) ResetState() {
	s.lastKey = nil
}

func (s *OrderedLookups) CheckIfKeyOutOfOrder(key []byte) error {
	if s.lastKey != nil && bytes.Compare(*s.lastKey, key) > 0 {
		return fmt.Errorf("Keys our of order! %v > %v", *s.lastKey, key)
	}
	s.lastKey = &key
	return nil
}
