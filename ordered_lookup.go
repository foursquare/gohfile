package hfile

import (
	"bytes"
	"fmt"
)

type OrderedOps struct {
	LastKey *[]byte
}

func (s *OrderedOps) ResetState() {
	s.LastKey = nil
}

func (s *OrderedOps) CheckIfKeyOutOfOrder(key []byte) error {
	if s.LastKey != nil && bytes.Compare(*s.LastKey, key) > 0 {
		return fmt.Errorf("Keys out of order! %v > %v", *s.LastKey, key)
	}
	s.LastKey = &key
	return nil
}
