package lru

import (
	"testing"
)

func TestLRU(t *testing.T) {
	b1 := []byte{1}
	b2 := []byte{2}
	b3 := []byte{3}
	b4 := []byte{4}

	lru := NewLRU(3)

	if _, ok := lru.Get(1); ok {
		t.Fatal("empty lru should return nothing")
	}

	lru.Add(1, b1)
	if _, ok := lru.Get(1); !ok {
		t.Fatal("missing only item")
	}

	lru.Add(2, b2)
	lru.Add(3, b3)

	if _, ok := lru.Get(1); !ok {
		t.Fatal("missing 1")
	}
	if _, ok := lru.Get(2); !ok {
		t.Fatal("missing 2")
	}
	if _, ok := lru.Get(3); !ok {
		t.Fatal("missing 3")
	}
	if _, ok := lru.Get(4); ok {
		t.Fatal("phantom item")
	}
	lru.Add(4, b4)
	if _, ok := lru.Get(1); ok {
		t.Fatal("phantom item")
	}
	if _, ok := lru.Get(4); !ok {
		t.Fatal("item!")
	}

}
