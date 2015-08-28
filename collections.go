package hfile

import (
	"fmt"
)

type CollectionConfig struct {
	// The Name of the collection.
	Name string

	// The Hfile itself.
	Path string

	// If the collection data should be kept in-memory (via mlock).
	InMem bool

	// Should operations on this collection emit verbose debug output.
	Debug bool
}

type CollectionSet struct {
	Collections map[string]*Reader
}

func LoadCollections(collections []CollectionConfig, debug bool) (*CollectionSet, error) {
	cs := new(CollectionSet)
	cs.Collections = make(map[string]*Reader)

	if len(collections) < 1 {
		return nil, fmt.Errorf("no collections to load!")
	}

	for _, cfg := range collections {
		reader, err := NewReaderFromConfig(cfg)
		if err != nil {
			return nil, err
		}

		cs.Collections[cfg.Name] = reader
	}

	return cs, nil
}

func (cs *CollectionSet) ReaderFor(name string) (*Reader, error) {
	c, ok := cs.Collections[name]
	if !ok {
		return nil, fmt.Errorf("not configured with reader for collection %s", name)
	}
	return c, nil
}
