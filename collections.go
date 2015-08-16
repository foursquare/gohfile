package hfile

import (
	"fmt"
)

type CollectionConfig struct {
	Name  string
	Path  string
	Mlock bool
}

type Collection struct {
	Config *CollectionConfig
	reader *Reader
}

type CollectionSet struct {
	Collections map[string]Collection
}

func LoadCollections(collections []CollectionConfig, debug bool) (*CollectionSet, error) {
	cs := new(CollectionSet)
	cs.Collections = make(map[string]Collection)

	for _, cfg := range collections {
		reader, err := NewReaderFromConfig(&cfg, debug)
		if err != nil {
			return nil, err
		}

		cs.Collections[cfg.Name] = Collection{&cfg, reader}
	}

	return cs, nil
}

func (cs *CollectionSet) ReaderFor(name string) (*Reader, error) {
	c, ok := cs.Collections[name]
	if !ok {
		return nil, fmt.Errorf("not configured with reader for collection %s", name)
	}
	return c.reader, nil
}

func (cs *CollectionSet) ScannerFor(c string) (*Scanner, error) {
	reader, err := cs.ReaderFor(c)
	if err != nil {
		return nil, err
	}
	s := NewScanner(reader)
	return s, nil
}
