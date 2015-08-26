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

	if len(collections) < 1 {
		return nil, fmt.Errorf("no collections to load!")
	}

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

func (cs *CollectionSet) ScannerFor(name string) (*Scanner, error) {

	r, err := cs.ReaderFor(name)
	if err != nil {
		return nil, err
	}

	select {
	case s := <-r.scannerCache:
		return s, nil
	default:
	}

	s := NewScanner(r)
	return s, nil
}

func (cs *CollectionSet) IteratorFor(name string) (*Iterator, error) {
	r, err := cs.ReaderFor(name)
	if err != nil {
		return nil, err
	}

	select {
	case i := <-r.iteratorCache:
		return i, nil
	default:
		return NewIterator(r), nil
	}
}
