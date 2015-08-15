package hfile

import (
	"bytes"
	"testing"
)

var firstSampleKey = []byte{0, 0, 0, 1}
var firstSampleValue = []byte("~1")

var secondSampleKey = []byte{0, 0, 0, 2}
var secondSampleValue = []byte("~2")

var secondSampleBlockKey = []byte{0, 0, 229, 248}

var bigSampleKey = []byte{0, 0, 240, 248}
var bigSampleValue = []byte("~61688")

var biggerSampleKey = []byte{0, 1, 0, 1}
var biggerSampleValue = []byte("~65537")

func sampleReader(t *testing.T) *Reader {
	reader, err := NewReader("sample", "sample/pairs.hfile", false, testing.Verbose())
	if err != nil {
		t.Error(err)
	}
	return reader
}

func sampleScanner(t *testing.T) *Scanner {
	return NewScanner(sampleReader(t))
}

func sampleIterator(t *testing.T) *Iterator {
	return sampleReader(t).NewIterator()
}

func requireSame(t *testing.T, err error, what string, actual []byte, expected []byte) {
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(actual, expected) {
		t.Fatalf("%s returned '%v', expected '%v'\n", what, actual, expected)
	}
}

func requireTrue(t *testing.T, err error, what string, v bool) {
	if !v {
		t.Fatalf("%s is not true", what)
	}
}

func TestFirstKeys(t *testing.T) {
	r := sampleReader(t)
	requireSame(t, nil, "block0.firstKey", r.index[0].firstKeyBytes, firstSampleKey)
	requireSame(t, nil, "block1.firstKey", r.index[1].firstKeyBytes, secondSampleBlockKey)
}

func TestGetFirst(t *testing.T) {
	s := sampleScanner(t)
	v, err, _ := s.GetFirst(firstSampleKey)
	requireSame(t, err, "GetFirst.1", v, firstSampleValue)

	v, err, _ = s.GetFirst(bigSampleKey)
	requireSame(t, err, "GetFirst.3", v, bigSampleValue)

	v, err, _ = s.GetFirst(biggerSampleKey)
	requireSame(t, err, "GetFirst.4", v, biggerSampleValue)
}

func TestIterator(t *testing.T) {
	i := sampleIterator(t)
	ok, err := i.Next()
	requireTrue(t, err, "next", ok)
	requireSame(t, err, "it.Key", i.Key(), firstSampleKey)
	requireSame(t, err, "it.Value", i.Value(), firstSampleValue)

	ok, err = i.Next()
	requireTrue(t, err, "next.2", ok)
	requireSame(t, err, "it.Key.2", i.Key(), secondSampleKey)
	requireSame(t, err, "it.Value.2", i.Value(), secondSampleValue)
}
