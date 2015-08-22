package hfile

import (
	"bytes"
	"testing"
)

// The sample file `smaple/pairs.hfile` pairs of integers to strings
// The keys are sequential integers represented as 4 bytes (big-endian).
// The values are strings, containing ascii bytes of the string "~x", where x is the key's integer value.
// Thus, the 34th k-v pair has key 00 00 00 1C and value 7E 31 38 ("~18").

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
		t.Fatal("error creating reader:", err)
	}
	return reader
}

func sampleScanner(t *testing.T) *Scanner {
	return NewScanner(sampleReader(t))
}

func sampleIterator(t *testing.T) *Iterator {
	return sampleReader(t).NewIterator()
}

func TestFirstKeys(t *testing.T) {
	r := sampleReader(t)
	if !bytes.Equal(r.index[0].firstKeyBytes, firstSampleKey) {
		t.Fatalf("'%v', expected '%v'\n", r.index[0].firstKeyBytes, firstSampleKey)
	}
	if !bytes.Equal(r.index[1].firstKeyBytes, secondSampleBlockKey) {
		t.Fatalf("'%v', expected '%v'\n", r.index[1].firstKeyBytes, secondSampleBlockKey)
	}
}

func TestGetFirst(t *testing.T) {
	s := sampleScanner(t)
	var actual []byte
	var err error

	actual, err, _ = s.GetFirst(firstSampleKey)
	if err != nil {
		t.Fatal("error finding key:", err)
	}
	if !bytes.Equal(actual, firstSampleValue) {
		t.Fatalf("'%v', expected '%v'\n", actual, firstSampleValue)
	}

	actual, err, _ = s.GetFirst(bigSampleKey)
	if err != nil {
		t.Fatal("error finding key:", err)
	}
	if !bytes.Equal(actual, bigSampleValue) {
		t.Fatalf("'%v', expected '%v'\n", actual, bigSampleValue)
	}

	actual, err, _ = s.GetFirst(biggerSampleKey)
	if err != nil {
		t.Fatal("error finding key:", err)
	}
	if !bytes.Equal(actual, biggerSampleValue) {
		t.Fatalf("'%v', expected '%v'\n", actual, biggerSampleValue)
	}
}

func TestIterator(t *testing.T) {
	i := sampleIterator(t)
	ok, err := i.Next()

	if err != nil {
		t.Fatal("error in next:", err)
	}
	if !ok {
		t.Fatal("next is not true")
	}

	if !bytes.Equal(i.Key(), firstSampleKey) {
		t.Fatalf("'%v', expected '%v'\n", i.Key(), firstSampleKey)
	}
	if !bytes.Equal(i.Value(), firstSampleValue) {
		t.Fatalf("'%v', expected '%v'\n", i.Value(), firstSampleValue)
	}

	ok, err = i.Next()
	if err != nil {
		t.Fatal("error in next:", err)
	}
	if !ok {
		t.Fatal("next is not true")
	}

	if !bytes.Equal(i.Key(), secondSampleKey) {
		t.Fatalf("'%v', expected '%v'\n", i.Key(), secondSampleKey)
	}
	if !bytes.Equal(i.Value(), secondSampleValue) {
		t.Fatalf("'%v', expected '%v'\n", i.Value(), secondSampleValue)
	}

	ok, err = i.Seek(bigSampleKey)
	if err != nil {
		t.Fatal("error in seek:", err)
	}
	if !ok {
		t.Fatal("seek is not true")
	}

	if !bytes.Equal(i.Key(), bigSampleKey) {
		t.Fatalf("'%v', expected '%v'\n", i.Key(), bigSampleKey)
	}
	if !bytes.Equal(i.Value(), bigSampleValue) {
		t.Fatalf("'%v', expected '%v'\n", i.Value(), bigSampleValue)
	}

	ok, err = i.Seek(biggerSampleKey)
	if err != nil {
		t.Fatal("error in seek:", err)
	}
	if !ok {
		t.Fatal("seek is not true")
	}

	if !bytes.Equal(i.Key(), biggerSampleKey) {
		t.Fatalf("'%v', expected '%v'\n", i.Key(), biggerSampleKey)
	}
	if !bytes.Equal(i.Value(), biggerSampleValue) {
		t.Fatalf("'%v', expected '%v'\n", i.Value(), biggerSampleValue)
	}
}

func TestSinglePrefix(t *testing.T) {
	i := sampleIterator(t)

	res, err := i.AllForPrfixes([][]byte{[]byte{0, 0, 1}})
	if err != nil {
		t.Fatal("error finding all for prefixes:", err)
	}

	if len(res) != 256 {
		t.Fatalf("Wrong number of matched keys: %d instead of %d", len(res), 256)
	}

	k := string([]byte{0, 0, 1, 255})
	if v, ok := res[k]; !ok {
		t.Fatalf("Key %v not in res %v", k, res)
	} else {
		if len(v) != 1 {
			t.Fatalf("Wrong number of results for ~511: %d (%v)", len(v), v)
		}
		if !bytes.Equal(v[0], []byte("~511")) {
			t.Fatal("bad value:", v[0])
		}
	}

	k = string([]byte{0, 0, 1, 0})
	if v, ok := res[k]; !ok {
		t.Fatalf("Key %v not in res %v", k, res)
	} else {
		if len(v) != 1 {
			t.Fatalf("Wrong number of results for ~256: %d (%v)", len(v), v)
		}
		if !bytes.Equal(v[0], []byte("~256")) {
			t.Fatal("bad value:", v[0])
		}
	}

	k = string([]byte{0, 0, 0, 255})
	if _, ok := res[k]; ok {
		t.Fatalf("Key %v should not be in res %v", k, res)
	}

	k = string([]byte{0, 0, 2, 0})
	if _, ok := res[k]; ok {
		t.Fatalf("Key %v should not be in res %v", k, res)
	}

	k = string([]byte{0, 0, 1, 30})
	if v, ok := res[k]; !ok {
		t.Fatalf("Key %v not in res %v", k, res)
	} else {
		if !bytes.Equal(v[0], []byte("~286")) {
			t.Fatal("bad value:", v[0])
		}
	}
}
