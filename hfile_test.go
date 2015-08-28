package hfile

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

// The sample file `smaple/pairs.hfile` has pairs of integers to strings.
// It was generated from the "known-good" scala ver
// The keys are sequential integers represented as 4 bytes (big-endian).
// The values are strings, containing ascii bytes of the string "~x", where x is the key's integer value.
// Thus, the 34th k-v pair has key 00 00 00 1C and value 7E 31 38 ("~18").

var firstSampleKey = MockKeyInt(1)
var firstSampleValue = MockValueInt(1)
var secondSampleBlockKey = []byte{0, 0, 229, 248}

func fakeDataReader(t *testing.T, compress, multi bool) (string, *Reader) {
	f, err := ioutil.TempFile("", "hfile")
	if err != nil {
		t.Fatal("cannot create tempfile: ", err)
	}
	if multi {
		err = GenerateMockMultiHfile(f.Name(), 100000, 1024*4, compress, false, false)
		if err != nil {
			t.Fatal("cannot write to tempfile: ", err)
		}
	} else {
		err = GenerateMockHfile(f.Name(), 100000, 1024*4, compress, false, false)
		if err != nil {
			t.Fatal("cannot write to tempfile: ", err)
		}
	}
	reader, err := NewReader("sample", f.Name(), false, testing.Verbose())
	if err != nil {
		t.Fatal("error creating reader:", err)
	}
	return f.Name(), reader
}

func TestFirstKeys(t *testing.T) {
	r, err := NewReader("sample", "testdata/pairs.hfile", false, testing.Verbose())
	if err != nil {
		t.Fatal("cannot open sample: ", err)
	}

	if !bytes.Equal(r.index[0].firstKeyBytes, firstSampleKey) {
		t.Fatalf("'%v', expected '%v'\n", r.index[0].firstKeyBytes, firstSampleKey)
	}
	if !bytes.Equal(r.index[1].firstKeyBytes, secondSampleBlockKey) {
		t.Fatalf("'%v', expected '%v'\n", r.index[1].firstKeyBytes, secondSampleBlockKey)
	}
}

func TestGetFirstSample(t *testing.T) {
	f, r := fakeDataReader(t, true, false)
	defer os.Remove(f)
	s := r.GetScanner()

	var first, second []byte
	var err error

	first, err, _ = s.GetFirst(MockKeyInt(1))
	if err != nil {
		t.Fatal("error finding key:", err)
	}
	if !bytes.Equal(first, MockValueInt(1)) {
		t.Fatalf("'%v', expected '%v'\n", first, MockValueInt(1))
	}

	second, err, _ = s.GetFirst(MockKeyInt(1000))
	if err != nil {
		t.Fatal("error finding key:", err)
	}
	if !bytes.Equal(second, MockValueInt(1000)) {
		t.Fatalf("'%v', expected '%v'\n", second, MockValueInt(1000))
	}
	if !bytes.Equal(first, MockValueInt(1)) {
		t.Fatalf("First value CHANGED '%v', expected '%v'\n", first, MockValueInt(1))
	}

	second, err, _ = s.GetFirst(MockKeyInt(65547))
	if err != nil {
		t.Fatal("error finding key:", err)
	}
	if !bytes.Equal(second, MockValueInt(65547)) {
		t.Fatalf("'%v', expected '%v'\n", second, MockValueInt(65547))
	}
	if !bytes.Equal(first, MockValueInt(1)) {
		t.Fatalf("First value CHANGED '%v', expected '%v'\n", first, MockValueInt(1))
	}
}

func TestIterator(t *testing.T) {
	f, r := fakeDataReader(t, true, false)
	defer os.Remove(f)
	i := r.GetIterator()
	ok, err := i.Next()

	if err != nil {
		t.Fatal("error in next:", err)
	}
	if !ok {
		t.Fatal("next is not true")
	}

	if !bytes.Equal(i.Key(), MockKeyInt(0)) {
		t.Fatalf("'%v', expected '%v'\n", i.Key(), MockKeyInt(0))
	}
	if !bytes.Equal(i.Value(), MockValueInt(0)) {
		t.Fatalf("'%v', expected '%v'\n", i.Value(), MockValueInt(0))
	}

	ok, err = i.Next()
	if err != nil {
		t.Fatal("error in next:", err)
	}
	if !ok {
		t.Fatal("next is not true")
	}

	if !bytes.Equal(i.Key(), MockKeyInt(1)) {
		t.Fatalf("'%v', expected '%v'\n", i.Key(), MockKeyInt(1))
	}
	if !bytes.Equal(i.Value(), MockValueInt(1)) {
		t.Fatalf("'%v', expected '%v'\n", i.Value(), MockValueInt(1))
	}

	ok, err = i.Seek(MockKeyInt(65537))
	if err != nil {
		t.Fatal("error in seek:", err)
	}
	if !ok {
		t.Fatal("seek is not true")
	}

	if !bytes.Equal(i.Key(), MockKeyInt(65537)) {
		t.Fatalf("'%v', expected '%v'\n", i.Key(), MockKeyInt(65537))
	}
	if !bytes.Equal(i.Value(), MockValueInt(65537)) {
		t.Fatalf("'%v', expected '%v'\n", i.Value(), MockValueInt(65537))
	}

	ok, err = i.Seek(MockKeyInt(75537))
	if err != nil {
		t.Fatal("error in seek:", err)
	}
	if !ok {
		t.Fatal("seek is not true")
	}

	if !bytes.Equal(i.Key(), MockKeyInt(75537)) {
		t.Fatalf("'%v', expected '%v'\n", i.Key(), MockKeyInt(75537))
	}
	if !bytes.Equal(i.Value(), MockValueInt(75537)) {
		t.Fatalf("'%v', expected '%v'\n", i.Value(), MockValueInt(75537))
	}
}

func TestSinglePrefix(t *testing.T) {
	f, r := fakeDataReader(t, true, false)
	defer os.Remove(f)
	i := r.GetIterator()

	res, err := i.AllForPrfixes([][]byte{[]byte{0, 0, 1}})
	if err != nil {
		t.Fatal("error finding all for prefixes:", err)
	}

	if len(res) != 256 {
		t.Fatalf("Wrong number of matched keys: %d instead of %d", len(res), 256)
	}

	k := string(MockKeyInt(511))
	if v, ok := res[k]; !ok {
		t.Fatalf("Key %v not in res %v", k, res)
	} else {
		if len(v) != 1 {
			t.Fatalf("Wrong number of results for ~511: %d (%v)", len(v), v)
		}
		if !bytes.Equal(v[0], MockValueInt(511)) {
			t.Fatal("bad value:", v[0])
		}
	}

	k = string(MockKeyInt(256))
	if v, ok := res[k]; !ok {
		t.Fatalf("Key %v not in res %v", k, res)
	} else {
		if len(v) != 1 {
			t.Fatalf("Wrong number of results for ~256: %d (%v)", len(v), v)
		}
		if !bytes.Equal(v[0], MockValueInt(256)) {
			t.Fatalf("bad value: %s vs %s", v[0], MockValueInt(256))
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
		if !bytes.Equal(v[0], MockValueInt(286)) {
			t.Fatal("bad value:", v[0], MockValueInt(286))
		}
	}
}

func TestMulti(t *testing.T) {
	f, r := fakeDataReader(t, true, true)
	defer os.Remove(f)
	s := r.GetScanner()

	var first, second [][]byte
	expectedFirst := MockMultiValueInt(1, 0)
	var err error

	first, err = s.GetAll(MockKeyInt(1))
	if len(first) != 3 {
		t.Fatalf("wrong number of values for 1: %d", len(first))
	}
	if err != nil {
		t.Fatal("error finding key:", err)
	}
	if !bytes.Equal(first[0], expectedFirst) {
		t.Fatalf("'%v', expected '%v'\n", first[0], expectedFirst)
	}

	second, err = s.GetAll(MockKeyInt(1000))
	if err != nil {
		t.Fatal("error finding key:", err)
	}
	if actual := len(second); actual != 1 {
		t.Fatalf("wrong number of values for 1000: %d", actual)
	}
	if expected := MockValueInt(1000); !bytes.Equal(second[0], expected) {
		t.Fatalf("'%s', expected '%s'\n", second[0], expected)
	}

	if !bytes.Equal(first[0], expectedFirst) {
		t.Fatalf("First value CHANGED '%v', expected '%v'\n", first[0], expectedFirst)
	}

	second, err = s.GetAll(MockKeyInt(1001))
	if err != nil {
		t.Fatal("error finding key:", err)
	}
	if actual := len(second); actual != 3 {
		t.Fatalf("wrong number of values for 1001: %d", actual)
	}
	if expected, actual := MockMultiValueInt(1001, 0), second[0]; !bytes.Equal(actual, expected) {
		t.Fatalf("'%v', expected '%v'\n", actual, expected)
	}
	if expected, actual := MockMultiValueInt(1001, 1), second[1]; !bytes.Equal(actual, expected) {
		t.Fatalf("'%v', expected '%v'\n", actual, expected)
	}
	if expected, actual := MockMultiValueInt(1001, 2), second[2]; !bytes.Equal(actual, expected) {
		t.Fatalf("'%v', expected '%v'\n", actual, expected)
	}
	if !bytes.Equal(first[0], expectedFirst) {
		t.Fatalf("First value CHANGED '%v', expected '%v'\n", first[0], expectedFirst)
	}
}
