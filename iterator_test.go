package hfile

import (
	"bytes"
	"os"
	"testing"
)

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
