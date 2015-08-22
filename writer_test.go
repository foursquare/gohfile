package hfile

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func tempHfile(t *testing.T, compress bool, blockSize int, keys [][]byte, values [][]byte) (string, *Scanner) {
	fp, err := ioutil.TempFile("", "demohfile")
	if err != nil {
		t.Fatal("error creating tempfile:", err)
	}
	if testing.Verbose() {
		log.Println("###############")
		log.Println("Writing temp hfile:", fp.Name())
		log.Println("###############")
	}
	w, err := NewWriter(fp, compress, blockSize, testing.Verbose())
	if err != nil {
		t.Fatal("error creating writer:", err)
	}
	for i, _ := range keys {
		if err := w.Write(keys[i], values[i]); err != nil {
			t.Fatal("error writing k-v pair:", err)
		}
	}
	w.Close()

	if testing.Verbose() {
		log.Println("###############")
		log.Println("Done writing temp hfile:", fp.Name())
		log.Println("###############")
	}

	r, err := NewReader("demo", fp.Name(), false, testing.Verbose())
	if err != nil {
		t.Fatal("error creating reader:", err)
	}
	s := NewScanner(r)

	return fp.Name(), s
}

func keyI(i int) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(i))
	return buf
}

func valI(i int) []byte {
	return []byte(fmt.Sprintf("value-for-%d", i))
}

func TestRoundTrip(t *testing.T) {
	keys := [][]byte{keyI(1), keyI(2), keyI(3), keyI(4)}
	vals := [][]byte{valI(1), valI(2), valI(3), valI(4)}

	f, s := tempHfile(t, false, 4096, keys, vals)
	defer os.Remove(f)

	v, err, found := s.GetFirst(keyI(3))
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("not found")
	}
	if !bytes.Equal(v, valI(3)) {
		t.Fatal("bad value", v)
	}
	v, err, found = s.GetFirst(keyI(5))
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("missing key should not have been found.")
	}
}

func TestRoundTripCompressed(t *testing.T) {
	keys := [][]byte{keyI(1), keyI(2), keyI(3), keyI(4)}
	vals := [][]byte{valI(1), valI(2), valI(3), valI(4)}

	f, s := tempHfile(t, true, 4096, keys, vals)
	defer os.Remove(f)

	v, err, found := s.GetFirst(keyI(3))
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("not found")
	}
	if !bytes.Equal(v, valI(3)) {
		t.Fatal("bad value", v)
	}
	v, err, found = s.GetFirst(keyI(5))
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("missing key should not have been found.")
	}
}

func TestMultiValueRoundTripCompressed(t *testing.T) {
	keys := [][]byte{keyI(10), keyI(10), keyI(20), keyI(30), keyI(30), keyI(30), keyI(40)}
	vals := [][]byte{valI(10), valI(11), valI(20), valI(30), valI(31), valI(32), valI(40)}

	f, s := tempHfile(t, true, 4096, keys, vals)
	defer os.Remove(f)

	v, err := s.GetAll(keyI(30))
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 3 {
		t.Fatal("wrong number of values for key 30", len(v))
	}
	if !bytes.Equal(v[1], valI(31)) {
		t.Fatal("bad value for key 30 (1)", v[1])
	}

	v, err = s.GetAll(keyI(40))
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 1 {
		t.Fatal("wrong number of results for key 40", len(v))
	}
	if !bytes.Equal(v[0], valI(40)) {
		t.Fatal("bad value for key 40", v[0])
	}

	v, err = s.GetAll(keyI(50))
	if err != nil {
		t.Fatal(err)
	}
	if len(v) > 0 {
		t.Fatal("should not find missing keys")
	}
}

func TestBigRoundTripCompressed(t *testing.T) {
	keys := make([][]byte, 1000)
	vals := make([][]byte, 1000)

	for i, _ := range keys {
		keys[i] = keyI(i)
		vals[i] = valI(i)
	}

	f, s := tempHfile(t, true, 4096, keys, vals)
	defer os.Remove(f)

	v, err, found := s.GetFirst(keyI(501))
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("not found")
	}
	if !bytes.Equal(v, valI(501)) {
		t.Fatal("bad value")
	}
}
