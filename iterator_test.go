package hfile

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func compareBytes(a, e []byte) bool {
	b := bytes.Equal(a, e)
	if !b {
		fmt.Sprintf("'%v', expected '%v'\n", a, e)
	}
	return b
}

func TestIterator(t *testing.T) {
	f, r := fakeDataReader(t, true, false)
	defer os.Remove(f)
	i := r.GetIterator()
	ok, err := i.Next()

	assert.Nil(t, err, "error creating tempfile:", err)
	assert.True(t, ok, "next is not true")

	assert.True(t, compareBytes(i.Key(), MockKeyInt(0)))

	assert.True(t, compareBytes(i.Value(), MockValueInt(0)))

	ok, err = i.Next()
	assert.Nil(t, err, "error creating tempfile:", err)
	assert.True(t, ok, "next is not true")

	assert.True(t, compareBytes(i.Key(), MockKeyInt(1)))

	assert.True(t, compareBytes(i.Value(), MockValueInt(1)))

	ok, err = i.Seek(MockKeyInt(65537))
	assert.Nil(t, err, "error creating tempfile:", err)
	assert.True(t, ok, "next is not true")

	assert.True(t, compareBytes(i.Key(), MockKeyInt(65537)))

	assert.True(t, compareBytes(i.Value(), MockValueInt(65537)))

	ok, err = i.Seek(MockKeyInt(75537))
	assert.Nil(t, err, "error creating tempfile:", err)
	assert.True(t, ok, "next is not true")

	assert.True(t, compareBytes(i.Key(), MockKeyInt(75537)))

	assert.True(t, compareBytes(i.Value(), MockValueInt(75537)))
}

func TestSinglePrefix(t *testing.T) {
	f, r := fakeDataReader(t, true, false)
	defer os.Remove(f)
	i := r.GetIterator()

	res, err := i.AllForPrefixes([][]byte{[]byte{0, 0, 1}})
	assert.Nil(t, err, "error finding all for prefixes:", err)

	assert.Len(t, res, 256, "Wrong number of matched keys")

	k := string(MockKeyInt(511))
	v, ok := res[k]
	assert.True(t, ok, fmt.Sprintf("Key %v not in res %v", k, res))
	assert.Len(t, v, 1, "Wrong number of results for ~511")
	assert.True(t, compareBytes(v[0], MockValueInt(511)))

	k = string(MockKeyInt(256))
	v, ok = res[k]
	assert.True(t, ok, fmt.Sprintf("Key %v not in res %v", k, res))
	assert.Len(t, v, 1, "Wrong number of results for ~256")
	assert.True(t, compareBytes(v[0], MockValueInt(256)))

	k = string([]byte{0, 0, 0, 255})
	_, ok = res[k]
	assert.False(t, ok, fmt.Sprintf("Key %v should not be in res %v", k, res))

	k = string([]byte{0, 0, 2, 0})
	_, ok = res[k]
	assert.False(t, ok, fmt.Sprintf("Key %v should not be in res %v", k, res))

	k = string([]byte{0, 0, 1, 30})
	v, ok = res[k]
	assert.True(t, ok, fmt.Sprintf("Key %v not in res %v", k, res))
	assert.True(t, compareBytes(v[0], MockValueInt(286)))
}
