// Copyright (C) 2014 Daniel Harrison

package hfile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/edsrzf/mmap-go"
	"github.com/golang/snappy"
)

type Reader struct {
	mmap mmap.MMap

	majorVersion uint32
	minorVersion uint32

	header Header
	index  []Block
}

type Header struct {
	index int

	fileInfoOffset             uint64
	dataIndexOffset            uint64
	dataIndexCount             uint32
	metaIndexOffset            uint64
	metaIndexCount             uint32
	totalUncompressedDataBytes uint64
	entryCount                 uint32
	compressionCodec           uint32
}

type Block struct {
	offset        uint64
	size          uint32
	firstKeyBytes []byte
}

func NewReader(file *os.File) (Reader, error) {
	hfile := Reader{}
	var err error
	hfile.mmap, err = mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return hfile, err
	}

	v := binary.BigEndian.Uint32(hfile.mmap[len(hfile.mmap)-4:])
	hfile.majorVersion = v & 0x00ffffff
	hfile.minorVersion = v >> 24

	hfile.header, err = hfile.newHeader(hfile.mmap)
	if err != nil {
		return hfile, err
	}
	err = hfile.loadIndex(hfile.mmap)
	if err != nil {
		return hfile, err
	}

	return hfile, nil
}

func getDataBlock(key []byte, blocks *[]Block) (int, bool) {
	// TODO(dan): Binary search instead.
	for i := len(*blocks) - 1; i >= 0; i-- {
		block := (*blocks)[i]
		if cmp := bytes.Compare(key, block.firstKeyBytes); cmp == 0 || cmp == 1 {
			return i, true
		}
	}
	return -1, false
}

func (r *Reader) Get(key []byte) ([]byte, bool) {
	i, found := getDataBlock(key, &r.index)
	if !found {
		return nil, false
	}

	buf, _ := r.GetBlock(i)

	v, _, found := get(buf, key, true)
	return v, found
}

func (r *Reader) PrintDebugInfo(out io.Writer) {
	fmt.Fprintln(out, "entries: ", r.header.entryCount)
	fmt.Fprintln(out, "blocks: ", len(r.index))
	for i, blk := range r.index {
		fmt.Fprintf(out, "\t#%d: %s (%v)\n", i, blk.firstKeyBytes, blk.firstKeyBytes)
	}
}

func (r *Reader) newHeader(mmap mmap.MMap) (Header, error) {
	header := Header{}

	if r.majorVersion != 1 || r.minorVersion != 0 {
		return header, errors.New("wrong version")
	}

	header.index = len(mmap) - 60
	buf := bytes.NewReader(mmap[header.index:])

	headerMagic := make([]byte, 8)
	buf.Read(headerMagic)
	if bytes.Compare(headerMagic, []byte("TRABLK\"$")) != 0 {
		return header, errors.New("bad header magic")
	}

	binary.Read(buf, binary.BigEndian, &header.fileInfoOffset)
	binary.Read(buf, binary.BigEndian, &header.dataIndexOffset)
	binary.Read(buf, binary.BigEndian, &header.dataIndexCount)
	binary.Read(buf, binary.BigEndian, &header.metaIndexOffset)
	binary.Read(buf, binary.BigEndian, &header.metaIndexCount)
	binary.Read(buf, binary.BigEndian, &header.totalUncompressedDataBytes)
	binary.Read(buf, binary.BigEndian, &header.entryCount)
	binary.Read(buf, binary.BigEndian, &header.compressionCodec)
	return header, nil
}

func (r *Reader) loadIndex(mmap mmap.MMap) error {

	dataIndexEnd := r.header.metaIndexOffset
	if r.header.metaIndexOffset == 0 {
		dataIndexEnd = uint64(r.header.index)
	}
	buf := bytes.NewReader(mmap[r.header.dataIndexOffset:dataIndexEnd])

	dataIndexMagic := make([]byte, 8)
	buf.Read(dataIndexMagic)
	if bytes.Compare(dataIndexMagic, []byte("IDXBLK)+")) != 0 {
		return errors.New("bad data index magic")
	}

	for buf.Len() > 0 {
		dataBlock := Block{}

		binary.Read(buf, binary.BigEndian, &dataBlock.offset)
		binary.Read(buf, binary.BigEndian, &dataBlock.size)

		firstKeyLen, _ := binary.ReadUvarint(buf)
		dataBlock.firstKeyBytes = make([]byte, firstKeyLen)
		buf.Read(dataBlock.firstKeyBytes)

		r.index = append(r.index, dataBlock)
	}

	return nil
}

func (r *Reader) GetBlock(i int) (*bytes.Reader, error) {
	var buf *bytes.Reader

	block := r.index[i]

	switch {
	case r.header.compressionCodec == 2: // No compression
		buf = bytes.NewReader(r.mmap[block.offset : block.offset+uint64(block.size)])
	case r.header.compressionCodec == 3: // Snappy
		uncompressedByteSize := binary.BigEndian.Uint32(r.mmap[block.offset : block.offset+4])
		if uncompressedByteSize != block.size {
			return nil, errors.New("mismatched uncompressed block size")
		}
		compressedByteSize := binary.BigEndian.Uint32(r.mmap[block.offset+4 : block.offset+8])
		compressedBytes := r.mmap[block.offset+8 : block.offset+8+uint64(compressedByteSize)]
		uncompressedBytes, err := snappy.Decode(nil, compressedBytes)
		if err != nil {
			return nil, err
		}
		buf = bytes.NewReader(uncompressedBytes)
	default:
		return nil, errors.New("Unsupported compression codec " + string(r.header.compressionCodec))
	}

	dataBlockMagic := make([]byte, 8)
	buf.Read(dataBlockMagic)
	if bytes.Compare(dataBlockMagic, []byte("DATABLK*")) != 0 {
		return nil, errors.New("bad data block magic")
	}

	return buf, nil
}

func get(buf *bytes.Reader, key []byte, first bool) ([]byte, [][]byte, bool) {
	var acc [][]byte

	for buf.Len() > 0 {
		var keyLen, valLen uint32
		binary.Read(buf, binary.BigEndian, &keyLen)
		binary.Read(buf, binary.BigEndian, &valLen)
		keyBytes := make([]byte, keyLen)
		valBytes := make([]byte, valLen)
		buf.Read(keyBytes)
		buf.Read(valBytes)
		if bytes.Compare(key, keyBytes) == 0 {
			if first {
				return valBytes, nil, true
			} else {
				acc = append(acc, valBytes)
			}
		}
	}

	return nil, acc, len(acc) > 0
}
