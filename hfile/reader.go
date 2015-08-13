// Copyright (C) 2014 Daniel Harrison

package hfile

import (
	"bytes"
	"fmt"
	"io"
)
import "encoding/binary"
import "errors"
import "github.com/edsrzf/mmap-go"
import "os"
import "github.com/golang/snappy"

type Reader struct {
	mmap mmap.MMap

	majorVersion uint32
	minorVersion uint32

	header    Header
	dataIndex DataIndex
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
	hfile.dataIndex, err = hfile.header.newDataIndex(hfile.mmap)
	if err != nil {
		return hfile, err
	}

	return hfile, nil
}
func (hfile *Reader) String() string {
	return "hfile"
}
func getDataBlock(key []byte, blocks *[]DataBlock) (*DataBlock, bool) {
	// TODO(dan): Binary search instead.
	for i := len(*blocks) - 1; i >= 0; i-- {
		block := (*blocks)[i]
		if cmp := bytes.Compare(key, block.firstKeyBytes); cmp == 0 || cmp == 1 {
			return &block, true
		}
	}
	return nil, false
}
func (hfile *Reader) Get(key []byte) ([]byte, bool) {
	dataBlock, found := getDataBlock(key, &hfile.dataIndex.dataBlocks)
	if !found {
		return nil, false
	}
	return dataBlock.get(key)
}

func (r *Reader) PrintDebugInfo(out io.Writer) {
	fmt.Fprintln(out, "entries: ", r.header.entryCount)
	fmt.Fprintln(out, "blocks: ", len(r.dataIndex.dataBlocks))
	for i, blk := range r.dataIndex.dataBlocks {
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

func (header *Header) newDataIndex(mmap mmap.MMap) (DataIndex, error) {
	dataIndex := DataIndex{}
	dataIndexEnd := header.metaIndexOffset
	if header.metaIndexOffset == 0 {
		dataIndexEnd = uint64(header.index)
	}
	dataIndex.buf = bytes.NewReader(mmap[header.dataIndexOffset:dataIndexEnd])

	dataIndexMagic := make([]byte, 8)
	dataIndex.buf.Read(dataIndexMagic)
	if bytes.Compare(dataIndexMagic, []byte("IDXBLK)+")) != 0 {
		return dataIndex, errors.New("bad data index magic")
	}

	for dataIndex.buf.Len() > 0 {
		dataBlock := DataBlock{}

		binary.Read(dataIndex.buf, binary.BigEndian, &dataBlock.offset)
		binary.Read(dataIndex.buf, binary.BigEndian, &dataBlock.size)

		switch {
		case header.compressionCodec == 2: // No compression
			dataBlock.buf = bytes.NewReader(mmap[dataBlock.offset : dataBlock.offset+uint64(dataBlock.size)])
		case header.compressionCodec == 3: // Snappy
			uncompressedByteSize := binary.BigEndian.Uint32(mmap[dataBlock.offset : dataBlock.offset+4])
			if uncompressedByteSize != dataBlock.size {
				return dataIndex, errors.New("mismatched uncompressed block size")
			}
			compressedByteSize := binary.BigEndian.Uint32(mmap[dataBlock.offset+4 : dataBlock.offset+8])
			compressedBytes := mmap[dataBlock.offset+8 : dataBlock.offset+8+uint64(compressedByteSize)]
			uncompressedBytes, err := snappy.Decode(nil, compressedBytes)
			if err != nil {
				return dataIndex, err
			}
			dataBlock.buf = bytes.NewReader(uncompressedBytes)
		default:
			return dataIndex, errors.New("Unsupported compression codec " + string(header.compressionCodec))
		}

		dataBlockMagic := make([]byte, 8)
		dataBlock.buf.Read(dataBlockMagic)
		if bytes.Compare(dataBlockMagic, []byte("DATABLK*")) != 0 {
			return dataIndex, errors.New("bad data block magic")
		}

		firstKeyLen, _ := binary.ReadUvarint(dataIndex.buf)
		dataBlock.firstKeyBytes = make([]byte, firstKeyLen)
		dataIndex.buf.Read(dataBlock.firstKeyBytes)

		dataIndex.dataBlocks = append(dataIndex.dataBlocks, dataBlock)
	}

	return dataIndex, nil
}

type DataIndex struct {
	buf        *bytes.Reader
	dataBlocks []DataBlock
}

type DataBlock struct {
	buf           *bytes.Reader
	offset        uint64
	size          uint32
	firstKeyBytes []byte
}

func (dataBlock *DataBlock) reset() {
	dataBlock.buf.Seek(8, 0)
}
func (dataBlock *DataBlock) get(key []byte) ([]byte, bool) {
	dataBlock.reset()
	for dataBlock.buf.Len() > 0 {
		var keyLen, valLen uint32
		binary.Read(dataBlock.buf, binary.BigEndian, &keyLen)
		binary.Read(dataBlock.buf, binary.BigEndian, &valLen)
		keyBytes := make([]byte, keyLen)
		valBytes := make([]byte, valLen)
		dataBlock.buf.Read(keyBytes)
		dataBlock.buf.Read(valBytes)
		if bytes.Compare(key, keyBytes) == 0 {
			return valBytes, true
		}
	}
	return key, false
}
