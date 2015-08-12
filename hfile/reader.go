// Copyright (C) 2014 Daniel Harrison

package hfile

import "bytes"
import "encoding/binary"
import "errors"
import "github.com/edsrzf/mmap-go"
import "os"
import "github.com/golang/snappy"

type Reader struct {
	mmap      mmap.MMap
	version   Version
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

	versionIndex := len(hfile.mmap) - 4
	hfile.version, err = newVersion(bytes.NewReader(hfile.mmap[versionIndex:]))
	if err != nil {
		return hfile, err
	}
	hfile.header, err = hfile.version.newHeader(hfile.mmap)
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
func getDataBlock(key []byte, blocks *[]DataBlock) (*DataBlock, error) {
	// TODO(dan): Binary search instead.
	for i := len(*blocks) - 1; i >= 0; i-- {
		block := (*blocks)[i]
		if cmp := bytes.Compare(key, block.firstKeyBytes); cmp == 0 || cmp == 1 {
			return &block, nil
		}
	}
	return nil, errors.New("key not found")
}
func (hfile *Reader) Get(key []byte) ([]byte, error) {
	dataBlock, err := getDataBlock(key, &hfile.dataIndex.dataBlocks)
	if err != nil {
		return key, err
	}
	return dataBlock.get(key)
}

type Version struct {
	buf          *bytes.Reader
	majorVersion uint32
	minorVersion uint32
}

func newVersion(versionBuf *bytes.Reader) (Version, error) {
	version := Version{buf: versionBuf}
	var rawByte uint32
	binary.Read(version.buf, binary.BigEndian, &rawByte)
	version.majorVersion = rawByte & 0x00ffffff
	version.minorVersion = rawByte >> 24
	return version, nil
}
func (version *Version) newHeader(mmap mmap.MMap) (Header, error) {
	header := Header{}

	if version.majorVersion != 1 || version.minorVersion != 0 {
		return header, errors.New("wrong version")
	}

	header.index = len(mmap) - 60
	header.buf = bytes.NewReader(mmap[header.index:])
	headerMagic := make([]byte, 8)
	header.buf.Read(headerMagic)
	if bytes.Compare(headerMagic, []byte("TRABLK\"$")) != 0 {
		return header, errors.New("bad header magic")
	}

	binary.Read(header.buf, binary.BigEndian, &header.fileInfoOffset)
	binary.Read(header.buf, binary.BigEndian, &header.dataIndexOffset)
	binary.Read(header.buf, binary.BigEndian, &header.dataIndexCount)
	binary.Read(header.buf, binary.BigEndian, &header.metaIndexOffset)
	binary.Read(header.buf, binary.BigEndian, &header.metaIndexCount)
	binary.Read(header.buf, binary.BigEndian, &header.totalUncompressedDataBytes)
	binary.Read(header.buf, binary.BigEndian, &header.entryCount)
	binary.Read(header.buf, binary.BigEndian, &header.compressionCodec)
	return header, nil
}

type Header struct {
	buf   *bytes.Reader
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
func (dataBlock *DataBlock) get(key []byte) ([]byte, error) {
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
			return valBytes, nil
		}
	}
	return key, errors.New("key not found")
}
