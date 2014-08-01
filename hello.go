package main

import "bytes"
import "encoding/binary"
import "fmt"
import "github.com/edsrzf/mmap-go"
import "os"

func main() {
	file, err := os.OpenFile(os.Args[1], os.O_RDONLY, 0)
	if err != nil {
		fmt.Println(err)
		return
	}
	mmap, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		fmt.Println(err)
		return
	}

	size := len(mmap)

	var versionByte uint32
	versionBuf := bytes.NewReader(mmap[size-4:])
	binary.Read(versionBuf, binary.BigEndian, &versionByte)
	majorVersion := versionByte & 0x00ffffff
	minorVersion := versionByte >> 24

	if majorVersion != 1 || minorVersion != 0 {
		fmt.Println("Wrong version")
		return
	}

	headerIndex := size - 60
	headerBuf := bytes.NewReader(mmap[headerIndex:])
	var fileInfoOffset, dataIndexOffset, metaIndexOffset, totalUncompressedDataBytes uint64
	var dataIndexCount, metaIndexCount, entryCount, compressionCodec uint32
	headerMagic := make([]byte, 8)
	headerBuf.Read(headerMagic)
	if bytes.Compare(headerMagic, []byte("TRABLK\"$")) != 0 {
		fmt.Println("Bad header magic")
		return
	}
	binary.Read(headerBuf, binary.BigEndian, &fileInfoOffset)
	binary.Read(headerBuf, binary.BigEndian, &dataIndexOffset)
	binary.Read(headerBuf, binary.BigEndian, &dataIndexCount)
	binary.Read(headerBuf, binary.BigEndian, &metaIndexOffset)
	binary.Read(headerBuf, binary.BigEndian, &metaIndexCount)
	binary.Read(headerBuf, binary.BigEndian, &totalUncompressedDataBytes)
	binary.Read(headerBuf, binary.BigEndian, &entryCount)
	binary.Read(headerBuf, binary.BigEndian, &compressionCodec)
	fmt.Println("Header", fileInfoOffset, dataIndexOffset, metaIndexOffset, totalUncompressedDataBytes, dataIndexCount, metaIndexCount, entryCount, compressionCodec)

	dataIndexEnd := metaIndexOffset
	if metaIndexOffset == 0 {
		dataIndexEnd = uint64(headerIndex)
	}
	dataIndexBuf := bytes.NewReader(mmap[dataIndexOffset:dataIndexEnd])
	dataIndexMagic := make([]byte, 8)
	dataIndexBuf.Read(dataIndexMagic)
	if bytes.Compare(dataIndexMagic, []byte("IDXBLK)+")) != 0 {
		fmt.Println("Bad data index magic")
		return
	}

	for dataIndexBuf.Len() > 0 {
		var dataBlockOffset uint64
		var dataBlockSize uint32
		binary.Read(dataIndexBuf, binary.BigEndian, &dataBlockOffset)
		binary.Read(dataIndexBuf, binary.BigEndian, &dataBlockSize)
		keyLen, _ := binary.ReadUvarint(dataIndexBuf)
		keyBytes := make([]byte, keyLen)
		dataIndexBuf.Read(keyBytes)
		fmt.Println("Data block", dataBlockOffset, dataBlockSize, keyLen, keyBytes)

		dataBlockBuf := bytes.NewReader(mmap[dataBlockOffset : dataBlockOffset+uint64(dataBlockSize)])
		dataBlockMagic := make([]byte, 8)
		dataBlockBuf.Read(dataBlockMagic)
		if bytes.Compare(dataBlockMagic, []byte("DATABLK*")) != 0 {
			fmt.Println("Bad data block magic")
			return
		}
		for dataBlockBuf.Len() > 0 {
			var keyLen, valLen uint32
			binary.Read(dataBlockBuf, binary.BigEndian, &keyLen)
			binary.Read(dataBlockBuf, binary.BigEndian, &valLen)
			keyBytes := make([]byte, keyLen)
			valBytes := make([]byte, valLen)
			dataBlockBuf.Read(keyBytes)
			dataBlockBuf.Read(valBytes)
			fmt.Println("KeyVal", keyLen, valLen, keyBytes, valBytes)
		}
	}
}
