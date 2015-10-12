// Copyright (C) 2014 Daniel Harrison

package hfile

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"

	"github.com/edsrzf/mmap-go"
	//"github.com/golang/snappy"
	"github.com/cockroachdb/c-snappy"
)

type Reader struct {
	CollectionConfig

	mmap mmap.MMap

	majorVersion uint32
	minorVersion uint32

	Header
	index []Block

	scannerCache  chan *Scanner
	iteratorCache chan *Iterator
}

type Header struct {
	offset int

	fileInfoOffset             uint64
	dataIndexOffset            uint64
	dataIndexCount             uint32
	metaIndexOffset            uint64
	metaIndexCount             uint32
	totalUncompressedDataBytes uint64
	EntryCount                 uint32
	compressionCodec           uint32
}

type Block struct {
	offset        uint64
	size          uint32
	firstKeyBytes []byte
}

func NewReader(name, path string, lock, debug bool) (*Reader, error) {
	return NewReaderFromConfig(CollectionConfig{name, path, path, lock, debug, name, "", "", ""})
}

func NewReaderFromConfig(cfg CollectionConfig) (*Reader, error) {
	f, err := os.OpenFile(cfg.LocalPath, os.O_RDONLY, 0)

	if err != nil {
		return nil, fmt.Errorf("[Reader] Error opening file (%s): %v", cfg.LocalPath, err)
	}

	hfile := new(Reader)
	hfile.CollectionConfig = cfg

	hfile.mmap, err = mmap.Map(f, mmap.RDONLY, 0)

	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	if hfile.InMem {
		mb := 1024.0 * 1024.0
		log.Printf("[Reader.NewReader] locking %s (%.02fmb)...\n", hfile.Name, float64(fi.Size())/mb)
		if err = hfile.mmap.Lock(); err != nil {
			log.Printf("[Reader.NewReader] error locking %s: %s\n", hfile.Name, err.Error())
			return nil, err
		}
		log.Printf("[Reader.NewReader] locked %s.\n", hfile.Name)
	} else if hfile.Debug {
		log.Printf("[Reader.NewReader] Not locking %s...\n", hfile.Name)
	}

	v := binary.BigEndian.Uint32(hfile.mmap[len(hfile.mmap)-4:])
	hfile.majorVersion = v & 0x00ffffff
	hfile.minorVersion = v >> 24

	err = hfile.readHeader(hfile.mmap)
	if err != nil {
		return nil, err
	}

	err = hfile.loadIndex(hfile.mmap)
	if err != nil {
		return hfile, err
	}
	hfile.scannerCache = make(chan *Scanner, 5)
	hfile.iteratorCache = make(chan *Iterator, 5)
	return hfile, nil
}

func (r *Reader) PrintDebugInfo(out io.Writer, includeStartKeys int) {
	fmt.Fprintln(out, "entries: ", r.EntryCount)
	fmt.Fprintf(out, "compressed: %v (codec: %d)\n", r.compressionCodec != CompressionNone, r.compressionCodec)
	fmt.Fprintln(out, "blocks: ", len(r.index))
	for i, blk := range r.index {
		if i > includeStartKeys {
			fmt.Fprintf(out, "\t... and %d more\n", len(r.index)-i)
			return
		}
		fmt.Fprintf(out, "\t#%d: %s\n", i, hex.EncodeToString(blk.firstKeyBytes))
	}
}

func (r *Reader) readHeader(mmap mmap.MMap) error {
	if r.majorVersion != 1 || r.minorVersion != 0 {
		return fmt.Errorf("wrong version: %d.%d", r.majorVersion, r.minorVersion)
	}

	r.Header.offset = len(mmap) - 60
	buf := bytes.NewReader(mmap[r.Header.offset:])

	headerMagic := make([]byte, 8)
	buf.Read(headerMagic)
	if bytes.Compare(headerMagic, TrailerMagic) != 0 {
		return errors.New("bad header magic")
	}

	binary.Read(buf, binary.BigEndian, &r.fileInfoOffset)
	binary.Read(buf, binary.BigEndian, &r.dataIndexOffset)
	binary.Read(buf, binary.BigEndian, &r.dataIndexCount)
	binary.Read(buf, binary.BigEndian, &r.metaIndexOffset)
	binary.Read(buf, binary.BigEndian, &r.metaIndexCount)
	binary.Read(buf, binary.BigEndian, &r.totalUncompressedDataBytes)
	binary.Read(buf, binary.BigEndian, &r.EntryCount)
	binary.Read(buf, binary.BigEndian, &r.compressionCodec)
	return nil
}

func (r *Reader) loadIndex(mmap mmap.MMap) error {

	dataIndexEnd := r.metaIndexOffset
	if r.metaIndexOffset == 0 {
		dataIndexEnd = uint64(r.Header.offset)
	}

	i := r.dataIndexOffset

	if bytes.Compare(mmap[i:i+8], IndexMagic) != 0 {
		return errors.New("bad data index magic")
	}
	i += 8

	for i < dataIndexEnd {
		dataBlock := Block{}

		dataBlock.offset = binary.BigEndian.Uint64(mmap[i:])
		i += uint64(binary.Size(dataBlock.offset))

		dataBlock.size = binary.BigEndian.Uint32(mmap[i:])
		i += uint64(binary.Size(dataBlock.size))

		firstKeyLen, s := binary.Uvarint(mmap[i:])
		if s < 1 || firstKeyLen < 1 {
			return fmt.Errorf("Failed to read key length, err %d", s)
		}
		i += uint64(s)

		dataBlock.firstKeyBytes = mmap[i : i+firstKeyLen]
		i += firstKeyLen

		r.index = append(r.index, dataBlock)
	}

	return nil
}

func After(a, b []byte) bool {
	return bytes.Compare(a, b) > 0
}

func (b *Block) IsAfter(key []byte) bool {
	return After(b.firstKeyBytes, key)
}

func (r *Reader) FirstKey() ([]byte, error) {
	if len(r.index) < 1 {
		return nil, fmt.Errorf("empty collection has no first key")
	}
	return r.index[0].firstKeyBytes, nil
}

func (r *Reader) FindBlock(from int, key []byte) int {
	remaining := len(r.index) - from - 1
	if r.Debug {
		log.Printf("[Reader.findBlock] cur %d, remaining %d\n", from, remaining)
	}

	if remaining <= 0 {
		if r.Debug {
			log.Println("[Reader.findBlock] last block")
		}
		return from // s.cur is the last block, so it is only choice.
	}

	if r.index[from+1].IsAfter(key) {
		if r.Debug {
			log.Println("[Reader.findBlock] next block is past key")
		}
		return from
	}

	offset := sort.Search(remaining, func(i int) bool {
		return r.index[from+i+1].IsAfter(key)
	})

	return from + offset
}

func (r *Reader) GetBlockBuf(i int, dst []byte) ([]byte, error) {
	var err error

	block := r.index[i]

	switch r.compressionCodec {
	case CompressionNone:
		dst = r.mmap[block.offset : block.offset+uint64(block.size)]
	case CompressionSnappy:
		// TODO(davidt): do we sometimes split blocks into sub-blocks?
		uncompressedByteSize := binary.BigEndian.Uint32(r.mmap[block.offset : block.offset+4])
		if uncompressedByteSize != block.size {
			return nil, errors.New("mismatched uncompressed block size")
		}
		compressedByteSize := binary.BigEndian.Uint32(r.mmap[block.offset+4 : block.offset+8])
		compressedBytes := r.mmap[block.offset+8 : block.offset+8+uint64(compressedByteSize)]
		dst, err = snappy.Decode(dst, compressedBytes)
		if err != nil {
			return nil, err
		}
		if len(dst) != int(uncompressedByteSize) {
			return nil, fmt.Errorf("Wrong size after decompression (snappy sub-blocks?): %d != %d", uncompressedByteSize, len(dst))
		}
	default:
		return nil, errors.New("Unsupported compression codec " + string(r.compressionCodec))
	}

	if bytes.Compare(dst[0:8], DataMagic) != 0 {
		return nil, errors.New("bad data block magic")
	}

	return dst, nil
}

func (r *Reader) GetScanner() *Scanner {
	select {
	case s := <-r.scannerCache:
		return s
	default:
		return NewScanner(r)
	}
}

func (r *Reader) GetIterator() *Iterator {
	select {
	case i := <-r.iteratorCache:
		return i
	default:
		return NewIterator(r)
	}
}
