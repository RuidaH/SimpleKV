package db

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

// header: crc (uint32, 4B), timestamp (int64, 8B), keySize (uint32 4B), valueSize (uint32 4B)
// header size: 20B in total
const RecordHeaderSize = 20

// whether or not to include the operation type
const (
	Put int8 = iota
	Del
)

type Record struct {
	key       []byte
	value     []byte
	keySize   uint32
	valueSize uint32
	time      uint64
	crc       uint32
}

// create new record to be stored in data file
func NewRecord(key []byte, value []byte) *Record {
	return &Record{
		key:       key,
		value:     value,
		time:      uint64(time.Now().Unix()),
		keySize:   uint32(len(key)),
		valueSize: uint32(len(value)),
	}
}

// return the overall size of the record
func (rc *Record) Size() int64 {
	return int64(RecordHeaderSize + rc.keySize + rc.valueSize)
}

// encode the record so that it can be store in data file
func (rc *Record) Encode() ([]byte, error) {
	buffer := make([]byte, rc.Size())

	binary.BigEndian.PutUint64(buffer[4:12], rc.time)
	binary.BigEndian.PutUint32(buffer[12:16], rc.keySize)
	binary.BigEndian.PutUint32(buffer[16:20], rc.valueSize)
	copy(buffer[RecordHeaderSize:RecordHeaderSize+rc.keySize], rc.key)
	copy(buffer[RecordHeaderSize+rc.keySize:RecordHeaderSize+rc.keySize+rc.valueSize], rc.value)

	// compute the crc of the record
	temp_buffer := append(buffer[4:])
	crc32Hash := crc32.NewIEEE()
	crc32Hash.Write(temp_buffer)
	rc.crc = uint32(crc32Hash.Sum32())
	binary.BigEndian.PutUint32(buffer[:4], rc.crc)

	// DPrintf("The encoded byte: %v with len %v", buffer, len(buffer))

	return buffer, nil
}

// decode the given buffer and return record instance
func Decode(buffer []byte) (*Record, error) {
	keySize := binary.BigEndian.Uint32(buffer[12:16])
	valueSize := binary.BigEndian.Uint32(buffer[16:20])
	return &Record{
		crc:       binary.BigEndian.Uint32(buffer[:4]),
		time:      binary.BigEndian.Uint64(buffer[4:12]),
		keySize:   keySize,
		valueSize: valueSize,
		key:       buffer[RecordHeaderSize : RecordHeaderSize+keySize],
		value:     buffer[RecordHeaderSize+keySize : RecordHeaderSize+keySize+valueSize],
	}, nil
}
