package db

import (
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

type DBFile struct {
	file   *os.File
	offset int64
	mu     sync.RWMutex
}

func NewFile(fileName string) (*DBFile, error) {
	// create a new file
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	// get the file info
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	return &DBFile{
		file:   file,
		offset: fileInfo.Size(),
	}, nil
}

// create a new active data file
func NewDataFile(path string, ith int) (*DBFile, error) {
	fileName := filepath.Join(path, getNewDataFileName(ith))
	return NewFile(fileName)
}

// create a new merge file
func NewMergeFile(path string, ith int) (*DBFile, error) {
	fileName := filepath.Join(path, getNewMergeFileName(ith))
	return NewFile(fileName)
}

// read the record given the offset
func (df *DBFile) Read(offset int64, length int64) {
	df.mu.RLock()
	defer df.mu.RUnlock()

	buffer := make([]byte, RecordHeaderSize)
	if _, err := df.file.ReadAt(buffer, offset); err != nil {
		DPrintf("Error reading file: %v\n", err)
		return
	}
	record, err := Decode(buffer)
	if err != nil {
		DPrintf("Error occurs when decoding record buffer: %v\n", err)
		return
	}
	offset += RecordHeaderSize
	if record.keySize > 0 { // read the key of the record
		key := make([]byte, record.keySize)
		if _, err := df.file.ReadAt(key, offset); err != nil {
			DPrintf("Error reading key: %v\n", err)
			return
		}
		record.key = key
	}
	offset += int64(record.keySize)
	if record.valueSize > 0 { // read the value of the record
		value := make([]byte, record.valueSize)
		if _, err := df.file.ReadAt(value, offset); err != nil {
			DPrintf("Error reading value: %v\n", err)
			return
		}
		record.value = value
	}
}

// write the record into the datafile
func (df *DBFile) Write(record *Record) {
	df.mu.Lock()
	defer df.mu.RLock()

	recordByte, _ := record.Encode()
	_, err := df.file.WriteAt(recordByte, df.offset)
	if err != nil {
		DPrintf("Error writing file: %v\n", err)
		return
	}
	df.offset += record.Size()

	// need to check if the current size exceeds the maxFileSize

	DPrintf("Writing the record %v into the file %v", record, df.file.Name())
}

func getNewDataFileName(ith int) string {
	return DataFileName + strconv.Itoa(ith) + DataFileExtension
}

func getNewMergeFileName(ith int) string {
	return MergeFileName + strconv.Itoa(ith) + MergeFileExtension
}
