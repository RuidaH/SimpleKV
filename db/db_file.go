package db

import (
	"os"
	"path/filepath"
	"strconv"
)

type DBFile struct {
	file   *os.File
	offset int64
}

func NewFile(fileName string) (*DBFile, error) {
	// create a new file
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// get the file info
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}

	return &DBFile{
		file:   file,
		offset: fileInfo.Size(),
	}, nil
}

// create a new active data file
func NewDataFile(path string, ith int) (*DBFile, error) {
	fileName := filepath.Join(path, GetDataFileName(ith))
	// DPrintf("The newly created data file is %v", fileName)
	return NewFile(fileName)
}

// create a new merge file
func NewMergeFile(path string, ith int) (*DBFile, error) {
	fileName := filepath.Join(path, GetMergeFileName(ith))
	return NewFile(fileName)
}

// read the record given the offset
func (df *DBFile) Read(offset int64, length int64) (*Record, error) {
	buffer := make([]byte, length)
	if _, err := df.file.ReadAt(buffer, offset); err != nil {
		return nil, err
	}
	record, err := Decode(buffer)
	return record, err
}

// write the record into the datafile
func (df *DBFile) Write(record *Record) error {
	recordByte, _ := record.Encode()
	_, err := df.file.WriteAt(recordByte, df.offset)
	if err != nil {
		return err
	}
	df.offset += record.Size()

	// need to check if the current size exceeds the maxFileSize

	// DPrintf("Writing the record %v into the file %v", record, df.file.Name())
	return nil
}

func (df *DBFile) GetOffset() int64 {
	return df.offset
}

func GetDataFileName(ith int) string {
	return DataFileName + "-" + strconv.Itoa(ith) + DataFileExtension
}

func GetMergeFileName(ith int) string {
	return MergeFileName + "-" + strconv.Itoa(ith) + MergeFileExtension
}
