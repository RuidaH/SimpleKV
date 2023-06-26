package db

import (
	"errors"
	"os"
)

const (
	DataFileName       = "dataFile"
	MergeFileName      = "mergeFile"
	DataFileExtension  = ".seg"
	HintFileExtension  = ".hint"
	MergeFileExtension = ".merge"
	DefaultDirPath     = "./data_storage/"
)

type SimpleKV struct {
	keyMap      *KeyDir  // hash table that maps every key in a Bitcask to a fixed-size structure
	dirPath     string   // path to store data file
	numOfFiles  int      // keep track of the number of data files in DB
	curDataFile *DBFile  // keep track of active data file
	hintFile    *os.File // store info to speed up the reboot
}

func Open(dirPath string) (*SimpleKV, error) {
	// create a dir if the given database dirpath does not exist
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// create a new active data file if there is no one
	// for now just creating a new data file
	dataFile, err := NewDataFile(dirPath, 1)
	if err != nil {
		return nil, err
	}

	kv := &SimpleKV{
		keyMap:      NewKeyDir(),
		dirPath:     dirPath,
		numOfFiles:  1,
		curDataFile: dataFile,
		hintFile:    nil,
	}

	kv.LoadingIndices()
	return kv, nil
}

func (kv *SimpleKV) Close() error {
	if kv.curDataFile == nil {
		return errors.New("The current data file is invalid.")
	}
	return kv.curDataFile.file.Close()
}

func (kv *SimpleKV) Put(key string, value string) error {
	if len(key) == 0 || len(value) == 0 {
		return errors.New("The length of key or value is 0.")
	}

	// write the key-value info into the data file
	newRecord := NewRecord([]byte(key), []byte(value))
	prevOffset := kv.curDataFile.GetOffset()
	err := kv.curDataFile.Write(newRecord)
	if err != nil {
		return err
	}

	// update mapping table in kv
	valueSet := NewValueSet(1, newRecord.Size(), prevOffset)
	kv.keyMap.Put(key, valueSet)
	// DPrintf("The keyMap: %v", kv.keyMap)

	return nil
}

func (kv *SimpleKV) Get(key string) (string, error) {
	if len(key) == 0 {
		return "", errors.New("The length of key is 0.")
	}

	// read the valueSet from the keyMap
	valueSet, err := kv.keyMap.Get(key)
	if err != nil {
		return "", err
	}

	// fetch the record from the data file
	record, err := kv.FetchRecord(valueSet.GetFileId(), valueSet.GetRecordSize(), valueSet.GetRecordOffset())
	if err != nil {
		return "", nil
	}

	return string(record.value), nil
}

// Del() only delete the key-value pair in hash table
// the corresponding record will be deleted when Merge is called
func (kv *SimpleKV) Del(key string) error {
	return kv.keyMap.Del(key)
}

// get the record based on the offset and fileId from data files
func (kv *SimpleKV) FetchRecord(fileId int, size int64, offset int64) (*Record, error) {
	// the record is in the active data file
	if fileId == kv.numOfFiles {
		return kv.curDataFile.Read(offset, size)
	}

	// the record is the immutable data files
	// let's assume there is only one file right now
	// require optimisation on the data acquisition on those files
	return kv.curDataFile.Read(offset, size)
}

func (kv *SimpleKV) LoadingIndices() {
	DPrintf("Currently LoadingIndices is doing nothing")
}

func (kv *SimpleKV) Merge() {

}
